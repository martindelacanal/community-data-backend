'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { defaultSettings, validateSettings, validateCard, normalizeEvent, normalizeMetricFilters, metricWhere,
  changedFields, transaction, addHistory, fetchCampaignMetrics } = require('./homeCampaigns');

const validCard = { titleEn: 'Food', titleEs: 'Alimentos', linkUrl: '/resources' };
test('existing public cards remain available on web and apps by default', () => {
  assert.equal(validateCard(validCard).targetPlatform, 'all');
  const settings = defaultSettings();
  assert.equal(settings.web.cards.enabled, true); assert.equal(settings.app.cards.enabled, true);
  assert.equal(settings.web.app_banner.enabled, true); assert.equal(settings.app.app_banner.enabled, false);
});
test('campaign destinations reject script and protocol-relative links', () => {
  for (const linkUrl of ['javascript:alert(1)', 'data:text/html,hello', '//evil.example', '/\\evil.example', 'https://user:password@example.com']) {
    assert.throws(() => validateCard({ ...validCard, linkUrl }), /linkUrl/);
  }
  assert.equal(validateCard({ ...validCard, linkUrl: 'https://example.com/campaign?source=home' }).linkUrl, 'https://example.com/campaign?source=home');
});
test('settings validate independent block orders and all responsive images before activation', () => {
  const current = defaultSettings(); const input = defaultSettings();
  input.web.image_banner.order = 0;
  assert.throws(() => validateSettings(input, current), /different position/);
  input.web.image_banner.order = 1; input.web.image_banner.enabled = true;
  assert.throws(() => validateSettings(input, current), /mobile, tablet and desktop/);
  current.images = { mobile: 'm', tablet: 't', desktop: 'd' };
  input.imageBanner.altEn = 'Campaign banner'; input.imageBanner.altEs = 'Banner de campaña';
  const result = validateSettings(input, current);
  assert.equal(result.web.image_banner.enabled, true); assert.deepEqual(result.images, current.images);
});
test('card validation rejects unsupported targeting and oversized fields', () => {
  assert.throws(() => validateCard({ ...validCard, targetPlatform: 'unknown' }), /targetPlatform/);
  assert.throws(() => validateCard({ ...validCard, titleEn: 'x'.repeat(256) }), /255/);
  assert.throws(() => validateCard({ ...validCard, sortOrder: Infinity }), /sort order/);
});
test('clicks validate entities, device/platform combinations and replay identifier', () => {
  const event = { entityType: 'card', entityId: 9, platform: 'web', deviceType: 'mobile', eventId: '15847678-1234-1234-1234-123456789abc' };
  assert.equal(normalizeEvent(event).accessChannel, 'web_mobile');
  assert.throws(() => normalizeEvent({ ...event, entityId: '9 OR 1=1' }), /identifier/);
  assert.throws(() => normalizeEvent({ ...event, accessChannel: 'capacitor_ios' }), /accessChannel/);
  assert.throws(() => normalizeEvent({ ...event, eventId: 'anything' }), /eventId/);
  assert.equal(normalizeEvent({ ...event, entityType: 'app_banner', entityId: 123 }).entityId, 0);
});
test('metric defaults bound dates to 30 days and page sizes to 100', () => {
  const filters = normalizeMetricFilters({ pageSize: 5000, historyPageSize: 10000 }, new Date('2026-10-21T12:00:00Z'));
  assert.equal(filters.dateFrom, '2026-09-22'); assert.equal(filters.dateTo, '2026-10-21');
  assert.equal(filters.pageSize, 100); assert.equal(filters.historyPageSize, 100);
  assert.throws(() => normalizeMetricFilters({ dateFrom: '2025-01-01', dateTo: '2026-10-21' }), /366/);
  assert.throws(() => normalizeMetricFilters({ dateFrom: '2026-02-30' }), /calendar date/);
});
test('metric query predicates remain parameterized and include date indexes', () => {
  const filters = normalizeMetricFilters({ from_date: '2026-10-01', to_date: '2026-10-21', platform: 'app_ios', deviceType: 'mobile', entityType: 'card', entityId: 42 });
  const where = metricWhere(filters);
  assert.equal(where.sql, 'click_date >= ? AND click_date < DATE_ADD(?, INTERVAL 1 DAY) AND entity_type = ? AND entity_id = ? AND platform = ? AND access_channel = ? AND device_type = ?');
  assert.deepEqual(where.params, ['2026-10-01', '2026-10-21', 'card', 42, 'app', 'capacitor_ios', 'mobile']);
  const history = metricWhere(filters, { history: true });
  assert.ok(history.sql.includes("platform IN (?, 'all')")); assert.ok(!history.sql.includes('device_type'));
  assert.ok(history.sql.includes("(entity_id = ? OR (entity_type = 'card' AND entity_id = 0))"));
});
test('history only includes changed fields; block history has no invalid card FK', async () => {
  assert.deepEqual(changedFields({ title: 'Old', enabled: true }, { title: 'New', enabled: true }), { before: { title: 'Old' }, after: { title: 'New' } });
  const calls = [];
  await addHistory({ query: async (...args) => calls.push(args) }, { entityType: 'card', titleEn: 'Block', titleEs: 'Bloque', action: 'disabled', before: { enabled: true }, after: { enabled: false } });
  assert.equal(calls[0][1][2], null);
});
test('failed mutation rolls back and releases its dedicated connection', async () => {
  const calls = []; const connection = Object.fromEntries(['beginTransaction', 'commit', 'rollback', 'release'].map(name => [name, async () => calls.push(name)]));
  await assert.rejects(transaction({ getConnection: async () => connection }, async () => { throw new Error('history failure'); }), /history failure/);
  assert.deepEqual(calls, ['beginTransaction', 'rollback', 'release']);
});
test('campaign metrics use aggregate tables and bounded history without returning raw events', async () => {
  const calls = []; const answers = [
    [{ date: '2026-10-21', clicks: '3' }], [{ deviceType: 'mobile', clicks: '3' }], [{ platform: 'web', accessChannel: 'web_mobile', clicks: '3' }],
    [{ entity_type: 'card', entity_id: 1, clicks: '3', lastClickAt: '2026-10-21T12:00:00Z' }], [{ total: 2 }],
    [{ id: 1, title_en: 'Food', title_es: 'Alimentos', enabled: 'Y', target_platform: 'all' }], [{ total: 1 }],
    [{ id: 1, entityType: 'card', entityId: 1, action: 'created', platform: 'all', changes_json: '{"after":{"enabled":true}}' },
      { id: 2, entityType: 'card', entityId: 0, action: 'disabled', platform: 'web', changes_json: '{"after":{"enabled":false}}' },
      { id: 3, entityType: 'card', entityId: 0, action: 'enabled', platform: 'app', changes_json: '{"after":{"enabled":true}}' }]
  ];
  const pool = { query: async (sql, params) => { calls.push({ sql, params }); return [answers.shift()]; } };
  const result = await fetchCampaignMetrics(pool, normalizeMetricFilters({ entityType: 'card', entityId: 1 }, new Date('2026-10-21')), defaultSettings());
  assert.equal(result.summary.totalClicks, 3); assert.equal(result.summary.uniqueSessions, 2);
  assert.equal(result.campaigns[0].clicks, 3); assert.equal(result.history[0].changes.after.enabled, true);
  assert.equal(result.history[1].platform, 'web'); assert.equal(result.history[2].platform, 'app');
  assert.ok(calls.find(call => call.sql.includes('ORDER BY created_at')).sql.includes('action, platform,'));
  assert.equal(calls.filter(call => call.sql.includes('home_campaign_clicks')).length, 1);
  assert.ok(calls.find(call => call.sql.includes('ORDER BY created_at')).sql.includes('LIMIT ? OFFSET ?'));
  assert.equal('events' in result, false);
});
test('active campaigns respect selected platform, targeting and parent block visibility', async () => {
  const settings = defaultSettings(); settings.web.cards.enabled = false;
  const cards = [{ id: 1, title_en: 'App card', title_es: 'Tarjeta app', enabled: 'Y', target_platform: 'app' },
    { id: 2, title_en: 'Shared card', title_es: 'Tarjeta compartida', enabled: 'Y', target_platform: 'all' }];
  const query = async sql => {
    if (sql.includes('FROM home_cards')) return [cards];
    if (sql.includes('COUNT')) return [[{ total: 0 }]];
    return [[]];
  };
  const web = await fetchCampaignMetrics({ query }, normalizeMetricFilters({ platform: 'web' }), settings);
  assert.equal(web.summary.activeCampaigns, 1); // Only the web download banner remains visible.
  assert.ok(web.catalog.filter(item => item.entityType === 'card').every(item => !item.enabled));
  const app = await fetchCampaignMetrics({ query }, normalizeMetricFilters({ platform: 'app_ios' }), settings);
  assert.equal(app.summary.activeCampaigns, 2);
  assert.equal(app.catalog.find(item => item.entityType === 'app_banner').enabled, false);
});
