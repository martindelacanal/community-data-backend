'use strict';

const { randomUUID } = require('node:crypto');
const BLOCKS = ['cards', 'image_banner', 'app_banner'];
const ENTITY_TYPES = ['card', 'image_banner', 'app_banner'];
const IMAGE_SIZES = { mobile: 640, tablet: 1024, desktop: 1440 };
const DEFAULT_SETTINGS = {
  web: { cards: { enabled: true, order: 0 }, image_banner: { enabled: false, order: 1 }, app_banner: { enabled: true, order: 2 } },
  app: { cards: { enabled: true, order: 0 }, image_banner: { enabled: false, order: 1 }, app_banner: { enabled: false, order: 2 } },
  imageBanner: { titleEn: 'Image campaign', titleEs: 'Campaña de imagen', linkUrl: null, altEn: '', altEs: '' },
  images: { mobile: null, tablet: null, desktop: null }
};

function fail(message, status = 400) { const error = new Error(message); error.status = status; throw error; }
function parseJson(value, fallback = {}) { try { return typeof value === 'string' ? JSON.parse(value) : value || fallback; } catch { return fallback; } }
function defaultSettings() { return JSON.parse(JSON.stringify(DEFAULT_SETTINGS)); }
function readSettings(row) { const data = parseJson(row?.settings_json); return { ...defaultSettings(), ...data, trackingStartedAt: row?.created_at || null }; }
function text(value, max, name, required = false) {
  const result = String(value ?? '').trim();
  if ((required && !result) || result.length > max) fail(`${name} ${required ? 'is required and ' : ''}must not exceed ${max} characters`);
  return result;
}
function link(value) {
  const result = text(value, 500, 'linkUrl');
  if (!result) return null;
  // Allow internal Angular routes and explicit HTTP(S) destinations only.
  if (result.startsWith('/') && !result.startsWith('//') && !result.includes('\\') && !/[\u0000-\u001f]/.test(result)) return result;
  try { const parsed = new URL(result); if (['https:', 'http:'].includes(parsed.protocol) && !parsed.username && !parsed.password) return result; } catch {}
  fail('linkUrl must be an internal /route or an http(s) URL');
}
function positiveId(value) { const number = Number(value); if (!Number.isSafeInteger(number) || number <= 0) fail('Invalid campaign identifier'); return number; }
function validateCard(body = {}) {
  const color = value => /^#(?:[a-f\d]{3}|[a-f\d]{6}|[a-f\d]{8})$/i.test(value);
  const card = {
    titleEn: text(body.titleEn ?? body.title_en, 255, 'titleEn', true),
    titleEs: text(body.titleEs ?? body.title_es, 255, 'titleEs', true),
    backgroundColor: body.backgroundColor ?? body.background_color ?? '#D1F8F8',
    textColor: body.textColor ?? body.text_color ?? '#434543',
    linkUrl: link(body.linkUrl ?? body.link_url),
    sortOrder: Number(body.sortOrder ?? body.sort_order ?? 0),
    enabled: body.enabled === false || body.enabled === 'N' ? 'N' : 'Y',
    targetPlatform: body.targetPlatform ?? 'all'
  };
  if (!color(card.backgroundColor) || !color(card.textColor)) fail('Colors must be valid hex values');
  if (!Number.isInteger(card.sortOrder) || Math.abs(card.sortOrder) > 100000) fail('Invalid sort order');
  if (!['all', 'web', 'app'].includes(card.targetPlatform)) fail('Invalid targetPlatform');
  return card;
}
function validateSettings(body, current) {
  const next = { ...current, imageBanner: { ...current.imageBanner } };
  for (const platform of ['web', 'app']) {
    if (!body?.[platform]) fail(`Missing ${platform} block settings`);
    next[platform] = {};
    for (const block of BLOCKS) {
      const value = body[platform][block];
      if (!value || typeof value.enabled !== 'boolean' || ![0, 1, 2].includes(value.order)) fail(`Invalid ${platform}.${block} settings`);
      next[platform][block] = { enabled: value.enabled, order: value.order };
    }
    if (new Set(BLOCKS.map(block => next[platform][block].order)).size !== 3) fail('Each block must have a different position');
  }
  if (body.imageBanner) {
    next.imageBanner = {
      titleEn: text(body.imageBanner.titleEn, 255, 'titleEn', true), titleEs: text(body.imageBanner.titleEs, 255, 'titleEs', true),
      altEn: text(body.imageBanner.altEn, 500, 'altEn'), altEs: text(body.imageBanner.altEs, 500, 'altEs'), linkUrl: link(body.imageBanner.linkUrl)
    };
  }
  if ((next.web.image_banner.enabled || next.app.image_banner.enabled) && Object.keys(IMAGE_SIZES).some(size => !next.images[size])) {
    fail('Upload mobile, tablet and desktop images before enabling the image banner');
  }
  if ((next.web.image_banner.enabled || next.app.image_banner.enabled) && (!next.imageBanner.altEn || !next.imageBanner.altEs)) {
    fail('Provide English and Spanish alternative image descriptions before enabling the banner');
  }
  delete next.trackingStartedAt;
  return next;
}
function changedFields(before, after) {
  const oldValues = {}; const newValues = {};
  for (const key of new Set([...Object.keys(before || {}), ...Object.keys(after || {})])) {
    if (JSON.stringify(before?.[key]) !== JSON.stringify(after?.[key])) { oldValues[key] = before?.[key] ?? null; newValues[key] = after?.[key] ?? null; }
  }
  return { before: oldValues, after: newValues };
}
async function transaction(pool, operation) {
  const connection = await pool.getConnection();
  try { await connection.beginTransaction(); const result = await operation(connection); await connection.commit(); return result; }
  catch (error) { await connection.rollback(); throw error; }
  finally { connection.release(); }
}
async function addHistory(connection, { entityType, entityId = 0, titleEn, titleEs, action, before = {}, after = {}, actorId = null, platform = 'all' }) {
  const changes = changedFields(before, after);
  if (!Object.keys(changes.after).length && !['created', 'image_removed'].includes(action)) return;
  await connection.query(`INSERT INTO home_campaign_history
    (entity_type, entity_id, card_id, title_en, title_es, action, changes_json, actor_id, platform, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP())`,
  [entityType, entityId, entityType === 'card' && entityId > 0 ? entityId : null, titleEn, titleEs, action, JSON.stringify(changes), actorId, platform]);
}
function normalizeEvent(body = {}) {
  if (!ENTITY_TYPES.includes(body.entityType)) fail('Invalid entityType');
  if (!['web', 'app'].includes(body.platform)) fail('Invalid platform');
  if (!['desktop', 'tablet', 'mobile'].includes(body.deviceType)) fail('Invalid deviceType');
  const allowedChannels = body.platform === 'app' ? ['capacitor_android', 'capacitor_ios', 'unknown'] : ['web_desktop', 'web_mobile'];
  const accessChannel = body.accessChannel || (body.platform === 'app' ? 'unknown' : body.deviceType === 'desktop' ? 'web_desktop' : 'web_mobile');
  if (!allowedChannels.includes(accessChannel)) fail('Invalid accessChannel for platform');
  const eventId = body.eventId || randomUUID();
  if (!/^[a-f\d]{8}(?:-[a-f\d]{4}){3}-[a-f\d]{12}$/i.test(eventId)) fail('Invalid eventId');
  const sessionId = body.sessionId ? text(body.sessionId, 64, 'sessionId') : null;
  if (sessionId && !/^[\w-]+$/.test(sessionId)) fail('Invalid sessionId');
  return { entityType: body.entityType, entityId: body.entityType === 'card' ? positiveId(body.entityId) : 0, platform: body.platform,
    deviceType: body.deviceType, accessChannel, eventId, sessionId, destination: text(body.destination, 500, 'destination') || null };
}
function normalizeMetricFilters(body = {}, now = new Date()) {
  const isoDate = value => {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value))) fail('Dates must use YYYY-MM-DD');
    const parsed = new Date(`${value}T00:00:00Z`);
    if (!Number.isFinite(parsed.valueOf()) || parsed.toISOString().slice(0, 10) !== value) fail('Invalid calendar date');
    return value;
  };
  const dateTo = isoDate(body.dateTo || body.to_date || now.toISOString().slice(0, 10));
  const defaultStart = new Date(`${dateTo}T00:00:00Z`); defaultStart.setUTCDate(defaultStart.getUTCDate() - 29);
  const dateFrom = isoDate(body.dateFrom || body.from_date || defaultStart.toISOString().slice(0, 10));
  const days = (Date.parse(dateTo) - Date.parse(dateFrom)) / 86400000 + 1;
  if (days < 1 || days > 366) fail('Choose a date range between 1 and 366 days');
  const platform = body.platform || 'all'; const deviceType = body.deviceType || 'all'; const entityType = body.entityType || 'all';
  if (!['all', 'web', 'app', 'app_ios', 'app_android'].includes(platform)) fail('Invalid platform filter');
  if (!['all', 'desktop', 'tablet', 'mobile'].includes(deviceType)) fail('Invalid device filter');
  if (!['all', ...ENTITY_TYPES].includes(entityType)) fail('Invalid campaign filter');
  const integer = (value, fallback, max) => value == null ? fallback : Math.min(max, Math.max(1, Number.isSafeInteger(Number(value)) ? Number(value) : fallback));
  return { dateFrom, dateTo, platform, deviceType, entityType, entityId: body.entityId == null || body.entityId === '' || entityType !== 'card' ? null : positiveId(body.entityId),
    page: integer(body.page, 1, 100000), pageSize: integer(body.pageSize, 20, 100), historyPage: integer(body.historyPage, 1, 100000), historyPageSize: integer(body.historyPageSize, 20, 100) };
}
function metricWhere(filters, { history = false, raw = false } = {}) {
  const date = history ? 'created_at' : raw ? 'clicked_at' : 'click_date';
  const clauses = [`${date} >= ?`, `${date} < DATE_ADD(?, INTERVAL 1 DAY)`]; const params = [filters.dateFrom, filters.dateTo];
  if (filters.entityType !== 'all') { clauses.push('entity_type = ?'); params.push(filters.entityType); }
  if (filters.entityId != null) { clauses.push(history ? "(entity_id = ? OR (entity_type = 'card' AND entity_id = 0))" : 'entity_id = ?'); params.push(filters.entityId); }
  if (filters.platform !== 'all') {
    const platform = filters.platform.startsWith('app') ? 'app' : 'web';
    clauses.push(history ? "platform IN (?, 'all')" : 'platform = ?'); params.push(platform);
    if (!history && ['app_ios', 'app_android'].includes(filters.platform)) { clauses.push('access_channel = ?'); params.push(filters.platform === 'app_ios' ? 'capacitor_ios' : 'capacitor_android'); }
  }
  if (!history && filters.deviceType !== 'all') { clauses.push('device_type = ?'); params.push(filters.deviceType); }
  return { sql: clauses.join(' AND '), params };
}
const pagination = (page, pageSize, total) => ({ page, pageSize, total, totalPages: Math.ceil(total / pageSize) });

async function fetchCampaignMetrics(pool, filters, settings) {
  const where = metricWhere(filters); const raw = metricWhere(filters, { raw: true }); const history = metricWhere(filters, { history: true });
  // Charts query compact daily counters rather than scanning the click log repeatedly.
  // Run sequentially to leave the shared production pool available to public requests.
  const [timeline] = await pool.query(`SELECT DATE_FORMAT(click_date, '%Y-%m-%d') AS date, SUM(clicks) AS clicks
    FROM home_campaign_daily WHERE ${where.sql} GROUP BY click_date ORDER BY click_date`, where.params);
  const [devices] = await pool.query(`SELECT device_type AS deviceType, SUM(clicks) AS clicks
    FROM home_campaign_daily WHERE ${where.sql} GROUP BY device_type`, where.params);
  const [platforms] = await pool.query(`SELECT platform, access_channel AS accessChannel, SUM(clicks) AS clicks
    FROM home_campaign_daily WHERE ${where.sql} GROUP BY platform, access_channel`, where.params);
  const [campaignCounts] = await pool.query(`SELECT entity_type, entity_id, SUM(clicks) AS clicks,
    DATE_FORMAT(MAX(last_clicked_at), '%Y-%m-%dT%H:%i:%sZ') AS lastClickAt
    FROM home_campaign_daily WHERE ${where.sql} GROUP BY entity_type, entity_id LIMIT 502`, where.params);
  const [unique] = await pool.query(`SELECT COUNT(DISTINCT session_id) AS total FROM home_campaign_clicks WHERE ${raw.sql}`, raw.params);
  const [cards] = await pool.query(`SELECT id, title_en, title_es, enabled, target_platform FROM home_cards ORDER BY sort_order, id LIMIT 501`);
  const [historyCount] = await pool.query(`SELECT COUNT(*) AS total FROM home_campaign_history WHERE ${history.sql}`, history.params);
  const [historyRows] = await pool.query(`SELECT id, entity_type AS entityType, entity_id AS entityId, title_en AS titleEn, title_es AS titleEs,
    action, platform, DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS createdAt, changes_json
    FROM home_campaign_history WHERE ${history.sql} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`,
  [...history.params, filters.historyPageSize, (filters.historyPage - 1) * filters.historyPageSize]);
  const selectedPlatforms = filters.platform === 'all' ? ['web', 'app'] : [filters.platform.startsWith('app') ? 'app' : 'web'];
  const cardEnabled = card => card.enabled === 'Y' && selectedPlatforms.some(platform =>
    settings[platform].cards.enabled && ['all', platform].includes(card.target_platform || 'all'));
  const blockEnabled = block => selectedPlatforms.some(platform => settings[platform][block].enabled);
  const catalog = cards.slice(0, 500).map(card => ({ entityType: 'card', entityId: card.id, titleEn: card.title_en, titleEs: card.title_es,
    enabled: cardEnabled(card), targetPlatform: card.target_platform }));
  catalog.push({ entityType: 'image_banner', entityId: 0, titleEn: settings.imageBanner.titleEn, titleEs: settings.imageBanner.titleEs,
    enabled: blockEnabled('image_banner') });
  catalog.push({ entityType: 'app_banner', entityId: 0, titleEn: 'Download the app banner', titleEs: 'Banner de descarga de apps',
    enabled: blockEnabled('app_banner') });
  const totalClicks = timeline.reduce((sum, row) => sum + Number(row.clicks), 0);
  const totals = new Map(campaignCounts.map(row => [`${row.entity_type}:${row.entity_id}`, Number(row.clicks)]));
  const lastByEntity = new Map(campaignCounts.map(row => [`${row.entity_type}:${row.entity_id}`, row.lastClickAt]));
  const selected = catalog.filter(item => (filters.entityType === 'all' || item.entityType === filters.entityType) && (filters.entityId == null || item.entityId === filters.entityId));
  const campaigns = selected.map(item => ({ ...item, clicks: totals.get(`${item.entityType}:${item.entityId}`) || 0, lastClickAt: lastByEntity.get(`${item.entityType}:${item.entityId}`) || null }))
    .sort((a, b) => b.clicks - a.clicks || a.titleEn.localeCompare(b.titleEn));
  const start = (filters.page - 1) * filters.pageSize;
  return { filters, summary: { totalClicks, uniqueSessions: Number(unique[0]?.total || 0), activeCampaigns: selected.filter(item => item.enabled).length },
    timeline: timeline.map(row => ({ ...row, clicks: Number(row.clicks) })),
    devices: devices.map(row => ({ ...row, clicks: Number(row.clicks) })), platforms: platforms.map(row => ({ ...row, clicks: Number(row.clicks) })),
    campaigns: campaigns.slice(start, start + filters.pageSize), pagination: pagination(filters.page, filters.pageSize, campaigns.length),
    history: historyRows.map(({ changes_json, ...row }) => ({ ...row, changes: parseJson(changes_json) })),
    historyPagination: pagination(filters.historyPage, filters.historyPageSize, Number(historyCount[0]?.total || 0)),
    catalog, catalogTruncated: cards.length > 500, trackingStartedAt: settings.trackingStartedAt };
}

module.exports = { BLOCKS, ENTITY_TYPES, IMAGE_SIZES, defaultSettings, readSettings, validateCard, validateSettings, changedFields,
  positiveId, normalizeEvent, normalizeMetricFilters, metricWhere, transaction, addHistory, fetchCampaignMetrics, fail };
