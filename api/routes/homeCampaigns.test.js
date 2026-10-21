'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const express = require('express');
const { registerHomeCampaignRoutes } = require('./homeCampaigns');
const { defaultSettings } = require('../services/homeCampaigns');

async function server(t, query) {
  const app = express(); app.use(express.json()); const router = express.Router(); const calls = [];
  const connection = { query: async (sql, params) => { calls.push({ sql, params }); return query(sql, params); },
    beginTransaction: async () => calls.push('begin'), commit: async () => calls.push('commit'), rollback: async () => calls.push('rollback'), release: () => calls.push('release') };
  const pool = { query: connection.query, getConnection: async () => connection };
  registerHomeCampaignRoutes(router, { pool,
    verifyToken: (req, res, next) => req.headers.authorization ? next() : res.status(401).end(),
    ensureAdmin: (req, res) => req.headers.authorization === 'admin' ? { id: 1 } : (res.status(401).end(), null),
    getSignedUrlsForImages: async keys => keys.map(key => `https://images.example/${key}`), uploadImageWithVariants: async () => ({}),
    deleteS3Objects: async () => {}, uploadBannerImage: async () => {}, logger: { error() {} } });
  app.use(router); const listener = app.listen(0, '127.0.0.1'); await once(listener, 'listening');
  t.after(() => new Promise(resolve => { listener.closeAllConnections(); listener.close(resolve); }));
  const request = (path, { method = 'GET', body, auth } = {}) => fetch(`http://127.0.0.1:${listener.address().port}${path}`, {
    method, headers: { 'Content-Type': 'application/json', ...(auth ? { Authorization: auth } : {}) }, ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(5000) });
  return { request, calls };
}

test('settings and analytics require an authenticated administrator', async t => {
  const { request, calls } = await server(t, () => { throw new Error('No database access expected'); });
  for (const path of ['/home-cards', '/home-cards/settings']) {
    assert.equal((await request(path)).status, 401); assert.equal((await request(path, { auth: 'beneficiary' })).status, 401);
  }
  assert.equal((await request('/metrics/interaction/campaigns', { method: 'POST', body: {}, auth: 'beneficiary' })).status, 401);
  assert.equal(calls.length, 0);
});
test('permanent deletion requires explicit metrics confirmation before touching SQL', async t => {
  const { request, calls } = await server(t, () => { throw new Error('No database access expected'); });
  assert.equal((await request('/home-cards/9', { method: 'DELETE', auth: 'admin' })).status, 400);
  assert.equal(calls.length, 0);
});
test('duplicate delivery increments daily totals only once and commits atomically', async t => {
  let inserted = false;
  const { request, calls } = await server(t, sql => {
    if (sql.includes('FROM home_promotion_settings')) return [[{ settings_json: JSON.stringify(defaultSettings()), created_at: '2026-10-21T00:00:00Z' }]];
    if (sql.includes('FROM home_cards')) return [[{ enabled: 'Y', target_platform: 'all' }]];
    if (sql.includes('INSERT IGNORE INTO home_campaign_clicks')) { const affectedRows = inserted ? 0 : 1; inserted = true; return [{ affectedRows }]; }
    if (sql.includes('INSERT INTO home_campaign_daily')) return [{ affectedRows: 1 }];
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  const body = { entityType: 'card', entityId: 9, platform: 'web', deviceType: 'mobile', eventId: '15847678-1234-1234-1234-123456789abc' };
  assert.equal((await request('/home-cards/events', { method: 'POST', body })).status, 202);
  assert.equal((await request('/home-cards/events', { method: 'POST', body })).status, 202);
  assert.equal(calls.filter(call => call.sql?.includes('INSERT INTO home_campaign_daily')).length, 1);
  assert.equal(calls.filter(call => call === 'commit').length, 2);
});
test('events for disabled campaigns are rejected and rolled back', async t => {
  const { request, calls } = await server(t, sql => {
    if (sql.includes('FROM home_promotion_settings')) return [[{ settings_json: JSON.stringify(defaultSettings()) }]];
    if (sql.includes('FROM home_cards')) return [[{ enabled: 'N', target_platform: 'all' }]];
    throw new Error('No write expected');
  });
  const result = await request('/home-cards/events', { method: 'POST', body: { entityType: 'card', entityId: 9, platform: 'web', deviceType: 'mobile' } });
  assert.equal(result.status, 409); assert.ok(calls.includes('rollback')); assert.ok(!calls.includes('commit'));
});
test('public endpoint caches platform/language separately and hides disabled blocks', async t => {
  const settings = defaultSettings(); settings.web.cards.enabled = false;
  const { request, calls } = await server(t, sql => {
    if (sql.includes('FROM home_promotion_settings')) return [[{ settings_json: JSON.stringify(settings) }]];
    if (sql.includes('FROM home_cards')) return [[{ id: 9, title_en: 'Food', title_es: 'Alimentos', target_platform: 'app', enabled: 'Y' }]];
    throw new Error('Unexpected query');
  });
  const web = await (await request('/home-cards/public?platform=web')).json(); assert.deepEqual(web.cards, []);
  await request('/home-cards/public?platform=web');
  assert.equal(calls.filter(call => call.sql?.includes('FROM home_promotion_settings')).length, 1);
  const app = await (await request('/home-cards/public?platform=app&language=es')).json(); assert.equal(app.cards[0].title, 'Alimentos');
  assert.equal(calls.filter(call => call.sql?.includes('FROM home_promotion_settings')).length, 2);
});
test('public tracking rate limit bounds requests before database work', async t => {
  const { request, calls } = await server(t, () => { throw new Error('No SQL expected'); });
  for (let i = 0; i < 60; i++) assert.equal((await request('/home-cards/events', { method: 'POST', body: {} })).status, 400);
  const limited = await request('/home-cards/events', { method: 'POST', body: {} });
  assert.equal(limited.status, 429); assert.equal(limited.headers.get('retry-after'), '60'); assert.equal(calls.length, 0);
});
