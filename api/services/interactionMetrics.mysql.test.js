'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { fetchInteractionAcquisition } = require('./interactionMetrics');

// Opt-in, read-only SQL regression. All rows are inline CTE fixtures: no real
// user/session records are selected and no tables or data are written.
test('MySQL acquisition preserves first touch, timestamp ties and returning visitors', {
  skip: process.env.INTERACTION_METRICS_SQL_TEST !== '1'
}, async () => {
  require('dotenv').config({ quiet: true });
  const db = await require('mysql2/promise').createConnection({ host: process.env.DB_HOST,
    user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_DATABASE,
    port: Number(process.env.DB_PORT || 3306), decimalNumbers: true });
  const fixtures = `fixture_sessions AS (
    SELECT 1 AS id, 'returning' AS visitor_id, CAST('2026-08-01 00:00:00' AS DATETIME) AS started_at,
      'web_desktop' AS access_channel, 'direct' AS utm_source, 1 AS is_authenticated
    UNION ALL SELECT 2,'returning','2026-10-01 00:00:00','capacitor_ios','email',1
    UNION ALL SELECT 3,'same-time','2026-10-03 00:00:00','capacitor_ios','newsletter',0
    UNION ALL SELECT 4,'same-time','2026-10-03 00:00:00','web_desktop','other',1
    UNION ALL SELECT 5,'backfilled','2026-10-02 00:00:00','web_desktop','direct',1
    UNION ALL SELECT 6,'backfilled','2026-09-01 00:00:00','capacitor_android','direct',1
    UNION ALL SELECT 7,'fresh','2026-10-04 00:00:00','web_mobile','direct',1
  )`;
  const connection = { query(sql, params) {
    const substituted = sql.replace(/\binteraction_sessions\b/g, 'fixture_sessions');
    const statement = /\bWITH\b/.test(substituted)
      ? substituted.replace(/\bWITH\b/, `WITH ${fixtures},`)
      : `WITH ${fixtures} ${substituted}`;
    return db.query(statement, params);
  } };
  try {
    await db.query('SET SESSION transaction_read_only=1, max_execution_time=10000');
    const filters = { from_date: '2026-10-01', to_date: '2026-10-31' };
    const all = await fetchInteractionAcquisition(connection, 'en', filters);
    assert.equal(all.totalNewVisitors, 2);
    assert.equal(all.newAppVisitors, 1);
    assert.equal(all.activeVisitors, 4);
    assert.equal(all.returningVisitors, 2);
    assert.deepEqual(all.timeline.categories, ['2026-10-03', '2026-10-04']);
    assert.equal(all.acquisitionSources.find(row => row.key === 'newsletter')?.totalNewVisitors, 1);
    assert.equal(all.acquisitionSources.some(row => row.key === 'other'), false);
    const app = await fetchInteractionAcquisition(connection, 'en', { ...filters, platform: 'app' });
    assert.equal(app.totalNewVisitors, 1);
    assert.equal(app.activeVisitors, 2);
    assert.equal(app.returningVisitors, 1);
    const webAuthenticated = await fetchInteractionAcquisition(connection, 'en', { ...filters, platform: 'web', auth_scope: 'authenticated' });
    assert.equal(webAuthenticated.totalNewVisitors, 1);
    assert.equal(webAuthenticated.newAppVisitors, 0);
  } finally { await db.end(); }
});
