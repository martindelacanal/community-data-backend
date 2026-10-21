'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { addHistory, fetchCampaignMetrics, normalizeMetricFilters, readSettings } = require('./homeCampaigns');

// Opt-in only, after explicitly migrating the development database. All test
// data is inserted inside a transaction that is always rolled back.
test('MySQL campaign schema, aggregate filters and cascading deletion', { skip: process.env.HOME_CAMPAIGNS_DB_TEST !== '1' }, async () => {
  const { databaseConfig } = require('../../scripts/migrateHealthWristbands');
  const config = databaseConfig('development');
  const production = databaseConfig('production');
  assert.ok(config.host !== production.host || config.database !== production.database, 'Development target must differ from production');
  const mysql = require('mysql2/promise');
  const connection = await mysql.createConnection(config);
  try {
    await connection.beginTransaction();
    const [insert] = await connection.query(`INSERT INTO home_cards (title_en, title_es, enabled, target_platform) VALUES ('Synthetic campaign test', 'Campaña de prueba', 'Y', 'all')`);
    const id = insert.insertId; const eventId = randomUUID();
    await addHistory(connection, { entityType: 'card', entityId: id, titleEn: 'Synthetic', titleEs: 'Prueba', action: 'created', after: { enabled: true } });
    const params = [eventId, id, id, 'synthetic-session'];
    const statement = `INSERT IGNORE INTO home_campaign_clicks (event_id, entity_type, entity_id, card_id, session_id, platform, device_type, access_channel, clicked_at)
      VALUES (?, 'card', ?, ?, ?, 'web', 'mobile', 'web_mobile', UTC_TIMESTAMP())`;
    const [first] = await connection.query(statement, params); const [duplicate] = await connection.query(statement, params);
    assert.equal(first.affectedRows, 1); assert.equal(duplicate.affectedRows, 0);
    await connection.query(`INSERT INTO home_campaign_daily (click_date, entity_type, entity_id, card_id, platform, device_type, access_channel, clicks, last_clicked_at)
      VALUES (UTC_DATE(), 'card', ?, ?, 'web', 'mobile', 'web_mobile', 1, UTC_TIMESTAMP())`, [id, id]);
    const [settingsRows] = await connection.query('SELECT settings_json, created_at FROM home_promotion_settings WHERE id = 1');
    const settings = readSettings(settingsRows[0]);
    const filters = normalizeMetricFilters({ entityType: 'card', entityId: id });
    const result = await fetchCampaignMetrics(connection, filters, settings);
    assert.equal(result.summary.totalClicks, 1); assert.equal(result.summary.uniqueSessions, 1); assert.equal(result.history.length, 1);
    const appOnly = await fetchCampaignMetrics(connection, { ...filters, platform: 'app' }, settings);
    assert.equal(appOnly.summary.totalClicks, 0);
    await connection.query('DELETE FROM home_cards WHERE id = ?', [id]);
    for (const table of ['home_campaign_clicks', 'home_campaign_daily', 'home_campaign_history']) {
      const [remaining] = await connection.query(`SELECT COUNT(*) AS total FROM ${table} WHERE card_id = ?`, [id]);
      assert.equal(Number(remaining[0].total), 0);
    }
  } finally { await connection.rollback(); await connection.end(); }
});
