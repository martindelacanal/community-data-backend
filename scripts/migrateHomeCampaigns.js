'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { defaultSettings } = require('../api/services/homeCampaigns');

async function migrateHomeCampaigns(connection) {
  const [columns] = await connection.query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'home_cards' AND COLUMN_NAME = 'target_platform'");
  if (!columns.length) await connection.query("ALTER TABLE home_cards ADD COLUMN target_platform VARCHAR(8) NOT NULL DEFAULT 'all'");
  const [indexes] = await connection.query("SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'home_cards' AND INDEX_NAME = 'idx_home_cards_platform_enabled_sort'");
  if (!indexes.length) await connection.query('ALTER TABLE home_cards ADD INDEX idx_home_cards_platform_enabled_sort (target_platform, enabled, sort_order, id)');
  const sql = fs.readFileSync(path.join(__dirname, '../migrations/2026-10-21_home_campaigns.sql'), 'utf8');
  for (const statement of sql.split(';').map(value => value.trim()).filter(Boolean)) await connection.query(statement);
  await connection.beginTransaction();
  try {
    const [seed] = await connection.query('INSERT IGNORE INTO home_promotion_settings (id, settings_json, created_at, updated_at) VALUES (1, ?, UTC_TIMESTAMP(), UTC_TIMESTAMP())', [JSON.stringify(defaultSettings())]);
    // Seed only on first installation. Otherwise a rerun could misrepresent newly
    // created cards, or resurrect history that an administrator had deleted.
    if (seed.affectedRows) {
      await connection.query(`INSERT INTO home_campaign_history
        (entity_type, entity_id, card_id, title_en, title_es, action, changes_json, created_at)
        SELECT 'card', id, id, title_en, title_es, 'created',
          JSON_OBJECT('before', JSON_OBJECT(), 'after', JSON_OBJECT('createdAt', creation_date, 'source', 'original_creation_date')),
          creation_date
        FROM home_cards WHERE creation_date IS NOT NULL`);
      await connection.query(`INSERT INTO home_campaign_history
        (entity_type, entity_id, card_id, title_en, title_es, action, changes_json, created_at)
        SELECT 'card', id, id, title_en, title_es, 'imported',
          JSON_OBJECT('before', JSON_OBJECT(), 'after', JSON_OBJECT('enabled', enabled, 'targetPlatform', target_platform,
            'titleEn', title_en, 'titleEs', title_es, 'linkUrl', link_url, 'sortOrder', sort_order,
            'note', 'Current state when tracking started; earlier changes are not available')),
          UTC_TIMESTAMP() FROM home_cards`);
      for (const [entityType, titleEn, titleEs] of [['image_banner', 'Image campaign', 'Campaña de imagen'], ['app_banner', 'Download the app banner', 'Banner de descarga de apps']]) {
        await connection.query(`INSERT INTO home_campaign_history (entity_type, entity_id, title_en, title_es, action, changes_json, created_at)
          VALUES (?, 0, ?, ?, 'imported', ?, UTC_TIMESTAMP())`, [entityType, titleEn, titleEs, JSON.stringify({ before: {}, after: { tracking: 'started', note: 'Earlier clicks are not available' } })]);
      }
    }
    await connection.commit();
  } catch (error) { await connection.rollback(); throw error; }
  return { migrated: true };
}

if (require.main === module) {
  const mysql = require('mysql2/promise');
  const { databaseConfig } = require('./migrateHealthWristbands');
  (async () => {
    const target = process.argv.find(value => value.startsWith('--target='))?.slice(9);
    let config;
    if (target === 'current-env') {
      require('dotenv').config();
      if (!process.env.DB_HOST || !process.env.DB_DATABASE || !process.env.DB_USER) throw new Error('Missing current database configuration');
      config = { host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD,
        database: process.env.DB_DATABASE, port: Number(process.env.DB_PORT || 3306), connectTimeout: 20000 };
    } else { config = databaseConfig(target); }
    const connection = await mysql.createConnection(config);
    try { await migrateHomeCampaigns(connection); console.log(`Home campaign migration complete (${target}; existing data preserved).`); } finally { await connection.end(); }
  })().catch(error => { console.error('Home campaign migration failed:', error.code || '', error.message); process.exitCode = 1; });
}
module.exports = { migrateHomeCampaigns };
