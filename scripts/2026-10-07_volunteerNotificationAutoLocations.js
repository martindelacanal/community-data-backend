'use strict';

// Usage from BACKEND/:
//   node scripts/2026-10-07_volunteerNotificationAutoLocations.js --target=development --dry-run
//   node scripts/2026-10-07_volunteerNotificationAutoLocations.js --target=production --apply
// Uses the existing .env development/production database blocks. Credentials
// never appear in command arguments or output. Run before deploying the API.
const mysql = require('mysql2/promise');
const { databaseConfig } = require('./migrateHealthWristbands');

const DEFAULT_EMAILS = ['alex@bienestariswellbeing.org', 'karenantillon22@yahoo.com'];
const TRIGGER_NAME = 'location_volunteer_notification_recipients_ai';
const LOCK_TABLE = 'volunteer_notification_recipient_settings';
const TRIGGER_BODY = `BEGIN
  UPDATE volunteer_notification_recipient_settings SET id = id WHERE id = 1;
  INSERT INTO volunteer_notification_recipient_location (recipient_id, location_id)
    SELECT id, NEW.id FROM volunteer_notification_recipient
    WHERE enabled = 'Y' AND auto_include_new_locations = 1;
END`;
const normalizeSql = sql => sql.replace(/\s+/g, ' ').trim().toLowerCase();

async function migrate(target, { apply = false, connect = mysql.createConnection, config = databaseConfig, log = console.log } = {}) {
  const connection = await connect(config(target));
  try {
    const [tables] = await connection.query(
      `SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN ('location', 'volunteer_notification_recipient', 'volunteer_notification_recipient_location')`
    );
    if (tables.length !== 3 || tables.some(table => table.ENGINE.toLowerCase() !== 'innodb')) {
      throw new Error('The three existing location/recipient tables must use InnoDB');
    }

    const [columns] = await connection.query(
      `SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME = 'volunteer_notification_recipient' AND COLUMN_NAME = 'auto_include_new_locations'`
    );
    const [settingsTables] = await connection.query(
      `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
      [LOCK_TABLE]
    );
    const [triggers] = await connection.query(
      `SELECT ACTION_STATEMENT, ACTION_TIMING, EVENT_MANIPULATION, EVENT_OBJECT_TABLE
       FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME = ?`,
      [TRIGGER_NAME]
    );
    if (triggers.length && (
      normalizeSql(triggers[0].ACTION_STATEMENT) !== normalizeSql(TRIGGER_BODY) ||
      triggers[0].ACTION_TIMING !== 'AFTER' || triggers[0].EVENT_MANIPULATION !== 'INSERT' ||
      triggers[0].EVENT_OBJECT_TABLE !== 'location'
    )) {
      throw new Error('An unexpected trigger already uses the migration trigger name');
    }

    if (!apply) {
      log(`Volunteer auto locations (${target}, dry run): column=${columns.length ? 'exists' : 'add'}, settings=${settingsTables.length ? 'exists' : 'add'}, legacy_trigger=${triggers.length ? 'remove' : 'absent'}. No changes written.`);
      return;
    }

    // DDL implicitly commits in MySQL. Each step is repeatable so a failed
    // migration can safely resume without resetting preferences after seeding.
    if (!columns.length) {
      await connection.query(
        `ALTER TABLE volunteer_notification_recipient ADD COLUMN auto_include_new_locations TINYINT(1) NOT NULL DEFAULT 0 AFTER language`
      );
    }
    await connection.query(
      `CREATE TABLE IF NOT EXISTS volunteer_notification_recipient_settings (
        id TINYINT UNSIGNED NOT NULL,
        defaults_initialized TINYINT(1) NOT NULL DEFAULT 0,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB`
    );
    await connection.query(`INSERT IGNORE INTO volunteer_notification_recipient_settings (id) VALUES (1)`);

    await connection.beginTransaction();
    try {
      const [[settings]] = await connection.query(
        `SELECT defaults_initialized FROM volunteer_notification_recipient_settings WHERE id = 1 FOR UPDATE`
      );
      if (!settings.defaults_initialized) {
        await connection.query(
          `UPDATE volunteer_notification_recipient
           SET auto_include_new_locations = (LOWER(TRIM(email)) IN (?, ?)), modification_date = modification_date`,
          DEFAULT_EMAILS
        );
        await connection.query(
          `UPDATE volunteer_notification_recipient_settings SET defaults_initialized = 1 WHERE id = 1`
        );
      }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }

    if (triggers.length) {
      // RDS binary-log restrictions disallow trigger creation with this DB
      // account. Only remove the exact trigger from an earlier dev migration;
      // the transactional helper now handles every application insert path.
      await connection.query(`DROP TRIGGER \`${TRIGGER_NAME}\``);
    }
    const [[summary]] = await connection.query(
      `SELECT COUNT(*) AS total, SUM(auto_include_new_locations = 1) AS opted_in FROM volunteer_notification_recipient`
    );
    log(`Volunteer auto locations (${target}): verified column and settings lock, legacy trigger absent; recipients=${summary.total}, opted_in=${Number(summary.opted_in || 0)}. Existing location selections preserved.`);
  } finally {
    await connection.end();
  }
}

if (require.main === module) {
  const targetArg = process.argv.slice(2).find(arg => arg.startsWith('--target='));
  const apply = process.argv.includes('--apply');
  const dryRun = process.argv.includes('--dry-run');
  const target = targetArg && targetArg.slice(9);
  const valid = ['development', 'production'].includes(target) && apply !== dryRun &&
    process.argv.slice(2).every(arg => arg === targetArg || arg === '--apply' || arg === '--dry-run');
  if (!valid) {
    console.error('Usage: node scripts/2026-10-07_volunteerNotificationAutoLocations.js --target=<development|production> <--dry-run|--apply>');
    process.exitCode = 1;
  } else {
    migrate(target, { apply }).catch(error => {
      // Database errors can include SQL details; print only the safe error code.
      console.error('Volunteer auto locations migration failed:', error.code || error.message);
      process.exitCode = 1;
    });
  }
}

module.exports = { migrate, TRIGGER_NAME, TRIGGER_BODY, DEFAULT_EMAILS };
