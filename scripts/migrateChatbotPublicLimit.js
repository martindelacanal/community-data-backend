'use strict';

const mysql = require('mysql2/promise');
const { databaseConfig } = require('./migrateHealthWristbands');
const { DEFAULT_PUBLIC_DAILY_MESSAGE_LIMIT } = require('../api/services/chatbotLimits');

async function migratePublicLimit(db) {
  const [[column]] = await db.query(`SELECT 1 AS present FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='chatbot_settings' AND COLUMN_NAME='public_daily_message_limit'`);
  if (column) return;
  try {
    await db.query(`ALTER TABLE chatbot_settings ADD COLUMN public_daily_message_limit
      INT UNSIGNED NOT NULL DEFAULT ${DEFAULT_PUBLIC_DAILY_MESSAGE_LIMIT}`);
  } catch (error) {
    // Another migration process may have added the column since the check.
    if (error.code !== 'ER_DUP_FIELDNAME') throw error;
  }
}

async function main() {
  const target = process.argv.find(arg => arg.startsWith('--target='))?.slice(9);
  let config;
  if (process.argv.includes('--server-env')) {
    require('dotenv').config({ quiet: true });
    const { DB_HOST: host, DB_PORT: port, DB_USER: user, DB_PASSWORD: password, DB_DATABASE: database } = process.env;
    if (!host || !user || !database || password === undefined) throw new Error('Incomplete server database environment');
    config = { host, port: Number(port || 3306), user, password, database };
  } else config = databaseConfig(target);
  const db = await mysql.createConnection(config);
  try {
    await migratePublicLimit(db);
    console.log('Chatbot public daily limit column ready. Existing settings preserved.');
  } finally {
    await db.end();
  }
}

if (require.main === module) main().catch(error => {
  console.error('Chatbot public limit migration failed:', error.code || error.message);
  process.exitCode = 1;
});

module.exports = { migratePublicLimit };
