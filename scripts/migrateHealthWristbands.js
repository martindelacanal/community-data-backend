'use strict';

const fs = require('node:fs');
const path = require('node:path');
const mysql = require('mysql2/promise');
const dotenv = require('dotenv');

function databaseConfig(target, envPath = path.resolve(__dirname, '..', '.env')) {
  if (!['development', 'production'].includes(target)) throw new Error('Choose --target=development or --target=production');
  const envText = fs.readFileSync(envPath, 'utf8');
  const lines = envText.split(/\r?\n/);
  const heading = target === 'production' ? 'PRODUCTION DATABASE' : 'DEVELOPMENT DATABASE';
  const assignments = [];
  let inside = false;
  for (const line of lines) {
    if (!inside) {
      inside = new RegExp(`^\\s*#\\s*${heading}\\s*$`, 'i').test(line);
      continue;
    }
    if (assignments.length && (line.trim() === '' || /^\s*#\s*[A-Z][A-Z ]+\s*$/.test(line))) break;
    const match = line.match(/^\s*(?:#\s*)?(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/);
    if (match) assignments.push(`${match[1]}=${match[2]}`);
  }
  const values = dotenv.parse(assignments.join('\n'));
  if (!['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT'].every(key => values[key] != null)) {
    throw new Error(`Incomplete ${target} database block`);
  }
  return { host: values.DB_HOST, user: values.DB_USER, password: values.DB_PASSWORD,
    database: values.DB_DATABASE, port: Number(values.DB_PORT), connectTimeout: 30000, multipleStatements: true };
}

async function migrate(target) {
  const connection = await mysql.createConnection(databaseConfig(target));
  try {
    const sql = fs.readFileSync(path.resolve(__dirname, '..', 'migrations', '2026-10-02_health_event_wristbands.sql'), 'utf8');
    await connection.query(sql);
    const [rows] = await connection.query(
      `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN ('health_event_wristband','health_event_staff_attendance','health_event_wristband_scan')`);
    if (rows.length !== 3) throw new Error('Required Health Events wristband tables are missing');
    console.log(`Health Events wristband migration verified (${target}): 3 tables. Safe to run again.`);
  } finally {
    await connection.end();
  }
}

if (require.main === module) {
  const argument = process.argv.find(value => value.startsWith('--target='));
  migrate(argument && argument.slice(9)).catch(error => {
    console.error('Health Events wristband migration failed:', error.code || error.message);
    process.exitCode = 1;
  });
}

module.exports = { databaseConfig, migrate };
