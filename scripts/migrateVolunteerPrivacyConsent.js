'use strict';
require('dotenv').config({ quiet: true });
const fs = require('node:fs');
const path = require('node:path');
const mysql = require('mysql2/promise');
(async () => {
  const connection = await mysql.createConnection({ host: process.env.DB_HOST, user: process.env.DB_USER,
    password: process.env.DB_PASSWORD, database: process.env.DB_DATABASE, port: process.env.DB_PORT });
  try {
    await connection.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-30_volunteer_privacy_consent.sql'), 'utf8'));
    const [columns] = await connection.query('SHOW COLUMNS FROM volunteer_privacy_consent');
    if (columns.length !== 11) throw new Error('Unexpected privacy consent schema');
    console.log('Volunteer privacy consent schema ready.');
  } finally { await connection.end(); }
})().catch(error => { console.error(error.code || error.message); process.exitCode = 1; });
