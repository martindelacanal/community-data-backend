'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const mysql = require('mysql2/promise');
const { findRecentHealthScan } = require('./healthScanGuard');

const RUN_DEVELOPMENT_DB_TEST = process.env.RUN_HEALTH_SCAN_DB_TEST === 'development';

function readProductionDatabaseBlock(envText) {
  const lines = envText.split(/\r?\n/);
  const marker = lines.findIndex(line => /^\s*#\s*PRODUCTION DATABASE\s*$/i.test(line));
  const config = {};
  if (marker < 0) return config;

  for (let index = marker + 1; index < lines.length && lines[index].trim(); index += 1) {
    const match = lines[index].match(/^\s*#\s*(DB_[A-Z]+)\s*=\s*(.*?)\s*$/);
    if (match) config[match[1]] = match[2];
  }
  return config;
}

function sameDatabase(left, right) {
  return ['DB_HOST', 'DB_USER', 'DB_DATABASE', 'DB_PORT']
    .every(key => String(left[key] || '') === String(right[key] || ''));
}

test('development DB guard query and beneficiary row lock leave no scan rows', {
  skip: !RUN_DEVELOPMENT_DB_TEST
}, async (t) => {
  const backendRoot = path.resolve(__dirname, '..', '..');
  const envPath = path.join(backendRoot, '.env');
  const envText = fs.readFileSync(envPath, 'utf8');
  require('dotenv').config({ path: envPath });

  const activeConfig = {
    DB_HOST: process.env.DB_HOST,
    DB_USER: process.env.DB_USER,
    DB_PASSWORD: process.env.DB_PASSWORD,
    DB_DATABASE: process.env.DB_DATABASE,
    DB_PORT: process.env.DB_PORT
  };
  const productionConfig = readProductionDatabaseBlock(envText);
  assert.equal(
    sameDatabase(activeConfig, productionConfig),
    false,
    'Refusing to run the mutating integration fixture against production'
  );

  const connectionOptions = {
    host: activeConfig.DB_HOST,
    user: activeConfig.DB_USER,
    password: activeConfig.DB_PASSWORD,
    database: activeConfig.DB_DATABASE,
    port: Number(activeConfig.DB_PORT || 3306),
    connectTimeout: 30_000,
    decimalNumbers: true
  };
  const connection = await mysql.createConnection(connectionOptions);
  const lockConnection = await mysql.createConnection(connectionOptions);
  const insertedIds = [];

  try {
    const [fixtures] = await connection.query(
      `SELECT r.id AS registration_id, r.health_event_id AS event_id, r.user_id,
              st.id AS stand_id,
              COALESCE((SELECT vr.user_id
                          FROM health_event_registration vr
                         WHERE vr.health_event_id = r.health_event_id
                           AND vr.registration_role = 'volunteer'
                         LIMIT 1), r.user_id) AS volunteer_user_id
         FROM health_event_registration r
         INNER JOIN user u ON u.id = r.user_id
         INNER JOIN health_event_stand st ON st.health_event_id = r.health_event_id
        WHERE r.registration_role = 'beneficiary' AND r.status = 'registered'
          AND u.deleted = 'N' AND u.enabled = 'Y' AND st.enabled = 'Y'
          AND NOT EXISTS (
            SELECT 1 FROM health_event_scan existing
             WHERE existing.health_event_id = r.health_event_id
               AND existing.stand_id = st.id
               AND existing.scanned_user_id = r.user_id
          )
        ORDER BY r.id, st.id
        LIMIT 1`
    );
    if (!fixtures.length) {
      t.skip('Development database has no isolated health-event fixture');
      return;
    }
    const fixture = fixtures[0];

    await connection.beginTransaction();
    await connection.query('SELECT id FROM user WHERE id = ? FOR UPDATE', [fixture.user_id]);

    const [checkinInsert] = await connection.query(
      `INSERT INTO health_event_scan(
         health_event_id, stand_id, service_id, registration_id,
         scanned_user_id, volunteer_user_id, scan_type, paired_scan_id
       ) VALUES (?, ?, NULL, ?, ?, ?, 'checkin', NULL)`,
      [fixture.event_id, fixture.stand_id, fixture.registration_id,
        fixture.user_id, fixture.volunteer_user_id]
    );
    insertedIds.push(Number(checkinInsert.insertId));

    assert.deepEqual(await findRecentHealthScan(connection, {
      eventId: fixture.event_id,
      standId: fixture.stand_id,
      userId: fixture.user_id,
      windowSeconds: 10
    }), { scanId: Number(checkinInsert.insertId), scanType: 'checkin' });

    await connection.query(
      'UPDATE health_event_scan SET scanned_at = NOW(3) - INTERVAL 11 SECOND WHERE id = ?',
      [checkinInsert.insertId]
    );
    assert.equal(await findRecentHealthScan(connection, {
      eventId: fixture.event_id,
      standId: fixture.stand_id,
      userId: fixture.user_id,
      windowSeconds: 10
    }), null);

    const [checkoutInsert] = await connection.query(
      `INSERT INTO health_event_scan(
         health_event_id, stand_id, service_id, registration_id,
         scanned_user_id, volunteer_user_id, scan_type, paired_scan_id
       ) VALUES (?, ?, NULL, ?, ?, ?, 'checkout', ?)`,
      [fixture.event_id, fixture.stand_id, fixture.registration_id,
        fixture.user_id, fixture.volunteer_user_id, checkinInsert.insertId]
    );
    insertedIds.push(Number(checkoutInsert.insertId));
    assert.deepEqual(await findRecentHealthScan(connection, {
      eventId: fixture.event_id,
      standId: fixture.stand_id,
      userId: fixture.user_id,
      windowSeconds: 10
    }), { scanId: Number(checkoutInsert.insertId), scanType: 'checkout' });

    await connection.rollback();

    const placeholders = insertedIds.map(() => '?').join(',');
    const [remaining] = await connection.query(
      `SELECT COUNT(*) AS total FROM health_event_scan WHERE id IN (${placeholders})`,
      insertedIds
    );
    assert.equal(Number(remaining[0].total), 0);

    // Verify the exact FOR UPDATE primitive used by the route really blocks a
    // concurrent transaction until the first beneficiary lock is released.
    await connection.beginTransaction();
    await connection.query('SELECT id FROM user WHERE id = ? FOR UPDATE', [fixture.user_id]);
    await lockConnection.beginTransaction();
    let secondLockResolved = false;
    const secondLock = lockConnection
      .query('SELECT id FROM user WHERE id = ? FOR UPDATE', [fixture.user_id])
      .then(() => { secondLockResolved = true; });

    await new Promise(resolve => setTimeout(resolve, 150));
    assert.equal(secondLockResolved, false);
    await connection.rollback();
    await secondLock;
    assert.equal(secondLockResolved, true);
    await lockConnection.rollback();
  } finally {
    try { await connection.rollback(); } catch (error) { /* no active transaction */ }
    try { await lockConnection.rollback(); } catch (error) { /* no active transaction */ }
    await connection.end();
    await lockConnection.end();
  }
});
