'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const jwt = require('jsonwebtoken');
const moment = require('moment-timezone');

const RUN_INTEGRATION = process.env.RUN_HEALTH_SCAN_INTEGRATION === 'development';
const BACKEND_ROOT = path.resolve(__dirname, '..', '..');
const ENV_PATH = path.join(BACKEND_ROOT, '.env');
const SLIDING_REFRESH_DELAY_MS = 6_000;
const SLIDING_EXPIRY_DELAY_MS = 10_500;
const CONCURRENT_REQUESTS = 12;

// The app's connection module is a process-wide cached pool: ending it inside
// one test would break every test that runs after it in the same file.
let sharedPool = null;
test.after(async () => {
  if (sharedPool) await sharedPool.promise().end();
});

function normalizeEnvValue(value) {
  return String(value == null ? '' : value)
    .trim()
    .replace(/^(['"])(.*)\1$/, '$2')
    .trim()
    .toLowerCase();
}

function readDatabaseBlock(envText, heading, commentedAssignments) {
  const lines = String(envText).split(/\r?\n/);
  const headingPattern = new RegExp(`^\\s*#\\s*${heading}\\s*$`, 'i');
  const assignmentPattern = commentedAssignments
    ? /^\s*#\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i
    : /^\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i;
  const values = {};
  let inside = false;
  let foundAssignment = false;

  for (const line of lines) {
    if (!inside) {
      inside = headingPattern.test(line);
      continue;
    }

    const match = line.match(assignmentPattern);
    if (match) {
      values[match[1].toUpperCase()] = match[2].trim();
      foundAssignment = true;
      continue;
    }

    if (foundAssignment && line.trim() === '') break;
    if (foundAssignment && /^\s*#\s*[A-Z][A-Z ]+\s*$/i.test(line)) break;
  }

  return values;
}

function assertDevelopmentDatabaseTarget() {
  if (!fs.existsSync(ENV_PATH)) {
    throw new Error('Integration test refused: BACKEND/.env was not found.');
  }

  const envText = fs.readFileSync(ENV_PATH, 'utf8');
  const development = readDatabaseBlock(envText, 'DEVELOPMENT DATABASE', false);
  const production = readDatabaseBlock(envText, 'PRODUCTION DATABASE', true);
  const requiredTargetKeys = ['DB_HOST', 'DB_USER', 'DB_DATABASE', 'DB_PORT'];

  if (!requiredTargetKeys.every(key => development[key]) || !production.DB_HOST) {
    throw new Error('Integration test refused: database safety blocks in .env are incomplete.');
  }

  const active = Object.fromEntries(requiredTargetKeys.map(key => [key, process.env[key]]));
  const matchesDevelopment = requiredTargetKeys.every(
    key => normalizeEnvValue(active[key]) === normalizeEnvValue(development[key])
  );
  const matchesProductionHost = normalizeEnvValue(active.DB_HOST) === normalizeEnvValue(production.DB_HOST);

  if (matchesProductionHost) {
    throw new Error('Integration test refused: the active database host matches the PRODUCTION DATABASE block.');
  }
  if (!matchesDevelopment) {
    throw new Error('Integration test refused: the active target does not match the DEVELOPMENT DATABASE block.');
  }
}

function delay(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function listenOnEphemeralPort(app) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1');
    server.once('listening', () => resolve(server));
    server.once('error', reject);
  });
}

async function closeServer(server) {
  if (!server) return;
  await new Promise((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve());
  });
}

async function postScan(baseUrl, token, payload) {
  const response = await fetch(`${baseUrl}/api/health-events/scan`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${token}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify(payload)
  });
  const rawBody = await response.text();
  let body = null;
  try {
    body = rawBody ? JSON.parse(rawBody) : null;
  } catch (error) {
    assert.fail(`Scan endpoint returned invalid JSON (HTTP ${response.status}).`);
  }

  assert.equal(
    response.status,
    200,
    `Scan endpoint returned HTTP ${response.status}: ${JSON.stringify(body)}`
  );
  return body;
}

function selectUsableEvent(rows) {
  const byEvent = new Map();
  for (const row of rows) {
    const timezone = row.event_timezone || 'America/Los_Angeles';
    const today = moment().tz(timezone).format('YYYY-MM-DD');
    if (!row.event_end_date || row.event_end_date < today) continue;

    if (!byEvent.has(row.event_id)) byEvent.set(row.event_id, []);
    byEvent.get(row.event_id).push({ ...row, event_timezone: timezone });
  }

  for (const stands of byEvent.values()) {
    const primary = stands.find(row => Number(row.checkout_form_count) > 0);
    const secondary = stands.find(row => !primary || row.stand_id !== primary.stand_id);
    if (primary && secondary) return { primary, secondary };
  }
  return null;
}

async function createFixture(pool, selectedEvent) {
  const suffix = crypto.randomUUID().replace(/-/g, '').slice(0, 16);
  const username = `__health_scan_${suffix}`;
  const connection = await pool.promise().getConnection();
  let userId = null;
  let registrationId = null;
  let phone = null;

  try {
    await connection.beginTransaction();
    const [roleRows] = await connection.query(
      'SELECT id FROM role WHERE name = ? LIMIT 1',
      ['beneficiary']
    );
    assert.equal(roleRows.length, 1, 'Development database has no beneficiary role.');

    for (let attempt = 0; attempt < 20; attempt += 1) {
      const candidate = `999${crypto.randomInt(0, 10_000_000).toString().padStart(7, '0')}`;
      const [phoneRows] = await connection.query(
        `SELECT COUNT(*) AS total
           FROM user
          WHERE RIGHT(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone,
              '-', ''), ' ', ''), '(', ''), ')', ''), '+', ''), '.', ''),
            10
          ) = ?`,
        [candidate]
      );
      if (Number(phoneRows[0].total) === 0) {
        phone = candidate;
        break;
      }
    }
    assert.ok(phone, 'Could not allocate a unique synthetic phone number.');

    const [userInsert] = await connection.query(
      `INSERT INTO user
         (username, firstname, lastname, phone, role_id, enabled, deleted, language)
       VALUES (?, ?, ?, ?, ?, 'Y', 'N', 'en')`,
      [username, 'Health Scan', 'Integration Test', phone, roleRows[0].id]
    );
    userId = Number(userInsert.insertId);

    const [registrationInsert] = await connection.query(
      `INSERT INTO health_event_registration
         (health_event_id, user_id, registration_role, status, source, submitted_at)
       VALUES (?, ?, 'beneficiary', 'registered', 'admin', NOW())`,
      [selectedEvent.event_id, userId]
    );
    registrationId = Number(registrationInsert.insertId);
    await connection.commit();

    return { userId, registrationId, username, phone };
  } catch (error) {
    try { await connection.rollback(); } catch (rollbackError) { /* preserve original error */ }
    throw error;
  } finally {
    connection.release();
  }
}

async function cleanupFixture(pool, fixture) {
  if (!fixture || !fixture.userId) return;

  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();
    await connection.query(
      'UPDATE health_event_scan SET paired_scan_id = NULL WHERE scanned_user_id = ?',
      [fixture.userId]
    );
    await connection.query('DELETE FROM health_event_scan WHERE scanned_user_id = ?', [fixture.userId]);
    await connection.query('DELETE FROM health_event_registration WHERE user_id = ?', [fixture.userId]);
    await connection.query(
      'DELETE FROM user WHERE id = ? AND username = ?',
      [fixture.userId, fixture.username]
    );
    await connection.commit();
  } catch (error) {
    try { await connection.rollback(); } catch (rollbackError) { /* preserve original error */ }
    throw error;
  } finally {
    connection.release();
  }

  const [auditRows] = await pool.promise().query(
    `SELECT
       (SELECT COUNT(*) FROM health_event_scan WHERE scanned_user_id = ?) AS scans,
       (SELECT COUNT(*) FROM health_event_registration WHERE user_id = ?) AS registrations,
       (SELECT COUNT(*) FROM user WHERE id = ?) AS users`,
    [fixture.userId, fixture.userId, fixture.userId]
  );
  assert.deepEqual(
    {
      scans: Number(auditRows[0].scans),
      registrations: Number(auditRows[0].registrations),
      users: Number(auditRows[0].users)
    },
    { scans: 0, registrations: 0, users: 0 },
    'Integration fixture cleanup left database rows behind.'
  );
}

async function loadFixtureScans(pool, fixture, eventId, standId) {
  const [rows] = await pool.promise().query(
    `SELECT id, scan_type, paired_scan_id
       FROM health_event_scan
      WHERE health_event_id = ? AND stand_id = ? AND scanned_user_id = ?
      ORDER BY id ASC`,
    [eventId, standId, fixture.userId]
  );
  return rows.map(row => ({
    id: Number(row.id),
    scan_type: row.scan_type,
    paired_scan_id: row.paired_scan_id == null ? null : Number(row.paired_scan_id)
  }));
}

test('health scan endpoint serializes QR/phone requests and applies a ten-second sliding QR guard', {
  skip: RUN_INTEGRATION
    ? false
    : 'Set RUN_HEALTH_SCAN_INTEGRATION=development to run against the development database.',
  timeout: 90_000
}, async () => {
  let pool = null;
  let server = null;
  let fixture = null;
  let testError = null;
  const cleanupErrors = [];

  try {
    require('dotenv').config({ path: ENV_PATH });
    assertDevelopmentDatabaseTarget();

    pool = require('../connection/connection');
    sharedPool = pool;
    const app = require('../../app');
    const { formatDailyQrDate } = require('../utils/dailyBeneficiaryQr');

    const [standRows] = await pool.promise().query(
      `SELECT he.id AS event_id,
              DATE_FORMAT(he.end_date, '%Y-%m-%d') AS event_end_date,
              he.timezone AS event_timezone,
              st.id AS stand_id,
              (SELECT COUNT(*)
                 FROM health_event_form f
                WHERE f.health_event_id = he.id AND f.stand_id = st.id
                  AND f.audience = 'checkout' AND f.enabled = 'Y') AS checkout_form_count
         FROM health_event he
         INNER JOIN health_event_stand st ON st.health_event_id = he.id
        WHERE he.enabled = 'Y' AND st.enabled = 'Y' AND st.has_checkout = 'Y'
        ORDER BY he.end_date ASC, he.id ASC, checkout_form_count DESC, st.id ASC`
    );
    const selected = selectUsableEvent(standRows);
    assert.ok(selected, 'Development database needs a non-ended event with two checkout stands.');

    const [volunteerRows] = await pool.promise().query(
      `SELECT u.id, r.name AS role
         FROM user u
         INNER JOIN role r ON r.id = u.role_id
        WHERE r.name IN ('eventvolunteer', 'opsmanager', 'admin')
          AND u.enabled = 'Y' AND u.deleted = 'N'
        ORDER BY FIELD(r.name, 'eventvolunteer', 'opsmanager', 'admin'), u.id ASC
        LIMIT 1`
    );
    assert.equal(volunteerRows.length, 1, 'Development database needs an active volunteer or admin user.');

    fixture = await createFixture(pool, selected.primary);
    const token = jwt.sign({
      data: JSON.stringify({
        id: Number(volunteerRows[0].id),
        role: volunteerRows[0].role
      })
    }, process.env.JWT_SECRET, { expiresIn: '10m' });
    const qrDate = formatDailyQrDate(new Date(), selected.primary.event_timezone);
    const qr = `B${fixture.userId}.${qrDate}`;
    const primaryPayload = {
      event_id: Number(selected.primary.event_id),
      stand_id: Number(selected.primary.stand_id),
      qr,
      confirmed: true
    };

    server = await listenOnEphemeralPort(app);
    const address = server.address();
    assert.ok(address && typeof address === 'object', 'Ephemeral HTTP server has no address.');
    const baseUrl = `http://127.0.0.1:${address.port}`;

    const concurrent = await Promise.all(
      Array.from({ length: CONCURRENT_REQUESTS }, () => postScan(baseUrl, token, primaryPayload))
    );
    const firstScanId = Number(concurrent[0].scan_id);
    assert.ok(Number.isInteger(firstScanId) && firstScanId > 0);
    assert.equal(new Set(concurrent.map(result => Number(result.scan_id))).size, 1);
    assert.deepEqual(new Set(concurrent.map(result => result.scan_type)), new Set(['checkin']));
    assert.equal(concurrent.filter(result => result.duplicate === false).length, 1);
    assert.equal(concurrent.filter(result => result.duplicate === true).length, CONCURRENT_REQUESTS - 1);
    assert.deepEqual(
      await loadFixtureScans(pool, fixture, selected.primary.event_id, selected.primary.stand_id),
      [{ id: firstScanId, scan_type: 'checkin', paired_scan_id: null }]
    );

    await delay(SLIDING_REFRESH_DELAY_MS);
    const firstRefresh = await postScan(baseUrl, token, primaryPayload);
    assert.equal(Number(firstRefresh.scan_id), firstScanId);
    assert.equal(firstRefresh.scan_type, 'checkin');
    assert.equal(firstRefresh.duplicate, true);

    await delay(SLIDING_REFRESH_DELAY_MS);
    const secondRefresh = await postScan(baseUrl, token, primaryPayload);
    assert.equal(Number(secondRefresh.scan_id), firstScanId);
    assert.equal(secondRefresh.scan_type, 'checkin');
    assert.equal(secondRefresh.duplicate, true);
    assert.deepEqual(
      await loadFixtureScans(pool, fixture, selected.primary.event_id, selected.primary.stand_id),
      [{ id: firstScanId, scan_type: 'checkin', paired_scan_id: null }],
      'Repeated frames more than ten seconds after the original scan must remain debounced.'
    );

    await delay(SLIDING_EXPIRY_DELAY_MS);
    const phonePayload = {
      event_id: Number(selected.primary.event_id),
      stand_id: Number(selected.primary.stand_id),
      phone: fixture.phone,
      user_id: fixture.userId,
      confirmed: true
    };
    const concurrentCheckouts = await Promise.all(
      Array.from({ length: CONCURRENT_REQUESTS }, () => postScan(baseUrl, token, phonePayload))
    );
    const createdCheckout = concurrentCheckouts.find(result => result.duplicate === false);
    assert.ok(createdCheckout, 'Exactly one concurrent phone request should create the checkout.');
    const checkoutScanId = Number(createdCheckout.scan_id);
    assert.ok(Number.isInteger(checkoutScanId) && checkoutScanId > firstScanId);
    assert.equal(new Set(concurrentCheckouts.map(result => Number(result.scan_id))).size, 1);
    assert.deepEqual(new Set(concurrentCheckouts.map(result => result.scan_type)), new Set(['checkout']));
    assert.equal(concurrentCheckouts.filter(result => result.duplicate === false).length, 1);
    assert.equal(
      concurrentCheckouts.filter(result => result.duplicate === true).length,
      CONCURRENT_REQUESTS - 1
    );
    assert.ok(createdCheckout.checkout_form, 'The request that creates checkout should return its form once.');
    for (const duplicate of concurrentCheckouts.filter(result => result.duplicate === true)) {
      assert.equal(duplicate.checkout_form, null);
    }
    assert.deepEqual(
      await loadFixtureScans(pool, fixture, selected.primary.event_id, selected.primary.stand_id),
      [
        { id: firstScanId, scan_type: 'checkin', paired_scan_id: null },
        { id: checkoutScanId, scan_type: 'checkout', paired_scan_id: firstScanId }
      ]
    );

    const qrAfterPhoneCheckout = await postScan(baseUrl, token, primaryPayload);
    assert.equal(Number(qrAfterPhoneCheckout.scan_id), checkoutScanId);
    assert.equal(qrAfterPhoneCheckout.scan_type, 'checkout');
    assert.equal(qrAfterPhoneCheckout.duplicate, true);
    assert.equal(qrAfterPhoneCheckout.checkout_form, null);
    assert.equal(
      (await loadFixtureScans(pool, fixture, selected.primary.event_id, selected.primary.stand_id)).length,
      2
    );

    const secondaryPayload = {
      ...primaryPayload,
      stand_id: Number(selected.secondary.stand_id)
    };
    const otherStand = await postScan(baseUrl, token, secondaryPayload);
    assert.equal(otherStand.scan_type, 'checkin');
    assert.equal(otherStand.duplicate, false);
    assert.notEqual(Number(otherStand.scan_id), checkoutScanId);
    assert.equal(
      (await loadFixtureScans(pool, fixture, selected.secondary.event_id, selected.secondary.stand_id)).length,
      1,
      'A different stand must have an independent debounce key.'
    );
  } catch (error) {
    testError = error;
  } finally {
    try {
      await closeServer(server);
    } catch (error) {
      cleanupErrors.push(error);
    }
    try {
      // cleanupFixture commits the deletes and then asserts scan/registration/user are all zero.
      if (pool) await cleanupFixture(pool, fixture);
    } catch (error) {
      cleanupErrors.push(error);
    }
  }

  const errors = testError ? [testError, ...cleanupErrors] : cleanupErrors;
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) {
    throw new AggregateError(errors, 'Health scan integration test and/or cleanup failed.');
  }
});

// ---------------------------------------------------------------------------
// One visit per stand per day (temporary event with its own stands)
// ---------------------------------------------------------------------------

const DB_WINDOW_EXPIRY_DELAY_MS = 10_500;

async function createDailyRuleEvent(pool) {
  const suffix = crypto.randomUUID().replace(/-/g, '').slice(0, 12);
  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();
    const [locationRows] = await connection.query('SELECT id FROM location ORDER BY id ASC LIMIT 1');
    assert.equal(locationRows.length, 1, 'Development database needs at least one location.');
    const timezone = 'America/Los_Angeles';
    const today = moment().tz(timezone).format('YYYY-MM-DD');
    const [eventInsert] = await connection.query(
      `INSERT INTO health_event (slug, name_en, name_es, location_id, start_date, end_date, timezone, enabled, landing_enabled)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'Y', 'N')`,
      [`__daily_rule_${suffix}`, 'Daily rule test', 'Prueba regla diaria', locationRows[0].id, today, today, timezone]
    );
    const eventId = Number(eventInsert.insertId);
    const [haircuts] = await connection.query(
      `INSERT INTO health_event_stand (health_event_id, name_en, name_es, is_entry, has_checkout, sort_order, enabled)
       VALUES (?, 'Haircuts', 'Cortes de pelo', 'N', 'N', 1, 'Y')`, [eventId]);
    const [dental] = await connection.query(
      `INSERT INTO health_event_stand (health_event_id, name_en, name_es, is_entry, has_checkout, sort_order, enabled)
       VALUES (?, 'Dental', 'Dental', 'N', 'Y', 2, 'Y')`, [eventId]);
    await connection.commit();
    return {
      event_id: eventId,
      event_timezone: timezone,
      event_end_date: today,
      haircuts_stand_id: Number(haircuts.insertId),
      dental_stand_id: Number(dental.insertId)
    };
  } catch (error) {
    try { await connection.rollback(); } catch (rollbackError) { /* preserve original error */ }
    throw error;
  } finally {
    connection.release();
  }
}

async function cleanupDailyRuleEvent(pool, event) {
  if (!event) return;
  // Scans/stands/registrations cascade from the event row; the fixture user is
  // removed by cleanupFixture, which also asserts nothing was left behind.
  await pool.promise().query('DELETE FROM health_event_scan WHERE health_event_id = ?', [event.event_id]);
  await pool.promise().query('DELETE FROM health_event WHERE id = ?', [event.event_id]);
  const [rows] = await pool.promise().query(
    'SELECT COUNT(*) AS total FROM health_event_stand WHERE health_event_id = ?', [event.event_id]);
  assert.equal(Number(rows[0].total), 0, 'Temporary event stands were not removed.');
}

test('health scan endpoint enforces one visit per stand per day', {
  skip: RUN_INTEGRATION
    ? false
    : 'Set RUN_HEALTH_SCAN_INTEGRATION=development to run against the development database.',
  timeout: 120_000
}, async () => {
  let pool = null;
  let server = null;
  let event = null;
  let fixture = null;
  let testError = null;
  const cleanupErrors = [];

  try {
    require('dotenv').config({ path: ENV_PATH });
    assertDevelopmentDatabaseTarget();

    pool = require('../connection/connection');
    sharedPool = pool;
    const app = require('../../app');

    const [volunteerRows] = await pool.promise().query(
      `SELECT u.id, r.name AS role
         FROM user u
         INNER JOIN role r ON r.id = u.role_id
        WHERE r.name IN ('eventvolunteer', 'opsmanager', 'admin')
          AND u.enabled = 'Y' AND u.deleted = 'N'
        ORDER BY FIELD(r.name, 'eventvolunteer', 'opsmanager', 'admin'), u.id ASC
        LIMIT 1`
    );
    assert.equal(volunteerRows.length, 1, 'Development database needs an active volunteer or admin user.');

    event = await createDailyRuleEvent(pool);
    fixture = await createFixture(pool, { event_id: event.event_id });
    const token = jwt.sign({
      data: JSON.stringify({ id: Number(volunteerRows[0].id), role: volunteerRows[0].role })
    }, process.env.JWT_SECRET, { expiresIn: '10m' });

    server = await listenOnEphemeralPort(app);
    const address = server.address();
    const baseUrl = `http://127.0.0.1:${address.port}`;

    // Phone identity skips the process-local QR debounce; the 10 s DB window still applies.
    const basePayload = { event_id: event.event_id, phone: fixture.phone, user_id: fixture.userId, confirmed: true, allow_repeat: false };
    const haircuts = { ...basePayload, stand_id: event.haircuts_stand_id };
    const dental = { ...basePayload, stand_id: event.dental_stand_id };

    // ---- stand without check-out: the second scan of the day is refused ----
    const first = await postScan(baseUrl, token, haircuts);
    assert.equal(first.scan_type, 'checkin');
    assert.equal(first.duplicate, false);
    assert.equal(first.duplicate_reason, null);
    const firstId = Number(first.scan_id);

    await delay(DB_WINDOW_EXPIRY_DELAY_MS);
    const refused = await postScan(baseUrl, token, haircuts);
    assert.equal(refused.duplicate, true);
    assert.equal(refused.duplicate_reason, 'already_served_today');
    assert.equal(refused.scan_type, 'checkin');
    assert.equal(Number(refused.scan_id), firstId);
    assert.ok(refused.previous_scan, 'The refused scan reports the one that already counts.');
    assert.equal(Number(refused.previous_scan.scan_id), firstId);
    assert.match(String(refused.previous_scan.scanned_at_local), /^\d{2}:\d{2}$/);
    assert.equal(refused.person.firstname, 'Health Scan');
    assert.deepEqual(
      await loadFixtureScans(pool, fixture, event.event_id, event.haircuts_stand_id),
      [{ id: firstId, scan_type: 'checkin', paired_scan_id: null }],
      'A stand without check-out must keep a single check-in per person per day.'
    );

    // A QR frame right after the refusal must not replay it as a plain 'recent'
    // duplicate: the refusal is not remembered in the process-local debounce.
    const { formatDailyQrDate } = require('../utils/dailyBeneficiaryQr');
    const qrHaircuts = {
      event_id: event.event_id, stand_id: event.haircuts_stand_id, confirmed: true, allow_repeat: false,
      qr: `B${fixture.userId}.${formatDailyQrDate(new Date(), event.event_timezone)}`
    };
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const qrRefused = await postScan(baseUrl, token, qrHaircuts);
      assert.equal(qrRefused.duplicate_reason, 'already_served_today');
      assert.equal(Number(qrRefused.previous_scan.scan_id), firstId);
    }
    assert.equal((await loadFixtureScans(pool, fixture, event.event_id, event.haircuts_stand_id)).length, 1);

    // Old clients (no allow_repeat flag) are refused too: the rule is not optional.
    const { allow_repeat, ...legacyHaircuts } = haircuts;
    const legacyRefused = await postScan(baseUrl, token, legacyHaircuts);
    assert.equal(legacyRefused.duplicate_reason, 'already_served_today');
    assert.equal((await loadFixtureScans(pool, fixture, event.event_id, event.haircuts_stand_id)).length, 1);

    // ---- stand with check-out: check-in, check-out, then explicit confirmation ----
    const dentalIn = await postScan(baseUrl, token, dental);
    assert.equal(dentalIn.scan_type, 'checkin');
    assert.equal(dentalIn.duplicate, false);
    const dentalInId = Number(dentalIn.scan_id);

    await delay(DB_WINDOW_EXPIRY_DELAY_MS);
    const dentalOut = await postScan(baseUrl, token, dental);
    assert.equal(dentalOut.scan_type, 'checkout');
    assert.equal(dentalOut.duplicate, false);
    const dentalOutId = Number(dentalOut.scan_id);

    await delay(DB_WINDOW_EXPIRY_DELAY_MS);
    const asked = await postScan(baseUrl, token, dental);
    assert.equal(asked.requires_repeat_confirmation, true);
    assert.equal(asked.scan_id, undefined, 'Nothing is recorded until the volunteer confirms.');
    assert.equal(Number(asked.previous_visit.scan_id), dentalInId);
    assert.match(String(asked.previous_visit.checkin_at_local), /^\d{2}:\d{2}$/);
    assert.match(String(asked.previous_visit.checkout_at_local), /^\d{2}:\d{2}$/);
    assert.deepEqual(
      await loadFixtureScans(pool, fixture, event.event_id, event.dental_stand_id),
      [
        { id: dentalInId, scan_type: 'checkin', paired_scan_id: null },
        { id: dentalOutId, scan_type: 'checkout', paired_scan_id: dentalInId }
      ]
    );

    const confirmed = await postScan(baseUrl, token, { ...dental, allow_repeat: true });
    assert.equal(confirmed.scan_type, 'checkin');
    assert.equal(confirmed.duplicate, false);
    assert.ok(Number(confirmed.scan_id) > dentalOutId);
    assert.equal((await loadFixtureScans(pool, fixture, event.event_id, event.dental_stand_id)).length, 3);
  } catch (error) {
    testError = error;
  } finally {
    try {
      await closeServer(server);
    } catch (error) {
      cleanupErrors.push(error);
    }
    try {
      if (pool) await cleanupDailyRuleEvent(pool, event);
    } catch (error) {
      cleanupErrors.push(error);
    }
    try {
      if (pool) await cleanupFixture(pool, fixture);
    } catch (error) {
      cleanupErrors.push(error);
    }
  }

  const errors = testError ? [testError, ...cleanupErrors] : cleanupErrors;
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) {
    throw new AggregateError(errors, 'Daily-rule integration test and/or cleanup failed.');
  }
});
