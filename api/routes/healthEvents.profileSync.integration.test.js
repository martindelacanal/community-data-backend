'use strict';

/**
 * End-to-end check of the question -> participant-profile mapping against the
 * DEVELOPMENT database: a temporary event with a beneficiary form whose gender
 * question maps to the app gender catalogue and whose zip question maps to
 * user.zipcode; a public registration must create the account WITH those
 * profile fields, and a second submission must never overwrite them.
 *
 *   RUN_HEALTH_SCAN_INTEGRATION=development node --test api/routes/healthEvents.profileSync.integration.test.js
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const moment = require('moment-timezone');

const RUN_INTEGRATION = process.env.RUN_HEALTH_SCAN_INTEGRATION === 'development';
const BACKEND_ROOT = path.resolve(__dirname, '..', '..');
const ENV_PATH = path.join(BACKEND_ROOT, '.env');

function normalizeEnvValue(value) {
  return String(value == null ? '' : value).trim().replace(/^(['"])(.*)\1$/, '$2').trim().toLowerCase();
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
  const envText = fs.readFileSync(ENV_PATH, 'utf8');
  const development = readDatabaseBlock(envText, 'DEVELOPMENT DATABASE', false);
  const production = readDatabaseBlock(envText, 'PRODUCTION DATABASE', true);
  const keys = ['DB_HOST', 'DB_USER', 'DB_DATABASE', 'DB_PORT'];
  const active = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  if (normalizeEnvValue(active.DB_HOST) === normalizeEnvValue(production.DB_HOST)) {
    throw new Error('Integration test refused: the active database host matches the PRODUCTION DATABASE block.');
  }
  if (!keys.every(key => normalizeEnvValue(active[key]) === normalizeEnvValue(development[key]))) {
    throw new Error('Integration test refused: the active target does not match the DEVELOPMENT DATABASE block.');
  }
}

async function listen(app) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1');
    server.once('listening', () => resolve(server));
    server.once('error', reject);
  });
}

async function postJson(baseUrl, route, payload) {
  const response = await fetch(`${baseUrl}${route}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload)
  });
  const text = await response.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch (error) { assert.fail(`Invalid JSON (HTTP ${response.status})`); }
  return { status: response.status, body };
}

async function createEventWithMappedForm(pool) {
  const suffix = crypto.randomUUID().replace(/-/g, '').slice(0, 12);
  const connection = await pool.promise().getConnection();
  try {
    await connection.beginTransaction();
    const [[location]] = await connection.query('SELECT id FROM location ORDER BY id ASC LIMIT 1');
    assert.ok(location, 'Development database needs at least one location.');
    const [[female]] = await connection.query("SELECT id FROM gender WHERE name = 'Female' LIMIT 1");
    const [[male]] = await connection.query("SELECT id FROM gender WHERE name = 'Male' LIMIT 1");
    assert.ok(female && male, 'gender catalogue needs Female and Male');

    const timezone = 'America/Los_Angeles';
    const today = moment().tz(timezone);
    const [eventInsert] = await connection.query(
      `INSERT INTO health_event (slug, name_en, name_es, location_id, start_date, end_date, timezone, enabled, landing_enabled)
       VALUES (?, 'Profile sync test', 'Prueba sync perfil', ?, ?, ?, ?, 'Y', 'N')`,
      [`__profile_sync_${suffix}`, location.id, today.format('YYYY-MM-DD'), today.clone().add(1, 'day').format('YYYY-MM-DD'), timezone]);
    const eventId = Number(eventInsert.insertId);
    const [formInsert] = await connection.query(
      `INSERT INTO health_event_form (health_event_id, audience, stand_id, title_en, title_es, section_order, required_before_qr, enabled)
       VALUES (?, 'beneficiary', NULL, 'About you', 'Sobre vos', 1, 'N', 'Y')`, [eventId]);
    const formId = Number(formInsert.insertId);
    const [genderQ] = await connection.query(
      `INSERT INTO health_event_question (form_id, question_type, name_en, name_es, required, allow_other, maps_to, sort_order, enabled)
       VALUES (?, 'single', 'Gender identity', 'Identidad de género', 'N', 'N', 'profile_gender', 1, 'Y')`, [formId]);
    const genderQuestionId = Number(genderQ.insertId);
    const [optFemale] = await connection.query(
      `INSERT INTO health_event_question_option (question_id, name_en, name_es, is_other, profile_option_id, sort_order, enabled)
       VALUES (?, 'Woman', 'Mujer', 'N', ?, 1, 'Y')`, [genderQuestionId, female.id]);
    const [optUnmapped] = await connection.query(
      `INSERT INTO health_event_question_option (question_id, name_en, name_es, is_other, profile_option_id, sort_order, enabled)
       VALUES (?, 'Rather not say here', 'Prefiero no decir acá', 'N', NULL, 2, 'Y')`, [genderQuestionId]);
    const [zipQ] = await connection.query(
      `INSERT INTO health_event_question (form_id, question_type, name_en, name_es, required, allow_other, maps_to, sort_order, enabled)
       VALUES (?, 'text', 'Zip code', 'Código postal', 'N', 'N', 'profile_zipcode', 2, 'Y')`, [formId]);
    await connection.commit();
    return {
      slug: `__profile_sync_${suffix}`, eventId, formId,
      genderQuestionId, femaleOptionId: Number(optFemale.insertId), unmappedOptionId: Number(optUnmapped.insertId),
      zipQuestionId: Number(zipQ.insertId), femaleGenderId: Number(female.id), maleGenderId: Number(male.id)
    };
  } catch (error) {
    try { await connection.rollback(); } catch (_) { /* keep original error */ }
    throw error;
  } finally {
    connection.release();
  }
}

async function uniquePhone(pool) {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const candidate = `998${crypto.randomInt(0, 10_000_000).toString().padStart(7, '0')}`;
    const [[row]] = await pool.promise().query('SELECT COUNT(*) AS total FROM user WHERE phone = ?', [candidate]);
    if (Number(row.total) === 0) return candidate;
  }
  throw new Error('Could not allocate a unique phone');
}

async function cleanup(pool, fixture, usernames) {
  if (!fixture) return;
  const [users] = await pool.promise().query(
    `SELECT id FROM user WHERE username IN (${usernames.map(() => '?').join(',') || 'NULL'})`, usernames);
  const userIds = users.map(row => Number(row.id));
  if (userIds.length) {
    await pool.promise().query('DELETE FROM health_event_registration WHERE user_id IN (?)', [userIds]);
    await pool.promise().query('DELETE FROM user WHERE id IN (?)', [userIds]);
  }
  await pool.promise().query('DELETE FROM health_event WHERE id = ?', [fixture.eventId]);
  const [[left]] = await pool.promise().query(
    `SELECT (SELECT COUNT(*) FROM health_event_form WHERE health_event_id = ?) AS forms,
            (SELECT COUNT(*) FROM user WHERE id IN (${userIds.length ? userIds.map(() => '?').join(',') : 'NULL'})) AS users`,
    [fixture.eventId, ...userIds]);
  assert.deepEqual({ forms: Number(left.forms), users: Number(left.users) }, { forms: 0, users: 0 }, 'cleanup left rows behind');
}

test('public registration copies mapped answers into the new account profile without overwriting later', {
  skip: RUN_INTEGRATION ? false : 'Set RUN_HEALTH_SCAN_INTEGRATION=development to run against the development database.',
  timeout: 90_000
}, async () => {
  let pool = null;
  let server = null;
  let fixture = null;
  const usernames = [];
  let testError = null;
  const cleanupErrors = [];
  try {
    require('dotenv').config({ path: ENV_PATH });
    assertDevelopmentDatabaseTarget();
    pool = require('../connection/connection');
    const app = require('../../app');
    fixture = await createEventWithMappedForm(pool);
    server = await listen(app);
    const baseUrl = `http://127.0.0.1:${server.address().port}`;

    // 1. Registration with a mapped gender option + zip text -> profile filled.
    const username = `__profile_sync_${crypto.randomUUID().slice(0, 8)}`;
    usernames.push(username);
    const registered = await postJson(baseUrl, `/api/health-events/${fixture.slug}/register`, {
      account: { username, firstName: 'Profile', lastName: 'Sync Test', phone: await uniquePhone(pool) },
      answers: [
        { question_id: fixture.genderQuestionId, answer: fixture.femaleOptionId },
        { question_id: fixture.zipQuestionId, answer: ' 92220 ' }
      ],
      appointments: []
    });
    assert.equal(registered.status, 200, JSON.stringify(registered.body));
    const [[user]] = await pool.promise().query(
      'SELECT id, gender_id, ethnicity_id, zipcode FROM user WHERE username = ? LIMIT 1', [username]);
    assert.ok(user, 'account was created');
    assert.equal(Number(user.gender_id), fixture.femaleGenderId, 'gender copied from the mapped option');
    assert.equal(user.ethnicity_id, null, 'unmapped profile fields stay empty');
    assert.equal(user.zipcode, '92220', 'zip code copied from the text answer');

    // 2. An account that already has a gender keeps it even when the form says otherwise.
    const username2 = `__profile_sync_${crypto.randomUUID().slice(0, 8)}`;
    usernames.push(username2);
    const registered2 = await postJson(baseUrl, `/api/health-events/${fixture.slug}/register`, {
      account: { username: username2, firstName: 'Profile', lastName: 'Sync Test', phone: await uniquePhone(pool), zipcode: '90001' },
      answers: [
        { question_id: fixture.genderQuestionId, answer: fixture.unmappedOptionId },
        { question_id: fixture.zipQuestionId, answer: '92223' }
      ],
      appointments: []
    });
    assert.equal(registered2.status, 200, JSON.stringify(registered2.body));
    const [[user2]] = await pool.promise().query(
      'SELECT gender_id, zipcode FROM user WHERE username = ? LIMIT 1', [username2]);
    assert.equal(user2.gender_id, null, 'an unmapped option copies nothing');
    assert.equal(user2.zipcode, '90001', 'the zip code typed in the account form is never overwritten by the question');
  } catch (error) {
    testError = error;
  } finally {
    try { if (server) await new Promise((resolve, reject) => server.close(err => err ? reject(err) : resolve())); } catch (error) { cleanupErrors.push(error); }
    try { if (pool) await cleanup(pool, fixture, usernames); } catch (error) { cleanupErrors.push(error); }
    try { if (pool) await pool.promise().end(); } catch (error) { cleanupErrors.push(error); }
  }
  const errors = testError ? [testError, ...cleanupErrors] : cleanupErrors;
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) throw new AggregateError(errors, 'Profile sync integration test and/or cleanup failed.');
});
