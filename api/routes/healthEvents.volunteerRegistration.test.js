'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { DatabaseSync } = require('node:sqlite');
const express = require('express');
const jwt = require('jsonwebtoken');

test('volunteers apply for another clinic with their own login and staff review only that clinic', async t => {
  const db = new DatabaseSync(':memory:');
  t.after(() => db.close());
  db.exec(`
    CREATE TABLE user(id INTEGER PRIMARY KEY, email TEXT, role_id INTEGER, enabled TEXT, deleted TEXT DEFAULT 'N', password TEXT,
      username TEXT UNIQUE, client_id, firstname, lastname, date_of_birth, phone, zipcode, first_location_id, location_id,
      household_size, language, legal_consent_accepted, legal_consent_accepted_at, legal_consent_version, reset_password);
    CREATE TABLE role(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE health_event_registration(id INTEGER PRIMARY KEY, health_event_id INTEGER, user_id INTEGER,
      registration_role TEXT, status TEXT, contact_email TEXT, source TEXT, submitted_at TEXT,
      UNIQUE(health_event_id,user_id,registration_role));
    CREATE TABLE volunteer_privacy_consent(user_id, volunteer_id, registration_id, document_version, document_path,
      full_name, participant_role, email, device_model, accepted_at);
    INSERT INTO role VALUES (1,'admin'),(11,'eventvolunteer');
    INSERT INTO user(id,email,role_id,enabled,deleted,password) VALUES (1,'manager@example.test',1,'Y','N','manager'),(2,'returning@example.test',11,'Y','N','original'),
      (3,'new@example.test',11,'N','N','initial'),(4,'other@example.test',11,'Y','N','other');
    INSERT INTO health_event_registration VALUES (1,1,2,'volunteer','registered','returning@example.test','web',NULL),
      (2,2,3,'volunteer','pending_activation','new@example.test','web',NULL);
  `);
  // Exercise the real SQL predicates and transactions in an isolated database.
  // SQLite serializes writers at database level and has no per-row lock syntax.
  const connection = {
    async query(sql, params = []) {
      if (sql.includes('WHERE he.slug = ?')) {
        return [[{ id: Number(params[0]), location_id: 1, enabled: 'Y', timezone: 'America/Los_Angeles', end_date: '2099-01-01' }]];
      }
      if (/FROM (?:health_event_form f|health_event_notification_recipient|client_location)/.test(sql)) return [[]];
      const stmt = db.prepare(sql.replace(/ FOR UPDATE| LOCK IN SHARE MODE/g, '').replace(/NOW\(\d*\)/g, 'CURRENT_TIMESTAMP'));
      if (/^\s*SELECT/.test(sql)) return [stmt.all(...params)];
      const result = stmt.run(...params.map(value => value instanceof Date ? value.toISOString() : value));
      return [{ insertId: result.lastInsertRowid, affectedRows: result.changes }];
    },
    beginTransaction: async () => db.exec('BEGIN'),
    commit: async () => db.exec('COMMIT'),
    rollback: async () => db.exec('ROLLBACK'),
    release() {}
  };
  const pool = { ...connection, getConnection: async () => connection };
  const sent = [];
  function replaceModule(path, exports) {
    const id = require.resolve(path);
    const original = require.cache[id];
    require.cache[id] = { id, filename: id, loaded: true, exports };
    t.after(() => { if (original) require.cache[id] = original; else delete require.cache[id]; });
  }
  replaceModule('../connection/connection', { promise: () => pool });
  replaceModule('../email/email', { sendVolunteerConfirmation: async email => { sent.push(email); } });
  replaceModule('../services/imageVariants', {});
  replaceModule('../utils/logger', { error: console.error });
  const secret = 'synthetic-volunteer-clinic-test-secret';
  const oldSecret = process.env.JWT_SECRET;
  process.env.JWT_SECRET = secret;
  t.after(() => { if (oldSecret == null) delete process.env.JWT_SECRET; else process.env.JWT_SECRET = oldSecret; });
  const app = express();
  app.use(express.json(), require('./healthEvents'));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => server.close(resolve)));
  const url = 'http://127.0.0.1:' + server.address().port + '/health-events';
  const token = (id, role) => jwt.sign({ data: JSON.stringify({ id, role }) }, secret, { expiresIn: '5m' });
  const volunteer = token(2, 'eventvolunteer'), manager = token(1, 'admin');
  const body = { use_existing_account: true, user_id: 4, account: { id: 4, email: 'other@example.test',
    legalConsentAccepted: true, privacyConsent: { accepted: true, fullName: 'Synthetic Volunteer',
      role: 'volunteer', version: '2026-10-30', deviceModel: 'Test device' } }, answers: [] };
  async function request(method, route, auth, payload) {
    const response = await fetch(url + route, { method, headers: {
      'Content-Type': 'application/json', ...(auth ? { Authorization: 'Bearer ' + auth } : {}) },
      body: payload ? JSON.stringify(payload) : undefined });
    return { status: response.status, body: await response.json() };
  }
  const apply = (event, auth = volunteer, data = body) => request('POST', '/' + event + '/register/volunteer', auth, data);
  const decide = (event, user, decision, auth = manager) =>
    request('PUT', (event ? '/' + event : '') + '/volunteers/' + user + '/' + decision, auth);
  const status = (event, user) => db.prepare('SELECT status FROM health_event_registration WHERE health_event_id=? AND user_id=?')
    .get(event, user)?.status;

  assert.equal((await apply(2, null)).status, 401);
  assert.equal((await apply(2, manager)).status, 403);
  assert.equal((await apply(2, token(3, 'eventvolunteer'))).status, 403);
  assert.equal((await apply(2, volunteer, { ...body, account: { ...body.account, legalConsentAccepted: false } })).status, 400);
  const submitted = await apply(2);
  assert.deepEqual(submitted, { status: 200, body: { registration_id: 3, credentials: null, already_registered: false,
    approval_status: 'pending', pending_approval: true } });
  assert.equal((await apply(2)).body.already_registered, true);
  assert.deepEqual(sent, ['returning@example.test']);
  assert.deepEqual({ ...db.prepare('SELECT email,user_id,COUNT(*) AS total FROM volunteer_privacy_consent').get() },
    { email: 'returning@example.test', user_id: 2, total: 1 });
  assert.equal(status(2, 4), undefined);
  assert.equal((await decide(2, 2, 'approve', volunteer)).status, 403);
  assert.equal((await decide(null, 2, 'approve')).status, 409);
  assert.equal((await decide(3, 2, 'approve')).status, 404);
  assert.equal(status(1, 2), 'registered');
  assert.equal(status(2, 2), 'pending');
  assert.equal((await decide(2, 2, 'approve')).body.approval_status, 'approved');
  assert.equal((await decide(2, 2, 'approve')).status, 200);
  assert.equal((await apply(2)).body.approval_status, 'approved');
  assert.equal((await apply(3)).body.pending_approval, true);
  assert.equal((await decide(3, 2, 'reject')).body.approval_status, 'rejected');
  assert.equal((await apply(3)).body.approval_status, 'rejected');
  assert.equal((await decide(3, 2, 'approve')).status, 409);
  assert.equal(status(1, 2), 'registered');
  assert.equal(status(2, 2), 'registered');
  assert.deepEqual({ ...db.prepare('SELECT enabled,password FROM user WHERE id=2').get() }, { enabled: 'Y', password: 'original' });
  assert.equal((await decide(null, 3, 'approve')).status, 200);
  assert.equal(db.prepare('SELECT enabled FROM user WHERE id=3').get().enabled, 'Y');
  assert.equal(status(2, 3), 'registered');

  const anonymous = { account: { ...body.account, firstName: 'New', lastName: 'Applicant',
    email: 'newapplicant@example.test', phone: '9515550100' }, answers: [] };
  const newSignup = await apply(3, null, anonymous);
  assert.equal(newSignup.status, 200);
  assert.equal(newSignup.body.credentials.username, 'new.applicant');
  const account = db.prepare('SELECT id,enabled FROM user WHERE email=?').get(anonymous.account.email);
  assert.equal(account.enabled, 'N');
  assert.equal(status(3, account.id), 'pending_activation');
  assert.equal((await apply(3, null, anonymous)).body.error, 'EMAIL_TAKEN');
  assert.equal((await decide(3, account.id, 'reject')).body.approval_status, 'rejected');
  assert.equal(db.prepare('SELECT enabled FROM user WHERE id=?').get(account.id).enabled, 'N');

  assert.equal((await apply(4)).status, 200);
  db.exec("UPDATE user SET enabled='N' WHERE id=2");
  assert.equal((await decide(4, 2, 'approve')).body.error, 'ACCOUNT_DISABLED');
  assert.equal((await decide(1, 2, 'approve')).body.error, 'ACCOUNT_DISABLED');
  assert.equal(status(4, 2), 'pending');
  assert.equal(db.prepare('SELECT enabled FROM user WHERE id=2').get().enabled, 'N');
});
