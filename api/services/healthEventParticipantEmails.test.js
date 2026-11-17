'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { DatabaseSync } = require('node:sqlite');
const { normalizeParticipantEmail } = require('../utils/participantEmailAddress');
const { readReservationToken } = require('./healthEventReservation');
const { parseMessage, previewAudience, enqueue, getJob, processQueue } = require('./healthEventParticipantEmails');

const draft = { subject: 'Clinic reminder', message: 'Please use the east entrance.',
  request_id: '31958820-4bfb-4dc2-8f84-767237ff1019' };

function fixture(t) {
  const sql = new DatabaseSync(':memory:');
  t.after(() => sql.close());
  const oldSecret = process.env.JWT_SECRET;
  process.env.JWT_SECRET = 'synthetic-participant-links-test';
  t.after(() => {
    if (oldSecret === undefined) delete process.env.JWT_SECRET;
    else process.env.JWT_SECRET = oldSecret;
  });
  sql.function('DATE_FORMAT', (date, format) => date);
  sql.function('TIME_FORMAT', (time, format) => time.slice(0, 5));
  sql.exec(`
    CREATE TABLE health_event (id INTEGER PRIMARY KEY, name_en TEXT DEFAULT 'Clinic', name_es TEXT DEFAULT 'Clínica');
    CREATE TABLE user (id INTEGER PRIMARY KEY, email TEXT, deleted TEXT DEFAULT 'N', firstname TEXT, language TEXT DEFAULT 'en');
    CREATE TABLE health_event_slot (id INTEGER PRIMARY KEY, health_event_id INTEGER, service_key TEXT DEFAULT 'vision',
      slot_date TEXT DEFAULT '2099-12-05', start_time TEXT DEFAULT '09:30');
    CREATE TABLE health_event_registration (id INTEGER PRIMARY KEY, health_event_id INTEGER, user_id INTEGER,
      contact_email TEXT, registration_role TEXT DEFAULT 'beneficiary', status TEXT DEFAULT 'registered',
      source TEXT DEFAULT 'web', reservation_version INTEGER DEFAULT 1);
    CREATE TABLE health_event_confirmation (registration_id INTEGER, reservation_version INTEGER);
    CREATE TABLE health_event_appointment (registration_id INTEGER, slot_id INTEGER, status TEXT DEFAULT 'booked');
    CREATE TABLE health_event_participant_email (id INTEGER PRIMARY KEY, health_event_id INTEGER, request_id TEXT,
      created_by_user_id INTEGER, slot_id INTEGER, subject TEXT, message TEXT, registrations INTEGER,
      recipients INTEGER, without_email INTEGER, duplicates INTEGER, UNIQUE(health_event_id, request_id));
    CREATE TABLE health_event_participant_delivery (id INTEGER PRIMARY KEY, job_id INTEGER, email TEXT,
      status TEXT DEFAULT 'pending', UNIQUE(job_id, email));
    INSERT INTO health_event(id) VALUES (1),(2);
    INSERT INTO health_event_slot(id,health_event_id) VALUES (11,1),(12,1),(22,2);
    INSERT INTO user(id,email) VALUES (1,'account@example.test'),(2,'other@example.test'),(3,'prior@example.test'),
      (4,'unbooked@example.test'),(5,NULL),(6,'staff@example.test'),(7,'removed@example.test'),(8,'otherclinic@example.test'),(9,NULL);
    UPDATE user SET firstname='Rosa',language='es' WHERE id=1;
    UPDATE user SET firstname='Alex' WHERE id=2;
    UPDATE user SET deleted='Y' WHERE id=7;
    INSERT INTO health_event_registration (id, health_event_id, user_id, contact_email) VALUES
      (1,1,1,' Family@example.test '),(2,1,2,'family@EXAMPLE.TEST'),(3,1,3,NULL),
      (4,1,4,NULL),(5,1,5,'bogus'),(6,1,6,NULL),(7,1,7,NULL),(8,2,8,NULL),(9,1,9,'staff@example.test');
    UPDATE health_event_registration SET status='cancelled' WHERE id=3;
    UPDATE health_event_registration SET registration_role='volunteer' WHERE id=6;
    INSERT INTO health_event_appointment(registration_id,slot_id,status) VALUES
      (1,11,'booked'),(2,11,'booked'),(4,11,'cancelled'),(5,11,'booked'),(8,22,'booked');
  `);
  // Run the production SQL with SQLite; translate only mysql2 bulk VALUES
  // binding and connection-level locks, which are outside SQLite's dialect.
  const db = {
    getConnection: async () => db,
    beginTransaction: () => sql.exec('BEGIN'), commit: () => sql.exec('COMMIT'), rollback: () => sql.exec('ROLLBACK'),
    release() {},
    async query(raw, params = []) {
      if (raw.includes('GET_LOCK')) return [[{ acquired: 1 }]];
      if (raw.includes('RELEASE_LOCK')) return [[{}]];
      if (raw.endsWith('VALUES ?')) {
        const insert = sql.prepare(raw.replace('VALUES ?', 'VALUES (?,?)'));
        for (const row of params[0]) insert.run(...row);
        return [{}];
      }
      const stmt = sql.prepare(raw.replace(' FOR UPDATE', ''));
      if (/^SELECT/i.test(raw.trim())) return [stmt.all(...params).map(row => ({ ...row }))];
      const result = stmt.run(...params);
      return [{ affectedRows: result.changes, insertId: result.lastInsertRowid }];
    }
  };
  return { db, sql };
}

test('only accepts a single mailbox and validates a plain-text subject and body', () => {
  for (const value of ['john@example.test,others', 'x,y@example.test', 'other:me@example.test',
    'me@example.test\n', '<me@example.test>', 'me@abc.test/extra', 'two..dots@example.test', 'me@host..test']) {
    assert.equal(normalizeParticipantEmail(value), null);
  }
  assert.equal(normalizeParticipantEmail(" O'Brian+clinic@Example.test "), "O'Brian+clinic@example.test");
  assert.equal(normalizeParticipantEmail('guest@café.test'), 'guest@xn--caf-dma.test');
  for (const input of [{subject:'bad\nheader'}, {subject: 'x'.repeat(161)}, {message: ' '},
    {message: 'x'.repeat(5001)}, {request_id:'bad'}, {slot_id:'11x'}, {slot_id:[]}, {slot_id:0}]) {
    assert.throws(() => parseMessage({ ...draft, ...input }));
  }
});

test('audience counts by clinic and booked slot, retaining only eligible, unique addresses', async t => {
  const { db } = fixture(t);
  assert.deepEqual(await previewAudience(db, 1), { registrations: 5, recipients: 2, without_email: 2, duplicates: 1 });
  assert.deepEqual(await previewAudience(db, 1, '11'), { registrations: 3, recipients: 1, without_email: 1, duplicates: 1 });
  await assert.rejects(previewAudience(db, 1, 22), { code: 'INVALID_SLOT' });
  await assert.rejects(previewAudience(db, 3), { code: 'NOT_FOUND' });
  await assert.rejects(enqueue(db, 1, 42, {...draft, slot_id:12}), { code:'NO_RECIPIENTS' });
  assert.equal(await getJob(db, 1), null);
});

test('transaction snapshots the audience once across retries and sends privately to still-registered contacts', async t => {
  const { db, sql } = fixture(t);
  const first = await enqueue(db, 1, 42, draft);
  assert.equal(first.status, 'pending');
  assert.equal(first.pending, 2);
  assert.equal((await enqueue(db, 1, 42, draft)).id, first.id);
  await assert.rejects(enqueue(db, 1, 42, {...draft, subject:'different'}), { code:'REQUEST_ID_CONFLICT' });
  await assert.rejects(getJob(db, 2, first.id), { code:'NOT_FOUND' });
  sql.exec("UPDATE health_event_registration SET status='cancelled' WHERE id=4");
  const calls = [];
  const smtp = async mail => { calls.push(mail); return { accepted: [mail.to], rejected: [] }; };
  await processQueue(db, smtp);
  const latest = await getJob(db, 1);
  assert.equal(latest.status, 'completed');
  assert.equal(latest.sent, 1);
  assert.equal(latest.skipped, 1);
  assert.equal(calls.length, 1);
  const [{ reservations, ...mail }] = calls;
  assert.deepEqual(mail, { to:'Family@example.test', subject:draft.subject, message:draft.message });
  assert.deepEqual(reservations.map(r => [r.firstname,r.language,r.eventName,r.appointments[0].start_time]),
    [['Rosa','es','Clínica','09:30'],['Alex','en','Clinic','09:30']]);
  assert.deepEqual(reservations.map(r => readReservationToken(new URL(r.manageUrl).hash.slice(1))),
    [{id:1, version:1}, {id:2, version:1}]);
  assert.equal(sql.prepare('SELECT COUNT(email) AS n FROM health_event_participant_delivery').get().n, 0);
  assert.equal((await enqueue(db, 1, 42, draft)).id, first.id);
  await processQueue(db, smtp);
  assert.equal(calls.length, 1);
});

test('distinguishes SMTP rejection and interrupted delivery and never repeats ambiguous sends', async t => {
  const { db, sql } = fixture(t);
  const first = await enqueue(db, 1, 42, draft);
  sql.exec("UPDATE health_event_participant_delivery SET status='sending' WHERE id=1");
  let attempted = 0;
  const reject = async () => { attempted++; throw Object.assign(new Error('rejected'), { responseCode: 550 }); };
  await processQueue(db, reject);
  const result = await getJob(db, 1, first.id);
  assert.equal(attempted, 1);
  assert.equal(result.failed, 1);
  assert.equal(result.uncertain, 1);
  assert.equal(result.sent, 0);
  assert.equal(result.pending, 0);
  await processQueue(db, reject);
  assert.equal(attempted, 1);
});
