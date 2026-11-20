'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { DatabaseSync } = require('node:sqlite');
const { normalizeParticipantEmail } = require('../utils/participantEmailAddress');
const { readReservationToken } = require('./healthEventReservation');
const { parseMessage, sampleDraft, sendTest, previewAudience, enqueue, getJob, processQueue } = require('./healthEventParticipantEmails');

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
  sql.function('UTC_TIMESTAMP', () => Math.floor(Date.now() / 1000));
  sql.exec(`
    CREATE TABLE health_event (id INTEGER PRIMARY KEY, name_en TEXT DEFAULT 'Clinic', name_es TEXT DEFAULT 'Clínica',
      enabled TEXT DEFAULT 'Y', timezone TEXT DEFAULT 'America/Los_Angeles', end_date TEXT DEFAULT '2099-12-06',
      registration_closes_at TEXT DEFAULT '2001-01-01');
    CREATE TABLE user (id INTEGER PRIMARY KEY, email TEXT, deleted TEXT DEFAULT 'N', firstname TEXT, language TEXT DEFAULT 'en');
    CREATE TABLE health_event_slot (id INTEGER PRIMARY KEY, health_event_id INTEGER, service_key TEXT DEFAULT 'vision',
      slot_date TEXT DEFAULT '2099-12-05', start_time TEXT DEFAULT '09:30');
    CREATE TABLE health_event_registration (id INTEGER PRIMARY KEY, health_event_id INTEGER, user_id INTEGER,
      contact_email TEXT, registration_role TEXT DEFAULT 'beneficiary', status TEXT DEFAULT 'registered',
      source TEXT DEFAULT 'web', reservation_version INTEGER DEFAULT 1);
    CREATE TABLE health_event_confirmation (registration_id INTEGER, reservation_version INTEGER);
    CREATE TABLE health_event_registration_contact_review (registration_id INTEGER PRIMARY KEY, reservation_version INTEGER,
      contact_email TEXT COLLATE NOCASE NOT NULL);
    CREATE TABLE health_event_appointment (registration_id INTEGER, slot_id INTEGER, status TEXT DEFAULT 'booked');
    CREATE TABLE health_event_participant_email (id INTEGER PRIMARY KEY, health_event_id INTEGER, request_id TEXT,
      created_by_user_id INTEGER, slot_id INTEGER, subject TEXT, message TEXT, registrations INTEGER,
      recipients INTEGER, without_email INTEGER, duplicates INTEGER, UNIQUE(health_event_id, request_id));
    CREATE TABLE health_event_participant_delivery (id INTEGER PRIMARY KEY, job_id INTEGER, email TEXT,
      status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0, retry_at INTEGER,
      recipient_rejected INTEGER DEFAULT 0, UNIQUE(job_id, email));
    CREATE TABLE health_event_participant_mail_sender (id INTEGER PRIMARY KEY, paused_until INTEGER);
    INSERT INTO health_event_participant_mail_sender (id) VALUES (1);
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
      const stmt = sql.prepare(raw.replace(' FOR UPDATE', '')
        .replaceAll('TIMESTAMPADD(MINUTE, ?, UTC_TIMESTAMP())', '(UTC_TIMESTAMP() + 60 * ?)'));
      if (/^SELECT/i.test(raw.trim())) return [stmt.all(...params).map(row => ({ ...row }))];
      const result = stmt.run(...params);
      return [{ affectedRows: result.changes, insertId: result.lastInsertRowid }];
    }
  };
  return { db, sql };
}

test('preview and test render the same draft as a mailing, using only event configuration and an example reservation', async t => {
  const { db, sql } = fixture(t);
  const eventOnly = { query: (query, params) => {
    assert.match(query, /^SELECT /);
    assert.match(query, / FROM health_event(?:_slot)? WHERE /);
    assert.doesNotMatch(query, /JOIN|registration|appointment|user\b/i);
    return db.query(query, params);
  } };
  const sent = [];
  t.mock.method(require('nodemailer'), 'createTransport', () => ({
    sendMail: async mail => { sent.push(mail); return { accepted: [mail.to.address], rejected: [] }; }
  }));
  const { buildHealthEventParticipantEmailContent, sendHealthEventParticipantEmail } = require('../email/email');
  const body = { subject: 'Join the clinic', message: 'Hello <team>\nBring a friend.', slot_id: 11, language: 'es' };
  const preview = buildHealthEventParticipantEmailContent(await sampleDraft(eventOnly, 1, body));
  assert.equal(preview.subject, body.subject);
  assert.match(preview.html, /Hello &lt;team&gt;<br>Bring a friend\./);
  assert.match(preview.html, /Participante de ejemplo · Clínica/);
  assert.match(preview.html, /Vision · .*2099 · 09:30/);
  assert.match(preview.text, /https:\/\/example\.invalid\/reservation/);
  assert.equal(sent.length, 0);
  const testResult = await sendTest(eventOnly, 1, { ...body, to: ' editor@Example.test ' }, sendHealthEventParticipantEmail);
  assert.deepEqual(testResult, { accepted: true });
  assert.equal(sent.length, 1);
  assert.deepEqual(sent[0].to, { address: 'editor@example.test', name: '' });
  assert.equal(sent[0].subject, '[Prueba] ' + preview.subject);
  assert.equal(sent[0].html, preview.html);
  assert.equal(sent[0].text, preview.text);
  assert.equal(sql.prepare('SELECT COUNT(*) AS n FROM health_event_participant_email').get().n, 0);
  sql.exec('DELETE FROM health_event_slot WHERE health_event_id = 2');
  const noSlot = await sampleDraft(eventOnly, 2, { subject: 'Info', message: 'Hello', language: 'en' });
  assert.deepEqual(noSlot.reservations[0].appointments, []);
  assert.equal(noSlot.reservations[0].firstname, 'Sample participant');
});

test('test sends reject invalid input before delivery and distinguish rejected mail from an unknown SMTP outcome', async t => {
  const { db } = fixture(t);
  const body = { ...draft, language: 'en', to: 'editor@example.test' };
  const neverSend = async () => { assert.fail('Invalid input must never reach SMTP'); };
  for (const [params, code] of [[{ to: ['a@example.test'] }, 'INVALID_TEST_EMAIL'],
    [{ to: 'a@example.test,b@example.test' }, 'INVALID_TEST_EMAIL'], [{ slot_id: 22 }, 'INVALID_SLOT'],
    [{ language: 'fr' }, 'INVALID_LANGUAGE'], [{ message: ' ' }, 'INVALID_MESSAGE']]) {
    await assert.rejects(sendTest(db, 1, { ...body, ...params }, neverSend), { code });
  }
  await assert.rejects(sampleDraft(db, 77, body), { code: 'NOT_FOUND' });
  await assert.rejects(sendTest(db, 1, body, async () => ({ accepted: [], rejected: [body.to] })), { code: 'TEST_EMAIL_FAILED' });
  await assert.rejects(sendTest(db, 1, body, async () => { throw new Error('Connection lost'); }), { code: 'TEST_EMAIL_UNCERTAIN' });
});

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
  assert.deepEqual(await previewAudience(db, 1), { registrations: 5, recipients: 2, without_email: 2, duplicates: 1, includes_reservation_links: true });
  assert.deepEqual(await previewAudience(db, 1, '11'), { registrations: 3, recipients: 1, without_email: 1, duplicates: 1, includes_reservation_links: true });
  await assert.rejects(previewAudience(db, 1, 22), { code: 'INVALID_SLOT' });
  await assert.rejects(previewAudience(db, 3), { code: 'NOT_FOUND' });
  await assert.rejects(enqueue(db, 1, 42, {...draft, slot_id:12}), { code:'NO_RECIPIENTS' });
  assert.equal(await getJob(db, 1), null);
});

test('a reviewed legacy contact authorizes only its exact address and reservation version', async t => {
  const { db, sql } = fixture(t);
  sql.exec(`
    INSERT INTO user(id,email) VALUES (10,NULL);
    INSERT INTO health_event_registration(id,health_event_id,user_id,contact_email)
      VALUES (10,1,10,'staff@example.test');
    INSERT INTO health_event_registration_contact_review(registration_id,reservation_version,contact_email)
      VALUES (9,1,'Staff@example.test');
  `);
  assert.equal((await previewAudience(db, 1)).recipients, 2);
  sql.exec("UPDATE health_event_registration_contact_review SET contact_email='staff@example.test'");
  assert.equal((await previewAudience(db, 1)).recipients, 3);
  await enqueue(db, 1, 42, draft);
  const received = [];
  await processQueue(db, async mail => {
    if (mail.to === 'staff@example.test') {
      for (const reservation of mail.reservations) {
        received.push(readReservationToken(new URL(reservation.manageUrl).hash.slice(1)));
      }
    }
    return { accepted: [mail.to], rejected: [] };
  });
  assert.deepEqual(received, [{ id: 9, version: 1 }]);

  const { previousParticipantContactEmailSql } = require('./healthEventParticipantContact');
  const prior = sql.prepare(`SELECT ${previousParticipantContactEmailSql} AS contact FROM health_event_registration r
    JOIN user u ON u.id=r.user_id WHERE r.id=9`).get();
  assert.equal(prior.contact, null);
  sql.exec("UPDATE health_event_registration SET reservation_version=2 WHERE id=9");
  assert.equal((await previewAudience(db, 1)).recipients, 2);
});

test('a mailing to a previous clinic keeps the invitation and omits the old reservations in preview, test and delivery', async t => {
  const { db, sql } = fixture(t);
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-12-07T07:59:59Z') });
  sql.exec("UPDATE health_event SET end_date='2026-12-06' WHERE id=1");
  const invitation = { ...draft, subject: 'San Jacinto', message: 'Join our next clinic! https://bienestarcommunity.org/san-jacinto',
    slot_id: 11, language: 'es' };
  const { buildHealthEventParticipantEmailContent: render } = require('../email/email');
  // Although registration closed earlier, a reservation still exists through the clinic's last local day.
  assert.equal((await previewAudience(db, 1, 11)).includes_reservation_links, true);
  assert.match(render(await sampleDraft(db, 1, invitation)).text, /https:\/\/example\.invalid\/reservation/);
  await enqueue(db, 1, 42, invitation);
  t.mock.timers.tick(1000); // Midnight in Los Angeles.
  assert.deepEqual(await previewAudience(db, 1, 11), { registrations: 3, recipients: 1, without_email: 1,
    duplicates: 1, includes_reservation_links: false });
  const expected = { subject: invitation.subject, html: '<p>' + invitation.message + '</p>', text: invitation.message };
  assert.deepEqual(render(await sampleDraft(db, 1, invitation)), expected);
  await assert.rejects(sampleDraft(db, 1, { ...invitation, slot_id: 22 }), { code: 'INVALID_SLOT' });
  const sent = [];
  const smtp = async mail => { sent.push([mail.to, render(mail)]); return { accepted: [mail.to], rejected: [] }; };
  await sendTest(db, 1, { ...invitation, to: 'editor@example.test' }, smtp);
  await processQueue(db, smtp);
  assert.deepEqual(sent, [
    ['editor@example.test', { ...expected, subject: '[Prueba] ' + expected.subject }],
    ['Family@example.test', expected]
  ]);
  sql.exec("UPDATE health_event SET end_date='2099-12-06', enabled='N' WHERE id=1");
  assert.equal((await previewAudience(db, 1)).includes_reservation_links, false);
  assert.deepEqual(render(await sampleDraft(db, 1, invitation)), expected);
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

test('sender quota pauses all campaigns durably before touching another address and resumes after the cooldown', async t => {
  const { db, sql } = fixture(t);
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-20T18:00:00Z') });
  const first = await enqueue(db, 1, 42, draft);
  await enqueue(db, 2, 42, draft);
  const calls = [];
  const smtp = async mail => {
    calls.push(mail.to);
    if (calls.length === 1) throw Object.assign(new Error('refused'), {
      responseCode: 550, response: '550-5.4.5 Daily user sending limit exceeded', command: 'DATA'
    });
    return { accepted: [mail.to], rejected: [] };
  };
  await processQueue(db, smtp);
  await processQueue(db, smtp);
  const paused = await getJob(db, 1, first.id);
  assert.deepEqual([paused.pending, paused.deferred, paused.sender_paused, paused.failed, paused.recipient_rejections],
    [2, 1, true, 0, 0]);
  assert.equal((await getJob(db, 2)).sender_paused, true);
  assert.equal(sql.prepare('SELECT COUNT(email) AS n FROM health_event_participant_delivery').get().n, 3);
  t.mock.timers.tick(24 * 60 * 60 * 1000 - 1000);
  await processQueue(db, smtp);
  assert.deepEqual(calls, ['Family@example.test']);
  t.mock.timers.tick(1000);
  // Another registration may have been cancelled or its address corrected while waiting.
  sql.exec("UPDATE health_event_registration SET status='cancelled' WHERE id IN (1,2)");
  await processQueue(db, smtp);
  await processQueue(db, smtp);
  const completed = await getJob(db, 1);
  assert.deepEqual([completed.sent, completed.skipped, completed.pending, completed.deferred, completed.sender_paused],
    [1, 1, 0, 0, false]);
  assert.deepEqual(calls, ['Family@example.test', 'unbooked@example.test', 'otherclinic@example.test']);
  assert.equal(sql.prepare('SELECT COUNT(email) AS n FROM health_event_participant_delivery').get().n, 0);
});

test('one temporary mailbox rejection does not block other recipients and is retried only a bounded number of times', async t => {
  const { db, sql } = fixture(t);
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-11-20T18:00:00Z') });
  await enqueue(db, 1, 42, draft);
  const calls = [];
  const smtp = async mail => {
    calls.push(mail.to);
    if (mail.to === 'Family@example.test') throw Object.assign(new Error(), {
      responseCode: 452, response: '452 4.2.2 The recipient inbox is full', command: 'RCPT TO'
    });
    return { accepted: [mail.to], rejected: [] };
  };
  await processQueue(db, smtp);
  const partial = await getJob(db, 1);
  assert.deepEqual([partial.sent, partial.pending, partial.deferred, partial.sender_paused], [1, 1, 1, false]);
  for (const minutes of [15, 60, 360]) {
    await processQueue(db, smtp);
    t.mock.timers.tick(minutes * 60 * 1000);
    await processQueue(db, smtp);
  }
  await processQueue(db, smtp);
  assert.equal(calls.filter(address => address === 'Family@example.test').length, 4);
  assert.equal(calls.filter(address => address === 'unbooked@example.test').length, 1);
  const completed = await getJob(db, 1);
  assert.deepEqual([completed.sent, completed.failed, completed.recipient_rejections, completed.pending], [1, 1, 0, 0]);
  assert.equal(sql.prepare('SELECT COUNT(email) AS n FROM health_event_participant_delivery').get().n, 0);
});

test('a confirmed invalid recipient, an old failure without a reason, and a lost SMTP response stay distinct', async t => {
  const { db, sql } = fixture(t);
  await enqueue(db, 1, 42, draft);
  const job = await enqueue(db, 2, 42, draft);
  sql.prepare("UPDATE health_event_participant_delivery SET status='failed', email=NULL WHERE job_id=?").run(job.id);
  let attempts = 0;
  const smtp = async () => {
    attempts++;
    if (attempts === 1) throw Object.assign(new Error(), { code: 'EENVELOPE', rejectedErrors: [
      { responseCode: 550, response: '550 5.1.1 Mailbox does not exist', command: 'RCPT TO' }
    ] });
    // Nodemailer can report CONN even when a connection breaks after DATA.
    throw Object.assign(new Error(), { code: 'ECONNECTION', command: 'CONN' });
  };
  await processQueue(db, smtp);
  await processQueue(db, smtp);
  assert.equal(attempts, 2);
  const current = await getJob(db, 1);
  assert.deepEqual([current.failed, current.recipient_rejections, current.uncertain, current.pending], [1, 1, 1, 0]);
  const historical = await getJob(db, 2);
  assert.deepEqual([historical.failed, historical.recipient_rejections, historical.sender_paused], [1, 0, false]);
});

test('only specific enhanced SMTP responses identify an invalid mailbox or a daily sender quota', () => {
  const { classifyMailFailure } = require('./healthEventMailFailure');
  assert.equal(classifyMailFailure({ command: 'DATA', responseCode: 550,
    response: '550 5.7.1 Daily SMTP relay sending limit exceeded for this customer' }), 'daily_sender_limit');
  assert.equal(classifyMailFailure({ command: 'DATA', responseCode: 550, response: '550 5.7.1 Blocked by policy' }), 'rejected');
  assert.equal(classifyMailFailure({ command: 'RCPT TO', responseCode: 550, response: '550 Not delivered' }), 'rejected');
  assert.equal(classifyMailFailure({ command: 'RCPT TO', responseCode: 550, response: '550 No such mailbox' }), 'invalid_recipient');
  assert.equal(classifyMailFailure({ command: 'RCPT TO', responseCode: 450, response: '450 4.2.1 Peak SMTP relay limit exceeded' }), 'temporary_sender');
  assert.equal(classifyMailFailure({ command: 'MAIL FROM', responseCode: 550, response: '550 5.1.1 Invalid origin' }), 'rejected');
  assert.equal(classifyMailFailure({ command: 'AUTH', responseCode: 454, response: '454 4.7.0 Try again later' }), 'temporary_sender');
  assert.equal(classifyMailFailure({ command: 'DATA', code: 'ETIMEDOUT' }), 'uncertain');
  assert.equal(classifyMailFailure({ command: 'CONN', code: 'ETIMEDOUT', timeoutType: 'SOCKET_TIMEOUT' }), 'uncertain');
  assert.equal(classifyMailFailure({ command: 'CONN', code: 'ETIMEDOUT', timeoutType: 'GREETING_TIMEOUT' }), 'temporary_sender');
  assert.equal(classifyMailFailure({ command: 'CONN', code: 'ESOCKET', originalCode: 'ECONNREFUSED' }), 'temporary_sender');
  assert.equal(classifyMailFailure({ command: 'CONN', code: 'EDNS' }), 'temporary_sender');
});
