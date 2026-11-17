'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { manageReservation, readReservationToken, buildReservationUrl } = require('./healthEventReservation');
const { saveParticipantRegistration } = require('./healthEventParticipantRegistration');
const { processDueConfirmations } = require('./healthEventConfirmation');
const { buildHealthBeneficiaryConfirmationContent } = require('../email/email');

const secret = 'synthetic-only-clinic-test-secret';
const claims = { id: 7, version: 1 };

test('reservation links are scoped to one registration version and do not put the token in the HTTP request', () => {
  const url = new URL(buildReservationUrl({ id: 7, reservation_version: 1 }, 'es', secret));
  assert.equal(url.origin + url.pathname, 'https://api.bienestarcommunity.org/api/health-events/reservation/open');
  assert.equal(url.search, '?lang=es');
  const token = url.hash.slice(1);
  assert.deepEqual(readReservationToken(token, secret), claims);
  for (const invalid of [token.replace('7.1.', '8.1.'), token.replace('7.1.', '7.2.'), {}, '']) {
    assert.equal(readReservationToken(invalid, secret), null);
  }
  assert.equal(readReservationToken(token, 'another-secret'), null);
  assert.match(buildHealthBeneficiaryConfirmationContent({ language: 'es', eventNameEs: 'Clínica', manageUrl: url.toString() }).text, /Ver o cancelar mi reserva/);
  assert.ok(buildHealthBeneficiaryConfirmationContent({ language: 'en', eventNameEn: 'Clinic', manageUrl: url.toString() }).html.includes(url.toString()));
});

function reservationDatabase({ checkedIn = false, failCancel = false } = {}) {
  const registration = { id: 7, user_id: 13, health_event_id: 5, status: 'registered', reservation_version: 1 };
  const slot = { status: 'booked', service_key: 'dental', slot_date: '2099-12-05', start_time: '10:00', end_time: '10:30' };
  let snapshot;
  let locks = 0;
  const db = {
    async beginTransaction() { snapshot = [registration.status, slot.status]; },
    async commit() {},
    async rollback() { [registration.status, slot.status] = snapshot; },
    release() {},
    async query(sql, args) {
      if (sql.startsWith('SELECT id, health_event_id')) {
        if (sql.includes('FOR UPDATE')) locks++;
        return [[...(args[0] === registration.id && args[1] === registration.reservation_version ? [{ ...registration }] : [])]];
      }
      if (sql.includes('FROM health_event e JOIN location')) return [[{ name_es: 'Clínica', name_en: 'Clinic', enabled: 'Y', firstname: 'Ana' }]];
      if (sql.includes('FROM health_event_scan')) return [checkedIn ? [{ id: 1 }] : []];
      if (sql.includes('FROM health_event_registration_date')) return [[{ event_date: '2099-12-05', priority_service: 'dental' }]];
      if (sql.includes('FROM health_event_appointment')) return [[{ ...slot }]];
      if (sql.startsWith('UPDATE health_event_appointment')) { slot.status = 'cancelled'; return [{}]; }
      if (sql.startsWith('UPDATE health_event_registration')) {
        if (failCancel) throw new Error('fixture DB error');
        registration.status = 'cancelled'; return [{}];
      }
      throw new Error('Unexpected query ' + sql);
    }
  };
  return { pool: { getConnection: async () => db }, registration, slot, get locks() { return locks; } };
}

test('viewing preserves the booking and cancellation atomically frees the slot, including retries', async () => {
  const f = reservationDatabase();
  const view = await manageReservation(f.pool, claims, false, () => false);
  assert.equal(view.can_cancel, true);
  assert.equal(view.participant_firstname, 'Ana');
  assert.equal(view.appointments[0].start_time, '10:00');
  assert.deepEqual(view.dates, [{ event_date: '2099-12-05' }]);
  assert.equal(f.slot.status, 'booked');
  for (let i = 0; i < 2; i++) {
    const result = await manageReservation(f.pool, claims, true, () => false);
    assert.equal(result.status, 'cancelled');
    assert.deepEqual(result.appointments, []);
    assert.equal(result.can_cancel, false);
  }
  assert.equal(f.slot.status, 'cancelled');
  assert.equal(f.registration.status, 'cancelled');
  assert.equal(f.locks, 2);
  await assert.rejects(manageReservation(f.pool, { ...claims, version: 2 }, true, () => false), { code: 'INVALID_RESERVATION' });
});

test('expired and checked-in reservations cannot be cancelled, and failed writes roll back', async () => {
  for (const checkedIn of [true, false]) {
    const f = reservationDatabase({ checkedIn });
    await assert.rejects(manageReservation(f.pool, claims, true, () => !checkedIn), { code: 'CANCELLATION_CLOSED' });
    assert.equal(f.slot.status, 'booked');
  }
  const f = reservationDatabase({ failCancel: true });
  await assert.rejects(manageReservation(f.pool, claims, true, () => false), /fixture DB error/);
  assert.equal(f.slot.status, 'booked');
  assert.equal(f.registration.status, 'registered');
});

function registrationDatabase(email, existing, previousContact = null) {
  const queries = [];
  return {
    queries,
    async query(sql, args) {
      queries.push({ sql, args });
      if (sql.includes('FROM user WHERE')) return [[{ email, language: 'es' }]];
      if (sql.startsWith('SELECT id, status')) return [existing ? [existing] : []];
      if (sql.startsWith('SELECT CASE')) return [[{ contact_email: previousContact }]];
      if (sql.startsWith('INSERT INTO health_event_registration(')) return [{ insertId: 7 }];
      return [{}];
    }
  };
}

test('confirmation is queued only once for the participant’s own address or their explicitly shared contact', async () => {
  for (const [account, submitted, expected] of [
    ['self@example.org', null, 'self@example.org'], [null, 'family@example.org', 'family@example.org'], [null, null, null],
    [null, 'a@example.org,b@example.org', null]
  ]) {
    const db = registrationDatabase(account);
    const result = await saveParticipantRegistration(db, 5, 13, { email: submitted });
    assert.equal(result.confirmation_email, expected ? 'queued' : 'no_email');
    assert.equal(db.queries.find(q => q.sql.startsWith('INSERT INTO health_event_registration(')).args[2], expected);
    assert.equal(db.queries.filter(q => q.sql.includes('INSERT INTO health_event_confirmation')).length, expected ? 1 : 0);
  }
  const db = registrationDatabase('self@example.org', { id: 7, status: 'registered', reservation_version: 1 });
  assert.equal((await saveParticipantRegistration(db, 5, 13)).confirmation_email, 'not_requested');
  assert.equal(db.queries.length, 2);
});

test('authenticated rebooking increments the version and stale form updates cannot restore a cancelled registration', async () => {
  const db = registrationDatabase('self@example.org', { id: 7, status: 'cancelled', reservation_version: 1 });
  await assert.rejects(saveParticipantRegistration(db, 5, 13, { previousVersion: 1 }), { code: 'REGISTRATION_CHANGED' });
  assert.deepEqual(await saveParticipantRegistration(db, 5, 13), { registration_id: 7, already_registered: false, confirmation_email: 'queued' });
  assert.equal(db.queries.find(q => q.sql.includes('SET status =')).args[0], 2);
  assert.deepEqual(db.queries.find(q => q.sql.includes('INSERT INTO health_event_confirmation')).args, [7, 2, 'es', 0]);
  const answerDeletes = db.queries.filter(q => q.sql.startsWith('DELETE') && q.sql.includes('health_event_answer'));
  assert.equal(answerDeletes.length, 1);
  assert.match(answerDeletes[0].sql, /q\.maps_to IN \('attend_date','priority_service'\) OR q\.question_type = 'appointment'/);
});

test('a rebooked participant without an account email keeps an explicitly shared contact and skips an ambiguous legacy contact', async () => {
  for (const contact of ['family@example.org', null]) {
    const db = registrationDatabase(null, { id: 7, status: 'cancelled', reservation_version: 1, contact_email: 'family@example.org' }, contact);
    const result = await saveParticipantRegistration(db, 5, 13);
    assert.equal(result.confirmation_email, contact ? 'queued' : 'no_email');
    assert.equal(db.queries.find(q => q.sql.includes('SET status =')).args[1], contact);
  }
});

test('queue sends individualized bilingual details, retries temporary failures and skips cancelled or already claimed mail', async t => {
  const previousSecret = process.env.JWT_SECRET;
  process.env.JWT_SECRET = secret;
  t.after(() => { if (previousSecret == null) delete process.env.JWT_SECRET; else process.env.JWT_SECRET = previousSecret; });
  const job = { registration_id: 7, reservation_version: 1, language: 'es', include_default_login: 0, attempts: 0 };
  let canClaim = true;
  let active = true;
  let finalState;
  const db = { async query(sql, args) {
    if (sql.startsWith('SELECT registration_id')) return [[job]];
    if (sql.includes('SET claim_id = ?')) return [{ affectedRows: canClaim ? 1 : 0 }];
    if (sql.includes('FROM health_event_registration r JOIN user')) return [active ? [{ id: 7, reservation_version: 1, contact_email: 'self@example.org', name_es: 'Clínica' }] : []];
    if (sql.includes('FROM health_event_appointment') || sql.includes('FROM health_event_registration_date')) return [[]];
    if (sql.includes('SET state = ?')) { finalState = args[0]; return [{}]; }
    throw new Error('Unexpected query ' + sql);
  } };
  const logger = { error() {} };
  await processDueConfirmations(db, async mail => {
    assert.equal(mail.to, 'self@example.org');
    assert.equal(mail.language, 'es');
    assert.equal(mail.credentials, null);
    assert.match(mail.manageUrl, /lang=es#7\.1\./);
  }, logger);
  assert.equal(finalState, 'sent');
  await processDueConfirmations(db, async () => { throw new Error('transient'); }, logger);
  assert.equal(finalState, 'queued');
  active = false;
  await processDueConfirmations(db, () => assert.fail(), logger);
  assert.equal(finalState, 'skipped');
  canClaim = false;
  finalState = null;
  await processDueConfirmations(db, () => assert.fail(), logger);
  assert.equal(finalState, null);
});
