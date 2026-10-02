'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const path = require('node:path');
const jwt = require('jsonwebtoken');
const express = require('express');
const { databaseConfig } = require('../../scripts/migrateHealthWristbands');

const RUN = process.env.RUN_HEALTH_WRISTBAND_INTEGRATION === 'development';

test('Health Events wristbands: ACL, concurrent bindings, history, reuse and attendance for both roles', {
  skip: RUN ? false : 'Set RUN_HEALTH_WRISTBAND_INTEGRATION=development after the wristband migration.', timeout: 120000
}, async () => {
  require('dotenv').config({ path: path.resolve(__dirname, '..', '..', '.env') });
  const development = databaseConfig('development');
  const production = databaseConfig('production');
  assert.notEqual(development.host.toLowerCase(), production.host.toLowerCase(), 'Refuse tests when development host equals production host.');
  const expected = { DB_HOST: development.host, DB_USER: development.user, DB_PASSWORD: development.password,
    DB_DATABASE: development.database, DB_PORT: String(development.port) };
  for (const [key, value] of Object.entries(expected)) process.env[key] = value;
  const pool = require('../connection/connection');
  const db = pool.promise();
  const suffix = crypto.randomBytes(8).toString('hex');
  const users = [], events = [];
  let server;
  let failure;
  const cleanupFailures = [];
  try {
    const [[location]] = await db.query('SELECT id FROM location ORDER BY id LIMIT 1');
    assert.ok(location, 'Development database needs a location.');
    const [roleRows] = await db.query("SELECT id, name FROM role WHERE name IN ('admin','beneficiary','eventvolunteer')");
    const roles = Object.fromEntries(roleRows.map(row => [row.name, row.id]));
    assert.ok(roles.admin && roles.beneficiary && roles.eventvolunteer);
    async function user(name, role = 'beneficiary', enabled = 'Y') {
      const [result] = await db.query(
        `INSERT INTO user(username, firstname, lastname, role_id, enabled, deleted, language)
         VALUES (?, ?, 'Synthetic Wristband Test', ?, ?, 'N', 'en')`, [`__wb_${suffix}_${name}`, name, roles[role], enabled]);
      const item = { id: Number(result.insertId), role };
      users.push(item);
      return item;
    }
    const admin = await user('Admin', 'admin');
    const entryOperator = await user('Entry Operator', 'eventvolunteer');
    const otherOperator = await user('Other Event Operator', 'eventvolunteer');
    const pendingOperator = await user('Pending Operator', 'eventvolunteer', 'N');
    const benA = await user('Beneficiary A');
    let syntheticPhone;
    for (let attempt = 0; attempt < 50; attempt++) {
      const candidate = `999${crypto.randomInt(0, 10000000).toString().padStart(7, '0')}`;
      const [[count]] = await db.query("SELECT COUNT(*) AS total FROM user WHERE RIGHT(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone,'-',''),' ',''),'(',''),')',''),'+',''),'.',''),10) = ?", [candidate]);
      if (Number(count.total) === 0) { syntheticPhone = candidate; break; }
    }
    assert.ok(syntheticPhone, 'A unique synthetic phone must be available.');
    await db.query('UPDATE user SET phone = ? WHERE id = ?', [syntheticPhone, benA.id]);
    const benB = await user('Beneficiary B');
    const benOther = await user('Other Event Beneficiary');
    const cancelledBen = await user('Cancelled Beneficiary');
    const staff = await user('Staff Participant', 'eventvolunteer');
    async function event(name) {
      const [result] = await db.query(
        `INSERT INTO health_event(slug,name_en,name_es,location_id,start_date,end_date,timezone,enabled,landing_enabled,created_by_user_id)
         VALUES (?, ?, ?, ?, CURDATE(), DATE_ADD(CURDATE(), INTERVAL 1 DAY), 'America/Los_Angeles', 'Y', 'N', ?)`,
        [`wb-test-${suffix}-${name}`, name, name, location.id, admin.id]);
      const id = Number(result.insertId);
      events.push(id);
      return id;
    }
    const eventA = await event('a'), eventB = await event('b');
    async function stand(eventId, entry) {
      const [result] = await db.query(
        `INSERT INTO health_event_stand(health_event_id,name_en,name_es,is_entry,has_checkout,enabled)
         VALUES (?, 'Wristband test stand', 'Puesto de prueba', ?, 'Y', 'Y')`, [eventId, entry ? 'Y' : 'N']);
      return Number(result.insertId);
    }
    const entry = await stand(eventA, true), booth = await stand(eventA, false), otherEntry = await stand(eventB, true);
    async function register(eventId, participant, status = 'registered') {
      const [result] = await db.query(
        `INSERT INTO health_event_registration(health_event_id,user_id,registration_role,status,source)
         VALUES (?,?,?,?, 'admin')`, [eventId, participant.id, participant.role === 'eventvolunteer' ? 'volunteer' : 'beneficiary', status]);
      return Number(result.insertId);
    }
    const regA = await register(eventA, benA), regB = await register(eventA, benB), regOther = await register(eventB, benOther);
    const regCancelled = await register(eventA, cancelledBen, 'cancelled'), regStaff = await register(eventA, staff);
    await register(eventA, entryOperator);
    await register(eventA, pendingOperator);
    await register(eventB, otherOperator);
    await db.query('INSERT INTO health_event_volunteer_assignment(health_event_id,user_id,stand_id) VALUES (?,?,?), (?,?,?), (?,?,?)',
      [eventA, entryOperator.id, booth, eventB, otherOperator.id, otherEntry, eventA, pendingOperator.id, entry]);
    const [formInsert] = await db.query(
      `INSERT INTO health_event_form(health_event_id,audience,stand_id,title_en,title_es,enabled)
       VALUES (?, 'checkout', ?, 'Checkout Test', 'Salida prueba', 'Y')`, [eventA, booth]);
    const [questionInsert] = await db.query(
      `INSERT INTO health_event_question(form_id,question_type,name_en,name_es,required,enabled)
       VALUES (?, 'text', 'Visit notes', 'Notas', 'N', 'Y')`, [formInsert.insertId]);
    const app = express();
    app.use(express.json());
    app.use('/api', require('./healthEvents'));
    server = await new Promise((resolve, reject) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); s.once('error', reject); });
    const baseUrl = `http://127.0.0.1:${server.address().port}/api/health-events`;
    function token(person) { return jwt.sign({ data: JSON.stringify({ id: person.id, role: person.role }) }, process.env.JWT_SECRET, { expiresIn: '5m' }); }
    async function request(route, person = admin, payload, method = payload === undefined ? 'GET' : 'POST') {
      const response = await fetch(baseUrl + route, { method,
        headers: { ...(person ? { authorization: `Bearer ${token(person)}` } : {}), 'content-type': 'application/json' },
        ...(payload === undefined ? {} : { body: JSON.stringify(payload) }) });
      return { status: response.status, body: await response.json() };
    }
    function ok(result, status = 200, code) { assert.equal(result.status, status, `Unexpected status: ${JSON.stringify(result.body)}`); if (code) assert.equal(result.body.error, code); return result.body; }
    const list = `/${eventA}/wristbands`;
    ok(await request(list, null), 401);
    ok(await request(list, benA), 403);
    ok(await request(list, entryOperator), 403);
    ok(await request(list, otherOperator), 403);
    ok(await request(list, pendingOperator), 403);
    await db.query('UPDATE health_event_volunteer_assignment SET ended_at=NOW() WHERE user_id = ?', [entryOperator.id]);
    await db.query('INSERT INTO health_event_volunteer_assignment(health_event_id,user_id,stand_id) VALUES (?,?,?)', [eventA, entryOperator.id, entry]);
    assert.equal(ok(await request(list, entryOperator)).rows.length, 3);
    assert.ok(ok(await request(`${list}?role=volunteer`, entryOperator)).rows.some(row => row.registration_id === regStaff));
    const uid1 = '04' + crypto.randomBytes(6).toString('hex').toUpperCase();
    const uid2 = '04' + crypto.randomBytes(6).toString('hex').toUpperCase();
    const uid3 = '04' + crypto.randomBytes(6).toString('hex').toUpperCase();
    const uidStaff = '04' + crypto.randomBytes(6).toString('hex').toUpperCase();
    const assign = (reg, uid, extra = {}) => ({ registration_id: reg, uid, source: 'usb', ...extra });
    ok(await request(`${list}/assign`, admin, assign(regA, 'bad')), 400, 'INVALID_WRISTBAND_UID');
    ok(await request(`${list}/assign`, admin, assign(regOther, uid1)), 404, 'NOT_REGISTERED');
    ok(await request(`${list}/assign`, admin, assign(regCancelled, uid1)), 409, 'PARTICIPANT_NOT_ACTIVE');
    const [pendingReg] = await db.query('SELECT id FROM health_event_registration WHERE user_id = ? AND health_event_id = ?', [pendingOperator.id, eventA]);
    ok(await request(`${list}/assign`, admin, assign(pendingReg[0].id, uid1)), 409, 'PARTICIPANT_NOT_ACTIVE');
    const concurrent = await Promise.all(Array.from({ length: 8 }, () => request(`${list}/assign`, entryOperator, assign(regA, uid1))));
    const bindings = concurrent.map(result => ok(result));
    assert.equal(bindings.filter(result => !result.already_assigned).length, 1);
    assert.equal(new Set(bindings.map(result => result.credential.id)).size, 1);
    const credential1 = bindings[0].credential.id;
    ok(await request(`${list}/assign`, admin, assign(regB, uid1)), 409, 'WRISTBAND_IN_USE');
    ok(await request(`/${eventB}/wristbands/assign`, admin, assign(regOther, uid1)), 409, 'WRISTBAND_IN_USE');
    ok(await request(`${list}/assign`, admin, assign(regA, uid2)), 409, 'PARTICIPANT_HAS_WRISTBAND');
    const scanPayload = { event_id: eventA, stand_id: booth, wristband_uid: uid1, wristband_source: 'nfc_native', confirmed: true, allow_repeat: false };
    ok(await request('/scan', entryOperator, scanPayload), 403, 'FORBIDDEN');
    const scans = (await Promise.all(Array.from({ length: 8 }, () => request('/scan', admin, scanPayload)))).map(result => ok(result));
    assert.equal(scans.filter(result => !result.duplicate).length, 1);
    assert.equal(new Set(scans.map(result => result.scan_id)).size, 1);
    assert.equal(scans[0].participant_role, 'beneficiary');
    const checkinId = scans[0].scan_id;
    const { formatDailyQrDate } = require('../utils/dailyBeneficiaryQr');
    const qrPayload = { event_id: eventA, stand_id: booth,
      qr: `B${benA.id}.${formatDailyQrDate(new Date(), 'America/Los_Angeles')}`, confirmed: true };
    const qrReplay = ok(await request('/scan', admin, qrPayload));
    assert.equal(qrReplay.scan_id, checkinId);
    assert.equal(qrReplay.duplicate, true);
    const phoneReplay = ok(await request('/scan', admin, { event_id: eventA, stand_id: booth, phone: syntheticPhone, confirmed: true }));
    assert.equal(phoneReplay.scan_id, checkinId);
    assert.equal(phoneReplay.duplicate, true);
    const { createBeneficiaryPin } = require('../utils/beneficiaryPin');
    const pin = await createBeneficiaryPin(benA.id, location.id);
    const pinReplay = ok(await request('/scan', admin, { event_id: eventA, stand_id: booth, pin: pin.pin, confirmed: true }));
    assert.equal(pinReplay.scan_id, checkinId);
    assert.equal(pinReplay.duplicate, true);
    const [[metadata]] = await db.query('SELECT source,wristband_id FROM health_event_wristband_scan WHERE scan_id = ?', [checkinId]);
    assert.equal(metadata.source, 'nfc_native');
    assert.equal(metadata.wristband_id, credential1);
    // Expire the durable window and wait until the process-local window expires.
    await db.query('UPDATE health_event_scan SET scanned_at=DATE_SUB(NOW(3), INTERVAL 20 SECOND) WHERE id = ?', [checkinId]);
    await new Promise(resolve => setTimeout(resolve, 10500));
    const checkout = ok(await request('/scan', admin, scanPayload));
    assert.equal(checkout.scan_type, 'checkout');
    assert.equal(checkout.checkout_form.id, formInsert.insertId);
    ok(await request(`/scan/${checkout.scan_id}/answers`, admin, { answers: [{ question_id: questionInsert.insertId, answer: 'Synthetic checkout note' }] }));
    const [[savedAnswer]] = await db.query('SELECT answer_text FROM health_event_scan_answer WHERE scan_id = ?', [checkout.scan_id]);
    assert.equal(savedAnswer.answer_text, 'Synthetic checkout note');
    const qrAfterCheckout = ok(await request('/scan', admin, qrPayload));
    assert.equal(qrAfterCheckout.scan_id, checkout.scan_id);
    assert.equal(qrAfterCheckout.scan_type, 'checkout');
    assert.equal(qrAfterCheckout.duplicate, true);
    const swapped = ok(await request(`${list}/assign`, admin, assign(regA, uid2, { replace: true })));
    const [[oldBinding]] = await db.query('SELECT active_uid,release_reason FROM health_event_wristband WHERE id = ?', [credential1]);
    assert.equal(oldBinding.active_uid, null);
    assert.equal(oldBinding.release_reason, 'replaced');
    ok(await request(`${list}/resolve`, admin, { uid: uid1 }), 404, 'WRISTBAND_NOT_ASSIGNED');
    const competing = await Promise.all([request(`${list}/assign`, admin, assign(regB, uid3)), request(`/${eventB}/wristbands/assign`, admin, assign(regOther, uid3))]);
    assert.deepEqual(competing.map(result => result.status).sort(), [200, 409]);
    const competingWinner = competing.find(result => result.status === 200).body.credential;
    const competingEvent = Number(competingWinner.registration_id) === regB ? eventA : eventB;
    ok(await request(`/${competingEvent}/wristbands/${competingWinner.id}/return`, admin, {}));
    const returns = (await Promise.all(Array.from({ length: 5 }, () => request(`${list}/${swapped.credential.id}/return`, entryOperator, {})))).map(result => ok(result));
    assert.equal(returns.filter(result => !result.already_returned).length, 1);
    ok(await request(`${list}/resolve`, admin, { uid: uid2 }), 404, 'WRISTBAND_NOT_ASSIGNED');
    const reused = ok(await request(`/${eventB}/wristbands/assign`, admin, assign(regOther, uid2)));
    assert.ok(reused.credential.id > swapped.credential.id);
    ok(await request(`${list}/resolve`, admin, { uid: uid2 }), 404, 'WRISTBAND_NOT_ASSIGNED');
    const staffBinding = ok(await request(`${list}/assign`, admin, assign(regStaff, uidStaff, { source: 'bluetooth' })));
    ok(await request('/scan', admin, { ...scanPayload, wristband_uid: uidStaff }), 400, 'VOLUNTEER_ENTRY_ONLY');
    const staffPayload = { event_id: eventA, stand_id: entry, wristband_uid: uidStaff, wristband_source: 'bluetooth' };
    const staffScans = (await Promise.all(Array.from({ length: 6 }, () => request('/scan', entryOperator, staffPayload)))).map(result => ok(result));
    assert.equal(staffScans.filter(result => !result.duplicate).length, 1);
    assert.equal(staffScans[0].participant_role, 'volunteer');
    assert.equal(staffScans[0].checkout_form, null);
    const [[staffCount]] = await db.query('SELECT COUNT(*) AS total FROM health_event_scan WHERE registration_id = ?', [regStaff]);
    assert.equal(Number(staffCount.total), 0);
    const staffCheckinId = staffScans[0].scan_id;
    await db.query('UPDATE health_event_staff_attendance SET scanned_at=DATE_SUB(NOW(3), INTERVAL 20 SECOND) WHERE id = ?', [staffCheckinId]);
    const lingeringStaff = ok(await request('/scan', entryOperator, staffPayload));
    assert.equal(lingeringStaff.duplicate, true);
    assert.equal(lingeringStaff.scan_type, 'checkin');
    await new Promise(resolve => setTimeout(resolve, 10500));
    const staffCheckout = ok(await request('/scan', entryOperator, staffPayload));
    assert.equal(staffCheckout.scan_type, 'checkout');
    const [[staffPair]] = await db.query('SELECT paired_scan_id FROM health_event_staff_attendance WHERE id = ?', [staffCheckout.scan_id]);
    assert.equal(Number(staffPair.paired_scan_id), staffCheckinId);
    assert.equal(ok(await request(`${list}/staff-attendance`, admin)).total, 2);
    const staffList = ok(await request(`${list}?role=volunteer`, admin));
    assert.equal(staffList.rows.find(row => row.registration_id === regStaff).staff_attendance.scan_type, 'checkout');
    ok(await request(`/${eventB}/wristbands/${staffBinding.credential.id}/return`, admin, {}), 404, 'WRISTBAND_NOT_FOUND');
    // A disabled entry stand cannot preserve its operator's hardware access.
    await db.query("UPDATE health_event_stand SET enabled='N' WHERE id = ?", [entry]);
    ok(await request(list, entryOperator), 403, 'FORBIDDEN');
  } catch (error) {
    failure = error;
  } finally {
    if (server) await new Promise(resolve => server.close(resolve));
    for (const eventId of events) {
      try {
        await db.query('UPDATE health_event_scan SET paired_scan_id=NULL WHERE health_event_id = ?', [eventId]);
        await db.query('DELETE FROM health_event_scan WHERE health_event_id = ?', [eventId]);
        await db.query('DELETE FROM health_event_staff_attendance WHERE health_event_id = ?', [eventId]);
        await db.query('DELETE FROM health_event WHERE id = ? AND slug LIKE ?', [eventId, `wb-test-${suffix}-%`]);
      } catch (error) { cleanupFailures.push(error); }
    }
    for (const person of users) {
      try {
        await db.query('DELETE FROM beneficiary_delivery_pin WHERE user_id = ?', [person.id]);
        await db.query('DELETE FROM user WHERE id = ? AND username LIKE ?', [person.id, `__wb_${suffix}_%`]);
      }
      catch (error) { cleanupFailures.push(error); }
    }
    if (!cleanupFailures.length) {
      const [[leftovers]] = await db.query('SELECT COUNT(*) AS total FROM user WHERE username LIKE ?', [`__wb_${suffix}_%`]);
      assert.equal(Number(leftovers.total), 0, 'Synthetic users must be fully removed.');
    }
    await db.end();
  }
  if (cleanupFailures.length) throw new AggregateError([...(failure ? [failure] : []), ...cleanupFailures], 'Wristband fixture cleanup failed.');
  if (failure) throw failure;
});
