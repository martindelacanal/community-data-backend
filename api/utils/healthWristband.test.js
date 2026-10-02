'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { normalizeWristbandUid, normalizeWristbandSource, assertAssignableRegistration, buildHealthStaffDebounceKey } = require('./healthWristband');
const { SlidingHealthQrDebounce, buildHealthScanDebounceKey } = require('./healthScanGuard');

test('normalizes seven-byte hex UIDs without losing zeroes or reversing bytes', () => {
  assert.equal(normalizeWristbandUid(' 04:00:a2:01:03:ff:09\r\n'), '0400A20103FF09');
  assert.equal(normalizeWristbandUid('04-00-A2-01-03-FF-09'), '0400A20103FF09');
  assert.equal(normalizeWristbandUid('04 00 A2 01 03 FF 09'), '0400A20103FF09');
  assert.equal(normalizeWristbandUid('00000000000000'), '00000000000000');
});

test('rejects malformed, truncated, oversized, URL and legacy RFID identities', () => {
  for (const value of [null, 123, {}, '', '0400A201', '0400A20103FF0900', '0x0400A20103FF09',
    'https://example.com/0400A20103FF09', '0400A20103FF0G', '04/00/A2/01/03/FF/09']) {
    assert.equal(normalizeWristbandUid(value), null);
  }
});

test('only accepts supported transport sources', () => {
  for (const source of ['usb', 'bluetooth', 'nfc_native', 'nfc_web']) assert.equal(normalizeWristbandSource(source), source);
  for (const source of ['ble_sdk', 'qr', null, 3]) assert.equal(normalizeWristbandSource(source), null);
});

test('registration must belong to the event and be active and approved', () => {
  const valid = { health_event_id: 4, registration_role: 'beneficiary', status: 'registered', user_enabled: 'Y', user_deleted: 'N' };
  assert.doesNotThrow(() => assertAssignableRegistration(valid, 4));
  assert.doesNotThrow(() => assertAssignableRegistration({ ...valid, registration_role: 'volunteer' }, 4));
  assert.throws(() => assertAssignableRegistration(valid, 5), { code: 'NOT_REGISTERED', status: 404 });
  for (const invalid of [{ status: 'cancelled' }, { user_enabled: 'N' }, { user_deleted: 'Y' }, { registration_role: 'admin' }]) {
    assert.throws(() => assertAssignableRegistration({ ...valid, ...invalid }, 4), { code: 'PARTICIPANT_NOT_ACTIVE', status: 409 });
  }
});

test('staff held in a reader for more than twenty seconds never toggles or collides with beneficiary scan IDs', () => {
  let clock = 0;
  const guard = new SlidingHealthQrDebounce({ now: () => clock });
  const staffKey = buildHealthStaffDebounceKey(3, 8, 12);
  const beneficiaryKey = buildHealthScanDebounceKey(3, 8, 12);
  guard.remember(staffKey, 4, 'checkin');
  guard.remember(beneficiaryKey, 90, 'checkout');
  for (clock = 6000; clock <= 24000; clock += 6000) {
    assert.deepEqual(guard.take(staffKey), { scanId: 4, scanType: 'checkin' });
  }
  assert.equal(guard.take(beneficiaryKey), null);
  clock = 34000;
  assert.equal(guard.take(staffKey), null, 'A later deliberate tap is accepted after ten seconds without reads.');
});
