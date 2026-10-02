'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { recordStaffAttendance, lockActiveWristband, withTransaction } = require('./healthEventWristbands');

test('staff duplicate replays its attendance and never writes a beneficiary visit', async () => {
  const sqls = [];
  const connection = { query: async sql => { sqls.push(sql); return [[{ id: 17, scan_type: 'checkin' }]]; } };
  const result = await recordStaffAttendance(connection, { eventId: 1, registration: { id: 9 }, credential: { id: 6 } });
  assert.deepEqual(result, { scan_id: 17, scan_type: 'checkin', duplicate: true, duplicate_reason: 'recent' });
  assert.equal(sqls.length, 1);
  assert.ok(sqls.every(sql => !sql.includes('health_event_scan')));
});

test('staff open checkin closes in a separate table with timezone and pairing', async () => {
  const calls = [];
  const replies = [[[]], [[{ id: 21, scan_type: 'checkin' }]], [{ insertId: 22 }]];
  const connection = { query: async (sql, params) => { calls.push({ sql, params }); return replies.shift(); } };
  const result = await recordStaffAttendance(connection, { eventId: 3, registration: { id: 9 }, credential: { id: 6 },
    standId: 2, operatorId: 5, source: 'usb', timezone: 'America/Los_Angeles' });
  assert.equal(result.scan_type, 'checkout');
  assert.deepEqual(calls[1].params, [9, 3, 'America/Los_Angeles', 'America/Los_Angeles']);
  assert.deepEqual(calls[2].params, [3, 9, 6, 2, 5, 'checkout', 21, 'usb']);
  assert.match(calls[2].sql, /INSERT INTO health_event_staff_attendance/);
});

test('scan rejects a binding that was returned or replaced after resolution', async () => {
  const connection = { query: async () => [[]] };
  await assert.rejects(() => lockActiveWristband(connection, 1, 2, 3), { code: 'WRISTBAND_NOT_ASSIGNED', status: 404 });
});

test('transaction rolls back a conflict and always releases its connection', async () => {
  const steps = [];
  const connection = { beginTransaction: async () => steps.push('begin'), commit: async () => steps.push('commit'),
    rollback: async () => steps.push('rollback'), release: () => steps.push('release') };
  const pool = { getConnection: async () => connection };
  await assert.rejects(() => withTransaction(pool, async () => { throw Object.assign(new Error('duplicate'), { code: 'ER_DUP_ENTRY' }); }),
    { code: 'WRISTBAND_IN_USE', status: 409 });
  assert.deepEqual(steps, ['begin', 'rollback', 'release']);
});

test('deadlock retry has bounded attempts and commits only the successful attempt', async () => {
  let attempts = 0, commits = 0, releases = 0, rollbacks = 0;
  const connection = { beginTransaction: async () => {}, commit: async () => commits++,
    rollback: async () => rollbacks++, release: () => releases++ };
  const result = await withTransaction({ getConnection: async () => connection }, async () => {
    if (++attempts < 3) throw Object.assign(new Error('deadlock'), { code: 'ER_LOCK_DEADLOCK' });
    return 'bound';
  });
  assert.equal(result, 'bound');
  assert.deepEqual({ attempts, commits, releases, rollbacks }, { attempts: 3, commits: 1, releases: 3, rollbacks: 2 });
});
