'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const express = require('express');
const bcrypt = require('bcryptjs');
const { createPasswordRecoveryHandler } = require('./passwordRecovery');
const { createStaffPasswordRecovery } = require('../services/staffPasswordRecovery');

const account = {
  id: 12, email: 'person@example.test', username: 'participant12', phone: '9515550123',
  dateOfBirth: '1985-07-19', role: 'beneficiary', enabled: 'Y', deleted: 'N',
  password: bcrypt.hashSync('previous synthetic password', 4),
  creation_date: '2020-01-02 03:04:05', reset_password: 'N'
};

async function fixture(t, users = [account], options = {}) {
  const calls = [], writes = [], logs = [], messages = [], hashCalls = [], lastHeaders = {};
  let now = Date.UTC(2026, 10, 2, 12);
  const active = user => user.enabled === 'Y' && user.deleted === 'N';
  const eligible = user => user.role === 'beneficiary' && active(user);
  const pool = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (options.failQuery) throw Object.assign(new Error('private database details'), { code: 'DB_UNAVAILABLE' });
      if (sql.startsWith('SELECT') && sql.includes('AS identifier_match')) {
        assert.match(sql, /u.enabled = 'Y' AND u.deleted = 'N'/);
        assert.match(sql, /u.date_of_birth = \?/);
        assert.match(sql, /r.name AS role/);
        assert.doesNotMatch(sql, /r.name = 'beneficiary'/);
        assert.match(sql, /\(TRIM\(u.email\) = \? OR TRIM\(u.username\) = \? OR u.phone = \?\) AS identifier_match/);
        assert.deepEqual(params.slice(0, 3), [params[0], params[0], params[0]]);
        if (options.afterSelect) queueMicrotask(() => options.afterSelect(users));
        return [users.filter(user => active(user) && user.dateOfBirth === params[3]).map(user => ({
          ...user,
          identifier_match: options.identifierMatch ?? Number(
            user.email?.trim().toLowerCase() === params[0]
            || user.username?.trim().toLowerCase() === params[1] || user.phone === params[2])
        }))];
      }
      if (sql.startsWith('SELECT') && sql.includes('WHERE u.id = ? AND u.date_of_birth = ? LIMIT 1')) {
        return [users.filter(user => user.id === params[0] && user.dateOfBirth === params[1]).map(user => ({ ...user }))];
      }
      if (sql.startsWith('SELECT') && sql.includes('WHERE u.id = ? LIMIT 1')) {
        return [users.filter(user => user.id === params[0]).map(user => ({ ...user }))];
      }
      if (sql.startsWith('SELECT') && sql.includes('TRIM(u.email)')) {
        assert.match(sql, /u.enabled = 'Y' AND u.deleted = 'N'/);
        assert.match(sql, /r.name IN/);
        return [users.filter(user => active(user) && user.role !== 'beneficiary'
          && [user.email, user.username].some(value => value?.trim().toLowerCase() === params[0].toLowerCase()))
          .slice(0, 2).map(user => ({ ...user }))];
      }
      if (sql.includes('BINARY u.password = BINARY ?')) {
        assert.match(sql, /BINARY u.email = BINARY \?/);
        assert.match(sql, /BINARY r.name = BINARY \?/);
        assert.match(sql, /u.creation_date <=> \? AND u.reset_password <=> \?/);
        assert.match(sql, /u.enabled = 'Y' AND u.deleted = 'N'/);
        const user = users.find(user => user.id === params[1] && user.password === params[2]
          && user.email === params[3] && user.role === params[4] && user.creation_date === params[5]
          && user.reset_password === params[6] && active(user));
        if (!user) return [{ affectedRows: 0 }];
        user.password = params[0]; user.reset_password = 'N';
        writes.push({ id: user.id, hash: params[0] });
        return [{ affectedRows: 1 }];
      }
      assert.match(sql, /r.name = 'beneficiary'/);
      assert.match(sql, /u.enabled = 'Y' AND u.deleted = 'N'/);
      assert.match(sql, /u.reset_password = 'Y'/);
      const user = users.find(user => user.id === params[1] && user.dateOfBirth === params[2] && eligible(user));
      if (!user) return [{ affectedRows: 0 }];
      writes.push({ id: user.id, hash: params[0] });
      return [{ affectedRows: 1 }];
    }
  };
  const logger = { error: message => logs.push(message) };
  const staffRecovery = createStaffPasswordRecovery({ pool, secret: 'synthetic-unified-recovery-test-secret',
    sendEmail: async message => { messages.push(message); }, logger, clock: () => now });
  const app = express();
  app.set('trust proxy', 1);
  app.use(express.json());
  app.put('/api/beneficiary/reset-password', createPasswordRecoveryHandler({ pool,
    bcrypt: { async hash(...args) { hashCalls.push(args); return bcrypt.hash(...args); } }, logger,
    staffRecovery, clock: () => now, perIp: options.perIp ?? 30, globalLimit: options.globalLimit ?? 100 }));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  return {
    calls, writes, logs, messages, hashCalls, staffRecovery, lastHeaders, advance: ms => { now += ms; },
    async reset(body = { email: account.email, dateOfBirth: account.dateOfBirth }, ip) {
      const response = await fetch(`http://127.0.0.1:${server.address().port}/api/beneficiary/reset-password`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json', ...(ip ? { 'X-Forwarded-For': ip } : {}) }, body: JSON.stringify(body),
        signal: AbortSignal.timeout(5000)
      });
      Object.assign(lastHeaders, Object.fromEntries(response.headers));
      return { status: response.status, data: await response.json() };
    }
  };
}

test('legacy mobile payload keeps its password response and marks the account for password change', async t => {
  const f = await fixture(t);
  const result = await f.reset();
  assert.deepEqual(result, { status: 200, data: { password: 'bienestar', username: account.username } });
  assert.equal(f.writes.length, 1);
  assert.equal(f.writes[0].id, account.id);
  assert.ok(await bcrypt.compare(result.data.password, f.writes[0].hash));
});

test('preserves database identifier collation and returns the canonical username for subsequent login', async t => {
  const f = await fixture(t, [{ ...account, username: 'José12' }], { identifierMatch: 1 });
  const result = await f.reset({ email: 'Jose12', dateOfBirth: account.dateOfBirth });
  assert.equal(result.status, 200);
  assert.equal(result.data.username, 'José12');
});

test('matches padded/case-varied usernames and emails, and formatted US phone numbers', async t => {
  const f = await fixture(t, [{ ...account, email: ' Person@Example.test ', username: ' Participant12 ', phone: '+1 (951) 555-0123' }]);
  for (const email of [' person@EXAMPLE.TEST ', ' PARTICIPANT12 ', '9515550123', '+1 951-555-0123']) {
    assert.equal((await f.reset({ email, dateOfBirth: account.dateOfBirth })).status, 200, email);
  }
  assert.equal(f.writes.length, 4);
});

test('country prefixes other than US are preserved', async t => {
  const f = await fixture(t, [{ ...account, phone: '+54 (11) 5555-1234' }]);
  assert.equal((await f.reset({ email: '541155551234', dateOfBirth: account.dateOfBirth })).status, 200);
  assert.equal((await f.reset({ email: '1155551234', dateOfBirth: account.dateOfBirth })).status, 401);
});

test('rejects missing, malformed and impossible dates or identifiers before any database access', async t => {
  const f = await fixture(t);
  const invalid = [
    {}, { email: ' ', dateOfBirth: account.dateOfBirth }, { email: {}, dateOfBirth: account.dateOfBirth },
    { email: ['person@example.test'], dateOfBirth: account.dateOfBirth },
    { email: 'x'.repeat(255), dateOfBirth: account.dateOfBirth },
    ...['07/19/1985', '1985-02-29', '2000-02-30', '1985-13-01', '1985-1-01', 'invalid', '2999-01-01', {}, null]
      .map(dateOfBirth => ({ email: account.email, dateOfBirth }))
  ];
  for (const body of invalid) assert.equal((await f.reset(body)).status, 400, JSON.stringify(body));
  assert.equal(f.calls.length, 0);
});

test('wrong birthday, unknown contacts, unrelated digits and ineligible accounts cannot reset', async t => {
  const users = [
    account,
    { ...account, id: 2, email: 'unsupported@example.test', username: 'unsupported', role: 'unsupported' },
    { ...account, id: 3, email: 'disabled@example.test', username: 'disabled', enabled: 'N' },
    { ...account, id: 4, email: 'deleted@example.test', username: 'deleted', deleted: 'Y' }
  ];
  const f = await fixture(t, users);
  for (const email of ['unknown', 'unsupported', 'disabled', 'deleted', '9515550123@example.test', '9515550123ext7']) {
    assert.equal((await f.reset({ email, dateOfBirth: account.dateOfBirth })).status, 401);
  }
  assert.equal((await f.reset({ email: account.email, dateOfBirth: '1985-07-20' })).status, 401);
  assert.equal(f.writes.length, 0);
});

test('the same identifier and birthday form queues staff recovery without hashing or replacing passwords', async t => {
  for (const role of ['admin', 'delivery']) {
    for (const email of [' STAFF@example.test ', ' staff12 ', '+1 (951) 555-0123']) {
      const staff = { ...account, role, username: 'staff12', email: 'staff@example.test' };
      const f = await fixture(t, [staff]);
      assert.deepEqual(await f.reset({ email, dateOfBirth: account.dateOfBirth, language: 'es' }), {
        status: 202, data: { status: 'email_sent' }
      });
      await f.staffRecovery.waitForIdle();
      assert.equal(f.messages.length, 1);
      assert.equal(f.messages[0].email, 'staff@example.test');
      assert.equal(f.messages[0].language, 'es');
      assert.equal(f.hashCalls.length, 0);
      assert.equal(f.writes.length, 0);
      assert.equal(staff.password, account.password);
      assert.equal(f.lastHeaders['cache-control'], 'no-store');
    }
  }
});

test('a queued common-form link completes through the real staff service only with mailbox token possession', async t => {
  const staff = { ...account, role: 'admin', username: 'staff12', email: 'staff@example.test' };
  const f = await fixture(t, [staff]);
  const submitted = await f.reset({ email: staff.username, dateOfBirth: account.dateOfBirth });
  assert.equal(submitted.status, 202);
  assert.equal(submitted.data.password, undefined);
  await f.staffRecovery.waitForIdle();
  assert.equal(f.writes.length, 0);
  const changed = await f.staffRecovery.complete({ token: f.messages[0].token, password: 'new chosen staff password' });
  assert.deepEqual(changed, { status: 'password_changed', username: staff.username });
  assert.equal(f.writes.length, 1);
  assert.ok(await bcrypt.compare('new chosen staff password', staff.password));
  await assert.rejects(f.staffRecovery.complete({ token: f.messages[0].token, password: 'another staff password' }),
    error => error.code === 'invalid_or_expired_token');
});

test('staff birthday mismatches, unsupported account roles and ambiguous cross-role contacts perform no action', async t => {
  const staff = { ...account, id: 13, role: 'admin', username: 'staff13', email: 'staff@example.test' };
  const f = await fixture(t, [staff]);
  assert.equal((await f.reset({ email: staff.email, dateOfBirth: '1985-07-20' })).status, 401);
  const ambiguous = await fixture(t, [account, { ...staff, email: account.email }]);
  assert.equal((await ambiguous.reset()).status, 409);
  assert.equal((await ambiguous.reset({ email: '+1 (951) 555-0123', dateOfBirth: account.dateOfBirth })).status, 409);
  for (const fixtureResult of [f, ambiguous]) {
    await fixtureResult.staffRecovery.waitForIdle();
    assert.equal(fixtureResult.messages.length, 0);
    assert.equal(fixtureResult.writes.length, 0);
    assert.equal(fixtureResult.hashCalls.length, 0);
  }
});

test('staff without a usable registered mailbox receive a support error without password modification', async t => {
  for (const email of [null, '', 'invalid', 'one@example.test,two@example.test']) {
    const f = await fixture(t, [{ ...account, role: 'delivery', email }]);
    assert.deepEqual(await f.reset({ email: account.username, dateOfBirth: account.dateOfBirth }), {
      status: 409, data: { error: 'contact_support' }
    });
    await f.staffRecovery.waitForIdle();
    assert.equal(f.messages.length, 0);
    assert.equal(f.writes.length, 0);
  }
});

test('staff eligibility is rechecked after the common lookup and before issuing mail', async t => {
  for (const change of [{ enabled: 'N' }, { deleted: 'Y' }, { role: 'beneficiary' }, { role: 'unsupported' }, { dateOfBirth: '1985-07-20' }]) {
    const staff = { ...account, role: 'admin' };
    const f = await fixture(t, [staff], { afterSelect: users => Object.assign(users[0], change) });
    assert.equal((await f.reset()).status, 401);
    await f.staffRecovery.waitForIdle();
    assert.equal(f.messages.length, 0);
    assert.equal(f.writes.length, 0);
    assert.equal(f.hashCalls.length, 0);
  }
});

test('caller-supplied userId cannot override the uniquely matched account or mailbox', async t => {
  const matched = { ...account, role: 'admin', email: 'matched@example.test', username: 'matched12' };
  const other = { ...account, id: 99, role: 'delivery', email: 'other@example.test', username: 'other99' };
  const f = await fixture(t, [matched, other]);
  assert.equal((await f.reset({ email: matched.username, dateOfBirth: account.dateOfBirth, userId: other.id,
    id: other.id, recipient: 'attacker@example.test' })).status, 202);
  await f.staffRecovery.waitForIdle();
  assert.equal(f.messages.length, 1);
  assert.equal(f.messages[0].email, matched.email);
  const recheck = f.calls.find(call => call.sql.includes('WHERE u.id = ? AND u.date_of_birth = ?'));
  assert.deepEqual(recheck.params, [matched.id, account.dateOfBirth]);
});

test('common-popup and email-only entry points share the same mailbox quota', async t => {
  const staff = { ...account, role: 'admin' };
  const f = await fixture(t, [staff]);
  await f.staffRecovery.request({ identifier: staff.username });
  for (let i = 0; i < 2; i += 1) assert.equal((await f.reset()).status, 202);
  assert.deepEqual(await f.reset(), { status: 429, data: { error: 'rate_limited' } });
  assert.equal(f.lastHeaders['retry-after'], '3600');
  await f.staffRecovery.waitForIdle();
  assert.equal(f.messages.length, 3);
  assert.equal(f.writes.length, 0);
  assert.equal(f.hashCalls.length, 0);
});

test('per-IP and global rate limits reject requests before database lookup and expire', async t => {
  const ipLimited = await fixture(t, [], { perIp: 1 });
  assert.equal((await ipLimited.reset()).status, 401);
  assert.equal((await ipLimited.reset()).status, 429);
  assert.equal(ipLimited.calls.length, 1);
  assert.equal(ipLimited.lastHeaders['retry-after'], '900');
  assert.equal(ipLimited.lastHeaders['cache-control'], 'no-store');
  ipLimited.advance(15 * 60000);
  assert.equal((await ipLimited.reset()).status, 401);
  const globalLimited = await fixture(t, [], { globalLimit: 1 });
  assert.equal((await globalLimited.reset(undefined, '192.0.2.1')).status, 401);
  assert.equal((await globalLimited.reset(undefined, '192.0.2.2')).status, 429);
  assert.equal(globalLimited.calls.length, 1);
});

test('shared contacts and cross-field collisions do not reset an arbitrary account', async t => {
  const f = await fixture(t, [account, { ...account, id: 13, username: 'participant13', phone: '+1 (951) 555-0123' }]);
  for (const email of [account.email, account.phone]) {
    assert.equal((await f.reset({ email, dateOfBirth: account.dateOfBirth })).status, 409);
  }
  assert.equal(f.writes.length, 0);
  assert.equal((await f.reset({ email: 'participant13', dateOfBirth: account.dateOfBirth })).status, 200);
  assert.deepEqual(f.writes.map(write => write.id), [13]);
  const collision = await fixture(t, [account, { ...account, id: 14, email: 'different@example.test', username: account.email }]);
  assert.equal((await collision.reset()).status, 409);
  assert.equal(collision.writes.length, 0);
});

test('account deactivation between lookup and update prevents reset', async t => {
  const users = [{ ...account }];
  const f = await fixture(t, users, { afterSelect: rows => { rows[0].enabled = 'N'; } });
  assert.equal((await f.reset()).status, 401);
  assert.equal(f.writes.length, 0);
});

test('database failures return a visible failure without leaking contacts or database messages', async t => {
  const f = await fixture(t, [account], { failQuery: true });
  assert.deepEqual(await f.reset(), { status: 500, data: 'Internal server error' });
  assert.deepEqual(f.logs, ['Password recovery failed: DB_UNAVAILABLE']);
  assert.equal(f.writes.length, 0);
});
