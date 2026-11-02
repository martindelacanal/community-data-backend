'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const express = require('express');
const bcrypt = require('bcryptjs');
const { createPasswordRecoveryHandler } = require('./passwordRecovery');

const account = {
  id: 12, email: 'person@example.test', username: 'participant12', phone: '9515550123',
  dateOfBirth: '1985-07-19', role: 'beneficiary', enabled: 'Y', deleted: 'N'
};

async function fixture(t, users = [account], options = {}) {
  const calls = [], writes = [], logs = [];
  const eligible = user => user.role === 'beneficiary' && user.enabled === 'Y' && user.deleted === 'N';
  const pool = {
    async query(sql, params) {
      calls.push({ sql, params });
      assert.match(sql, /r.name = 'beneficiary'/);
      assert.match(sql, /u.enabled = 'Y' AND u.deleted = 'N'/);
      assert.match(sql, /u.date_of_birth = \?/);
      if (options.failQuery) throw Object.assign(new Error('private database details'), { code: 'DB_UNAVAILABLE' });
      if (sql.startsWith('SELECT')) {
        assert.match(sql, /\(TRIM\(u.email\) = \? OR TRIM\(u.username\) = \? OR u.phone = \?\) AS identifier_match/);
        assert.deepEqual(params.slice(0, 3), [params[0], params[0], params[0]]);
        if (options.afterSelect) queueMicrotask(() => options.afterSelect(users));
        return [users.filter(user => eligible(user) && user.dateOfBirth === params[3]).map(user => ({
          ...user,
          identifier_match: options.identifierMatch ?? Number(
            user.email?.trim().toLowerCase() === params[0]
            || user.username?.trim().toLowerCase() === params[1] || user.phone === params[2])
        }))];
      }
      assert.match(sql, /u.reset_password = 'Y'/);
      const user = users.find(user => user.id === params[1] && user.dateOfBirth === params[2] && eligible(user));
      if (!user) return [{ affectedRows: 0 }];
      writes.push({ id: user.id, hash: params[0] });
      return [{ affectedRows: 1 }];
    }
  };
  const app = express();
  app.use(express.json());
  app.put('/api/beneficiary/reset-password', createPasswordRecoveryHandler({ pool, bcrypt, logger: { error: message => logs.push(message) } }));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  return {
    calls, writes, logs,
    async reset(body = { email: account.email, dateOfBirth: account.dateOfBirth }) {
      const response = await fetch(`http://127.0.0.1:${server.address().port}/api/beneficiary/reset-password`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
        signal: AbortSignal.timeout(5000)
      });
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
    { ...account, id: 2, email: 'admin@example.test', username: 'admin', role: 'admin' },
    { ...account, id: 3, email: 'disabled@example.test', username: 'disabled', enabled: 'N' },
    { ...account, id: 4, email: 'deleted@example.test', username: 'deleted', deleted: 'Y' }
  ];
  const f = await fixture(t, users);
  for (const email of ['unknown', 'admin', 'disabled', 'deleted', '9515550123@example.test', '9515550123ext7']) {
    assert.equal((await f.reset({ email, dateOfBirth: account.dateOfBirth })).status, 401);
  }
  assert.equal((await f.reset({ email: account.email, dateOfBirth: '1985-07-20' })).status, 401);
  assert.equal(f.writes.length, 0);
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
