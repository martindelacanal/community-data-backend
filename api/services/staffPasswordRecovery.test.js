'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { createStaffPasswordRecovery, createRecoveryLimiter } = require('./staffPasswordRecovery');

const SECRET = 'synthetic-staff-recovery-test-secret-never-used-in-production';
const KEY = crypto.createHmac('sha256', SECRET).update('staff-password-reset-signing-v1').digest();
const ROLES = ['admin', 'client', 'stocker', 'delivery', 'opsmanager', 'director', 'auditor', 'contentmanager', 'eventvolunteer'];
const OLD_HASH = bcrypt.hashSync('previous-synthetic-password', 4);
const baseUser = { id: 7, username: 'staff7', email: 'staff@example.test', password: OLD_HASH,
  enabled: 'Y', deleted: 'N', role: 'admin', creation_date: '2020-01-02 03:04:05', reset_password: 'Y',
  dateOfBirth: '1985-07-19' };

function fixture(changes = {}, options = {}) {
  let now = Date.UTC(2026, 10, 2, 12);
  const users = (options.users || [{ ...baseUser, ...changes }]).map(user => ({ ...user }));
  const queries = [], writes = [], messages = [], logs = [];
  const pool = { async query(sql, params) {
    queries.push({ sql, params });
    if (sql.startsWith('SELECT') && /TRIM\(u.email\)/.test(sql)) {
      assert.match(sql, /TRIM\(u.email\) = \? OR TRIM\(u.username\) = \?/);
      assert.match(sql, /u.enabled = 'Y' AND u.deleted = 'N'/);
      assert.match(sql, /LIMIT 2/);
      for (const role of ROLES) assert.ok(sql.includes(`'${role}'`));
      const normalized = params[0].trim().toLowerCase();
      assert.equal(params[1], params[0]);
      await options.beforeLookupReturn?.(normalized);
      return [users.filter(user => user.enabled === 'Y' && user.deleted === 'N' && ROLES.includes(user.role)
        && [user.email, user.username].some(value => typeof value === 'string' && value.trim().toLowerCase() === normalized))
        .slice(0, 2).map(user => ({ ...user }))];
    }
    if (sql.startsWith('SELECT')) {
      assert.match(sql, /WHERE u.id = \?(?: AND u.date_of_birth = \?)? LIMIT 1/);
      await options.beforeAccountLookupReturn?.(params[0]);
      return [users.filter(user => user.id === params[0]
        && (!sql.includes('AND u.date_of_birth = ?') || user.dateOfBirth === params[1])).map(user => ({ ...user }))];
    }
    assert.match(sql, /SET u.password = \?, u.reset_password = 'N'/);
    assert.match(sql, /BINARY u.password = BINARY \?/);
    assert.match(sql, /BINARY u.email = BINARY \?/);
    assert.match(sql, /BINARY r.name = BINARY \?/);
    assert.match(sql, /u.enabled = 'Y' AND u.deleted = 'N'/);
    assert.match(sql, /u.creation_date <=> \? AND u.reset_password <=> \?/);
    const user = users.find(user => user.id === params[1]);
    options.beforeUpdate?.(user);
    if (!user || user.password !== params[2] || user.email !== params[3] || user.role !== params[4]
      || user.creation_date !== params[5] || user.reset_password !== params[6]
      || user.enabled !== 'Y' || user.deleted !== 'N') return [{ affectedRows: 0 }];
    user.password = params[0]; user.reset_password = 'N';
    writes.push({ id: user.id, password: params[0] });
    return [{ affectedRows: 1 }];
  } };
  const service = createStaffPasswordRecovery({
    pool, secret: options.secret === undefined ? SECRET : options.secret,
    clock: () => now, logger: { error: value => logs.push(value) },
    sendEmail: options.sendEmail || (async message => { messages.push(message); }),
    ...(options.maxPending !== undefined ? { maxPending: options.maxPending } : {})
  });
  return { service, users, queries, writes, messages, logs, advance: ms => { now += ms; },
    async issue(identifier = users[0].username, language = 'en') {
      assert.deepEqual(await service.request({ identifier, language }), { status: 'accepted' });
      await service.waitForIdle();
      return messages.at(-1)?.token;
    }
  };
}

async function rejected(service, body, code = 'invalid_or_expired_token') {
  await assert.rejects(service.complete(body), error => error.code === code && error.status === 400);
}

test('admin and delivery can choose a new password only after receiving their registered-email token', async () => {
  for (const role of ['admin', 'delivery']) {
    const f = fixture({ role, email: ' Registered@Example.test ', username: ' staff7 ' });
    const token = await f.issue('staff7', 'es');
    assert.equal(f.writes.length, 0);
    assert.equal(f.users[0].password, OLD_HASH);
    assert.deepEqual(Object.keys(f.messages[0]).sort(), ['email', 'language', 'token']);
    assert.equal(f.messages[0].email, 'Registered@Example.test');
    assert.equal(f.messages[0].language, 'es');
    const claims = jwt.verify(token, KEY, { clockTimestamp: Date.UTC(2026, 10, 2, 12) / 1000 });
    assert.equal(claims.exp - claims.iat, 1200);
    assert.equal(claims.password, undefined);
    assert.equal(claims.data, undefined);
    const result = await f.service.complete({ token, password: 'New chosen password 42' });
    assert.deepEqual(result, { status: 'password_changed', username: ' staff7 ' });
    assert.equal(f.writes.length, 1);
    assert.equal(f.users[0].reset_password, 'N');
    assert.ok(await bcrypt.compare('New chosen password 42', f.users[0].password));
    assert.equal(await bcrypt.compare('previous-synthetic-password', f.users[0].password), false);
  }
});

test('unknown, ambiguous, disabled, deleted, beneficiary and unusable-email accounts get the same response without mail', async () => {
  const cases = [
    { users: [] }, { users: [{ ...baseUser }, { ...baseUser, id: 8 }] },
    ...[{ enabled: 'N' }, { deleted: 'Y' }, { role: 'beneficiary' }, { role: 'unknown' },
      { email: null }, { email: '' }, { email: 'no-at-sign' }, { email: 'one@example.test,two@example.test' },
      { email: 'one@example.test\r\nBcc: two@example.test' }].map(change => ({ users: [{ ...baseUser, ...change }] }))
  ];
  for (const options of cases) {
    const f = fixture({}, options);
    assert.deepEqual(await f.service.request({ identifier: 'staff7' }), { status: 'accepted' });
    await f.service.waitForIdle();
    assert.equal(f.messages.length, 0);
    assert.equal(f.writes.length, 0);
  }
});

test('email matching preserves the registered destination and cannot redirect mail using request fields', async () => {
  const f = fixture({ email: 'Staff@Example.test' });
  await f.service.request({ identifier: ' STAFF@example.test ', email: 'attacker@example.test', language: 'unexpected' });
  await f.service.waitForIdle();
  assert.equal(f.messages[0].email, 'Staff@Example.test');
  assert.equal(f.messages[0].language, 'en');
});

test('invalid identifiers and missing signing configuration fail without database work', async () => {
  const f = fixture();
  for (const identifier of [null, {}, [], '', ' ', 'x'.repeat(255)]) {
    await assert.rejects(f.service.request({ identifier }), error => error.code === 'invalid_request');
  }
  assert.equal(f.queries.length, 0);
  const unavailable = fixture({}, { secret: '' });
  await assert.rejects(unavailable.service.request({ identifier: 'staff7' }), error => error.status === 503);
  assert.equal(unavailable.queries.length, 0);
});

test('tokens are single use and concurrent completions have exactly one winner', async () => {
  const f = fixture();
  const token = await f.issue();
  const results = await Promise.allSettled([
    f.service.complete({ token, password: 'first chosen password' }),
    f.service.complete({ token, password: 'second chosen password' })
  ]);
  assert.equal(results.filter(result => result.status === 'fulfilled').length, 1);
  assert.equal(results.filter(result => result.status === 'rejected' && result.reason.code === 'invalid_or_expired_token').length, 1);
  assert.equal(f.writes.length, 1);
  await rejected(f.service, { token, password: 'third chosen password' });
});

test('a successful reset invalidates every other issued link for the account', async () => {
  const f = fixture();
  const first = await f.issue();
  const second = await f.issue();
  assert.notEqual(first, second);
  await f.service.complete({ token: first, password: 'new password' });
  await rejected(f.service, { token: second, password: 'another password' });
});

test('expired, tampered, access-session, wrong-algorithm and malformed claim tokens cannot change a password', async () => {
  const f = fixture();
  const token = await f.issue();
  const claims = jwt.decode(token);
  const forge = (change, algorithm = 'HS256') => jwt.sign({ ...claims, ...change }, KEY, { algorithm });
  // Encode directly so malformed registered claim types are exercised even
  // though jsonwebtoken.sign itself refuses to issue these tokens.
  const forgeClaimTypes = change => {
    const head = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const payload = Buffer.from(JSON.stringify({ ...claims, ...change })).toString('base64url');
    const content = `${head}.${payload}`;
    return `${content}.${crypto.createHmac('sha256', KEY).update(content).digest('base64url')}`;
  };
  const noExpiry = { ...claims }; delete noExpiry.exp;
  const noIssued = { ...claims }; delete noIssued.iat;
  const invalid = [
    token.slice(0, -6) + 'abcdef', '', null, 'x'.repeat(4097),
    jwt.sign({ data: JSON.stringify({ id: 7, role: 'admin' }) }, SECRET, { expiresIn: '1h' }),
    forge({}, 'HS384'), forge({ purpose: 'login' }), forge({ aud: 'different' }), forge({ iss: 'different' }),
    forge({ sub: '0' }), forge({ sub: '-1' }), forge({ sub: '1e2' }), forge({ sub: '9007199254740992' }),
    forgeClaimTypes({ sub: 7 }), forgeClaimTypes({ sub: ['7'] }), forgeClaimTypes({ sub: null }),
    forge({ jti: 'not-a-random-id' }), forge({ binding: 'invalid' }),
    forge({ iat: claims.iat + 60, exp: claims.exp + 60 }), forge({ exp: claims.iat + 1201 }),
    forge({ exp: claims.iat }), jwt.sign(noExpiry, KEY), jwt.sign(noIssued, KEY, { noTimestamp: true })
  ];
  for (const invalidToken of invalid) await rejected(f.service, { token: invalidToken, password: 'new password' });
  f.advance(20 * 60 * 1000);
  await rejected(f.service, { token, password: 'new password' });
  assert.equal(f.writes.length, 0);
});

test('changes to password, email, role or account state revoke outstanding tokens', async () => {
  for (const change of [
    { password: 'changed-hash' }, { email: 'other@example.test' }, { email: 'STAFF@example.test' },
    { role: 'delivery' }, { enabled: 'N' }, { deleted: 'Y' }, { reset_password: 'N' },
    { creation_date: '2021-01-02 03:04:05' }
  ]) {
    const f = fixture(); const token = await f.issue(); Object.assign(f.users[0], change);
    await rejected(f.service, { token, password: 'new password' });
    assert.equal(f.writes.length, 0);
  }
});

test('changes during password hashing are rejected by the atomic binary/state guards', async () => {
  for (const change of [
    { password: OLD_HASH.toUpperCase() }, { email: 'STAFF@example.test' }, { role: 'delivery' },
    { enabled: 'N' }, { deleted: 'Y' }, { reset_password: 'N' }, { creation_date: '2021-01-01 00:00:00' }
  ]) {
    const f = fixture({}, { beforeUpdate: user => Object.assign(user, change) });
    const token = await f.issue();
    await rejected(f.service, { token, password: 'new password' });
    assert.equal(f.writes.length, 0);
  }
});

test('password length is measured in Unicode characters and never silently truncated by bcrypt', async () => {
  const invalid = [undefined, null, {}, '1234567', '😀'.repeat(7), 'a'.repeat(73), '😀'.repeat(19)];
  const f = fixture(); const token = await f.issue();
  for (const password of invalid) await rejected(f.service, { token, password }, 'invalid_password');
  assert.equal(f.writes.length, 0);
  for (const password of ['😀'.repeat(8), '😀'.repeat(18), 'a'.repeat(72), '  padded password  ']) {
    const candidate = fixture(); const candidateToken = await candidate.issue();
    await candidate.service.complete({ token: candidateToken, password });
    assert.ok(await bcrypt.compare(password, candidate.users[0].password));
  }
});

test('concurrent requests reserve mailbox limits before email starts and the window expires', async () => {
  const f = fixture();
  const responses = await Promise.all(Array.from({ length: 8 }, () => f.service.request({ identifier: 'staff7' })));
  responses.forEach(response => assert.deepEqual(response, { status: 'accepted' }));
  await f.service.waitForIdle();
  assert.equal(f.messages.length, 3);
  f.advance(60 * 60 * 1000);
  await f.issue(); assert.equal(f.messages.length, 4);
});

test('SMTP work does not delay the accepted response and its failure logs no recipient or token', async () => {
  let rejectMail;
  const f = fixture({}, { sendEmail: () => new Promise((resolve, reject) => { rejectMail = reject; }) });
  assert.deepEqual(await f.service.request({ identifier: 'staff7' }), { status: 'accepted' });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(typeof rejectMail, 'function');
  rejectMail(Object.assign(new Error('recipient staff@example.test secret-token smtp details'), { code: 'ESMTP' }));
  await f.service.waitForIdle();
  assert.deepEqual(f.logs, ['Staff password recovery email failed: ESMTP']);
  assert.equal(f.writes.length, 0);
});

test('bounded pending email work fails safely for all identifiers while full', async () => {
  let release;
  const f = fixture({}, { maxPending: 1, sendEmail: () => new Promise(resolve => { release = resolve; }) });
  await f.service.request({ identifier: 'staff7' });
  await new Promise(resolve => setImmediate(resolve));
  for (const identifier of ['staff7', 'unknown']) {
    await assert.rejects(f.service.request({ identifier }), error => error.status === 503 && error.code === 'unavailable');
  }
  release(); await f.service.waitForIdle();
});

test('queue saturation during concurrent lookups does not disclose whether the second account exists', async () => {
  for (const secondIdentifier of ['staff8', 'unknown']) {
    const releaseLookup = new Map();
    let releaseMail;
    let sent = 0;
    const f = fixture({}, {
      maxPending: 1,
      users: [baseUser, { ...baseUser, id: 8, username: 'staff8', email: 'other@example.test' }],
      beforeLookupReturn: identifier => new Promise(resolve => releaseLookup.set(identifier, resolve)),
      sendEmail: () => { sent += 1; return new Promise(resolve => { releaseMail = resolve; }); }
    });
    const first = f.service.request({ identifier: 'staff7' });
    const second = f.service.request({ identifier: secondIdentifier });
    assert.equal(releaseLookup.size, 2, 'both lookups begin while the queue is empty');
    releaseLookup.get('staff7')();
    assert.deepEqual(await first, { status: 'accepted' });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(sent, 1, 'first request occupies the sole mail slot');
    releaseLookup.get(secondIdentifier)();
    assert.deepEqual(await second, { status: 'accepted' });
    assert.equal(sent, 1, 'second request does not exceed mail capacity');
    releaseMail();
    await f.service.waitForIdle();
  }
});

test('limiter bounds key storage, does not evict active limits and reuses expired capacity', () => {
  let now = 1000;
  const limiter = createRecoveryLimiter({ clock: () => now, maxEntries: 2 });
  assert.equal(limiter.take('first', 2, 100), true);
  assert.equal(limiter.take('first', 2, 100), true);
  assert.equal(limiter.take('first', 2, 100), false);
  assert.equal(limiter.take('second', 1, 200), true);
  assert.equal(limiter.take('third', 1, 100), false);
  now += 100;
  assert.equal(limiter.take('third', 1, 100), true);
  assert.equal(limiter.take('second', 1, 200), false);
});

test('internal common-form entry rechecks ID, birthday and active staff eligibility before sending', async () => {
  const f = fixture();
  assert.deepEqual(await f.service.requestForAccount({ userId: 7, dateOfBirth: baseUser.dateOfBirth, language: 'es' }), { status: 'accepted' });
  await f.service.waitForIdle();
  assert.equal(f.messages.length, 1);
  assert.equal(f.messages[0].email, baseUser.email);
  assert.equal(f.messages[0].language, 'es');
  assert.equal(f.writes.length, 0);
  assert.deepEqual(f.queries[0].params, [7, baseUser.dateOfBirth]);
  for (const changes of [{ enabled: 'N' }, { deleted: 'Y' }, { role: 'beneficiary' }, { role: 'unsupported' }, { dateOfBirth: '1985-07-20' }]) {
    const invalid = fixture(changes);
    await assert.rejects(invalid.service.requestForAccount({ userId: 7, dateOfBirth: baseUser.dateOfBirth }), error => error.status === 401);
    await invalid.service.waitForIdle();
    assert.equal(invalid.messages.length, 0);
    assert.equal(invalid.writes.length, 0);
  }
});

test('internal common-form entry rejects malformed identifiers and reports unusable mailboxes to support', async () => {
  const f = fixture();
  for (const input of [{ userId: '7', dateOfBirth: baseUser.dateOfBirth }, { userId: 0, dateOfBirth: baseUser.dateOfBirth },
    { userId: 7, dateOfBirth: '07/19/1985' }, { userId: 7, dateOfBirth: null }]) {
    await assert.rejects(f.service.requestForAccount(input), error => error.code === 'invalid_request');
  }
  assert.equal(f.queries.length, 0);
  for (const email of [null, '', 'invalid', 'one@example.test,two@example.test']) {
    const invalid = fixture({ email });
    await assert.rejects(invalid.service.requestForAccount({ userId: 7, dateOfBirth: baseUser.dateOfBirth }),
      error => error.status === 409 && error.code === 'contact_support');
    await invalid.service.waitForIdle();
    assert.equal(invalid.messages.length, 0);
  }
});

test('email-only and internal account request methods reserve one shared mailbox quota under concurrency', async () => {
  const f = fixture();
  const results = await Promise.allSettled([
    ...Array.from({ length: 3 }, () => f.service.request({ identifier: 'staff7' })),
    ...Array.from({ length: 3 }, () => f.service.requestForAccount({ userId: 7, dateOfBirth: baseUser.dateOfBirth }))
  ]);
  // Public calls retain a generic response, but the common form must report
  // that no new email was queued once its verified mailbox has reached quota.
  results.slice(0, 3).forEach(result => assert.deepEqual(result, { status: 'fulfilled', value: { status: 'accepted' } }));
  results.slice(3).forEach(result => {
    assert.equal(result.status, 'rejected');
    assert.equal(result.reason.code, 'rate_limited');
    assert.equal(result.reason.status, 429);
  });
  await f.service.waitForIdle();
  assert.equal(f.messages.length, 3);
  assert.equal(f.writes.length, 0);
});

test('internal queue saturation after lookup reports unavailable without consuming mailbox quota', async () => {
  let delayLookups = true;
  let releasePublicLookup, releaseInternalLookup, releaseFirstMail;
  const sent = [];
  const f = fixture({}, {
    maxPending: 1,
    users: [baseUser, { ...baseUser, id: 8, username: 'staff8', email: 'other@example.test' }],
    beforeLookupReturn: () => delayLookups ? new Promise(resolve => { releasePublicLookup = resolve; }) : undefined,
    beforeAccountLookupReturn: () => delayLookups ? new Promise(resolve => { releaseInternalLookup = resolve; }) : undefined,
    sendEmail: message => {
      sent.push(message);
      if (sent.length === 1) return new Promise(resolve => { releaseFirstMail = resolve; });
      return Promise.resolve();
    }
  });
  const first = f.service.request({ identifier: 'staff7' });
  const second = f.service.requestForAccount({ userId: 8, dateOfBirth: baseUser.dateOfBirth });
  assert.equal(typeof releasePublicLookup, 'function');
  assert.equal(typeof releaseInternalLookup, 'function');
  releasePublicLookup();
  assert.deepEqual(await first, { status: 'accepted' });
  await new Promise(resolve => setImmediate(resolve));
  const unavailable = assert.rejects(second, error => error.status === 503 && error.code === 'unavailable');
  releaseInternalLookup();
  await unavailable;
  delayLookups = false;
  releaseFirstMail();
  await f.service.waitForIdle();
  for (let i = 0; i < 3; i += 1) {
    assert.deepEqual(await f.service.requestForAccount({ userId: 8, dateOfBirth: baseUser.dateOfBirth }), { status: 'accepted' });
    await f.service.waitForIdle();
  }
  assert.equal(sent.filter(message => message.email === 'other@example.test').length, 3);
  await assert.rejects(f.service.requestForAccount({ userId: 8, dateOfBirth: baseUser.dateOfBirth }),
    error => error.status === 429 && error.code === 'rate_limited');
});
