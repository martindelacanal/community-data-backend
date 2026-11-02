'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const crypto = require('node:crypto');
const express = require('express');
const { createStaffPasswordRecoveryRouter } = require('./staffPasswordRecovery');
const { RecoveryError } = require('../services/staffPasswordRecovery');

async function fixture(t, options = {}) {
  const calls = [], logs = [];
  const service = options.service || {
    async request(body) { calls.push({ operation: 'request', body }); return { status: 'accepted' }; },
    async complete(body) { calls.push({ operation: 'complete', body }); return { status: 'password_changed', username: 'staff1' }; }
  };
  const app = express();
  // Production already installs a less restrictive parser globally.
  app.use(express.json({ limit: '100kb' }));
  app.use('/api/auth/password-reset', createStaffPasswordRecoveryRouter({ ...options, service,
    logger: { error: message => logs.push(message) } }));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const url = `http://127.0.0.1:${server.address().port}/api/auth/password-reset`;
  return { calls, logs,
    get: () => fetch(url + '/open'),
    post: (operation, body) => fetch(url + '/' + operation, { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  };
}

test('public staff recovery APIs work without a session and return no credentials from the request', async t => {
  const f = await fixture(t);
  const requested = await f.post('request', { identifier: 'staff1', language: 'es' });
  assert.equal(requested.status, 202);
  assert.deepEqual(await requested.json(), { status: 'accepted' });
  assert.equal(requested.headers.get('cache-control'), 'no-store');
  assert.equal(requested.headers.get('referrer-policy'), 'no-referrer');
  const completed = await f.post('complete', { token: 'synthetic-token', password: 'NewPassword9' });
  assert.equal(completed.status, 200);
  assert.deepEqual(await completed.json(), { status: 'password_changed', username: 'staff1' });
  assert.deepEqual(f.calls.map(call => call.operation), ['request', 'complete']);
});

test('enforces public rate limits before service/database/password hashing and permits later retry', async t => {
  let now = 0;
  const f = await fixture(t, { perIp: 2, clock: () => now });
  assert.equal((await f.post('request', { identifier: 'first' })).status, 202);
  assert.equal((await f.post('complete', { token: 'token', password: 'NewPassword9' })).status, 200);
  const rejected = await f.post('request', { identifier: 'third' });
  assert.equal(rejected.status, 429);
  assert.equal(rejected.headers.get('retry-after'), '900');
  assert.deepEqual(await rejected.json(), { error: 'rate_limited' });
  assert.equal(f.calls.length, 2);
  now = 15 * 60000;
  assert.equal((await f.post('request', { identifier: 'later' })).status, 202);
});

test('global request ceiling is enforced independently of IP quota', async t => {
  const f = await fixture(t, { globalLimit: 1, perIp: 100 });
  assert.equal((await f.post('request', { identifier: 'first' })).status, 202);
  assert.equal((await f.post('complete', { token: 'token', password: 'NewPassword9' })).status, 429);
  assert.equal(f.calls.length, 1);
});

test('rejects malformed or oversized bodies even after the global parser', async t => {
  const f = await fixture(t);
  for (const body of [[], { identifier: 'x'.repeat(9000) }]) {
    const response = await f.post('request', body);
    assert.equal(response.status, 400);
    assert.deepEqual(await response.json(), { error: 'invalid_request' });
  }
  assert.equal(f.calls.length, 0);
});

test('returns actionable public errors without logging tokens, identities or server exception messages', async t => {
  const f = await fixture(t, { service: {
    async request() { throw Object.assign(new Error('private account and token'), { code: 'DB_UNAVAILABLE' }); },
    async complete() { throw new RecoveryError('invalid_or_expired_token'); }
  } });
  const unavailable = await f.post('request', { identifier: 'staff1' });
  assert.equal(unavailable.status, 503);
  assert.deepEqual(await unavailable.json(), { error: 'unavailable' });
  const invalid = await f.post('complete', { token: 'synthetic-token', password: 'NewPassword9' });
  assert.equal(invalid.status, 400);
  assert.deepEqual(await invalid.json(), { error: 'invalid_or_expired_token' });
  assert.deepEqual(f.logs, ['Staff password recovery failed: DB_UNAVAILABLE']);
});

test('GET opens the browser form without consuming a token and permits only its exact inline code', async t => {
  const f = await fixture(t);
  const response = await f.get();
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /text\/html/);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal(response.headers.get('referrer-policy'), 'no-referrer');
  assert.equal(response.headers.get('x-frame-options'), 'DENY');
  const csp = response.headers.get('content-security-policy');
  assert.match(csp, /default-src 'none'/);
  assert.match(csp, /frame-ancestors 'none'/);
  assert.match(csp, /connect-src 'self'/);
  assert.doesNotMatch(csp, /unsafe-inline|unsafe-eval/);
  const html = await response.text();
  for (const tag of ['script', 'style']) {
    const blocks = [...html.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'g'))];
    assert.equal(blocks.length, 1);
    assert.ok(csp.includes('sha256-' + crypto.createHash('sha256').update(blocks[0][1]).digest('base64')));
  }
  assert.equal(f.calls.length, 0);
});
