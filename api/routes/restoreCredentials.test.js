const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const express = require('express');
const jwt = require('jsonwebtoken');
const { createRestoreCredentialsRouter, createRateLimiter } = require('./restoreCredentials');

const SECRET = 'isolated-router-test-secret-no-real-credentials';
const BINDING = 'b'.repeat(64);

async function serve(t, { service, limiterOptions, globalParser = false, logger } = {}) {
  const calls = [];
  const stub = service || Object.fromEntries([
    'registrationOptions', 'registrationVerify', 'authenticationOptions',
    'authenticationVerify', 'revoke', 'cancelRegistration',
  ].map((name) => [name, async (...args) => {
    calls.push({ name, args });
    return { ok: true };
  }]));
  const app = express();
  if (globalParser) app.use(express.json({ limit: '100kb' }));
  app.use('/api/auth/restore', createRestoreCredentialsRouter({
    env: { JWT_SECRET: SECRET }, service: stub, limiterOptions, logger,
  }));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve) => {
    server.closeAllConnections();
    server.close(resolve);
  }));
  const url = 'http://127.0.0.1:' + server.address().port + '/api/auth/restore';
  return {
    calls,
    post: (path, body = {}, token) => fetch(url + path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) },
      body: JSON.stringify(body),
    }),
  };
}

function token(options = {}) {
  return jwt.sign({
    data: JSON.stringify({ id: 7, role: 'beneficiary' }), restore_auth_binding: BINDING,
  }, SECRET, { algorithm: 'HS256', expiresIn: '6h', ...options });
}

test('enrollment requires a valid current JWT and takes identity only from signed claims', async (t) => {
  const f = await serve(t);
  assert.equal((await f.post('/registration/options')).status, 401);
  assert.equal((await f.post('/registration/options', {}, token({ expiresIn: -1 }))).status, 401);
  assert.equal((await f.post('/registration/options', {}, token({ algorithm: 'HS384' }))).status, 401);
  const response = await f.post('/registration/options', { userId: 99, role: 'admin' }, token());
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.deepEqual(f.calls, [{ name: 'registrationOptions', args: [7, BINDING] }]);
});

test('anonymous options do not receive account selectors and revoke does not need a JWT', async (t) => {
  const f = await serve(t);
  assert.equal((await f.post('/authentication/options', { userId: 99, email: 'ignored.invalid' })).status, 200);
  assert.equal((await f.post('/revoke', { credentialId: 'id', revocationToken: 'capability' })).status, 200);
  assert.equal((await f.post('/registration/cancel', { requestId: 'request', cancellationToken: 'capability' })).status, 200);
  assert.deepEqual(f.calls, [
    { name: 'authenticationOptions', args: [] },
    { name: 'revoke', args: [{ credentialId: 'id', revocationToken: 'capability' }] },
    { name: 'cancelRegistration', args: [{ requestId: 'request', cancellationToken: 'capability' }] },
  ]);
});

test('request-size limits apply before and after the existing global JSON parser', async (t) => {
  for (const globalParser of [false, true]) {
    await t.test('globalParser=' + globalParser, async (context) => {
      const f = await serve(context, { globalParser });
      const response = await f.post('/authentication/verify', { credential: 'x'.repeat(33000) });
      assert.equal(response.status, 413);
      assert.deepEqual(await response.json(), { error: 'invalid_restore_request' });
      assert.equal(f.calls.length, 0);
    });
  }
});

test('public request rate limiting rejects excess requests with a retry interval', async (t) => {
  const f = await serve(t, { limiterOptions: { perIp: 2 } });
  assert.equal((await f.post('/authentication/options')).status, 200);
  assert.equal((await f.post('/authentication/options')).status, 200);
  const response = await f.post('/authentication/options');
  assert.equal(response.status, 429);
  assert.equal(response.headers.get('retry-after'), '60');
  assert.equal(f.calls.length, 2);
});

test('rate limiter bounds storage and expires entries without persistent timers', () => {
  let now = 0;
  const limiter = createRateLimiter({ maxEntries: 2, clock: () => now });
  assert.equal(limiter.request('first'), true);
  assert.equal(limiter.request('second'), false);
  now = 60000;
  assert.equal(limiter.request('second'), true);
});

test('unexpected errors expose neither WebAuthn data nor DB detail', async (t) => {
  const logs = [];
  const f = await serve(t, {
    service: {
      authenticationVerify: async () => { throw new Error('private assertion and SQL parameters'); },
    },
    logger: { error: (...args) => logs.push(args) },
  });
  const response = await f.post('/authentication/verify', { requestId: 'private request' });
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: 'restore_unavailable' });
  assert.deepEqual(logs, [['Android Restore request failed', { code: 'RESTORE_BACKEND_ERROR' }]]);
});
