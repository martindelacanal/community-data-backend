const { test } = require('node:test');
const assert = require('node:assert/strict');
const jwt = require('jsonwebtoken');
const { buildRestoreAuthBinding } = require('../utils/restoreAuthBinding');
const { renewSession } = require('./beneficiarySession');

const secret = 'test-only-session-secret';
const account = {
  id: 42, firstname: 'Ana', username: 'ana42', email: null, client_id: 1,
  password: 'password-hash', enabled: 'Y', deleted: 'N', reset_password: 'N',
  creation_date: '2026-09-18', role: 'beneficiary', language: 'es',
};
const poolFor = (user = account) => ({ query: async () => [[user].filter(Boolean)] });
function token({ user = account, binding = true, expiresIn = -86400 } = {}) {
  return jwt.sign({
    data: JSON.stringify({ id: user.id, role: user.role }),
    ...(binding ? { restore_auth_binding: buildRestoreAuthBinding(user, secret) } : {}),
  }, secret, { expiresIn });
}

test('beneficiary returns after access expiry and receives a fresh finite token', async () => {
  const result = await renewSession(token(), poolFor(), secret);
  const claims = jwt.verify(result.token, secret);
  assert.equal(JSON.parse(claims.data).language, 'es');
  assert.ok(claims.exp > Date.now() / 1000);
  assert.equal(result.reset_password, 'N');
});

test('password reset, account deletion, disable and role change revoke beneficiary renewal', async () => {
  for (const change of [{ password: 'new-hash' }, { reset_password: 'Y' },
    { deleted: 'Y' }, { enabled: 'N' }, { role: 'admin' }]) {
    await assert.rejects(renewSession(token(), poolFor({ ...account, ...change }), secret), { status: 401 });
  }
  await assert.rejects(renewSession(token(), poolFor(null), secret), { status: 401 });
});

test('legacy tokens upgrade only before expiry', async () => {
  await assert.rejects(renewSession(token({ binding: false }), poolFor(), secret), { status: 401 });
  const result = await renewSession(token({ binding: false, expiresIn: 3600 }), poolFor(), secret);
  assert.equal(jwt.verify(result.token, secret).restore_auth_binding, buildRestoreAuthBinding(account, secret));
});

test('expired staff tokens cannot acquire a persistent session', async () => {
  await assert.rejects(renewSession(token({ user: { ...account, role: 'delivery' } }), poolFor(), secret), { status: 401 });
});

test('invalid signatures are rejected and database outages remain retryable server errors', async () => {
  await assert.rejects(renewSession(token(), poolFor(), 'different-secret'), { status: 401 });
  const unavailable = new Error('Database unavailable');
  await assert.rejects(renewSession(token(), { query: async () => { throw unavailable; } }, secret), unavailable);
});
