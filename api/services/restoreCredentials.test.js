const { test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const jwt = require('jsonwebtoken');
const { isoCBOR } = require('@simplewebauthn/server/helpers');
const { createRestoreCredentialsService } = require('./restoreCredentials');
const { getRestoreCredentialsConfig, fingerprintToOrigin } = require('./restoreCredentialsConfig');
const { buildRestoreAuthBinding } = require('../utils/restoreAuthBinding');

const TEST_ORIGIN = fingerprintToOrigin('11'.repeat(32));
const TEST_SECRET = 'isolated-test-secret-not-a-deployment-credential';
const HASH = (value) => crypto.createHash('sha256').update(value).digest();

// A deliberately isolated repository, with the same transaction boundary as
// MySQL. Nothing in this suite imports app/server, dotenv or the real DB pool.
function fixture() {
  let now = Date.UTC(2026, 7, 27);
  let queue = Promise.resolve();
  const exclusive = (operation) => {
    const pending = queue.then(operation);
    queue = pending.catch(() => {});
    return pending;
  };
  const account = {
    id: 7, firstname: 'Example', username: 'example', email: 'example.invalid',
    password: 'stored-password-hash', client_id: 2, role: 'beneficiary',
    language: 'en', enabled: 'Y', deleted: 'N', reset_password: 'N',
    creation_date: '2025-01-01T00:00:00.000Z',
  };
  const challenges = new Map();
  const credentials = new Map();
  const repository = {
    findAccount: async (id) => id === account.id ? { ...account } : null,
    issueChallenge: async (record) => {
      challenges.set(record.requestId, { ...record, expiresAt: now + record.ttlMs });
    },
    consumeChallenge: (requestId, purpose, userId = null) => exclusive(() => {
      const record = challenges.get(requestId);
      const alreadyUsed = record?.consumed || record?.cancelled;
      if (record?.purpose === 'registration') record.consumed = true;
      else challenges.delete(requestId);
      return record && !alreadyUsed && record.expiresAt > now && record.purpose === purpose && record.userId === userId
        ? record : null;
    }),
    registerCredential: async (record, validate) => {
      if (repository.onBeforeRegister) await repository.onBeforeRegister();
      return exclusive(() => {
        const operation = challenges.get(record.registrationRequestId);
        if (!validate(account) || !operation || !operation.consumed || operation.cancelled
            || operation.expiresAt <= now || operation.userId !== record.userId
            || operation.authBinding !== record.authBinding) return false;
        if (credentials.has(record.credentialId)) throw Object.assign(new Error(), { code: 'ER_DUP_ENTRY' });
        credentials.set(record.credentialId, { ...record });
        return true;
      });
    },
    authenticateCredential: (id, verify) => exclusive(async () => {
      const credential = credentials.get(id);
      if (!credential) return null;
      if (repository.onAuthenticationLock) await repository.onAuthenticationLock();
      const result = await verify({ user: { ...account }, credential });
      credential.counter = result.counter;
      return result.session;
    }),
    revokeCredential: (id, hash) => exclusive(() => {
      if (credentials.get(id)?.revocationHash === hash) credentials.delete(id);
    }),
    cancelRegistration: (requestId) => exclusive(() => {
      const operation = challenges.get(requestId);
      if (operation?.purpose === 'registration') operation.cancelled = true;
      for (const [id, credential] of credentials) {
        if (credential.registrationRequestId === requestId) credentials.delete(id);
      }
    }),
  };
  const config = {
    enabled: true, rpID: 'bienestarcommunity.org', rpName: 'Bienestar Community',
    origins: [TEST_ORIGIN], jwtSecret: TEST_SECRET, challengeTtlMs: 300000,
  };
  const rawService = createRestoreCredentialsService({ repository, config, clock: () => now });
  const sessionBinding = buildRestoreAuthBinding(account, TEST_SECRET);
  return {
    account, config, repository, credentials, challenges,
    rawService, sessionBinding,
    service: {
      ...rawService,
      registrationOptions: (id, binding = sessionBinding) => rawService.registrationOptions(id, binding),
      registrationVerify: (id, body, binding = sessionBinding) => rawService.registrationVerify(id, body, binding),
    },
    advance: (milliseconds) => { now += milliseconds; },
  };
}

function authenticator() {
  const { publicKey, privateKey } = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const jwk = publicKey.export({ format: 'jwk' });
  const publicKeyCose = isoCBOR.encode(new Map([
    [1, 2], [3, -7], [-1, 1],
    [-2, new Uint8Array(Buffer.from(jwk.x, 'base64url'))],
    [-3, new Uint8Array(Buffer.from(jwk.y, 'base64url'))],
  ]));
  return { privateKey, publicKeyCose, id: crypto.randomBytes(32).toString('base64url') };
}

function clientData(options, type, overrides) {
  return Buffer.from(JSON.stringify({
    type, challenge: options.publicKey.challenge, origin: TEST_ORIGIN,
    crossOrigin: false, ...overrides,
  }));
}

function registration(options, key, { rpID = 'bienestarcommunity.org', client = {} } = {}) {
  const id = Buffer.from(key.id, 'base64url');
  const length = Buffer.alloc(2);
  length.writeUInt16BE(id.length);
  const authData = Buffer.concat([
    HASH(rpID), Buffer.from([0x40]), Buffer.alloc(4), // AT, without UP or UV
    Buffer.alloc(16), length, id, Buffer.from(key.publicKeyCose),
  ]);
  return {
    id: key.id, rawId: key.id, type: 'public-key',
    response: {
      clientDataJSON: clientData(options, 'webauthn.create', client).toString('base64url'),
      attestationObject: Buffer.from(isoCBOR.encode(new Map([
        ['fmt', 'none'], ['attStmt', new Map()], ['authData', new Uint8Array(authData)],
      ]))).toString('base64url'),
      transports: ['internal'],
    },
    clientExtensionResults: {},
  };
}

function assertion(options, key, { rpID = 'bienestarcommunity.org', client = {}, counter = 0,
  userHandle = key.userHandle, privateKey = key.privateKey } = {}) {
  const count = Buffer.alloc(4);
  count.writeUInt32BE(counter);
  const authData = Buffer.concat([HASH(rpID), Buffer.from([0]), count]); // Silent Restore: UP=UV=0
  const data = clientData(options, 'webauthn.get', client);
  return {
    id: key.id, rawId: key.id, type: 'public-key',
    response: {
      clientDataJSON: data.toString('base64url'),
      authenticatorData: authData.toString('base64url'),
      signature: crypto.sign('sha256', Buffer.concat([authData, HASH(data)]), privateKey).toString('base64url'),
      userHandle,
    },
    clientExtensionResults: {},
  };
}

async function enroll(f) {
  const key = authenticator();
  const options = await f.service.registrationOptions(7);
  key.userHandle = options.publicKey.user.id;
  const result = await f.service.registrationVerify(7, {
    requestId: options.requestId, credential: registration(options, key),
  });
  return { key, result };
}

const rejected = (operation) => assert.rejects(operation, { status: 401, code: 'restore_verification_failed' });

test('silent Android registration and real signature restore issue fresh compatible JWT claims', async () => {
  const f = fixture();
  const { key, result } = await enroll(f);
  f.account.role = 'client';
  f.account.client_id = 19;
  f.account.language = 'es';
  const options = await f.service.authenticationOptions();
  assert.equal(options.publicKey.userVerification, 'discouraged');
  assert.equal(options.publicKey.allowCredentials, undefined);
  assert.ok(Number.isFinite(Date.parse(options.expiresAt)));
  const session = await f.service.authenticationVerify({
    requestId: options.requestId, credential: assertion(options, key),
    userId: 999, role: 'admin',
  });
  const decoded = jwt.verify(session.token, TEST_SECRET, { algorithms: ['HS256'] });
  assert.deepEqual(JSON.parse(decoded.data), {
    id: 7, firstname: 'Example', username: 'example', email: 'example.invalid',
    client_id: 19, role: 'client', language: 'es', enabled: 'Y',
  });
  assert.equal(decoded.exp - decoded.iat, 21600);
  assert.equal(decoded.restore_auth_binding, buildRestoreAuthBinding(f.account, TEST_SECRET));
  assert.equal(Date.parse(session.expiresAt), decoded.exp * 1000);
  assert.equal(session.reset_password, 'N');
  assert.equal(session.language, 'es');
  assert.equal(session.credentialId, result.credentialId);
  assert.equal(session.revocationToken, result.revocationToken);
  assert.equal(f.credentials.get(key.id).revocationHash, HASH(result.revocationToken).toString('hex'));
});

test('registration options contain an opaque stable user handle, not account PII', async () => {
  const f = fixture();
  const first = await f.service.registrationOptions(7);
  const second = await f.service.registrationOptions(7);
  assert.equal(first.publicKey.user.id, second.publicKey.user.id);
  assert.notEqual(first.requestId, second.requestId);
  assert.notEqual(first.publicKey.challenge, second.publicKey.challenge);
  assert.ok(!JSON.stringify(first).includes(f.account.email));
  assert.equal(first.publicKey.authenticatorSelection.userVerification, 'discouraged');
  await rejected(() => f.service.registrationOptions(999));
});

test('registration binds the challenge to the authenticated selected account', async () => {
  const f = fixture();
  const options = await f.service.registrationOptions(7);
  await rejected(() => f.service.registrationVerify(8, {
    requestId: options.requestId, credential: registration(options, authenticator()),
  }));
  assert.equal(f.credentials.size, 0);
});

test('registration rejects changed account authentication state after options were issued', async () => {
  const f = fixture();
  const options = await f.service.registrationOptions(7);
  f.account.password = 'a-new-password-hash';
  await rejected(() => f.service.registrationVerify(7, {
    requestId: options.requestId, credential: registration(options, authenticator()),
  }));
  assert.equal(f.credentials.size, 0);
});

test('registration rejects wrong origin, RP ID, challenge and ceremony type', async (t) => {
  const cases = [
    { client: { origin: 'https://untrusted.invalid' } },
    { rpID: 'untrusted.invalid' },
    { client: { challenge: crypto.randomBytes(32).toString('base64url') } },
    { client: { type: 'webauthn.get' } },
  ];
  for (const [index, changes] of cases.entries()) {
    await t.test('invalid registration ' + index, async () => {
      const f = fixture();
      const options = await f.service.registrationOptions(7);
      await rejected(() => f.service.registrationVerify(7, {
        requestId: options.requestId, credential: registration(options, authenticator(), changes),
      }));
      assert.equal(f.credentials.size, 0);
    });
  }
});

test('a valid key is still rejected with wrong origin, RP, challenge, type, handle or signature', async (t) => {
  const cases = [
    { client: { origin: 'https://bienestarcommunity.org' } }, // Only Android signing origin is allowed.
    { rpID: 'untrusted.invalid' },
    { client: { challenge: crypto.randomBytes(32).toString('base64url') } },
    { client: { type: 'webauthn.create' } },
    { userHandle: crypto.randomBytes(32).toString('base64url') },
    { privateKey: authenticator().privateKey },
  ];
  for (const [index, changes] of cases.entries()) {
    await t.test('invalid assertion ' + index, async () => {
      const f = fixture();
      const { key } = await enroll(f);
      const options = await f.service.authenticationOptions();
      await rejected(() => f.service.authenticationVerify({
        requestId: options.requestId, credential: assertion(options, key, changes),
      }));
      // Failure also consumes the challenge, so retrying with a corrected
      // signed response does not resurrect the ceremony.
      await rejected(() => f.service.authenticationVerify({
        requestId: options.requestId, credential: assertion(options, key),
      }));
    });
  }
});

test('a challenge can be used once, including two simultaneous signed requests', async () => {
  const f = fixture();
  const { key } = await enroll(f);
  const options = await f.service.authenticationOptions();
  const request = { requestId: options.requestId, credential: assertion(options, key) };
  const results = await Promise.allSettled([
    f.service.authenticationVerify(request), f.service.authenticationVerify(request),
  ]);
  assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1);
  assert.equal(results.filter((r) => r.status === 'rejected' && r.reason.status === 401).length, 1);
});

test('expired challenges and a challenge from another purpose cannot authenticate', async () => {
  const f = fixture();
  const { key } = await enroll(f);
  const expired = await f.service.authenticationOptions();
  f.advance(300000);
  await rejected(() => f.service.authenticationVerify({
    requestId: expired.requestId, credential: assertion(expired, key),
  }));
  const wrongPurpose = await f.service.registrationOptions(7);
  await rejected(() => f.service.authenticationVerify({
    requestId: wrongPurpose.requestId, credential: assertion(wrongPurpose, key),
  }));
});

test('password resets, disable and soft deletion invalidate an enrolled restore key', async (t) => {
  for (const [field, value] of [['password', 'new-password-hash'], ['reset_password', 'Y'], ['enabled', 'N'], ['deleted', 'Y']]) {
    await t.test(field, async () => {
      const f = fixture();
      const { key } = await enroll(f);
      f.account[field] = value;
      const options = await f.service.authenticationOptions();
      await rejected(() => f.service.authenticationVerify({
        requestId: options.requestId, credential: assertion(options, key),
      }));
    });
  }
});

test('enrollment is not available while a password change is mandatory', async () => {
  const f = fixture();
  f.account.reset_password = 'Y';
  await rejected(() => f.service.registrationOptions(7));
});

test('legacy JWTs and JWTs issued before a password reset cannot enroll a new restore key', async () => {
  const f = fixture();
  await rejected(() => f.rawService.registrationOptions(7, undefined));
  f.account.password = 'new-hash-after-reset';
  await rejected(() => f.service.registrationOptions(7));
  const newlyAuthenticatedBinding = buildRestoreAuthBinding(f.account, TEST_SECRET);
  const options = await f.rawService.registrationOptions(7, newlyAuthenticatedBinding);
  await rejected(() => f.rawService.registrationVerify(7, {
    requestId: options.requestId, credential: registration(options, authenticator()),
  }, f.sessionBinding));
});

test('signature counters advance atomically and cannot decrease', async () => {
  const f = fixture();
  const { key } = await enroll(f);
  const first = await f.service.authenticationOptions();
  await f.service.authenticationVerify({ requestId: first.requestId, credential: assertion(first, key, { counter: 1 }) });
  const second = await f.service.authenticationOptions();
  await rejected(() => f.service.authenticationVerify({
    requestId: second.requestId, credential: assertion(second, key, { counter: 1 }),
  }));
  assert.equal(f.credentials.get(key.id).counter, 1);
});

test('revocation capability deletes only its credential and works without JWT', async () => {
  const f = fixture();
  const { key, result } = await enroll(f);
  const other = await enroll(f);
  const wrongToken = crypto.randomBytes(32).toString('base64url');
  assert.deepEqual(await f.service.revoke({ credentialId: key.id, revocationToken: wrongToken }), { revoked: true });
  assert.ok(f.credentials.has(key.id));
  assert.deepEqual(await f.service.revoke(result), { revoked: true });
  assert.deepEqual(await f.service.revoke(result), { revoked: true });
  assert.equal(f.credentials.has(key.id), false);
  assert.ok(f.credentials.has(other.key.id));
  const options = await f.service.authenticationOptions();
  await rejected(() => f.service.authenticationVerify({
    requestId: options.requestId, credential: assertion(options, key),
  }));
});

test('logout racing with an already locked authentication is serialized, never resurrecting the key', async () => {
  const f = fixture();
  const { key, result } = await enroll(f);
  let release;
  let locked;
  const gate = new Promise((resolve) => { release = resolve; });
  const entered = new Promise((resolve) => { locked = resolve; });
  f.repository.onAuthenticationLock = async () => { locked(); await gate; };
  const options = await f.service.authenticationOptions();
  const pendingAuthentication = f.service.authenticationVerify({
    requestId: options.requestId, credential: assertion(options, key),
  });
  await entered;
  const pendingLogout = f.service.revoke(result);
  release();
  assert.ok((await pendingAuthentication).token);
  await pendingLogout;
  assert.equal(f.credentials.has(key.id), false);
  const retry = await f.service.authenticationOptions();
  await rejected(() => f.service.authenticationVerify({
    requestId: retry.requestId, credential: assertion(retry, key),
  }));
});

test('cancellation before create prevents registration and is idempotent', async () => {
  const f = fixture();
  const options = await f.service.registrationOptions(7);
  const capability = { requestId: options.requestId, cancellationToken: options.cancellationToken };
  assert.deepEqual(await f.service.cancelRegistration(capability), { cancelled: true });
  assert.deepEqual(await f.service.cancelRegistration(capability), { cancelled: true });
  await rejected(() => f.service.registrationVerify(7, {
    requestId: options.requestId, credential: registration(options, authenticator()),
  }));
  assert.equal(f.credentials.size, 0);
});

test('cancellation after challenge consumption but before final insert prevents a key from reappearing', async () => {
  const f = fixture();
  const options = await f.service.registrationOptions(7);
  let release;
  let entered;
  const waiting = new Promise((resolve) => { entered = resolve; });
  const gate = new Promise((resolve) => { release = resolve; });
  f.repository.onBeforeRegister = async () => { entered(); await gate; };
  const pending = f.service.registrationVerify(7, {
    requestId: options.requestId, credential: registration(options, authenticator()),
  });
  await waiting;
  await f.service.cancelRegistration({ requestId: options.requestId, cancellationToken: options.cancellationToken });
  release();
  await rejected(() => pending);
  assert.equal(f.credentials.size, 0);
});

test('lost registration response can be cancelled even after challenge expiry and cleanup', async () => {
  const f = fixture();
  const options = await f.service.registrationOptions(7);
  await f.service.registrationVerify(7, {
    requestId: options.requestId, credential: registration(options, authenticator()),
  }); // Deliberately discard the post-commit response.
  f.advance(24 * 60 * 60000);
  f.challenges.clear();
  const other = await enroll(f);
  await f.service.cancelRegistration({ requestId: options.requestId, cancellationToken: options.cancellationToken });
  assert.equal(f.credentials.size, 1);
  assert.ok(f.credentials.has(other.key.id));
});

test('a cancellation token cannot cancel a different registration request', async () => {
  const f = fixture();
  const first = await f.service.registrationOptions(7);
  const other = await f.service.registrationOptions(7);
  await f.service.cancelRegistration({ requestId: first.requestId, cancellationToken: other.cancellationToken });
  const key = authenticator();
  assert.equal((await f.service.registrationVerify(7, {
    requestId: first.requestId, credential: registration(first, key),
  })).credentialId, key.id);
});

test('configuration never allows a debug certificate in a production or unconfigured environment', () => {
  assert.deepEqual(getRestoreCredentialsConfig({ NODE_ENV: 'production', JWT_SECRET: TEST_SECRET }).origins, [
    'android:apk-key-hash:FRtTKyWQgusfzI5bPoivUCSGRDrkQ6nlxowP9EtEdEI',
    'android:apk-key-hash:ammHNJJ1zu3aE0fbzHYrKSXvG08kdO8fkZ1vZXVmdDE',
  ]);
  for (const NODE_ENV of ['production', undefined]) {
    assert.throws(() => getRestoreCredentialsConfig({
      NODE_ENV, JWT_SECRET: TEST_SECRET, RESTORE_CREDENTIALS_DEV_CERT_SHA256: '11'.repeat(32),
      RESTORE_CREDENTIALS_ALLOW_DEV_CERT: 'true',
    }), /development certificates are disabled/);
  }
  const config = getRestoreCredentialsConfig({
    NODE_ENV: 'test', JWT_SECRET: TEST_SECRET, RESTORE_CREDENTIALS_DEV_CERT_SHA256: '11'.repeat(32),
    RESTORE_CREDENTIALS_ALLOW_DEV_CERT: 'true',
  });
  assert.ok(config.origins.includes(TEST_ORIGIN));
});
