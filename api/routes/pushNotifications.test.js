'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { once } = require('node:events');
const express = require('express');
const jwt = require('jsonwebtoken');

const source = fs.readFileSync(path.join(__dirname, 'pushNotifications.js'), 'utf8');
const SECRET = 'synthetic-push-notification-test-secret';
const copy = value => JSON.parse(JSON.stringify(value));

function device(id, language = 'en', extra = {}) {
  return { id, installation_id: `installation-${id}`, push_token: `token-${id}`,
    platform: 'android', device_language: language, audience_user_id: id, ...extra };
}

async function serve(t, options = {}) {
  const calls = { apps: 0, app: 0, certificates: [], initialized: [], messaging: [], messages: [],
    queries: [], logs: [], releases: 0, connections: 0 };
  const firebaseApp = { name: 'synthetic-firebase-app' };
  const certificate = { synthetic: 'certificate' };
  const devices = options.devices || [device(1)];
  const connection = {
    async query(sql, params = []) {
      calls.queries.push({ sql, params: copy(params) });
      if (/FROM push_notifications\s+WHERE id/.test(sql)) return [[{
        id: 7, audience_type: 'all', message_en: 'Synthetic English message', message_es: 'Mensaje de prueba',
        filters_json: '{}', filters_chip_json: '[]', image_s3_key: null,
      }]];
      if (/COUNT\(DISTINCT push_token\)/.test(sql)) return [[{ total: 999 }]];
      if (/FROM push_device_tokens d/.test(sql)) return [devices];
      if (/INSERT INTO push_notification_executions/.test(sql)) return [{ insertId: 71 }];
      if (/INSERT INTO push_notification_recipients/.test(sql)) {
        if (options.inboxFailure) throw new Error('Synthetic inbox failure');
        return [{ affectedRows: 1 }];
      }
      if (/UPDATE push_device_tokens/.test(sql) || /UPDATE push_notifications SET last_executed_at/.test(sql)) {
        return [{ affectedRows: 1 }];
      }
      throw new Error(`Unexpected SQL in isolated push test: ${sql}`);
    },
    release() { calls.releases++; },
  };
  const firebase = {
    getApps() { calls.apps++; return options.existingApp === false ? [] : [firebaseApp]; },
    getApp() { calls.app++; return firebaseApp; },
    cert(input) { calls.certificates.push(copy(input)); return certificate; },
    initializeApp(input) { calls.initialized.push(input); assert.equal(input.credential, certificate); return firebaseApp; },
  };
  const messaging = {
    getMessaging(app) {
      assert.equal(app, firebaseApp, 'the modular messaging API must use the selected app');
      calls.messaging.push(app);
      return { async sendEachForMulticast(message) {
        calls.messages.push(copy(message));
        if (options.sendError) throw options.sendError;
        return { responses: message.tokens.map(token => options.results?.[token] || { success: true }) };
      } };
    },
  };
  class S3Client {
    send() { assert.fail('A push execution without an image must never contact S3'); }
  }
  class Command {}
  const mocks = {
    express, jsonwebtoken: jwt, multer: require('multer'), crypto: require('node:crypto'), path,
    'firebase-admin/app': firebase, 'firebase-admin/messaging': messaging,
    '@aws-sdk/client-s3': { S3Client, PutObjectCommand: Command, DeleteObjectCommand: Command, GetObjectCommand: Command },
    '@aws-sdk/s3-request-presigner': { getSignedUrl() { assert.fail('Unexpected image URL signing'); } },
    '../connection/connection': { promise: () => ({ async getConnection() { calls.connections++; return connection; } }) },
    '../utils/logger.js': { error: (...args) => calls.logs.push(args) },
    '../utils/pushNotificationAccess': require('../utils/pushNotificationAccess'),
  };
  // Evaluate the complete production router with an allowlist of dependencies.
  // Never import its real DB, Firebase, AWS or logger initializers.
  const context = vm.createContext({
    module: { exports: {} }, process: { env: { JWT_SECRET: SECRET, ...options.env } },
    require(name) {
      assert.ok(Object.hasOwn(mocks, name), `Unmocked dependency: ${name}`);
      return mocks[name];
    },
  });
  vm.runInContext(source, context, { filename: 'pushNotifications.js' });
  const app = express();
  app.use(express.json());
  app.use(context.module.exports);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  return { calls, async execute({ role = 'admin', authenticated = true } = {}) {
    const token = jwt.sign({ data: JSON.stringify({ id: 42, role }) }, SECRET, { expiresIn: '1h' });
    const response = await fetch(`http://127.0.0.1:${server.address().port}/push-notifications/7/execute`, {
      method: 'POST', headers: authenticated ? { Authorization: `Bearer ${token}` } : {},
      signal: AbortSignal.timeout(5000),
    });
    return { status: response.status, body: await response.json() };
  } };
}

test('push execution reuses the existing Firebase app without reading credentials or initializing again', async t => {
  const f = await serve(t);
  const response = await f.execute();
  assert.equal(response.status, 200);
  assert.equal(response.body.successful_devices, 1);
  assert.equal(f.calls.app, 1);
  assert.equal(f.calls.certificates.length, 0);
  assert.equal(f.calls.initialized.length, 0);
  assert.equal(f.calls.messaging.length, 1);
  assert.equal(f.calls.releases, 1);
});

test('push execution initializes modular Firebase from environment credentials and normalizes escaped newlines', async t => {
  const f = await serve(t, { existingApp: false, env: {
    FIREBASE_PROJECT_ID: 'synthetic-project', FIREBASE_CLIENT_EMAIL: 'synthetic@example.invalid',
    FIREBASE_PRIVATE_KEY: 'synthetic-line-one\\nsynthetic-line-two\\n',
  } });
  assert.equal((await f.execute()).status, 200);
  assert.deepEqual(f.calls.certificates, [{ projectId: 'synthetic-project', clientEmail: 'synthetic@example.invalid',
    privateKey: 'synthetic-line-one\nsynthetic-line-two\n' }]);
  assert.equal(f.calls.initialized.length, 1);
  assert.equal(f.calls.app, 0);
});

test('missing Firebase credentials return the stable error and release the connection before sending or recording', async t => {
  const f = await serve(t, { existingApp: false });
  assert.deepEqual(await f.execute(), { status: 500, body: {
    error: 'Internal Server Error', message: 'Could not execute push notification',
  } });
  assert.equal(f.calls.messages.length, 0);
  assert.equal(f.calls.initialized.length, 0);
  assert.equal(f.calls.queries.length, 1);
  assert.equal(f.calls.releases, 1);
});

test('modular multicast preserves language groups, 500-token batches, deduplication and invalid-token accounting', async t => {
  const devices = Array.from({ length: 501 }, (_, i) => device(i + 1));
  devices.push(device(600, 'es'), device(601, 'en', { user_language: 'es' }));
  devices.push(device(9000, 'en', { push_token: 'token-1', audience_user_id: 1 }));
  const f = await serve(t, { devices, results: {
    'token-1': { success: false, error: { code: 'messaging/registration-token-not-registered', message: 'Expired synthetic token' } },
    'token-600': { success: false, error: { code: 'messaging/internal-error', message: 'Synthetic temporary failure' } },
  } });
  assert.deepEqual(await f.execute(), { status: 200, body: {
    notification_id: 7, execution_id: 71, total_registered_devices: 999,
    targeted_devices: 503, successful_devices: 501, failed_devices: 2, invalidated_devices: 1,
  } });
  assert.deepEqual(f.calls.messages.map(message => message.tokens.length), [500, 1, 2]);
  assert.deepEqual(f.calls.messages.map(message => message.data.language), ['en', 'en', 'es']);
  assert.deepEqual(f.calls.messages.map(message => message.notification.body),
    ['Synthetic English message', 'Synthetic English message', 'Mensaje de prueba']);
  assert.equal(new Set(f.calls.messages.flatMap(message => message.tokens)).size, 503);
  const invalidation = f.calls.queries.find(query => /UPDATE push_device_tokens/.test(query.sql));
  assert.deepEqual(invalidation.params, [9000], 'invalidate the latest device row, and only for a permanent token error');
  const execution = f.calls.queries.find(query => /INSERT INTO push_notification_executions/.test(query.sql));
  assert.deepEqual(execution.params.slice(0, 8), [7, 42, 'all', 999, 503, 501, 2, 1]);
  const result = JSON.parse(execution.params[8]);
  assert.deepEqual(result.invalidated_device_ids, [9000]);
  assert.deepEqual(result.error_samples.map(error => error.device_id), [9000, 600]);
  assert.equal(f.calls.releases, 1);
});

test('Firebase send failures do not record a completed execution or expose provider details', async t => {
  const f = await serve(t, { sendError: new Error('Synthetic private provider diagnostic') });
  assert.deepEqual(await f.execute(), { status: 500, body: {
    error: 'Internal Server Error', message: 'Could not execute push notification',
  } });
  assert.equal(f.calls.messages.length, 1);
  assert.equal(f.calls.queries.some(query => /INSERT|UPDATE/.test(query.sql)), false);
  assert.equal(f.calls.releases, 1);
});

test('inbox persistence failures preserve completed delivery accounting', async t => {
  const f = await serve(t, { inboxFailure: true });
  assert.equal((await f.execute()).status, 200);
  assert.equal(f.calls.messages.length, 1);
  assert.ok(f.calls.logs.some(args => args[0] === 'Error recording push notification recipients:'));
  assert.ok(f.calls.queries.some(query => /UPDATE push_notifications SET last_executed_at/.test(query.sql)));
  assert.equal(f.calls.releases, 1);
});

test('push execution rejects unauthenticated and non-manager requests before external work', async t => {
  const f = await serve(t);
  assert.equal((await f.execute({ authenticated: false })).status, 401);
  assert.equal((await f.execute({ role: 'beneficiary' })).status, 403);
  assert.equal(f.calls.connections, 0);
  assert.equal(f.calls.apps, 0);
  assert.equal(f.calls.messages.length, 0);
});
