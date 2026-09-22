'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { once } = require('node:events');
const express = require('express');
const jwt = require('jsonwebtoken');
const { createObjectCsvStringifier } = require('csv-writer');

// Execute the real handlers and JWT middleware without opening the production
// database or importing the monolithic router's S3/email/background services.
const source = fs.readFileSync(path.join(__dirname, 'user.js'), 'utf8').replace(/\r\n/g, '\n');
const secret = 'isolated-volunteer-route-test-secret';
const protectedRoutes = [
  ['post', '/table/volunteer'],
  ['post', '/table/volunteer/download-csv'],
  ['get', '/view/volunteer/:idVolunteer'],
  ['get', '/view/volunteer/images/:idVolunteer'],
  ['get', '/volunteer/notification-recipients'],
  ['put', '/volunteer/notification-recipients'],
];

function functionSource(name) {
  const start = source.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `${name} exists`);
  const end = source.indexOf('\n}', start);
  return source.slice(start, end + 2);
}

function routeSource(method, route) {
  const start = source.indexOf(`router.${method}('${route}',`);
  assert.ok(start >= 0, `${method} ${route} exists`);
  const end = source.indexOf('\nrouter.', start + 1);
  assert.ok(end > start, `${route} ends before the next route`);
  return source.slice(start, end);
}

async function fixture(t) {
  const statements = [];
  const signedImages = [];
  const transactions = [];
  let recipients = [{ id: 1, email: 'operations@example.test', language: 'en', location_ids: '2,3' }];
  const volunteer = {
    id: 42, firstname: 'Ana', lastname: 'Rivera', email: 'ana@example.test',
    date_of_birth: '01/01/1990', phone: '5551234567', zipcode: '90210',
    gender: 'Woman', ethnicity: 'Latina', location: 'Central',
    legal_consent_accepted: 1, legal_consent_version: '2026',
  };
  const connection = {
    beginTransaction: async () => { transactions.push('begin'); },
    commit: async () => { transactions.push('commit'); },
    rollback: async () => { transactions.push('rollback'); },
    release: () => { transactions.push('release'); },
    getConnection: async () => connection,
    query: async (sql, params = []) => {
      statements.push({ sql, params: Array.from(params) });
      if (/FROM volunteer_notification_recipient AS recipient/i.test(sql)) return [recipients.map(row => ({ ...row }))];
      if (/^DELETE FROM volunteer_notification_recipient$/.test(sql)) recipients = [];
      if (/^DELETE FROM volunteer_notification_recipient/.test(sql)) return [{ affectedRows: 1 }];
      if (/INSERT INTO volunteer_notification_recipient\(/.test(sql)) {
        const id = recipients.length + 1;
        recipients.push({ id, email: params[0], language: params[1], location_ids: '' });
        return [{ insertId: id }];
      }
      if (/INSERT INTO volunteer_notification_recipient_location/.test(sql)) {
        const recipient = recipients.find(row => row.id === params[0][0][0]);
        recipient.location_ids = params[0].map(row => row[1]).join(',');
        return [{ affectedRows: params[0].length }];
      }
      if (/FROM volunteer_signature/i.test(sql)) return [[{ id: 9, file: 'signatures/42.png' }]];
      if (/COUNT\(\*\) as count/i.test(sql)) return [[{ count: 1 }]];
      if (/FROM volunteer as v/i.test(sql)) return [[{ ...volunteer }]];
      if (/FROM client c/.test(sql)) return [[{ client_id: 1, client_name: 'Community', location_id: 2, community_city: 'Central' }]];
      if (/FROM location/i.test(sql)) return [[{ id: 2, community_city: 'Central' }, { id: 3, community_city: 'North' }]];
      if (/FROM gender/i.test(sql)) return [[{ id: 4, name: 'Woman' }]];
      if (/FROM ethnicity/i.test(sql)) return [[{ id: 5, name: 'Latina' }]];
      throw new Error(`Unexpected query in isolated volunteer test: ${sql}`);
    },
  };
  const app = express();
  const router = express.Router();
  app.use(express.json());
  app.use(router);
  const context = vm.createContext({
    router, jwt, process: { env: { JWT_SECRET: secret } },
    mysqlConnection: { promise: () => connection },
    createCsvStringifier: createObjectCsvStringifier,
    console, logger: { error: () => {} },
    bucketName: 'test-signature-bucket', s3: {},
    GetObjectCommand: class { constructor(input) { this.input = input; } },
    getSignedUrl: async (_client, command, options) => {
      signedImages.push({ input: command.input, options });
      return `https://signed.example.test/${command.input.Key}`;
    },
  });
  const helpers = ['verifyToken', 'buildTableOrder', 'fetchVolunteerNotificationRecipients',
    'normalizeVolunteerNotificationLocationIds'].map(functionSource);
  const routes = [...protectedRoutes,
    ['get', '/locations'], ['get', '/client/locations'], ['get', '/gender'], ['get', '/ethnicity'],
    ['post', '/metrics/volunteer/gender'], ['put', '/enable-disable/:id'],
  ].map(([method, route]) => routeSource(method, route));
  vm.runInContext([...helpers, ...routes].join('\n'), context);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => {
    server.closeAllConnections();
    server.close(resolve);
  }));
  async function request(method, route, { role = 'opsmanager', token, body = {}, query = '' } = {}) {
    const authorization = token === null ? undefined : token ?? jwt.sign({
      data: JSON.stringify({ id: 7, role }),
    }, secret, { expiresIn: '1h' });
    return fetch(`http://127.0.0.1:${server.address().port}${route.replace(':idVolunteer', '42')}${query}`, {
      method: method.toUpperCase(),
      headers: { 'Content-Type': 'application/json', ...(authorization ? { Authorization: `Bearer ${authorization}` } : {}) },
      ...(method !== 'get' ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(5000),
    });
  }
  return { request, statements, signedImages, transactions };
}

test('opsmanager and admin can use all six volunteer endpoints', async t => {
  const f = await fixture(t);
  for (const role of ['opsmanager', 'admin']) {
    for (const [method, route] of protectedRoutes) {
      const response = await f.request(method, route, { role, body: { recipients: [] } });
      assert.equal(response.status, 200, `${role}: ${method} ${route}`);
      await response.text();
    }
  }
  assert.deepEqual(f.transactions, ['begin', 'commit', 'release', 'begin', 'commit', 'release']);
});

test('unrelated roles cannot read volunteers or change recipients; existing client CSV access remains', async t => {
  const f = await fixture(t);
  for (const role of ['beneficiary', 'eventvolunteer', 'delivery', 'stocker', 'director', 'auditor', 'contentmanager', 'unknown', 'client']) {
    for (const [method, route] of protectedRoutes) {
      if (role === 'client' && route.endsWith('/download-csv')) continue;
      const response = await f.request(method, route, { role });
      assert.equal(response.status, 401, `${role}: ${method} ${route}`);
      await response.text();
    }
  }
  assert.equal(f.statements.length, 0, 'denied requests never reach database reads or writes');
  assert.equal(f.signedImages.length, 0);
  const response = await f.request('post', '/table/volunteer/download-csv', { role: 'client' });
  assert.equal(response.status, 200);
  assert.match(await response.text(), /ana@example\.test/);
});

test('volunteer endpoints require a valid unexpired JWT', async t => {
  const f = await fixture(t);
  const expired = jwt.sign({ data: JSON.stringify({ id: 7, role: 'opsmanager' }) }, secret, { expiresIn: -1 });
  for (const [method, route] of protectedRoutes) {
    for (const [token, status] of [[null, 401], ['invalid-token', 403], [expired, 403]]) {
      const response = await f.request(method, route, { token });
      assert.equal(response.status, status, `${method} ${route}`);
      await response.text();
    }
  }
  assert.equal(f.statements.length, 0);
});

test('opsmanager can filter and paginate the table and export the same volunteer filters', async t => {
  const f = await fixture(t);
  const body = { from_date: '2026-01-01', to_date: '2026-09-22', locations: [2, 3], genders: [4],
    ethnicities: [5], min_age: 18, max_age: 80, zipcode: '90210' };
  const list = await f.request('post', '/table/volunteer', {
    body, query: '?page=2&pageSize=25&orderBy=lastname&orderType=asc&language=es&search=Ana',
  });
  assert.equal(list.status, 200);
  const table = await list.json();
  assert.equal(table.results[0].id, 42);
  assert.equal(table.page, 1);
  assert.equal(table.totalItems, 1);
  assert.deepEqual(f.statements[0].params, [25, 25]);
  assert.match(f.statements[0].sql, /ORDER BY v.lastname asc, v.id asc/);
  assert.match(f.statements[0].sql, /g.name_es as gender/);
  assert.match(f.statements[0].sql, /%Ana%/);
  const csv = await f.request('post', '/table/volunteer/download-csv', { body });
  assert.equal(csv.status, 200);
  assert.match(csv.headers.get('content-disposition'), /volunteers-table\.csv/);
  assert.match(csv.headers.get('content-type'), /text\/csv/);
  assert.match(await csv.text(), /42;Ana;Rivera;/);
  assert.deepEqual(f.statements.at(-1).params, ['2026-01-01', '2026-09-22']);
  for (const { sql } of f.statements) {
    assert.match(sql, /v.location_id IN \(2,3\)/);
    assert.match(sql, /v.gender_id IN \(4\)/);
    assert.match(sql, /v.ethnicity_id IN \(5\)/);
    assert.match(sql, />= 18/);
    assert.match(sql, /<= 80/);
    assert.match(sql, /v.zipcode = 90210/);
  }
});

test('opsmanager can view consent, signature URLs and every filter lookup', async t => {
  const f = await fixture(t);
  const detail = await f.request('get', '/view/volunteer/:idVolunteer');
  assert.equal(detail.status, 200);
  assert.equal((await detail.json()).legal_consent_accepted, true);
  assert.deepEqual(f.statements[0].params, ['42']);
  const images = await f.request('get', '/view/volunteer/images/:idVolunteer');
  assert.equal(images.status, 200);
  assert.equal((await images.json())[0].file, 'https://signed.example.test/signatures/42.png');
  assert.equal(f.signedImages[0].input.Bucket, 'test-signature-bucket');
  assert.equal(f.signedImages[0].options.expiresIn, 3600);
  for (const route of ['/locations', '/client/locations', '/gender', '/ethnicity']) {
    const response = await f.request('get', route);
    assert.equal(response.status, 200, route);
    assert.ok((await response.json()).length > 0, route);
  }
});

test('opsmanager can save notification recipients with language and location scope', async t => {
  const f = await fixture(t);
  const response = await f.request('put', '/volunteer/notification-recipients', { body: {
    recipients: [{ email: ' team@example.test ', language: 'es', location_ids: [2, '3', 2] }],
  } });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), [{ id: 1, email: 'team@example.test', language: 'es', location_ids: [2, 3] }]);
  assert.deepEqual(f.transactions, ['begin', 'commit', 'release']);
  const saved = await f.request('get', '/volunteer/notification-recipients');
  assert.deepEqual(await saved.json(), [{ id: 1, email: 'team@example.test', language: 'es', location_ids: [2, 3] }]);
});

test('opsmanager recipient edits still validate emails and location membership before replacement', async t => {
  const f = await fixture(t);
  for (const recipient of [
    { email: 'invalid', location_ids: [2] },
    { email: 'team@example.test', location_ids: [] },
    { email: 'team@example.test', location_ids: [999] },
  ]) {
    const response = await f.request('put', '/volunteer/notification-recipients', { body: { recipients: [recipient] } });
    assert.equal(response.status, 400);
    await response.text();
  }
  assert.equal(f.statements.some(({ sql }) => /DELETE|INSERT/.test(sql)), false);
  assert.deepEqual(f.transactions, ['begin', 'rollback', 'release']);
});

test('the volunteer access grant does not unlock metrics or enable-disable actions for opsmanager', async t => {
  const f = await fixture(t);
  for (const [method, route, status] of [['post', '/metrics/volunteer/gender', 403], ['put', '/enable-disable/42', 401]]) {
    const response = await f.request(method, route);
    assert.equal(response.status, status, route);
    await response.text();
  }
  assert.equal(f.statements.length, 0);
});
