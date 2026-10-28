'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { once } = require('node:events');
const express = require('express');
const jwt = require('jsonwebtoken');

// Exercise production routes and JWT middleware over HTTP without importing
// integrations that open live database, storage or mail connections.
const source = fs.readFileSync(path.join(__dirname, 'healthEvents.js'), 'utf8').replace(/\r\n/g, '\n');
const secret = 'synthetic-health-event-access-test-secret';
const eventPath = '/health-events/:id(\\\\d+)';
const adminRoutes = [
  ['get', '/health-events'], ['post', '/health-events'],
  ['put', eventPath], ['get', `${eventPath}/full`], ['put', `${eventPath}/landing`],
  ['post', `${eventPath}/duplicate`],
];

function functionSource(name) {
  const start = source.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `${name} exists`);
  for (let end = source.indexOf('\n}', start); end >= 0; end = source.indexOf('\n}', end + 2)) {
    const candidate = source.slice(start, end + 2);
    try { new vm.Script(candidate); return candidate; } catch {}
  }
  assert.fail(`Could not isolate ${name}`);
}

function routeSource(method, route) {
  const start = source.indexOf(`router.${method}('${route}',`);
  assert.ok(start >= 0, `${method} ${route} exists`);
  return source.slice(start, source.indexOf('\n});', start) + '\n});'.length);
}

function signedToken(role, options = {}, signingSecret = secret) {
  return jwt.sign({ data: JSON.stringify({ id: role === 'opsmanager' ? 8 : 7, role }) }, signingSecret, { expiresIn: '1h', ...options });
}

function eventBody(extra = {}) {
  return { slug: 'synthetic-clinic', name_en: 'Synthetic clinic', name_es: 'Clínica sintética',
    location_id: 10, start_date: '2099-01-01', end_date: '2099-01-02', ...extra };
}

async function fixture(t) {
  const statements = [];
  const duplicates = [];
  const events = new Map();
  const users = new Map([
    [7, { id: 7, role: 'admin', enabled: 'Y', deleted: 'N' }],
    [8, { id: 8, role: 'opsmanager', enabled: 'Y', deleted: 'N' }],
  ]);
  let nextId = 1;
  function addEvent(extra = {}) {
    const event = { id: nextId++, ...eventBody(), enabled: 'Y', landing_enabled: 'Y',
      timezone: 'America/Los_Angeles', organization: 'Synthetic location',
      community_city: 'Los Angeles', address: 'Synthetic address',
      landing_json: JSON.stringify({ hero: { title_en: 'Saved draft' } }), ...extra };
    events.set(event.id, event);
    return event;
  }
  const db = {
    async query(sql, rawParams = []) {
      const params = Array.from(rawParams);
      statements.push({ sql, params });
      const flat = sql.replace(/\s+/g, ' ').trim();
      if (/FROM user u INNER JOIN role/.test(flat)) {
        const user = users.get(Number(params[0]));
        if (!user) return [[]];
        if (/u.enabled = 'Y'/.test(flat) && user.enabled !== 'Y') return [[]];
        if (/u.deleted = 'N'/.test(flat) && user.deleted !== 'N') return [[]];
        if (/r.name IN \(\?, \?\)/.test(flat) && !params.slice(1).includes(user.role)) return [[]];
        return [[{ id: user.id }]];
      }
      if (/^SELECT id FROM health_event WHERE slug/.test(flat)) {
        return [[...events.values()].filter(e => e.slug === params[0] && e.id !== params[1]).map(e => ({ id: e.id }))];
      }
      if (/^INSERT INTO health_event\(/.test(flat)) {
        const columns = flat.match(/^INSERT INTO health_event\(([^)]+)\)/)[1].split(',').map(c => c.trim());
        const event = addEvent(Object.fromEntries(columns.map((column, index) => [column, params[index]])));
        return [{ insertId: event.id, affectedRows: 1 }];
      }
      if (/^UPDATE health_event SET/.test(flat)) {
        const columns = flat.match(/^UPDATE health_event SET (.+) WHERE id = \?/)[1].split(',').map(c => c.trim().split(' = ')[0]);
        const event = events.get(Number(params.at(-1)));
        assert.ok(event, 'Only update a fixture event');
        columns.forEach((column, index) => { event[column] = params[index]; });
        return [{ affectedRows: 1 }];
      }
      if (/FROM health_event he INNER JOIN location/.test(flat)) {
        if (/WHERE he.slug = \?/.test(flat)) return [[...events.values()].filter(e => e.slug === params[0])];
        if (/WHERE he.id = \?/.test(flat)) return [events.has(Number(params[0])) ? [events.get(Number(params[0]))] : []];
        return [[...events.values()]];
      }
      if (/^SELECT public_home_visible/.test(flat)) return [[events.get(Number(params[0]))]];
      if (/FROM health_event_(?:image|stand) /.test(flat)) return [[]];
      throw new Error(`Unexpected synthetic SQL: ${flat}`);
    }
  };
  const router = express.Router();
  const helpers = ['getBearerToken', 'getOptionalAuthUser', 'verifyToken', 'requireRoles',
    'nowInTimezone', 'toSqlDateTimeString', 'toSqlDateString', 'isRegistrationOpen', 'hasEventEnded',
    'safeParseJson', 'getEventBySlug', 'getEventById', 'publicEventShape', 'validateEventBody'];
  const roleConstants = source.slice(source.indexOf('const ADMIN_ROLES ='), source.indexOf('// ============ HELPERS'));
  const validationConstants = source.slice(source.indexOf('const SLUG_REGEX ='), source.indexOf('const DUPLICATE_SCAN_WINDOW_SECONDS'));
  vm.runInContext([
    ...helpers.map(functionSource), roleConstants, validationConstants,
    routeSource('get', '/health-events/landing/:slug'),
    ...adminRoutes.map(([method, route]) => routeSource(method, route)),
  ].join('\n'), vm.createContext({
    router, jwt, process: { env: { JWT_SECRET: secret } }, logger: { error(message) { throw new Error(message); } },
    mysqlConnection: { promise: () => db }, publicHomeCache: { at: 0 },
    attachImageUrls: async images => images, fetchForms: async () => [], fetchSlotsWithBooked: async () => [],
    duplicateHealthEvent: async options => { duplicates.push(options); return { id: 123 }; },
    HealthEventDuplicateError: require('../services/healthEventDuplicate').HealthEventDuplicateError,
    deleteS3Objects: async () => {}, s3: { send: async () => {} }, bucketName: 'synthetic', CopyObjectCommand: class {},
  }));
  const app = express();
  app.use(express.json());
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  async function request(method, route, { role, token = role ? signedToken(role) : null, body } = {}) {
    const response = await fetch(`http://127.0.0.1:${server.address().port}${route}`, {
      method: method.toUpperCase(),
      headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(5000),
    });
    return { status: response.status, data: await response.json(), headers: response.headers };
  }
  return { request, addEvent, events, users, statements, duplicates };
}

test('Admin and Ops Manager can create, list, inspect, edit and save a health-event landing', async t => {
  const f = await fixture(t);
  for (const role of ['admin', 'opsmanager']) {
    const body = eventBody({ slug: `synthetic-${role}`, enabled: 'N' });
    const created = await f.request('post', '/health-events', { role, body });
    assert.equal(created.status, 200, role);
    const id = created.data.id;
    assert.equal(f.events.get(id).created_by_user_id, role === 'opsmanager' ? 8 : 7);
    assert.equal(f.events.get(id).timezone, 'America/Los_Angeles');
    const listed = await f.request('get', '/health-events', { role });
    assert.equal(listed.status, 200, role);
    assert.ok(listed.data.events.some(e => e.id === id));
    const detail = await f.request('get', `/health-events/${id}/full`, { role });
    assert.equal(detail.status, 200, role);
    assert.equal(detail.data.event.enabled, 'N');
    const updated = await f.request('put', `/health-events/${id}`, {
      role, body: { ...body, name_en: 'Updated clinic', landing_enabled: 'N' },
    });
    assert.equal(updated.status, 200, role);
    assert.equal(f.events.get(id).name_en, 'Updated clinic');
    const landing = { hero: { title_en: `Preview for ${role}` } };
    assert.equal((await f.request('put', `/health-events/${id}/landing`, { role, body: { landing_json: landing } })).status, 200);
    const preview = await f.request('get', `/health-events/landing/${body.slug}`, { role });
    assert.equal(preview.status, 200, role);
    assert.deepEqual(preview.data.event.landing_json, landing);
    assert.equal(preview.data.event.is_preview, true);
  }
});

test('Non-managers and unauthenticated users cannot read or mutate health-event management routes', async t => {
  const f = await fixture(t);
  const paths = [['get', '/health-events'], ['post', '/health-events'], ['put', '/health-events/1'],
    ['get', '/health-events/1/full'], ['put', '/health-events/1/landing'], ['post', '/health-events/1/duplicate']];
  for (const role of [undefined, 'beneficiary', 'eventvolunteer', 'delivery', 'stocker', 'client', 'contentmanager', 'auditor', 'director']) {
    for (const [method, route] of paths) {
      const response = await f.request(method, route, { role, body: method === 'get' ? undefined : eventBody() });
      assert.equal(response.status, role ? 403 : 401, `${role || 'public'} ${method} ${route}`);
    }
  }
  for (const [method, route] of paths) {
    assert.equal((await f.request(method, route, { token: signedToken('admin', {}, 'wrong-secret') })).status, 403);
  }
  assert.equal(f.statements.length, 0, 'Denied requests never reach the database');
});

test('Only Admin and Ops Manager can duplicate an event with the new validated identity', async t => {
  const f = await fixture(t);
  for (const role of ['admin', 'opsmanager']) {
    const response = await f.request('post', '/health-events/7/duplicate', { role, body: eventBody({ slug: 'SAN-JACINTO' }) });
    assert.equal(response.status, 200);
    assert.deepEqual(response.data, { id: 123 });
    const call = f.duplicates.at(-1);
    assert.equal(call.sourceId, 7);
    assert.equal(call.userId, role === 'admin' ? 7 : 8);
    assert.equal(call.details.slug, 'san-jacinto');
  }
  for (const body of [eventBody({ slug: 'home' }), eventBody({ name_en: '' }), eventBody({ location_id: null })]) {
    const response = await f.request('post', '/health-events/7/duplicate', { role: 'admin', body });
    assert.equal(response.status, 400);
  }
  assert.equal(f.duplicates.length, 2);
});

test('Only authenticated Admin and Ops Manager can preview an unpublished or disabled landing', async t => {
  const f = await fixture(t);
  for (const [enabled, landing_enabled] of [['Y', 'N'], ['N', 'Y'], ['N', 'N']]) {
    const event = f.addEvent({ slug: `draft-${enabled.toLowerCase()}-${landing_enabled.toLowerCase()}`, enabled, landing_enabled });
    const route = `/health-events/landing/${event.slug}`;
    for (const role of ['admin', 'opsmanager']) {
      const preview = await f.request('get', route, { role });
      assert.equal(preview.status, 200, `${role} ${event.slug}`);
      assert.equal(preview.data.event.is_preview, true);
      assert.equal(preview.data.event.enabled, enabled);
      assert.equal(preview.data.event.landing_enabled, landing_enabled);
      assert.deepEqual(preview.data.event.landing_json, { hero: { title_en: 'Saved draft' } });
      assert.match(preview.headers.get('cache-control'), /private/);
      assert.match(preview.headers.get('cache-control'), /no-store/);
      assert.match(preview.headers.get('vary'), /Authorization/);
    }
    for (const role of [undefined, 'beneficiary', 'eventvolunteer', 'delivery', 'stocker', 'client', 'contentmanager', 'auditor', 'director']) {
      const before = f.statements.length;
      const denied = await f.request('get', route, { role });
      assert.equal(denied.status, 404, `${role || 'public'} ${event.slug}`);
      assert.deepEqual(denied.data, { error: 'NOT_FOUND' });
      assert.equal(f.statements.length, before + 1, 'Denied previews never load or sign images');
    }
    // Untrusted query parameters cannot grant preview permission.
    assert.equal((await f.request('get', `${route}?preview=true&role=admin`)).status, 404);
  }
});

test('Forged, expired and malformed credentials cannot reveal drafts', async t => {
  const f = await fixture(t);
  const event = f.addEvent({ landing_enabled: 'N' });
  const invalidTokens = [
    'invalid', 'null', 'undefined',
    signedToken('admin', {}, 'wrong-secret'), signedToken('opsmanager', { expiresIn: -1 }),
    jwt.sign({ data: JSON.stringify({ id: 7, role: 'admin' }) }, null, { algorithm: 'none' }),
    jwt.sign({ data: 'not-json' }, secret), jwt.sign({ data: 'null' }, secret),
    jwt.sign({ data: { id: 7, role: 'admin' } }, secret),
  ];
  for (const token of invalidTokens) {
    assert.equal((await f.request('get', `/health-events/landing/${event.slug}`, { token })).status, 404);
  }
  assert.ok(f.statements.every(s => s.sql.includes('WHERE he.slug = ?')), 'No draft assets are accessed');
});

test('A previously issued manager JWT cannot preview after account removal, disabling or demotion', async t => {
  const f = await fixture(t);
  const event = f.addEvent({ landing_enabled: 'N' });
  for (const role of ['admin', 'opsmanager']) {
    const id = role === 'opsmanager' ? 8 : 7;
    const token = signedToken(role);
    const user = { id, role, enabled: 'Y', deleted: 'N' };
    for (const change of [{ enabled: 'N' }, { deleted: 'Y' }, { role: 'eventvolunteer' }, null]) {
      if (change) f.users.set(id, { ...user, ...change });
      else f.users.delete(id);
      const before = f.statements.length;
      const response = await f.request('get', `/health-events/landing/${event.slug}`, { token });
      assert.equal(response.status, 404, `${role} ${JSON.stringify(change)}`);
      assert.equal(f.statements.length, before + 2, 'Only event and live permission checks run');
    }
  }
});

test('Published landings remain public, and missing events stay hidden even from managers', async t => {
  const f = await fixture(t);
  const event = f.addEvent();
  for (const options of [{}, { role: 'admin' }, { role: 'opsmanager' }, { role: 'beneficiary' }, { token: 'expired-or-invalid' }]) {
    const response = await f.request('get', `/health-events/landing/${event.slug}`, options);
    assert.equal(response.status, 200);
    assert.equal(response.data.event.is_preview, false);
    assert.equal(response.data.event.enabled, 'Y');
    assert.equal(response.data.event.landing_enabled, 'Y');
  }
  for (const role of [undefined, 'admin', 'opsmanager']) {
    assert.equal((await f.request('get', '/health-events/landing/missing', { role })).status, 404);
  }
});
