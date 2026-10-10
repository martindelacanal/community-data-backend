'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { once } = require('node:events');
const express = require('express');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const access = require('../services/ticketAccess');
const userTableAccess = require('../services/userTableAccess');
const engagement = require('../services/participantEngagement');
const escapeSqlValue = require('mysql2').escape;
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;

// Run the production route bodies and authentication/access helpers, but never
// import user.js (which initializes live database, S3 and email integrations).
const source = fs.readFileSync(path.join(__dirname, 'user.js'), 'utf8').replace(/\r\n/g, '\n');
const secret = 'synthetic-role-boundary-regression-secret';
function functionSource(name) {
  const start = source.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `${name} exists`);
  // Destructured multiline parameters also have a column-zero closing brace.
  for (let end = source.indexOf('\n}', start); end >= 0; end = source.indexOf('\n}', end + 2)) {
    const candidate = source.slice(start, end + 2);
    try { new vm.Script(candidate); return candidate; } catch {}
  }
  assert.fail(`Could not isolate ${name}`);
}
function routeSource(method, route) {
  const start = source.indexOf(`router.${method}('${route}',`);
  assert.ok(start >= 0, `${method} ${route} exists`);
  const endings = ['\n});', '\n})', '\n}\n);'].map(end => ({ end, at: source.indexOf(end, start) }))
    .filter(item => item.at >= 0).sort((a, b) => a.at - b.at);
  return source.slice(start, endings[0].at + endings[0].end.length);
}

async function serve(t, context, routes, helpers, bootstrap = []) {
  const app = express();
  const router = express.Router();
  app.use(express.json());
  app.use(router);
  vm.runInContext([...helpers.map(functionSource), ...routes.map(([method, route]) => routeSource(method, route)), ...bootstrap].join('\n'),
    vm.createContext({ router, jwt, process: { env: { JWT_SECRET: secret } },
      console: { log() {}, error() {} }, logger: { error() {} }, ...context }));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  return async (method, route, { role = 'stocker', id = 7, clientId = 21, body = {}, form, image = false, token } = {}) => {
    const authorization = token === null ? null : token ?? jwt.sign({ data: JSON.stringify({ id, role, client_id: clientId }) }, secret, { expiresIn: '1h' });
    const headers = authorization ? { Authorization: `Bearer ${authorization}` } : {};
    let payload;
    if (form) {
      payload = new FormData();
      payload.append('form', JSON.stringify(form));
      if (image) payload.append('ticket[]', new Blob(['synthetic image'], { type: 'image/png' }), 'synthetic.png');
    } else if (method !== 'get') {
      headers['Content-Type'] = 'application/json';
      payload = JSON.stringify(body);
    }
    const response = await fetch(`http://127.0.0.1:${server.address().port}${route}`, {
      method: method.toUpperCase(), headers, body: payload, signal: AbortSignal.timeout(5000),
    });
    const text = await response.text();
    let data; try { data = JSON.parse(text); } catch { data = text; }
    return { status: response.status, data };
  };
}

const ticketRoutes = [
  ['get', '/upload/ticket/:id'], ['get', '/view/ticket/:idTicket'],
  ['get', '/view/ticket/images/:idTicket'], ['put', '/upload/ticket/:id'],
  ['put', '/notes/:id'], ['delete', '/notes/:id'],
];
const readPaths = ['/upload/ticket/', '/view/ticket/', '/view/ticket/images/'];
function validForm(extra = {}) {
  return { donation_id: 'SYNTHETIC-UPDATED', total_weight: 12, provider: 31,
    transported_by: 41, delivered_by: 51, date: '2026-10-10', destination: 61,
    products: [{ product: 71, quantity: 12 }], ...extra };
}

async function ticketFixture(t) {
  const statements = [], writes = [], signedImages = [], uploads = [], transactions = [];
  const tickets = new Map([
    [100, { id: 100, owner: 7, client: 21, enabled: 'Y', audit_status: 1 }],
    [200, { id: 200, owner: 8, client: 22, enabled: 'Y', audit_status: 1 }],
    [300, { id: 300, owner: 7, client: 21, enabled: 'N', audit_status: 1 }],
  ]);
  const notes = new Map([
    [501, { id: 501, ticket: 100, user_id: 7 }],
    [502, { id: 502, ticket: 200, user_id: 7 }], // own note, somebody else's ticket
    [503, { id: 503, ticket: 100, user_id: 8 }],
    [504, { id: 504, ticket: 100, user_id: 9 }],
  ]);
  // This small SQL double only enforces predicates actually present in the
  // production query; it intentionally returns foreign records when omitted.
  function visible(ticket, sql, params) {
    if (!ticket || (/enabled\s*=\s*'Y'/i.test(sql) && ticket.enabled !== 'Y')) return false;
    if (/ticket_owner\.user_id\s*=\s*\?/.test(sql) && ticket.owner !== Number(params.at(-1))) return false;
    if (/ticket_client\.client_id\s*=\s*\?/.test(sql) && ticket.client !== Number(params.at(-1))) return false;
    if (/cl_access\.client_id\s*=\s*\?/.test(sql) && ticket.client !== Number(params.at(-1))) return false;
    return true;
  }
  const db = {
    async beginTransaction() { transactions.push('begin'); },
    async commit() { transactions.push('commit'); },
    async rollback() { transactions.push('rollback'); },
    release() { transactions.push('release'); },
    async getConnection() { return db; },
    async query(sql, rawParams = []) {
      const params = Array.from(rawParams);
      statements.push({ sql, params });
      const flat = sql.replace(/\s+/g, ' ').trim();
      if (/^(INSERT|UPDATE|DELETE)\b/i.test(flat)) {
        writes.push({ sql, params });
        if (/^UPDATE donation_ticket SET/i.test(flat)) {
          const row = tickets.get(Number(params.at(-1)));
          row.donation_id = params[0]; row.total_weight = params[1];
          if (/audit_status_id = \?/.test(flat)) row.audit_status = params[7];
        }
        if (/^DELETE FROM donation_ticket_note /i.test(flat)) notes.delete(Number(params[0]));
        return [{ affectedRows: /^INSERT INTO donation_ticket_location/i.test(flat) ? params[0].length : 1, insertId: 900 }];
      }
      if (/^SELECT dt.id FROM donation_ticket AS dt /i.test(flat)) {
        const ticket = tickets.get(Number(params[0]));
        return [visible(ticket, flat, params) ? [{ id: ticket.id }] : []];
      }
      if (/SELECT n.user_id FROM donation_ticket_note AS n/i.test(flat)) {
        const note = notes.get(Number(params[0]));
        return [note && visible(tickets.get(note.ticket), flat, params) ? [{ user_id: note.user_id }] : []];
      }
      if (/^SELECT id FROM location/i.test(flat)) return [params.map(id => ({ id }))];
      if (/FROM donation_ticket_image/i.test(flat)) {
        return [[{ id: 801, file: `synthetic/${Number(params[0])}.png`, display_order: 1 }]];
      }
      if (/FROM donation_ticket_location AS dtl/i.test(flat)) {
        return [[{ location_id: 61, total_weight: 12, display_order: 1, location: 'Synthetic location' }]];
      }
      if (/INNER JOIN donation_ticket_note as dtn/i.test(flat)) return [[]];
      if (/FROM donation_ticket as (?:t|dt) /i.test(flat)) {
        const row = tickets.get(Number(params[0]));
        return [visible(row, flat, params) ? [{ ...row, donation_id: 'SYNTHETIC', total_weight: 12,
          provider: 'Synthetic provider', transported_by: 'Synthetic transport', delivered_by: 'Synthetic staff',
          destination: 61, date: '2026-10-10', product: 'Synthetic product', product_id: 71,
          product_type: 1, quantity: 12, image_count: 1 }] : []];
      }
      throw new Error(`Unexpected synthetic query: ${flat}`);
    },
    async execute(sql, params) { return db.query(sql, params); },
  };
  const imageCommand = class { constructor(input) { this.input = input; } };
  const helpers = ['verifyToken', 'requireTicketReadAccess', 'handleTicketImageUpload',
    'normalizeTicketImageInteger', 'validateTicketImageUpdate', 'createTicketRequestError', 'assertSingleAffectedRow',
    'normalizeTicketLocationId', 'normalizeTicketLocationWeight', 'normalizeTicketDestinations',
    'assertTicketDestinationLocationsExist', 'replaceTicketDestinations'];
  const request = await serve(t, {
    ...access, mysqlConnection: { promise: () => db }, MAX_TICKET_IMAGES: 4,
    ticketImageUpload: multer({ storage: multer.memoryStorage() }).array('ticket[]', 4),
    bucketName: 'synthetic-no-network', GetObjectCommand: imageCommand, PutObjectCommand: imageCommand,
    s3: { async send(command) { uploads.push(command.input); return {}; } },
    async getSignedUrl(_s3, command) { signedImages.push(command.input); return `https://synthetic.invalid/${command.input.Key}`; },
    randomImageName: () => 'synthetic-new.png',
    async deleteTicketImageObjectsBestEffort(keys) { if (keys.length) uploads.push({ deleted: Array.from(keys) }); },
    invalidateTicketMetricsCache() {},
  }, ticketRoutes, helpers);
  return { request, statements, writes, signedImages, uploads, transactions, tickets, notes };
}

test('Stocker direct reads deny foreign and disabled tickets before image signing or detail queries', async t => {
  const f = await ticketFixture(t);
  for (const id of [200, 300, 999]) for (const prefix of readPaths) {
    assert.equal((await f.request('get', `${prefix}${id}`)).status, 404, `${prefix}${id}`);
  }
  assert.equal(f.signedImages.length, 0);
  assert.equal(f.writes.length, 0);
  assert.equal(f.statements.length, 9, 'each denial performs only the authorization lookup');
});

test('Stocker retains own form, detail and image reads; broad readers retain global access', async t => {
  const f = await ticketFixture(t);
  for (const prefix of readPaths) assert.equal((await f.request('get', `${prefix}100`)).status, 200, prefix);
  for (const role of ['admin', 'opsmanager', 'auditor']) for (const prefix of readPaths) {
    assert.equal((await f.request('get', `${prefix}200`, { role })).status, 200, `${role} ${prefix}`);
  }
  assert.ok(f.signedImages.length > 0);
});

test('Client reads only organization tickets and images; invalid organization never reaches S3', async t => {
  const f = await ticketFixture(t);
  for (const prefix of ['/view/ticket/', '/view/ticket/images/']) {
    assert.equal((await f.request('get', `${prefix}100`, { role: 'client' })).status, 200);
    const before = f.signedImages.length;
    assert.equal((await f.request('get', `${prefix}200`, { role: 'client' })).status, 404);
    for (const clientId of [null, 0, '21 OR 1=1', [21], true]) {
      assert.equal((await f.request('get', `${prefix}100`, { role: 'client', clientId })).status, 403);
    }
    assert.equal(f.signedImages.length, before);
  }
});

test('Direct ticket routes reject malformed identifiers and unauthorized tokens without SQL or signing', async t => {
  const f = await ticketFixture(t);
  for (const rawId of ['0', '-1', '1e2', '100.0', '100abc', '9007199254740992', '100%20OR%201=1']) {
    for (const prefix of readPaths) assert.equal((await f.request('get', `${prefix}${rawId}`)).status, 404, rawId);
  }
  for (const prefix of readPaths) {
    assert.equal((await f.request('get', `${prefix}100`, { token: null })).status, 401);
    assert.equal((await f.request('get', `${prefix}100`, { token: 'invalid' })).status, 403);
    assert.equal((await f.request('get', `${prefix}100`, { role: 'beneficiary' })).status, 401);
  }
  assert.equal(f.statements.length, 0);
  assert.equal(f.signedImages.length, 0);
});

test('Stocker PUT locks and denies foreign ticket before catalog, notes, products or S3 writes', async t => {
  const f = await ticketFixture(t);
  const response = await f.request('put', '/upload/ticket/200', {
    form: validForm({ provider: 'Would create a provider', notes: 'Would add a note' }), image: true,
  });
  assert.equal(response.status, 404);
  assert.equal(f.statements.length, 1);
  assert.match(f.statements[0].sql, /FOR UPDATE/);
  assert.deepEqual(f.transactions, ['begin', 'rollback', 'release']);
  assert.equal(f.writes.length, 0);
  assert.equal(f.uploads.length, 0);
});

test('Stocker own and Opsmanager global general edits preserve legacy scalar-destination payload', async t => {
  const f = await ticketFixture(t);
  for (const [role, id] of [['stocker', 100], ['opsmanager', 200]]) {
    const response = await f.request('put', `/upload/ticket/${id}`, { role, form: validForm() });
    assert.equal(response.status, 200, `${role}: ${JSON.stringify(response.data)}`);
    assert.equal(response.data, 'Data edited successfully');
    assert.equal(f.tickets.get(id).donation_id, 'SYNTHETIC-UPDATED');
    assert.equal(f.tickets.get(id).audit_status, 1, 'omitted audit state stays intact');
  }
  const firstLock = f.statements.findIndex(row => /FOR UPDATE/.test(row.sql));
  const firstWrite = f.statements.findIndex(row => /^(INSERT|UPDATE|DELETE)/i.test(row.sql.trim()));
  assert.ok(firstLock >= 0 && firstLock < firstWrite);
  assert.deepEqual(f.transactions, ['begin', 'commit', 'release', 'begin', 'commit', 'release']);
});

test('Stocker and Opsmanager audit writes are forbidden before any side effects', async t => {
  const f = await ticketFixture(t);
  for (const role of ['stocker', 'opsmanager']) for (const audit_status of [2, '2', [2], true]) {
    const response = await f.request('put', '/upload/ticket/100', { role, form: validForm({ audit_status }), image: true });
    assert.equal(response.status, 403, `${role}: ${JSON.stringify(audit_status)}`);
  }
  assert.equal(f.writes.length, 0);
  assert.equal(f.uploads.length, 0);
  assert.equal(f.statements.length, 0);
});

test('Admin and Auditor retain global general and audit edits, including modern destinations', async t => {
  const f = await ticketFixture(t);
  for (const role of ['admin', 'auditor']) {
    const response = await f.request('put', '/upload/ticket/200', { role,
      form: validForm({ audit_status: 2, destination: undefined, destinations: [{ location_id: 61, total_weight: 12 }] }),
    });
    assert.equal(response.status, 200, `${role}: ${JSON.stringify(response.data)}`);
    assert.equal(f.tickets.get(200).donation_id, 'SYNTHETIC-UPDATED');
    assert.equal(f.tickets.get(200).audit_status, 2);
  }
});

test('Admin and Auditor reject invalid audit identifiers without SQL or S3 writes', async t => {
  const f = await ticketFixture(t);
  for (const role of ['admin', 'auditor']) for (const audit_status of [0, -1, '2abc', '2e0', [2], true, {}]) {
    assert.equal((await f.request('put', '/upload/ticket/200', { role, form: validForm({ audit_status }), image: true })).status, 400);
  }
  assert.equal(f.statements.length, 0);
  assert.equal(f.uploads.length, 0);
});

test('Stocker cannot edit/delete own note on a foreign parent ticket or another authors note', async t => {
  const f = await ticketFixture(t);
  for (const method of ['put', 'delete']) {
    assert.equal((await f.request(method, '/notes/502', { body: { note: 'Denied' } })).status, 404);
    assert.equal((await f.request(method, '/notes/503', { body: { note: 'Denied' } })).status, 403);
  }
  assert.equal(f.writes.length, 0);
});

test('Note owner access remains, and only Admin/Opsmanager can edit/delete other authors notes', async t => {
  const f = await ticketFixture(t);
  for (const [role, id, note] of [['stocker', 7, 501], ['auditor', 9, 504], ['admin', 1, 503], ['opsmanager', 2, 503]]) {
    assert.equal((await f.request('put', `/notes/${note}`, { role, id, body: { note: 'Allowed' } })).status, 200);
  }
  assert.equal((await f.request('delete', '/notes/501')).status, 200);
  assert.equal((await f.request('delete', '/notes/504', { role: 'auditor', id: 9 })).status, 200);
  assert.equal((await f.request('delete', '/notes/502', { role: 'admin' })).status, 200);
});

const userRoutes = [
  ['post', '/table/user'], ['post', '/table/user/client/download-csv'],
  ['post', '/table/user/beneficiary/download-csv'],
];
const catalogPaths = ['/providers', '/delivered-by', '/transported-by', '/stocker-upload'];

async function userFixture(t) {
  const statements = [];
  const users = [
    { id: 101, role: 'beneficiary', roleId: 5, client: 21, username: 'own-participant-a' },
    { id: 102, role: 'beneficiary', roleId: 5, client: 22, username: 'foreign-participant' },
    { id: 103, role: 'client', roleId: 2, client: 21, username: 'own-client-a' },
    { id: 104, role: 'client', roleId: 2, client: 22, username: 'foreign-client' },
    { id: 105, role: 'stocker', roleId: 3, client: 21, username: 'system-stocker' },
    { id: 106, role: 'admin', roleId: 1, client: 21, username: 'system-admin' },
    { id: 107, role: 'beneficiary', roleId: 5, client: 21, username: 'own-participant-b' },
    { id: 108, role: 'client', roleId: 2, client: 21, username: 'own-client-b' },
  ];
  const catalog = [
    { id: 31, name: 'Own active catalog', client: 21, ticketEnabled: 'Y' },
    { id: 32, name: 'Foreign catalog', client: 22, ticketEnabled: 'Y' },
    { id: 33, name: 'Own disabled ticket catalog', client: 21, ticketEnabled: 'N' },
    { id: 34, name: 'Unlinked catalog', client: null, ticketEnabled: null },
  ];
  // Scope is applied only if the actual query contains the required predicate.
  // Binding position is calculated from preceding placeholders, so extra joins,
  // search clauses and CSV dates cannot silently use the wrong organization.
  function boundValue(sql, params, expression) {
    const match = expression.exec(sql);
    if (!match) return undefined;
    return params[(sql.slice(0, match.index).match(/\?/g) || []).length];
  }
  const db = {
    async query(sql, rawParams = []) {
      const params = Array.from(rawParams);
      statements.push({ sql, params });
      const flat = sql.replace(/\s+/g, ' ').trim();
      assert.doesNotMatch(flat, /^(INSERT|UPDATE|DELETE)\b/i, 'read requests never write');
      if (/FROM user as u/i.test(flat) && !/INNER JOIN stocker_log AS sl/i.test(flat)) {
        let rows = users;
        if (/(?:role\.id|u\.role_id) = 5/.test(flat)) rows = rows.filter(row => row.roleId === 5);
        if (/(?:role\.id|u\.role_id) = 2/.test(flat)) rows = rows.filter(row => row.roleId === 2);
        if (/role\.id != 2 AND role\.id != 5/.test(flat)) rows = rows.filter(row => ![2, 5].includes(row.roleId));
        const clientId = boundValue(flat, params, /(?:client_user|cu|u)\.client_id = \?/);
        if (clientId !== undefined) rows = rows.filter(row => row.client === Number(clientId));
        if (/SELECT COUNT\(/i.test(flat)) {
          assert.match(flat, /COUNT\(DISTINCT u\.id\)/i);
          assert.doesNotMatch(flat, /GROUP BY u\.id/i, 'total is one scalar count');
          return [[{ count: rows.length }]];
        }
        if (/LIMIT \?, \?/.test(flat)) rows = rows.slice(params.at(-2), params.at(-2) + params.at(-1));
        return [rows.map(row => ({ ...row, enabled: 'Y', firstname: 'Synthetic' }))];
      }
      if (/FROM (?:provider|delivered_by|transported_by) AS catalog/i.test(flat)
          || /INNER JOIN stocker_log AS sl/i.test(flat)) {
        let rows = catalog;
        const clientId = boundValue(flat, params, /ticket_client\.client_id = \?/);
        if (clientId !== undefined) rows = rows.filter(row => row.client === Number(clientId));
        if (/dt\.enabled = 'Y'/.test(flat)) rows = rows.filter(row => row.ticketEnabled === 'Y');
        return [rows.map(({ id, name }) => ({ id, name }))];
      }
      throw new Error(`Unexpected synthetic user query: ${flat}`);
    },
  };
  const request = await serve(t, {
    ...access, ...userTableAccess, ...engagement, escapeSqlValue, createCsvStringifier,
    mysqlConnection: { promise: () => db }, APP_PLATFORM_CSV_LABELS: {},
    async hydrateParticipantEngagement() {}, // enrichment is unrelated to row authorization
  }, [...userRoutes, ...catalogPaths.map(route => ['get', route])],
  ['verifyToken', 'validateUserTableInput', 'buildTableOrder', 'buildRegisterFormCondition']);
  return { request, statements };
}

test('Client user tables reject missing, all, unknown and non-scalar modes before querying', async t => {
  const f = await userFixture(t);
  const queries = ['', '?tableRole=', '?tableRole=all', '?tableRole=stocker', '?tableRole=unknown',
    '?tableRole=client&tableRole=beneficiary', '?tableRole[]=client', '?tableRole[x]=client'];
  for (const query of queries) {
    assert.equal((await f.request('post', `/table/user${query}`, { role: 'client' })).status, 403, query);
  }
  for (const clientId of [null, 0, -1, true, [21], '21 OR 1=1']) {
    assert.equal((await f.request('post', '/table/user?tableRole=client', { role: 'client', clientId })).status, 403);
  }
  assert.equal(f.statements.length, 0);
});

test('Client user rows and totals share organization scope; pagination and Admin system mode remain', async t => {
  const f = await userFixture(t);
  for (const [mode, expected] of [['client', [103, 108]], ['beneficiary', [101, 107]]]) {
    const response = await f.request('post', `/table/user?tableRole=${mode}`, { role: 'client' });
    assert.equal(response.status, 200);
    assert.deepEqual(response.data.results.map(row => row.id), expected);
    assert.equal(response.data.totalItems, 2);
    assert.equal(response.data.numOfPages, 1);
    const page = await f.request('post', `/table/user?tableRole=${mode}&pageSize=1&page=2`, { role: 'client' });
    assert.deepEqual(page.data.results.map(row => row.id), [expected[1]]);
    assert.equal(page.data.totalItems, 2);
    assert.equal(page.data.numOfPages, 2);
    assert.equal(page.data.page, 1);
  }
  const admin = await f.request('post', '/table/user?tableRole=all', { role: 'admin' });
  assert.equal(admin.status, 200);
  assert.deepEqual(admin.data.results.map(row => row.id), [105, 106]);
  assert.equal(admin.data.totalItems, 2);
});

test('Client table search is parameterized and ordering cannot replace the scope predicate', async t => {
  const f = await userFixture(t);
  const malicious = "x%' OR 1=1 --";
  for (const mode of ['client', 'beneficiary']) {
    const query = new URLSearchParams({ tableRole: mode, search: malicious,
      orderBy: 'u.id; DROP TABLE user', orderType: 'desc; --' });
    const response = await f.request('post', `/table/user?${query}`, { role: 'client' });
    assert.equal(response.status, 200);
    assert.equal(response.data.totalItems, 2);
    assert.ok(response.data.results.every(row => row.client === 21));
    assert.equal(response.data.orderBy, 'id');
    assert.equal(response.data.orderType, 'desc');
  }
  for (const statement of f.statements) {
    assert.ok(!statement.sql.includes(malicious));
    assert.ok(!statement.sql.includes('DROP TABLE'));
    assert.equal(statement.params.filter(value => value === `%${malicious}%`).length, 8);
    assert.ok(statement.params.includes(21));
  }
});

test('User table and CSV reject malicious filter arrays, ages, dates and search types before SQL', async t => {
  const f = await userFixture(t);
  const bodies = [
    ...['locations', 'genders', 'ethnicities', 'second_ethnicities', 'languages'].map(key => ({ [key]: ['1) OR 1=1 --'] })),
    { locations: '1' }, { genders: [true] }, { min_age: '0 OR 1=1' }, { max_age: [150] },
    { zipcode: { raw: 'OR 1=1' } }, { from_date: "2026-01-01' OR 1=1" }, { to_date: ['2026-10-10'] },
  ];
  for (const route of ['/table/user?tableRole=beneficiary', '/table/user/client/download-csv', '/table/user/beneficiary/download-csv']) {
    for (const body of bodies) {
      assert.equal((await f.request('post', route, { role: 'client', body })).status, 400, `${route} ${JSON.stringify(body)}`);
    }
  }
  for (const query of ['search[]=x', 'search=x&search=y', 'search[x]=y']) {
    assert.equal((await f.request('post', `/table/user?tableRole=client&${query}`, { role: 'client' })).status, 400);
  }
  assert.equal(f.statements.length, 0);
});

test('Client CSV exports preserve organization scope and valid legacy filters', async t => {
  const f = await userFixture(t);
  for (const [mode, ownName] of [['client', 'own-client'], ['beneficiary', 'own-participant']]) {
    const response = await f.request('post', `/table/user/${mode}/download-csv`, { role: 'client',
      body: { genders: ['1'], ethnicities: [2], min_age: '18', max_age: 120,
        from_date: '2026-01-01T00:00:00Z', to_date: '2026-10-10', zipcode: "90210' OR 1=1 --" } });
    assert.equal(response.status, 200, JSON.stringify(response.data));
    assert.match(response.data, /ID;Username;/);
    assert.ok(response.data.includes(ownName));
    assert.doesNotMatch(response.data, /foreign-|system-/);
    const statement = f.statements.at(-1);
    assert.ok(statement.sql.includes(`u.zipcode = ${escapeSqlValue("90210' OR 1=1 --")}`));
    assert.ok(statement.params.includes(21));
    assert.ok(statement.params.includes('2026-01-01'));
  }
  const before = f.statements.length;
  for (const mode of ['client', 'beneficiary']) {
    assert.equal((await f.request('post', `/table/user/${mode}/download-csv`, { role: 'client', clientId: null })).status, 403);
  }
  assert.equal(f.statements.length, before);
});

test('Client provider and ticket filter catalogs contain only links to enabled organization tickets', async t => {
  const f = await userFixture(t);
  for (const route of catalogPaths) {
    const client = await f.request('get', route, { role: 'client' });
    assert.equal(client.status, 200, route);
    assert.deepEqual(client.data.map(row => row.id), [31]);
    const admin = await f.request('get', route, { role: 'admin' });
    assert.equal(admin.status, 200, route);
    assert.deepEqual(admin.data.map(row => row.id), [31, 32, 33, 34]);
  }
  const before = f.statements.length;
  for (const route of catalogPaths) for (const clientId of [null, 0, true, [21], '21 OR 1=1']) {
    assert.equal((await f.request('get', route, { role: 'client', clientId })).status, 403, route);
  }
  assert.equal(f.statements.length, before);
});

async function graphicFixture(t) {
  const statements = [];
  const route = '/dashboard/graphic-line/:tabSelected';
  // user.js replaces the original route at startup. Exercise that replacement,
  // rather than reporting results for its unreachable legacy handler.
  const override = source.split('\n').find(line => line.startsWith(`overrideRouteHandler('${route}', 'post',`));
  assert.ok(override, 'production registers the optimized graphic handler');
  const request = await serve(t, {
    ...access,
    PRODUCT_METRICS_ACCESS_ROLES: new Set(['admin', 'client', 'director', 'auditor']),
    PRODUCT_METRICS_INTERVALS: new Set(['day', 'week', 'month', 'quarter', 'year']),
    PRODUCT_METRICS_CACHE_TTL_MS: 60000,
    buildParticipantMetricsCacheKey: () => 'synthetic-no-shared-cache',
    getCachedParticipantMetrics: async (_key, _ttl, factory) => factory(),
    mysqlConnection: { promise: () => ({ async query(sql, rawParams) {
      const params = Array.from(rawParams);
      statements.push({ sql, params });
      assert.match(sql, /GROUP BY period/);
      assert.equal((sql.match(/\?/g) || []).length, params.length, 'all placeholders have bindings');
      // There are two independently scoped expressions: accessible tickets and
      // their organization-owned destination weights. Both must be present.
      const orgBindings = [...sql.matchAll(/cl_(?:weight|client)\.client_id = \?/g)]
        .map(match => params[(sql.slice(0, match.index).match(/\?/g) || []).length]);
      const ownWeight = orgBindings.length === 2 && orgBindings.every(value => value === 21);
      return [[{ period: /QUARTER\(dt.date\)/.test(sql) ? '2026-Q4' : '2026-10', value: ownWeight ? 12 : 111 }]];
    } }) },
  }, [['post', route]], ['verifyToken', 'formatPeriod', 'generatePeriods', 'overrideRouteHandler',
    'hasProductMetricsAccess', 'normalizeProductMetricsDate', 'normalizeProductMetricsIdArray',
    'normalizeProductMetricsFilters', 'formatProductMetricsDateOnly', 'resolveProductMetricsDateRange',
    'addProductMetricsInCondition', 'getProductMetricsPeriodExpression', 'buildProductMetricsClientJoin',
    'buildProductMetricsWhere', 'buildProductMetricsWeightExpression', 'optimizedDashboardGraphicLineHandler'],
  [override]);
  return { request, statements };
}

test('Effective dashboard graphic handler discards hostile filter IDs and preserves Client organization scope', async t => {
  const f = await graphicFixture(t);
  const hostile = '0) OR 1=1 OR sl.user_id IN (0';
  for (const field of ['stocker_upload', 'transported_by']) {
    const response = await f.request('post', '/dashboard/graphic-line/pounds-filters', { role: 'client',
      body: { from_date: '2026-10-01', to_date: '2026-10-31', [field]: [hostile] } });
    assert.equal(response.status, 200, 'the effective normalizer discards invalid IDs; it does not reject the request');
    assert.deepEqual(response.data, { name: 'Pounds delivered', series: [{ name: '10/2026', value: 12 }] });
    const statement = f.statements.at(-1);
    assert.ok(!statement.sql.includes(hostile));
    assert.ok(!statement.params.includes(hostile));
    assert.deepEqual(statement.params, [21, 'Y', '2026-10-01', '2026-10-31', 21]);
  }
  assert.equal(f.statements.length, 2);
});

test('Effective dashboard graphic retains valid five-array filter bindings and month/quarter response contracts', async t => {
  const f = await graphicFixture(t);
  for (const [interval, label] of [['month', '10/2026'], ['quarter', 'Q4/2026']]) {
    const response = await f.request('post', '/dashboard/graphic-line/pounds-filters?language=es', { role: 'client',
      body: { from_date: '2026-10-01', to_date: '2026-10-31', interval,
        locations: ['61', 62], providers: ['31'], product_types: ['71'],
        stocker_upload: ['7'], transported_by: ['41'] } });
    assert.equal(response.status, 200);
    assert.deepEqual(response.data, { name: 'Libras entregadas', series: [{ name: label, value: 12 }] });
    assert.deepEqual(f.statements.at(-1).params,
      [61, 62, 21, 'Y', '2026-10-01', '2026-10-31', 61, 62, 31, 7, 41, 71, 61, 62, 21]);
  }
});
