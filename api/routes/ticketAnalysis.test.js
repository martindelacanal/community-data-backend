const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const express = require('express');
const jwt = require('jsonwebtoken');
const { createTicketAnalysisRouter, parseAnalysisForm, createAnalysisLimiter } = require('./ticketAnalysis');

const SECRET = 'isolated-ticket-analysis-test-secret';
function token(role = 'admin', id = 7, options = {}) {
  return jwt.sign({ data: JSON.stringify({ role, id }) }, SECRET, { algorithm: 'HS256', expiresIn: '1h', ...options });
}

async function serve(t, options = {}) {
  const queries = [];
  const calls = [];
  const reads = [];
  const pool = { query: async (sql, params) => {
    queries.push({ sql, params });
    assert.match(sql, /^SELECT /u, 'analysis must never write to the database');
    if (sql.includes('FROM donation_ticket AS dt')) return [options.ticketExists === false ? [] : [{ id: 45 }]];
    if (sql.includes('FROM donation_ticket_image')) return [[{ id: 8, file: 'private-photo-key' }]];
    if (sql.includes('FROM product_type')) return [[{ id: 2, name: 'Produce', name_es: 'Frutas' }]];
    if (sql.includes('FROM product ORDER')) return [[{ id: 1, name: 'Apples', product_type_id: 2 }]];
    throw new Error('Unexpected SQL');
  } };
  const app = express();
  app.use(express.json());
  app.use('/api', createTicketAnalysisRouter({
    pool, env: { JWT_SECRET: SECRET }, limiterOptions: options.limiterOptions,
    service: {
      isConfigured: () => options.configured !== false,
      analyze: async (input) => {
        calls.push(input);
        if (options.failure) throw new Error('private database details and API key');
        return { items: [], warnings: [], model: 'test-model', draft: true };
      }
    },
    logger: options.logger,
    readStoredImage: async (key) => { reads.push(key); return Buffer.from('stored-image'); }
  }));
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); }));
  return {
    queries, calls, reads,
    post: (form = { language: 'en' }, { auth = token(), files = [Buffer.from('new-image')], mime = 'image/jpeg', field = 'ticket[]' } = {}) => {
      const body = new FormData();
      body.append('form', JSON.stringify(form));
      files.forEach((buffer, index) => body.append(field, new Blob([buffer], { type: mime }), `photo${index}.jpg`));
      return fetch(`http://127.0.0.1:${server.address().port}/api/upload/ticket/analyze`, {
        method: 'POST', headers: auth ? { Authorization: 'Bearer ' + auth } : {}, body
      });
    }
  };
}

test('analysis requires valid current credentials and allowed ticket roles before any reads or AI call', async (t) => {
  const f = await serve(t);
  for (const auth of [null, 'not-a-token', token('admin', 7, { expiresIn: -1 }), token('admin', 7, { algorithm: 'HS384' })]) assert.equal((await f.post({}, { auth })).status, 401);
  for (const role of ['client', 'beneficiary', 'contentmanager']) assert.equal((await f.post({}, { auth: token(role) })).status, 403);
  assert.equal(f.queries.length, 0);
  assert.equal(f.calls.length, 0);
});

test('new ticket reads catalog and analyzes uploaded photos without persistence; auditor must edit an existing ticket', async (t) => {
  const f = await serve(t);
  const response = await f.post({ language: 'es' });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal((await response.json()).draft, true);
  assert.equal(f.queries.length, 2);
  assert.equal(f.calls[0].language, 'es');
  assert.equal(f.calls[0].images[0].toString(), 'new-image');
  assert.equal((await f.post({ language: 'en' }, { auth: token('auditor') })).status, 403);
});

test('existing images are resolved through ticket-owned DB records and mixed photo order is preserved', async (t) => {
  const f = await serve(t);
  const response = await f.post({ ticket_id: 45, existing_image_ids: [8], image_order: [{ kind: 'new', index: 0 }, { kind: 'existing', id: 8 }], language: 'en' }, { auth: token('auditor') });
  assert.equal(response.status, 200);
  assert.deepEqual(f.reads, ['private-photo-key']);
  assert.deepEqual(f.calls[0].images.map((buffer) => buffer.toString()), ['new-image', 'stored-image']);
  assert.deepEqual(f.queries[1].params, [45]);
});

test('rejects cross-ticket IDs, arbitrary URLs, missing tickets and unauthorized stocker ticket access before image reads', async (t) => {
  const f = await serve(t);
  assert.equal((await f.post({ ticket_id: 45, existing_image_ids: [99] })).status, 400);
  assert.equal((await f.post({ existing_image_ids: [8] })).status, 400);
  assert.equal((await f.post({ urls: ['https://internal/private'] })).status, 400);
  assert.equal(f.reads.length, 0);
  assert.equal(f.calls.length, 0);
  const absent = await serve(t, { ticketExists: false });
  assert.equal((await absent.post({ ticket_id: 45, existing_image_ids: [8] }, { auth: token('stocker', 42), files: [] })).status, 404);
  assert.match(absent.queries[0].sql, /sl\.user_id = \?/u);
  assert.deepEqual(absent.queries[0].params, [45, 42]);
  assert.equal(absent.reads.length, 0);
});

test('multipart enforces four photos, file type and per-image size before analysis', async (t) => {
  const f = await serve(t);
  assert.equal((await f.post({}, { files: Array.from({ length: 5 }, () => Buffer.from('photo')) })).status, 400);
  assert.equal((await f.post({}, { mime: 'text/plain' })).status, 400);
  assert.equal((await f.post({}, { files: [Buffer.alloc(10 * 1024 * 1024 + 1)] })).status, 413);
  assert.equal((await f.post({}, { field: 'url' })).status, 400);
  assert.equal(f.calls.length, 0);
});

test('form rejects duplicate or missing image-order references, invalid IDs and more than four mixed photos', () => {
  for (const form of [
    { ticket_id: '45' }, { ticket_id: 45, existing_image_ids: [8, 8] },
    { image_order: [] }, { image_order: [{ kind: 'new', index: 1 }] },
    { image_order: [{ kind: 'new', index: 0, url: 'https://example.invalid' }] },
    { ticket_id: 45, existing_image_ids: [8, 9, 10, 11] }, { language: 'fr' }
  ]) assert.throws(() => parseAnalysisForm(JSON.stringify(form), 1), { code: 'ai_invalid_request' });
  assert.throws(() => parseAnalysisForm('{broken', 1), { code: 'ai_invalid_request' });
  assert.throws(() => parseAnalysisForm('{}', 0), { code: 'ai_invalid_request' });
});

test('disabled configuration and internal errors expose only stable codes without sensitive diagnostics', async (t) => {
  const disabled = await serve(t, { configured: false });
  assert.equal((await disabled.post()).status, 503);
  assert.equal(disabled.queries.length, 0);
  const logs = [];
  const failing = await serve(t, { failure: true, logger: { error: (...args) => logs.push(args) } });
  const response = await failing.post();
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: 'ai_provider_unavailable', message: 'ai_provider_unavailable' });
  assert.deepEqual(logs, [['Ticket analysis failed', { code: 'TICKET_ANALYSIS_ERROR' }]]);
});

test('limits per-user API spending and concurrent processing; slots release even when an operation fails', async (t) => {
  let now = 1;
  const limiter = createAnalysisLimiter({ clock: () => now, perUser: 2, maxConcurrent: 1 });
  const release = limiter.acquire(1);
  assert.throws(() => limiter.acquire(2), { code: 'ai_busy' });
  release(); release();
  limiter.acquire(1)();
  assert.throws(() => limiter.acquire(1), { code: 'ai_rate_limited' });
  now += 60001;
  limiter.acquire(1)();
  const f = await serve(t, { limiterOptions: { perUser: 1 } });
  assert.equal((await f.post()).status, 200);
  const response = await f.post();
  assert.equal(response.status, 429);
  assert.equal(response.headers.get('retry-after'), '60');
  assert.equal(f.calls.length, 1);
});
