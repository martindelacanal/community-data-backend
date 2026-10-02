'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function loadHandlers() {
  const source = fs.readFileSync(require.resolve('./user'), 'utf8');
  const start = source.indexOf("const { sendReportDownload } = require('../services/reportStream');");
  const end = source.indexOf('// Headers base para CSV', start);
  assert.ok(start >= 0 && end > start);
  const handlers = new Map(), exports = [];
  const middleware = () => {};
  vm.runInNewContext(source.slice(start, end), {
    verifyToken: middleware,
    router: { post(path, auth, handler) { assert.equal(auth, middleware); handlers.set(path, handler); } },
    require(name) {
      if (name === '../services/reportStream') return { sendReportDownload: (req, res, create) => create(undefined) };
      if (name === '../services/healthMetrics') return { streamHealthMetricsCsv: options => { exports.push({ kind: 'health', options }); } };
      if (name === '../services/specificHealthReports') return { streamAllSpecificReportsZip: options => { exports.push({ kind: 'specific', options }); } };
      throw new Error(name);
    }
  });
  return { handlers, exports };
}

function response() {
  return { status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
}

test('CSV preserves role validation and forwards the authenticated client scope and exact filters', async () => {
  const loaded = loadHandlers();
  const handler = loaded.handlers.get('/metrics/health/download-csv');
  for (const [payload, status] of [['invalid JSON', 401], ['{"role":"director"}', 403], ['{"role":"beneficiary"}', 403], ['{"role":"client"}', 400]]) {
    const res = response();
    await handler({ data: { data: payload } }, res);
    assert.equal(res.code, status);
  }
  assert.equal(loaded.exports.length, 0);
  const filters = { client_id: 999, locations: [7], from_date: '2026-09-01' };
  await handler({ data: { data: '{"role":"client","client_id":1}' }, body: filters, query: { language: 'es' } }, response());
  assert.equal(loaded.exports[0].options.cabecera.client_id, 1);
  assert.equal(loaded.exports[0].options.filters, filters);
  assert.equal(loaded.exports[0].options.language, 'es');
});

test('specific ZIP stays admin-only and forwards the report filters', async () => {
  const loaded = loadHandlers();
  const handler = loaded.handlers.get('/metrics/health/download-specific-csv');
  const denied = response();
  await handler({ data: { data: '{"role":"client","client_id":1}' } }, denied);
  assert.equal(denied.code, 403);
  assert.equal(loaded.exports.length, 0);
  const filters = { from_date: '2026-09-01' };
  await handler({ data: { data: '{"role":"admin"}' }, body: filters, query: {} }, response());
  assert.equal(loaded.exports[0].kind, 'specific');
  assert.equal(loaded.exports[0].options.filters, filters);
});
