'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter, once } = require('node:events');
const { Readable, Writable } = require('node:stream');
const { createReportQuery, createReportReadable, sendReportDownload } = require('./reportStream');

test('cancellation destroys only the active export connection', async () => {
  const controller = new AbortController();
  let destroyed = 0, released = 0;
  const pool = { getConnection: callback => callback(null, {
    query() {}, destroy() { destroyed++; }, release() { released++; }
  }) };
  const query = createReportQuery(pool, { signal: controller.signal });
  const pending = query('SELECT 1');
  controller.abort();
  await assert.rejects(pending, { code: 'ABORT_ERR' });
  assert.equal(destroyed, 1);
  assert.equal(released, 0);
});

test('an export cancelled in the pool queue releases the eventual connection without running SQL', async () => {
  const controller = new AbortController();
  let acquire, queries = 0, released = 0;
  const pending = createReportQuery({ getConnection(callback) { acquire = callback; } }, { signal: controller.signal })('SELECT 1');
  controller.abort();
  await assert.rejects(pending, { code: 'ABORT_ERR' });
  acquire(null, { query() { queries++; }, release() { released++; } });
  assert.equal(queries, 0);
  assert.equal(released, 1);
});

test('query timeout bounds stalled work and destroys its connection', async () => {
  let destroyed = 0;
  const keepAlive = setTimeout(() => {}, 1000);
  try {
    const query = createReportQuery({ getConnection: callback => callback(null, {
      query(options) { assert.match(options.sql, /MAX_EXECUTION_TIME\(15\)/); },
      destroy() { destroyed++; }
    }) }, { timeoutMs: 15 });
    await assert.rejects(query('SELECT 1'), { code: 'REPORT_QUERY_TIMEOUT' });
    assert.equal(destroyed, 1);
  } finally { clearTimeout(keepAlive); }
});

test('destroying a report aborts its in-flight generator query immediately', async () => {
  const controller = new AbortController();
  let destroyed = 0;
  const query = createReportQuery({ getConnection: callback => callback(null, {
    query() {}, destroy() { destroyed++; }
  }) }, { signal: controller.signal });
  async function* rows() { await query('SELECT 1'); yield 'unreachable'; }
  const body = createReportReadable(rows(), controller);
  body.resume();
  await new Promise(resolve => setImmediate(resolve));
  const closed = new Promise(resolve => body.once('close', resolve));
  body.destroy();
  await closed;
  assert.equal(destroyed, 1);
  assert.equal(controller.signal.aborted, true);
});

function response(write) {
  const res = new Writable({ write: write || ((chunk, encoding, callback) => callback()) });
  res.headers = {};
  res.setHeader = (key, value) => { res.headers[key] = value; };
  res.removeHeader = key => { delete res.headers[key]; };
  res.status = code => { res.statusCode = code; return res; };
  res.json = value => { res.jsonBody = value; res.end(); };
  return res;
}

test('HTTP download uses pipeline and completes all bytes with private response headers', async () => {
  const chunks = [];
  const req = new EventEmitter();
  const res = response((chunk, encoding, callback) => { chunks.push(chunk); setImmediate(callback); });
  await sendReportDownload(req, res, async () => ({ body: Readable.from(['header\n', 'row\n']), fileName: 'health-metrics.csv' }));
  assert.equal(Buffer.concat(chunks).toString(), 'header\nrow\n');
  assert.equal(res.headers['Cache-Control'], 'no-store');
  assert.equal(res.headers['X-Accel-Buffering'], 'no');
  assert.equal(res.writableFinished, true);
});

test('HTTP disconnect cancels preparation before any body is produced', async () => {
  const req = new EventEmitter();
  const res = response();
  let aborted = false;
  const pending = sendReportDownload(req, res, signal => new Promise((resolve, reject) => {
    signal.addEventListener('abort', () => { aborted = true; reject(Object.assign(new Error('cancelled'), { code: 'ABORT_ERR' })); }, { once: true });
  }));
  res.destroy();
  await pending;
  assert.equal(aborted, true);
  assert.equal(req.listenerCount('aborted'), 0);
  assert.equal(res.listenerCount('close'), 0);
});

test('HTTP permission-independent preparation errors return JSON before streaming', async () => {
  const req = new EventEmitter();
  const res = response();
  await sendReportDownload(req, res, async () => { throw Object.assign(new Error('timeout'), { code: 'REPORT_QUERY_TIMEOUT' }); });
  assert.equal(res.statusCode, 504);
  assert.equal(res.jsonBody, 'Could not generate report');
});
