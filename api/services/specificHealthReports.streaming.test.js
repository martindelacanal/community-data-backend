'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { Readable } = require('node:stream');
const JSZip = require('jszip');
const { createReportReadable, createReportQuery } = require('./reportStream');
const realRequire = createRequire(__filename);

function loadSpecific(streamHealthMetricsCsv) {
  const pool = { getConnection: callback => callback(null, {
    query: (options, params, done) => done(null, [{ id: 11, question_id: 3, name: 'IEHP' }, { id: 12, question_id: 3, name: 'Molina' }]),
    release() {}, destroy() {}
  }) };
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(require.resolve('./specificHealthReports'), 'utf8'), {
    module, exports: module.exports, Buffer, console,
    require: name => {
      if (name === '../connection/connection') return pool;
      if (name === './healthMetrics') return { streamHealthMetricsCsv };
      if (name === '../utils/logger') return { warn() {} };
      if (name === './rawDataReport') return { EXCLUDED_REPORT_USER_IDS: [] };
      return realRequire(name);
    }
  });
  return module.exports;
}

test('streamed ZIP has the same four named CSVs, correct client scopes, and exact contents', async () => {
  const captures = [];
  const service = loadSpecific(options => {
    captures.push(options);
    return { body: Readable.from([`id;name\n${options.cabecera.client_id};${options.fileName}\n`]), fileName: options.fileName };
  });
  const filters = { from_date: '2026-09-01', to_date: '2026-09-30', locations: [7] };
  const report = await service.streamAllSpecificReportsZip({ filters, language: 'es' });
  const chunks = [];
  for await (const chunk of report.body) chunks.push(chunk);
  const zip = await JSZip.loadAsync(Buffer.concat(chunks));
  const names = ['IEHP-eligibility.csv', 'IEHP-members-exclusive.csv', 'Molina-eligibility.csv', 'Molina-members-exclusive.csv'];
  assert.deepEqual(Object.keys(zip.files), names);
  for (let i = 0; i < names.length; i++) {
    const clientId = i < 2 ? 1 : 2;
    assert.equal(await zip.file(names[i]).async('string'), `id;name\n${clientId};${names[i]}\n`);
    assert.equal(captures[i].cabecera.role, 'client');
    assert.equal(captures[i].cabecera.client_id, clientId);
    assert.equal(captures[i].language, 'es');
    assert.equal(captures[i].filters, filters);
    assert.equal(typeof captures[i].userFilter, 'function');
  }
});

test('destroying an unfinished ZIP destroys all its CSV sources', async () => {
  const bodies = [];
  const service = loadSpecific(options => {
    const body = new Readable({ read() {} });
    bodies.push(body);
    return { body, fileName: options.fileName };
  });
  const report = await service.streamAllSpecificReportsZip();
  report.body.on('error', () => {});
  const closed = new Promise(resolve => report.body.once('close', resolve));
  report.body.destroy();
  await closed;
  assert.equal(bodies.length, 4);
  assert.ok(bodies.every(body => body.destroyed));
});

test('AbortSignal cancellation handles errors from active and queued CSV sources', async () => {
  const controller = new AbortController();
  const bodies = [];
  let activeConnections = 0;
  const service = loadSpecific(options => {
    const innerController = new AbortController();
    const query = createReportQuery({ getConnection: callback => {
      activeConnections++;
      callback(null, { query() {}, destroy() { activeConnections--; } });
    } }, { signal: innerController.signal });
    async function* rows() {
      yield Buffer.from('header\n');
      await query('SELECT 1');
      yield Buffer.from('unreachable\n');
    }
    const body = createReportReadable(rows(), innerController, options.signal);
    bodies.push(body);
    return { body, fileName: options.fileName };
  });
  const report = await service.streamAllSpecificReportsZip({ signal: controller.signal });
  const errors = [];
  report.body.on('error', error => errors.push(error));
  report.body.resume();
  await new Promise(resolve => setImmediate(resolve));
  const sourceClosures = bodies.map(body => new Promise(resolve => body.once('close', resolve)));
  const archiveClosed = new Promise(resolve => report.body.once('close', resolve));
  controller.abort();
  await Promise.all([archiveClosed, ...sourceClosures]);
  assert.ok(bodies.every(body => body.destroyed));
  assert.equal(activeConnections, 0);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].code, 'ABORT_ERR');
});
