'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { createHash } = require('node:crypto');
const { once } = require('node:events');
const realRequire = createRequire(__filename);

function fixture(count = 1001) {
  const users = Array.from({ length: count }, (_, index) => ({
    user_id: index + 1, username: index ? `person-${index + 1}` : 'CSV; "quoted"',
    email: '', firstname: 'Fictitious', lastname: 'Person', language: 'es',
    date_of_birth: '01/01/1990', age: 36, phone: '', zipcode: '00000', household_size: 1,
    gender: 'Other', ethnicity: 'Other', other_ethnicity: '', second_ethnicity: null,
    other_second_ethnicity: '', preferred_language: 'Spanish', other_language: '',
    first_location_visited: 'Clinic', last_location_visited: 'Clinic',
    registration_date: '09/01/2026', registration_time: '08:00:00'
  }));
  const events = [
    { user_id: 1, location_id: 7, approved: 'N', event_date: '09/01/2026', event_time: '09:30:00', event_date_key: '2026-09-01', event_datetime_la: '2026-09-01 09:30:00', event_location: 'Clinic' },
    { user_id: 1, location_id: 7, approved: 'N', event_date: '09/01/2026', event_time: '17:00:00', event_date_key: '2026-09-01', event_datetime_la: '2026-09-01 17:00:00', event_location: 'Clinic' },
    { user_id: count, location_id: 7, approved: 'Y', event_date: '09/01/2026', event_time: '09:00:00', event_date_key: '2026-09-01', event_datetime_la: '2026-09-01 09:00:00', event_location: 'Clinic' }
  ];
  const queries = [];
  let active = 0, maxActive = 0, releases = 0, destroys = 0;
  function rows(sql, params = []) {
    queries.push({ sql, params });
    if (sql.includes('FROM question q')) return [
      { question_id: 1, question: 'Coverage; "sí"', question_order: 1, answer_type_id: 3, required: 'Y', answer_id: 11, answer: 'Sí', answer_order: 1 },
      { question_id: 2, question: 'Required missing', question_order: 2, answer_type_id: 3, required: 'Y', answer_id: 21, answer: 'Yes', answer_order: 1 },
      { question_id: 3, question: 'Conditional', question_order: 3, answer_type_id: 3, required: 'Y', depends_on_question_id: 1, depends_on_answer_id: 11, answer_id: 31, answer: 'Yes', answer_order: 1 }
    ];
    if (sql.includes('FROM user u') && !sql.includes('u.username')) return users.map(user => ({ user_id: user.user_id }));
    if (sql.includes('u.username')) return Array.isArray(params[0]) ? users.filter(user => params[0].includes(user.user_id)) : users;
    if (sql.includes('AS first_scan')) return params[0].includes(count) ? [{ location_id: 7, event_date_key: '2026-09-01', first_scan: '2026-09-01 09:00:00', last_scan: '2026-09-01 09:00:00' }] : [];
    if (sql.includes('AS delivery_count')) return params.at(-1).map(id => ({ user_id: id, locations_visited: id === 1 || id === count ? 'Clinic' : '', delivery_count: id === 1 ? 2 : id === count ? 1 : 0 }));
    if (sql.includes('AS delivery_beneficiary_id')) return events.filter(event => params[0].includes(event.user_id));
    if (sql.includes('AS answer_value')) return params[0].filter(id => id === 1 || id === count).map(id => ({ user_id: id, question_id: 1, answer_value: 'Sí; "quoted"', answer_ids: '11' }));
    throw new Error('Unrecognized synthetic query');
  }
  const pool = {
    promise: () => ({ query: async (sql, params) => [rows(sql, params)] }),
    getConnection(callback) {
      active++; maxActive = Math.max(maxActive, active);
      let ended = false;
      const finish = () => { if (!ended) { ended = true; active--; } };
      callback(null, {
        query: (options, params, done) => { try { done(null, rows(options.sql, params)); } catch (error) { done(error); } },
        release: () => { releases++; finish(); }, destroy: () => { destroys++; finish(); }
      });
    }
  };
  return { pool, queries, stats: () => ({ active, maxActive, releases, destroys }) };
}

function loadHealth(data, source = fs.readFileSync(require.resolve('./healthMetrics'), 'utf8')) {
  const module = { exports: {} };
  vm.runInNewContext(source, {
    module, exports: module.exports, Buffer, console, AbortController, setTimeout, clearTimeout,
    require: name => name === '../connection/connection' ? data.pool : realRequire(name)
  }, { filename: require.resolve('./healthMetrics') });
  return module.exports;
}

async function consume(body) {
  const chunks = [];
  for await (const chunk of body) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

test('paged health CSV preserves the pre-optimization golden bytes across the 1000-user boundary', async () => {
  const data = fixture();
  const service = loadHealth(data);
  const report = service.streamHealthMetricsCsv({ cabecera: { role: 'client', client_id: 1 }, language: 'es' });
  const csv = await consume(report.body);
  // Frozen from the previous implementation for these synthetic records. Includes
  // quoting, accents, missing/conditional answers, all rows and global scan times.
  assert.equal(createHash('sha256').update(csv).digest('hex'), '43b69b20dae4da8baf680a08e5020ed13108fb351a32d6691099f88817f6ba50');
  assert.equal(report.getRowCount(), 1002);
  assert.match(csv, /09:30:00;Clinic/);
  assert.match(csv, /Unmatched;Clinic/);
  const hydration = data.queries.filter(item => item.sql.includes('u.username'));
  assert.deepEqual(hydration.map(item => item.params[0].length), [1000, 1]);
  assert.ok(hydration.every(item => item.sql.includes('cu.client_id = ?') && item.params.at(-1) === 1));
  assert.equal(data.stats().active, 0);
  assert.equal(data.stats().destroys, 0);
});

test('consumer backpressure stops later hydration pages and destroying stops remaining queries', async () => {
  const data = fixture(6001);
  const report = loadHealth(data).streamHealthMetricsCsv({ cabecera: { role: 'admin' } });
  report.body.read(1);
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.ok(data.queries.filter(item => item.sql.includes('u.username')).length <= 1);
  const closed = once(report.body, 'close');
  report.body.destroy();
  await closed;
  const count = data.queries.length;
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(data.queries.length, count);
  assert.equal(data.stats().active, 0);
});

test('empty report still emits the complete header and no hydration query', async () => {
  const data = fixture(0);
  const report = loadHealth(data).streamHealthMetricsCsv({ cabecera: { role: 'admin' } });
  const csv = await consume(report.body);
  assert.equal(csv.split('\n').length, 2);
  assert.match(csv, /^User ID;Username;/);
  assert.match(csv, /Conditional/);
  assert.equal(report.getRowCount(), 0);
  assert.equal(data.queries.filter(item => item.sql.includes('u.username')).length, 0);
});

module.exports = { fixture, loadHealth, consume };
