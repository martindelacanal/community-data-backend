'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createBoundedAsyncCache } = require('./boundedAsyncCache');

function fixture() {
  const calls = [];
  const source = fs.readFileSync(path.join(__dirname, 'participantRegistrationMetrics.js'), 'utf8');
  const module = { exports: {} };
  vm.runInNewContext(source, {
    module,
    require(name) {
      if (name === './boundedAsyncCache') return { createBoundedAsyncCache };
      if (name === './rawDataReport') return { EXCLUDED_REPORT_USER_IDS: [9999] };
      assert.equal(name, '../connection/connection');
      return { promise: () => ({ async query(sql, params) {
        calls.push({ sql, params: Array.from(params) });
        return [[{ total: 10, new: 3, recurring: 8, recurring_without_new: 7, participations: 13 }]];
      } }) };
    }
  });
  return { api: module.exports, calls };
}

test('participant aggregates reuse normalized identical filters but isolate clients/exclusions', async () => {
  const { api, calls } = fixture();
  const scope = { role: 'client', client_id: 1 };
  const first = api.getParticipantRegisterSummary(scope, { locations: ['3', 2, 3], client_id: 9 });
  const repeated = api.getParticipantRegisterSummary(scope, { locations: [2, 3], client_id: 99 });
  const results = await Promise.all([first, repeated]);
  assert.equal(calls.length, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(results[0])), { total: 10, new: 3, recurring: 8, recurring_without_new: 7, participations: 13 });
  assert.equal(calls[0].params[0], 1, 'client cannot override scope through request filters');
  await api.getParticipantRegisterSummary({ role: 'client', client_id: 2 }, { locations: [2, 3] });
  await api.getParticipantRegisterSummary(scope, { locations: [2, 3] }, { excludedUserIds: [88] });
  await api.getParticipantRegisterSummary(scope, { locations: [2, 3], from_date: '2026-09-01' });
  assert.equal(calls.length, 4);
  assert.match(calls[0].sql, /db_same_day\.delivering_user_id IS NOT NULL/);
  assert.match(calls[0].sql, /db_prev\.creation_date < db_range\.creation_date/);
  assert.match(calls[0].sql, /America\/Los_Angeles/);
});
