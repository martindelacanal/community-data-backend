'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { backfillHealthEventConfiguration } = require('./backfillHealthEventConfiguration');

function fixture({ occupied = false, registered = false } = {}) {
  const lifecycle = [], statements = [];
  const events = [{id:1,slug:'banning',start_date:'2026-08-08',end_date:'2026-08-09'}, {id:3,slug:'san-jacinto',start_date:'2026-12-05',end_date:'2026-12-06',enabled:'Y',landing_enabled:'Y',public_home_visible:'N',promo_dialog_enabled:'N',landing_json:'{"edited":true}'}];
  const connection = {
    async beginTransaction() { lifecycle.push('begin'); }, async rollback() { lifecycle.push('rollback'); },
    async commit() { lifecycle.push('commit'); }, release() { lifecycle.push('release'); },
    async query(sql, params) {
      statements.push(sql);
      if (sql.startsWith('SELECT * FROM health_event WHERE')) return [events];
      if (/FROM health_event_registration/.test(sql)) return [registered ? [{id:123}] : []];
      if (occupied && params[0] === 3 && /FROM health_event_form/.test(sql)) return [[{id:7}]];
      if (sql.startsWith('SELECT')) return [[]];
      throw new Error('Unexpected write: ' + sql);
    }
  };
  const options = {pool:{async getConnection(){return connection;}},sourceId:1,targetId:3,sourceSlug:'banning',targetSlug:'san-jacinto'};
  return {options,lifecycle,statements,events};
}

test('dry-run preserves client edits and does not write', async () => {
  const f = fixture(), before = JSON.stringify(f.events);
  const result = await backfillHealthEventConfiguration(f.options);
  assert.equal(result.status, 'dry-run'); assert.equal(result.preservedFlags.enabled, 'Y');
  assert.deepEqual(result.dates, {start:'2026-12-05',end:'2026-12-06'});
  assert.equal(JSON.stringify(f.events), before);
  assert.deepEqual(f.lifecycle, ['begin','rollback','release']);
  assert.ok(f.statements.every(sql => sql.startsWith('SELECT')));
});
for (const mode of ['occupied','registered']) test(`refuses an ${mode} target before any writes`, async () => {
  const f = fixture({[mode]:true});
  await assert.rejects(backfillHealthEventConfiguration({...f.options,apply:true,backup:async()=>assert.fail('Must not backup or write')}), /Target already/);
  assert.deepEqual(f.lifecycle, ['begin','rollback','release']);
});
test('checks identities and requires a successful backup before applying', async () => {
  const wrong = fixture(); wrong.options.targetSlug = 'wrong';
  await assert.rejects(backfillHealthEventConfiguration(wrong.options), /Target identity changed/);
  const f = fixture();
  await assert.rejects(backfillHealthEventConfiguration({...f.options,apply:true}), /backup must be saved/);
  const failure = fixture();
  await assert.rejects(backfillHealthEventConfiguration({...failure.options,apply:true,backup:async()=>{throw new Error('disk unavailable');}}), /disk unavailable/);
  assert.deepEqual(failure.lifecycle, ['begin','rollback','release']);
});
test('applies an empty configuration only after backup and verifies unchanged presentation', async () => {
  const f = fixture(); let snapshot;
  const result = await backfillHealthEventConfiguration({...f.options,apply:true,backup:async data=>{snapshot=data;f.lifecycle.push('backup');}});
  assert.equal(snapshot.events[1].landing_json, '{"edited":true}');
  assert.equal(result.status, 'applied'); assert.equal(result.targetPresentationUnchanged, true);
  assert.deepEqual(f.lifecycle, ['begin','backup','commit','release']);
});
