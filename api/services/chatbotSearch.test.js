'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { indexedCandidates, serializedScan } = require('./chatbotSearch');

test('index searches all live sources but transfers at most 512 diverse vectors', async () => {
  const calls = [];
  const db = { query: async (sql, params) => {
    calls.push({ sql, params });
    assert.match(sql, /s.enabled=1 AND s.status='ready'/);
    if (sql.includes('MATCH(')) {
      assert.match(sql, /LIMIT 2048/);
      return [Array.from({ length: 2048 }, (_, i) => ({ chunk_id: 200000 + i, source_id: Math.floor(i / 16) }))];
    }
    return [params[0].map(id => ({ id }))];
  } };
  const result = await indexedCandidates(db, 'strawberry fresa');
  assert.equal(result.length, 512);
  assert.equal(calls.length, 5);
  assert.ok(calls.slice(1).every(call => call.params[0].length <= 128));
  assert.deepEqual(calls[0].params, ['strawberry fresa', 'strawberry fresa']);
  assert.equal(result[8].id, 200016);
  assert.ok(result.some(row => row.id > 200000));
});

test('index without matches avoids transferring vectors', async () => {
  let calls = 0;
  assert.deepEqual(await indexedCandidates({ query: async () => { calls++; return [[]]; } }, 'absent'), []);
  assert.equal(calls, 1);
});

test('fallback scans cannot overlap and failure releases the next scan', async () => {
  let active = 0, peak = 0;
  const results = await Promise.allSettled(Array.from({ length: 8 }, (_, i) => serializedScan(async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setImmediate(resolve));
    active--; if (i === 0) throw new Error('failure'); return i;
  })));
  assert.equal(peak, 1);
  assert.equal(results[0].status, 'rejected');
  assert.ok(results.slice(1).every(result => result.status === 'fulfilled'));
});
