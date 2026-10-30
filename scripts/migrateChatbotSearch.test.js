'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { migrateSearch } = require('./migrateChatbotSearch');

test('additive search migration backfills in bounded batches across ID gaps and can rerun', async () => {
  const ids = Array.from({ length: 11354 }, (_, i) => (i + 1) * 2);
  const indexed = new Set([2, 4]); const calls = [];
  const db = { query: async (sql, params) => {
    calls.push(sql);
    if (sql.startsWith('CREATE TABLE')) { assert.match(sql, /ON DELETE CASCADE/); assert.match(sql, /FULLTEXT/); return [[]]; }
    if (sql.startsWith('SELECT id')) return [ids.filter(id => id > params[0]).slice(0, 500).map(id => ({ id }))];
    if (sql.startsWith('INSERT')) {
      assert.match(sql, /i.chunk_id IS NULL/);
      ids.filter(id => id > params[0] && id <= params[1]).forEach(id => indexed.add(id)); return [{ affectedRows: 0 }];
    }
    return [[{ missing: ids.filter(id => !indexed.has(id)).length }]];
  }};
  await migrateSearch(db, () => {}); await migrateSearch(db, () => {});
  assert.equal(indexed.size, 11354);
  assert.equal(calls.filter(sql => sql.startsWith('INSERT')).length, 46);
  assert.ok(calls.every(sql => !/ALTER|DELETE|UPDATE/.test(sql.replace('ON DELETE CASCADE', ''))));
});

test('migration fails verification when a chunk was not indexed', async () => {
  const db = { query: async sql => sql.includes('SELECT COUNT(*)') ? [[{ missing: 1 }]] : [[]] };
  await assert.rejects(migrateSearch(db, () => {}), /chatbot_search_backfill_incomplete/);
});
