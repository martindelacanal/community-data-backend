'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createBoundedAsyncCache } = require('./boundedAsyncCache');

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

test('concurrent requests share work, expiry starts when computation finishes', async () => {
  let time = 0;
  let calls = 0;
  const work = deferred();
  const cache = createBoundedAsyncCache({ now: () => time });
  const compute = () => { calls++; return work.promise; };
  const first = cache.getOrCompute('same', 30, compute);
  const second = cache.getOrCompute('same', 30, compute);
  assert.equal(first, second);
  await Promise.resolve();
  assert.equal(calls, 1);
  time = 50;
  work.resolve({ count: 10 });
  assert.deepEqual(await first, { count: 10 });
  time = 79;
  assert.deepEqual(await cache.getOrCompute('same', 30, () => { throw Error('unexpected'); }), { count: 10 });
  time = 80;
  assert.equal(await cache.getOrCompute('same', 30, () => 'fresh'), 'fresh');
});

test('invalidation during an in-flight query cannot repopulate stale results', async () => {
  const oldWork = deferred();
  const cache = createBoundedAsyncCache();
  const old = cache.getOrCompute('key', 1000, () => oldWork.promise);
  await Promise.resolve();
  cache.clear();
  assert.equal(await cache.getOrCompute('key', 1000, () => 'new'), 'new');
  oldWork.resolve('old');
  assert.equal(await old, 'old');
  assert.equal(await cache.getOrCompute('key', 1000, () => 'incorrect'), 'new');
});

test('failures are retriable and an older rejection cannot remove a replacement', async () => {
  const cache = createBoundedAsyncCache();
  await assert.rejects(cache.getOrCompute('key', 1000, () => { throw Error('database failed'); }), /database failed/);
  assert.equal(await cache.getOrCompute('key', 1000, () => 7), 7);
  let rejectOld;
  const old = cache.getOrCompute('other', 1000, () => new Promise((_, reject) => { rejectOld = reject; }));
  await Promise.resolve();
  cache.clear();
  await cache.getOrCompute('other', 1000, () => 8);
  rejectOld(Error('old failed'));
  await assert.rejects(old, /old failed/);
  assert.equal(await cache.getOrCompute('other', 1000, () => 9), 8);
});

test('LRU entries and total bytes are bounded; oversized values are not retained', async () => {
  const cache = createBoundedAsyncCache({ maxEntries: 2, maxBytes: 12 });
  await cache.getOrCompute('a', 1000, () => 'aa');
  await cache.getOrCompute('b', 1000, () => 'bb');
  await cache.getOrCompute('a', 1000, () => 'wrong');
  await cache.getOrCompute('c', 1000, () => 'cc');
  assert.equal(await cache.getOrCompute('a', 1000, () => 'wrong'), 'aa');
  assert.equal(await cache.getOrCompute('b', 1000, () => 'new-b'), 'new-b');
  await cache.getOrCompute('large', 1000, () => 'x'.repeat(100));
  assert.equal(await cache.getOrCompute('large', 1000, () => 'retry'), 'retry');

  const bytes = createBoundedAsyncCache({ maxEntries: 100, maxBytes: 8 });
  await bytes.getOrCompute('first', 1000, () => 'aaaa'); // 6 JSON bytes
  await bytes.getOrCompute('second', 1000, () => 'bbbb');
  assert.equal(await bytes.getOrCompute('first', 1000, () => 'new'), 'new');
});

test('a full in-flight cache does not grow and keeps deduplicating admitted keys', async () => {
  const cache = createBoundedAsyncCache({ maxEntries: 1 });
  const work = deferred();
  const first = cache.getOrCompute('first', 1000, () => work.promise);
  assert.equal(cache.getOrCompute('first', 1000, () => 2), first);
  assert.equal(await cache.getOrCompute('bypass', 1000, () => 3), 3);
  assert.equal(await cache.getOrCompute('bypass', 1000, () => 4), 4);
  work.resolve(1);
  assert.equal(await first, 1);
});
