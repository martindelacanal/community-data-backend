'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  HEALTH_QR_SLIDING_DEBOUNCE_MS,
  SlidingHealthQrDebounce,
  buildHealthScanDebounceKey,
  findRecentHealthScan
} = require('./healthScanGuard');

test('builds an event + stand + beneficiary debounce key', () => {
  assert.equal(buildHealthScanDebounceKey(7, 11, 42), '7:11:42');
});

test('uses a ten-second sliding QR window refreshed by every repeated hit', () => {
  let nowMs = 1_000;
  const debounce = new SlidingHealthQrDebounce({ now: () => nowMs });
  const key = buildHealthScanDebounceKey(1, 2, 3);

  assert.equal(HEALTH_QR_SLIDING_DEBOUNCE_MS, 10_000);
  debounce.remember(key, 91, 'checkin');

  nowMs += 9_000;
  assert.deepEqual(debounce.take(key), { scanId: 91, scanType: 'checkin' });

  // This remains live more than ten seconds after the original scan because
  // the preceding repeated frame refreshed the sliding boundary.
  nowMs += 9_000;
  assert.deepEqual(debounce.take(key), { scanId: 91, scanType: 'checkin' });

  nowMs += 10_000;
  assert.equal(debounce.take(key), null);
});

test('keeps stands independent and remembers a later manual state transition', () => {
  let nowMs = 10_000;
  const debounce = new SlidingHealthQrDebounce({ now: () => nowMs });
  const dental = buildHealthScanDebounceKey(1, 2, 99);
  const vision = buildHealthScanDebounceKey(1, 3, 99);

  debounce.remember(dental, 100, 'checkin');
  assert.equal(debounce.take(vision), null);

  nowMs += 1_000;
  debounce.remember(dental, 101, 'checkout');
  assert.deepEqual(debounce.take(dental), { scanId: 101, scanType: 'checkout' });
});

test('bounds memory by evicting the least-recently-seen entry', () => {
  let nowMs = 0;
  const debounce = new SlidingHealthQrDebounce({ maxEntries: 2, now: () => nowMs });

  debounce.remember('first', 1, 'checkin');
  nowMs += 1;
  debounce.remember('second', 2, 'checkin');
  nowMs += 1;
  debounce.remember('third', 3, 'checkin');

  assert.equal(debounce.take('first'), null);
  assert.deepEqual(debounce.take('second'), { scanId: 2, scanType: 'checkin' });
  assert.deepEqual(debounce.take('third'), { scanId: 3, scanType: 'checkin' });
});

test('queries the latest scan of either type before state selection', async () => {
  let capturedSql = '';
  let capturedParams = null;
  const connection = {
    query: async (sql, params) => {
      capturedSql = sql;
      capturedParams = params;
      return [[{ id: '123', scan_type: 'checkout' }]];
    }
  };

  const result = await findRecentHealthScan(connection, {
    eventId: 5,
    standId: 8,
    userId: 13,
    windowSeconds: 10
  });

  assert.deepEqual(result, { scanId: 123, scanType: 'checkout' });
  assert.deepEqual(capturedParams, [5, 8, 13, 10]);
  assert.match(capturedSql, /health_event_id\s*=\s*\?/i);
  assert.match(capturedSql, /LIMIT 1/i);
  assert.doesNotMatch(capturedSql, /FOR UPDATE/i);
  assert.doesNotMatch(capturedSql, /AND\s+scan_type\s*=/i);
});

test('returns null when the durable window contains no scan', async () => {
  const connection = { query: async () => [[]] };
  const result = await findRecentHealthScan(connection, {
    eventId: 1,
    standId: 2,
    userId: 3,
    windowSeconds: 10
  });

  assert.equal(result, null);
});
