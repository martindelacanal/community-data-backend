'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  HEALTH_QR_SLIDING_DEBOUNCE_MS,
  SlidingHealthQrDebounce,
  buildHealthScanDebounceKey,
  findRecentHealthScan,
  findTodayHealthCheckins,
  resolveDailyScanDecision
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

// ---------------------------------------------------------------------------
// One visit per stand per day
// ---------------------------------------------------------------------------

function checkin(overrides = {}) {
  return {
    id: 10,
    serviceId: null,
    volunteerUserId: 500,
    scannedAt: new Date('2026-08-08T16:05:00Z'),
    checkoutId: null,
    checkoutAt: null,
    ...overrides
  };
}

test('daily rule: first scan of the day is a check-in everywhere', () => {
  assert.deepEqual(
    resolveDailyScanDecision({ hasCheckout: false, serviceId: null, todayCheckins: [], allowRepeat: false }),
    { kind: 'checkin' }
  );
  assert.deepEqual(
    resolveDailyScanDecision({ hasCheckout: true, serviceId: null, todayCheckins: [], allowRepeat: false }),
    { kind: 'checkin' }
  );
});

test('daily rule: a stand without check-out refuses a second scan the same day', () => {
  const previous = checkin({ id: 41 });
  const decision = resolveDailyScanDecision({
    hasCheckout: false, serviceId: null, todayCheckins: [previous], allowRepeat: false
  });
  assert.deepEqual(decision, { kind: 'already_served', previous });

  // Old clients (no allow_repeat flag) are refused as well: this rule is not optional.
  assert.equal(
    resolveDailyScanDecision({ hasCheckout: false, serviceId: null, todayCheckins: [previous], allowRepeat: null }).kind,
    'already_served'
  );
});

test('daily rule: services at the same stand are independent', () => {
  const ihss = checkin({ id: 41, serviceId: 1 });
  assert.equal(
    resolveDailyScanDecision({ hasCheckout: false, serviceId: 6, todayCheckins: [ihss], allowRepeat: false }).kind,
    'checkin'
  );
  assert.equal(
    resolveDailyScanDecision({ hasCheckout: false, serviceId: 1, todayCheckins: [ihss], allowRepeat: false }).kind,
    'already_served'
  );
  // service null and service 0 are the same bucket (COALESCE(service_id, 0)).
  const noService = checkin({ id: 42, serviceId: null });
  assert.equal(
    resolveDailyScanDecision({ hasCheckout: false, serviceId: 0, todayCheckins: [noService], allowRepeat: false }).kind,
    'already_served'
  );
});

test('daily rule: reports the newest scan of today when several exist', () => {
  const newest = checkin({ id: 90, scannedAt: new Date('2026-08-08T18:00:00Z') });
  const oldest = checkin({ id: 80, scannedAt: new Date('2026-08-08T16:00:00Z') });
  const decision = resolveDailyScanDecision({
    hasCheckout: false, serviceId: null, todayCheckins: [newest, oldest], allowRepeat: false
  });
  assert.equal(decision.previous.id, 90);
});

test('daily rule: a stand with check-out closes the open visit first', () => {
  const open = checkin({ id: 41 });
  assert.deepEqual(
    resolveDailyScanDecision({ hasCheckout: true, serviceId: null, todayCheckins: [open], allowRepeat: false }),
    { kind: 'checkout', pairedScanId: 41 }
  );
  // The open visit wins even when a completed one is newer in the list.
  const completed = checkin({ id: 45, checkoutId: 46, checkoutAt: new Date('2026-08-08T16:20:00Z') });
  assert.deepEqual(
    resolveDailyScanDecision({ hasCheckout: true, serviceId: null, todayCheckins: [completed, open], allowRepeat: false }),
    { kind: 'checkout', pairedScanId: 41 }
  );
});

test('daily rule: after a completed visit a new one needs explicit confirmation', () => {
  const completed = checkin({ id: 45, checkoutId: 46, checkoutAt: new Date('2026-08-08T16:20:00Z') });
  assert.deepEqual(
    resolveDailyScanDecision({ hasCheckout: true, serviceId: null, todayCheckins: [completed], allowRepeat: false }),
    { kind: 'confirm_repeat', previous: completed }
  );
  // Confirmed by the volunteer => a genuine new check-in.
  assert.deepEqual(
    resolveDailyScanDecision({ hasCheckout: true, serviceId: null, todayCheckins: [completed], allowRepeat: true }),
    { kind: 'checkin' }
  );
  // Old clients never send the flag and keep the pre-existing behaviour.
  assert.deepEqual(
    resolveDailyScanDecision({ hasCheckout: true, serviceId: null, todayCheckins: [completed], allowRepeat: null }),
    { kind: 'checkin' }
  );
});

test('daily lookup maps rows newest first with their paired check-out', async () => {
  const calls = [];
  const connection = {
    async query(sql, params) {
      calls.push({ sql, params });
      return [[
        { id: '52', service_id: null, volunteer_user_id: 7, scanned_at: new Date('2026-08-08T18:00:00Z'), checkout_id: null, checkout_at: null },
        { id: '40', service_id: '3', volunteer_user_id: 8, scanned_at: new Date('2026-08-08T16:00:00Z'), checkout_id: '41', checkout_at: new Date('2026-08-08T16:30:00Z') }
      ]];
    }
  };
  const rows = await findTodayHealthCheckins(connection, { standId: 2, userId: 99, timezone: 'America/Los_Angeles' });
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /scan_type = 'checkin'/);
  assert.match(calls[0].sql, /CONVERT_TZ/);
  assert.match(calls[0].sql, /FOR UPDATE/);
  assert.deepEqual(calls[0].params, [2, 99, 'America/Los_Angeles', 'America/Los_Angeles']);
  assert.deepEqual(rows.map(r => [r.id, r.serviceId, r.volunteerUserId, r.checkoutId]), [[52, null, 7, null], [40, 3, 8, 41]]);
  assert.equal(rows[1].checkoutAt.toISOString(), '2026-08-08T16:30:00.000Z');
});
