'use strict';

const HEALTH_QR_SLIDING_DEBOUNCE_MS = 10 * 1000;
const HEALTH_QR_DEBOUNCE_MAX_ENTRIES = 20_000;

function buildHealthScanDebounceKey(eventId, standId, userId) {
  return `${eventId}:${standId}:${userId}`;
}

/**
 * Process-local, sliding debounce for continuously decoded QR frames.
 *
 * Every repeated hit refreshes lastSeenAt. A QR that remains in the camera is
 * therefore suppressed indefinitely, while removing it for windowMs allows a
 * later intentional scan. The database guard remains the durable fallback for
 * process restarts and concurrent requests.
 */
class SlidingHealthQrDebounce {
  constructor({
    windowMs = HEALTH_QR_SLIDING_DEBOUNCE_MS,
    maxEntries = HEALTH_QR_DEBOUNCE_MAX_ENTRIES,
    now = () => Date.now()
  } = {}) {
    if (!Number.isFinite(windowMs) || windowMs <= 0) {
      throw new TypeError('windowMs must be a positive number');
    }
    if (!Number.isInteger(maxEntries) || maxEntries <= 0) {
      throw new TypeError('maxEntries must be a positive integer');
    }
    if (typeof now !== 'function') {
      throw new TypeError('now must be a function');
    }

    this.windowMs = windowMs;
    this.maxEntries = maxEntries;
    this.now = now;
    this.entries = new Map();
  }

  /** Return and refresh a live entry, or null after the QR has been absent. */
  take(key) {
    const entry = this.entries.get(key);
    if (!entry) return null;

    const nowMs = this.now();
    if (nowMs - entry.lastSeenAtMs >= this.windowMs) {
      this.entries.delete(key);
      return null;
    }

    const refreshed = { ...entry, lastSeenAtMs: nowMs };
    // Reinsert so Map iteration keeps the least-recently-seen entry first.
    this.entries.delete(key);
    this.entries.set(key, refreshed);
    return { scanId: refreshed.scanId, scanType: refreshed.scanType };
  }

  remember(key, scanId, scanType) {
    if (!Number.isInteger(Number(scanId)) || Number(scanId) <= 0) {
      throw new TypeError('scanId must be a positive integer');
    }
    if (!['checkin', 'checkout'].includes(scanType)) {
      throw new TypeError('scanType must be checkin or checkout');
    }

    const nowMs = this.now();
    this.pruneExpired(nowMs);
    this.entries.delete(key);
    while (this.entries.size >= this.maxEntries) {
      const oldestKey = this.entries.keys().next().value;
      this.entries.delete(oldestKey);
    }
    this.entries.set(key, {
      scanId: Number(scanId),
      scanType,
      lastSeenAtMs: nowMs
    });
  }

  forget(key) {
    this.entries.delete(key);
  }

  clear() {
    this.entries.clear();
  }

  pruneExpired(nowMs = this.now()) {
    for (const [key, entry] of this.entries) {
      if (nowMs - entry.lastSeenAtMs >= this.windowMs) {
        this.entries.delete(key);
      } else {
        // take()/remember() keep the Map ordered by last-seen time.
        break;
      }
    }
  }
}

/**
 * Durable any-type guard. It deliberately runs before check-in/check-out state
 * selection so an immediate second request replays the stored result instead
 * of toggling it.
 */
async function findRecentHealthScan(connection, {
  eventId,
  standId,
  userId,
  windowSeconds
}) {
  const [rows] = await connection.query(
    `SELECT id, scan_type
       FROM health_event_scan
      WHERE health_event_id = ? AND stand_id = ? AND scanned_user_id = ?
        AND scanned_at >= (NOW(3) - INTERVAL ? SECOND)
      ORDER BY scanned_at DESC, id DESC
      LIMIT 1`,
    [eventId, standId, userId, windowSeconds]
  );

  if (!rows.length) return null;
  return {
    scanId: Number(rows[0].id),
    scanType: rows[0].scan_type
  };
}

/**
 * Every check-in of this beneficiary at this stand during the CURRENT day of
 * the event's own timezone (server sessions run in UTC, so a plain DATE()
 * would split an afternoon in California across two days), newest first, with
 * its paired check-out when one exists. Locked FOR UPDATE: it is the durable
 * basis for the one-visit-per-day rule below and for check-in/check-out
 * pairing. COALESCE keeps the old behaviour on servers without timezone tables
 * (CONVERT_TZ => NULL).
 */
async function findTodayHealthCheckins(connection, { standId, userId, timezone }) {
  const [rows] = await connection.query(
    `SELECT s.id, s.service_id, s.volunteer_user_id, s.scanned_at,
            (SELECT c.id FROM health_event_scan c WHERE c.paired_scan_id = s.id ORDER BY c.id DESC LIMIT 1) AS checkout_id,
            (SELECT c.scanned_at FROM health_event_scan c WHERE c.paired_scan_id = s.id ORDER BY c.id DESC LIMIT 1) AS checkout_at
       FROM health_event_scan s
      WHERE s.stand_id = ? AND s.scanned_user_id = ? AND s.scan_type = 'checkin'
        AND DATE(COALESCE(CONVERT_TZ(s.scanned_at, @@session.time_zone, ?), s.scanned_at))
          = DATE(COALESCE(CONVERT_TZ(NOW(), @@session.time_zone, ?), NOW()))
      ORDER BY s.scanned_at DESC, s.id DESC
      FOR UPDATE`,
    [standId, userId, timezone, timezone]
  );
  return rows.map(row => ({
    id: Number(row.id),
    serviceId: row.service_id == null ? null : Number(row.service_id),
    volunteerUserId: row.volunteer_user_id == null ? null : Number(row.volunteer_user_id),
    scannedAt: row.scanned_at,
    checkoutId: row.checkout_id == null ? null : Number(row.checkout_id),
    checkoutAt: row.checkout_at == null ? null : row.checkout_at
  }));
}

function sameService(left, right) {
  return (left == null ? 0 : Number(left)) === (right == null ? 0 : Number(right));
}

/**
 * One person, one visit per stand per day.
 *
 * Volunteers scan the same participant more than once for the same service far
 * more often than anyone gets two haircuts: double taps a few seconds apart,
 * a second volunteer re-scanning the queue, someone showing the QR again on
 * the way out. The 10-second guard only catches the first case, so the durable
 * rule is decided here from the check-ins already stored for today:
 *
 * - Stand WITH check-out: an open visit closes (check-out) as before. After a
 *   completed visit the next scan is NOT recorded until the volunteer confirms
 *   a genuine new visit (allowRepeat === true). Clients that never send the
 *   flag keep the old behaviour (a new check-in) so old builds do not break.
 * - Stand WITHOUT check-out: a second check-in for the same stand + service on
 *   the same day is refused; the existing scan is reported back instead.
 *
 * `todayCheckins` must be ordered newest first (findTodayHealthCheckins).
 */
function resolveDailyScanDecision({ hasCheckout, serviceId, todayCheckins, allowRepeat }) {
  const checkins = Array.isArray(todayCheckins) ? todayCheckins : [];
  if (hasCheckout) {
    const open = checkins.find(scan => scan.checkoutId == null);
    if (open) {
      return { kind: 'checkout', pairedScanId: open.id };
    }
    const completed = checkins.find(scan => sameService(scan.serviceId, serviceId));
    if (completed && allowRepeat === false) {
      return { kind: 'confirm_repeat', previous: completed };
    }
    return { kind: 'checkin' };
  }
  const served = checkins.find(scan => sameService(scan.serviceId, serviceId));
  if (served) {
    return { kind: 'already_served', previous: served };
  }
  return { kind: 'checkin' };
}

module.exports = {
  HEALTH_QR_SLIDING_DEBOUNCE_MS,
  SlidingHealthQrDebounce,
  buildHealthScanDebounceKey,
  findRecentHealthScan,
  findTodayHealthCheckins,
  resolveDailyScanDecision
};
