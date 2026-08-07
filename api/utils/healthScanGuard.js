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

module.exports = {
  HEALTH_QR_SLIDING_DEBOUNCE_MS,
  SlidingHealthQrDebounce,
  buildHealthScanDebounceKey,
  findRecentHealthScan
};
