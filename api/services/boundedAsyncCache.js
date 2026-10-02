'use strict';

// Small process-local cache for aggregate responses. Bound both retained values
// and in-flight keys; never keep user/report-sized results without a byte limit.
function createBoundedAsyncCache({ maxEntries = 200, maxBytes = 8 * 1024 * 1024, now = Date.now } = {}) {
  const entries = new Map();
  let retainedBytes = 0;

  function remove(key, entry) {
    if (entries.get(key) !== entry) return;
    retainedBytes -= entry.bytes || 0;
    entries.delete(key);
  }

  function evictCompleted() {
    for (const [key, entry] of entries) {
      if (!entry.promise) {
        remove(key, entry);
        return true;
      }
    }
    return false;
  }

  function getOrCompute(key, ttlMs, compute) {
    const timestamp = now();
    for (const [expiredKey, entry] of entries) {
      if (!entry.promise && entry.expiresAt <= timestamp) remove(expiredKey, entry);
    }
    const cached = entries.get(key);
    if (cached) {
      // Refresh LRU order without interrupting an in-flight calculation.
      entries.delete(key);
      entries.set(key, cached);
      return cached.promise || Promise.resolve(cached.value);
    }

    while (entries.size >= maxEntries && evictCompleted()) { /* evict oldest value */ }
    if (entries.size >= maxEntries || ttlMs <= 0) {
      // All slots are already computing. Do not let distinct filters grow the map.
      return Promise.resolve().then(compute);
    }

    const entry = { bytes: 0 };
    entry.promise = Promise.resolve().then(compute).then(value => {
      // A clear()/replacement while SQL was running must not resurrect stale data.
      if (entries.get(key) !== entry) return value;
      let bytes;
      try {
        bytes = Buffer.byteLength(JSON.stringify(value) || '', 'utf8');
      } catch {
        remove(key, entry);
        return value;
      }
      if (bytes > maxBytes) {
        remove(key, entry);
        return value;
      }
      while (retainedBytes + bytes > maxBytes && evictCompleted()) { /* bound memory */ }
      entry.promise = null;
      entry.value = value;
      entry.bytes = bytes;
      entry.expiresAt = now() + ttlMs;
      retainedBytes += bytes;
      return value;
    }, error => {
      remove(key, entry);
      throw error;
    });
    entries.set(key, entry);
    return entry.promise;
  }

  return {
    getOrCompute,
    clear() { entries.clear(); retainedBytes = 0; }
  };
}

module.exports = { createBoundedAsyncCache };
