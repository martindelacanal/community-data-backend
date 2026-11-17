'use strict';
const { createHmac, randomBytes } = require('node:crypto');

// Only reuse source selection. The catalog and its publication checks run on every request.
function reuseCatalogDecisions(decide) {
  const completed = new Map();
  const pending = new Map();
  const secret = randomBytes(32);
  const ttlMs = 10 * 60_000;
  const capacity = 512;

  return async input => {
    if (input.history.length) return decide(input);
    const key = createHmac('sha256', secret)
      .update(JSON.stringify([input.fallbackLanguage, input.question])).digest('hex');
    const hit = completed.get(key);
    if (hit && hit.expires > Date.now()) return hit.decision;
    completed.delete(key);

    const running = pending.get(key);
    if (running) return { ...await running, inputTokens: 0 };
    const request = decide(input);
    pending.set(key, request);
    try {
      const decision = await request;
      if (decision.data?.allowed && !decision.enrich && ['calendar', 'articles', 'resources'].includes(decision.route)) {
        if (completed.size >= capacity) completed.delete(completed.keys().next().value);
        completed.set(key, { expires: Date.now() + ttlMs, decision: { ...decision, inputTokens: 0 } });
      }
      return decision;
    } finally {
      pending.delete(key);
    }
  };
}

module.exports = { reuseCatalogDecisions };
