'use strict';

const { MAX_SEARCH_MATCHES, MAX_SEARCH_CANDIDATES } = require('./chatbotLimits');

// Search the lightweight full-text index before loading large embedding JSON.
// All enabled documents participate; only the best diverse candidates cross
// the database connection and are reranked by the multilingual query vector.
async function indexedCandidates(db, query) {
  const [matches] = await db.query(`SELECT i.chunk_id,c.source_id,
    MATCH(i.document) AGAINST (? IN NATURAL LANGUAGE MODE) relevance
    FROM chatbot_search i JOIN chatbot_chunk c ON c.id=i.chunk_id JOIN chatbot_source s ON s.id=c.source_id
    WHERE MATCH(i.document) AGAINST (? IN NATURAL LANGUAGE MODE) AND s.enabled=1 AND s.status='ready'
    ORDER BY relevance DESC,i.chunk_id LIMIT ${MAX_SEARCH_MATCHES}`, [query, query]);
  const counts = new Map(), ids = [];
  for (const match of matches) {
    const count = counts.get(match.source_id) || 0;
    if (count >= 8) continue;
    counts.set(match.source_id, count + 1); ids.push(match.chunk_id);
    if (ids.length === MAX_SEARCH_CANDIDATES) break;
  }
  if (!ids.length) return [];
  const rows = [];
  // Small responses avoid a long synchronous JSON parse when chats overlap.
  for (let offset = 0; offset < ids.length; offset += 128) {
    const [batch] = await db.query(`SELECT c.id,c.source_id,c.content,c.embedding,s.title,s.url FROM chatbot_chunk c
      JOIN chatbot_source s ON s.id=c.source_id WHERE c.id IN (?) AND s.enabled=1 AND s.status='ready'`, [ids.slice(offset, offset + 128)]);
    rows.push(...batch);
    await new Promise(resolve => setImmediate(resolve));
  }
  return rows;
}

// A missing index during rollout keeps a bounded compatibility fallback, but
// cannot start multiple expensive corpus scans at once.
let scanTail = Promise.resolve();
async function serializedScan(scan) {
  const previous = scanTail;
  let release;
  scanTail = new Promise(resolve => { release = resolve; });
  await previous;
  try { return await scan(); } finally { release(); }
}

module.exports = { indexedCandidates, serializedScan };
