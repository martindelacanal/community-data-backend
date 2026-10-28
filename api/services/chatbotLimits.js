'use strict';

// Keep ingestion capacity and retrieval coverage aligned as the library grows.
const MAX_SOURCES = 2000;
const MAX_TOTAL_CHUNKS = 200000;
const MAX_SOURCE_CHUNKS = 1000;

module.exports = { MAX_SOURCES, MAX_TOTAL_CHUNKS, MAX_SOURCE_CHUNKS };
