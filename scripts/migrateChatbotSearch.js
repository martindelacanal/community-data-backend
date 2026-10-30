'use strict';

const mysql = require('mysql2/promise');
const { databaseConfig } = require('./migrateHealthWristbands');

// Additive: the existing chunk/vector table is never rebuilt or locked by an
// ALTER. Creating FULLTEXT on the initially empty search table avoids rebuilding
// the large embedding table. Re-running also catches imports made during rollout.
async function migrateSearch(db, log = console.log) {
  await db.query(`CREATE TABLE IF NOT EXISTS chatbot_search (
    chunk_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    document TEXT NOT NULL,
    FULLTEXT KEY ft_chatbot_document (document),
    CONSTRAINT fk_chatbot_search_chunk FOREIGN KEY (chunk_id) REFERENCES chatbot_chunk(id) ON DELETE CASCADE
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`);
  let after = 0, indexed = 0;
  while (true) {
    const [rows] = await db.query('SELECT id FROM chatbot_chunk WHERE id>? ORDER BY id LIMIT 500', [after]);
    if (!rows.length) break;
    const last = rows.at(-1).id;
    await db.query(`INSERT INTO chatbot_search(chunk_id,document)
      SELECT c.id,CONCAT_WS('\n',s.title,c.content) FROM chatbot_chunk c JOIN chatbot_source s ON s.id=c.source_id
      LEFT JOIN chatbot_search i ON i.chunk_id=c.id WHERE c.id>? AND c.id<=? AND i.chunk_id IS NULL`, [after, last]);
    indexed += rows.length; after = last;
    if (rows.length < 500) break;
  }
  const [[verification]] = await db.query(`SELECT COUNT(*) missing FROM chatbot_chunk c LEFT JOIN chatbot_search i ON i.chunk_id=c.id WHERE i.chunk_id IS NULL`);
  if (Number(verification.missing)) throw new Error('chatbot_search_backfill_incomplete');
  log(JSON.stringify({ chatbotSearch: 'ready', checkedChunks: indexed }));
}

async function main() {
  const target = process.argv.find(arg => arg.startsWith('--target='))?.slice(9);
  let config;
  if (process.argv.includes('--server-env')) {
    require('dotenv').config({ quiet: true });
    config = { host: process.env.DB_HOST, port: Number(process.env.DB_PORT), user: process.env.DB_USER,
      password: process.env.DB_PASSWORD, database: process.env.DB_DATABASE };
  } else config = databaseConfig(target);
  const db = await mysql.createConnection(config);
  try { await migrateSearch(db); } finally { await db.end(); }
}
if (require.main === module) main().catch(error => { console.error('Chatbot search migration failed:', error.code || error.message); process.exitCode = 1; });
module.exports = { migrateSearch };
