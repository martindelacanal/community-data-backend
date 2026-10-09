'use strict';
// Explicit initial references. Never crawls links or modifies existing sources/settings.
const mysql = require('mysql2/promise');
const { randomUUID } = require('node:crypto');
const path = require('node:path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { databaseConfig } = require('./migrateHealthWristbands');
const { ingestSource } = require('../api/services/chatbotKnowledge');
const SOURCES = [
  { url: 'https://www.who.int/news-room/fact-sheets/detail/healthy-diet', title: 'WHO / OMS · Healthy diet / Alimentación saludable' },
  { url: 'https://medlineplus.gov/healthyliving.html', title: 'MedlinePlus / NIH · Healthy living / Vida saludable' },
  { url: 'https://www.cdc.gov/diabetes/about/index.html', title: 'CDC · Diabetes basics / Información sobre diabetes' }
];
async function seed(target) {
  const db = await mysql.createConnection(databaseConfig(target));
  try {
    const [[admin]] = await db.query("SELECT u.id FROM user u JOIN role r ON r.id=u.role_id WHERE r.name='admin' AND u.enabled='Y' AND u.deleted='N' ORDER BY u.id LIMIT 1");
    if (!admin) throw Error('No active administrator');
    for (const source of SOURCES) {
      const [[existing]] = await db.query('SELECT id FROM chatbot_source WHERE url=? LIMIT 1', [source.url]);
      if (existing) { console.log(`${target}: reference already exists (${new URL(source.url).hostname})`); continue; }
      const data = await ingestSource(source);
      const id = randomUUID();
      await db.beginTransaction();
      try {
        await db.query('INSERT INTO chatbot_source(id,title,kind,url,filename,sha256,chunk_count,metadata,created_by) VALUES(?,?,?,?,?,?,?,?,?)',
          [id,data.title,data.kind,data.url,data.filename,data.sha256,data.chunks.length,JSON.stringify(data.metadata),admin.id]);
        await db.query('INSERT INTO chatbot_chunk(source_id,ordinal,content,embedding) VALUES ?', [data.chunks.map((c,i) => [id,i,c.text,JSON.stringify(c.embedding)])]);
        await db.query("INSERT INTO chatbot_audit(actor_user_id,actor_role,action,entity_id,details) VALUES(NULL,'deployment','source_seeded',?,?)", [id,JSON.stringify({url:source.url,sha256:data.sha256,chunkCount:data.chunks.length})]);
        await db.commit();
      } catch (error) { await db.rollback(); throw error; }
      console.log(`${target}: indexed ${new URL(source.url).hostname} (${data.chunks.length} fragments)`);
    }
  } finally { await db.end(); }
}
if (require.main === module) seed(process.argv.find(arg => arg.startsWith('--target='))?.slice(9))
  .catch(error => { console.error('Chatbot seed failed:',error.code || error.message); process.exitCode=1; });
module.exports = { seed, SOURCES };
