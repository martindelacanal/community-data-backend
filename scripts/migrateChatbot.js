'use strict';
const fs = require('node:fs');
const path = require('node:path');
const mysql = require('mysql2/promise');
const { databaseConfig } = require('./migrateHealthWristbands');

async function migrate(target) {
  const connection = await mysql.createConnection(databaseConfig(target));
  try {
    await connection.query(fs.readFileSync(path.resolve(__dirname, '..', 'migrations', '2026-10-09_chatbot.sql'), 'utf8'));
    const [[sequence]]=await connection.query("SELECT COUNT(*) AS total FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='chatbot_message' AND COLUMN_NAME='sequence'");
    if(!Number(sequence.total))await connection.query('ALTER TABLE chatbot_message ADD COLUMN sequence BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, ADD UNIQUE KEY uq_chatbot_message_sequence(sequence), ADD KEY idx_chatbot_message_order(conversation_id,sequence)');
    const [[requestIndex]]=await connection.query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='chatbot_message' AND INDEX_NAME='uq_chatbot_message_request_role'");
    if(!Number(requestIndex.total))await connection.query('ALTER TABLE chatbot_message ADD UNIQUE KEY uq_chatbot_message_request_role(conversation_id,request_id,role)');
    const [[row]] = await connection.query(`SELECT COUNT(*) AS total FROM information_schema.TABLES
      WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('chatbot_settings','chatbot_source','chatbot_chunk','chatbot_conversation','chatbot_message','chatbot_usage','chatbot_audit')`);
    if (Number(row.total) !== 7) throw new Error('Chatbot tables missing');
    console.log(`Chatbot migration verified (${target}): seven tables, existing settings preserved.`);
  } finally { await connection.end(); }
}

if (require.main === module) migrate(process.argv.find(arg => arg.startsWith('--target='))?.slice(9))
  .catch(error => { console.error('Chatbot migration failed:', error.code || error.message); process.exitCode=1; });
module.exports = { migrate };
