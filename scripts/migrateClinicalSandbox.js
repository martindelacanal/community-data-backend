'use strict';
const fs = require('node:fs');
const path = require('node:path');
const mysql = require('mysql2/promise');
const { databaseConfig } = require('./migrateHealthWristbands');
async function migrate(target) {
  const connection = await mysql.createConnection(databaseConfig(target));
  try {
    await connection.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-02_clinical_sandbox.sql'), 'utf8'));
    const [contextColumn] = await connection.query("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='clinical_sandbox_record' AND COLUMN_NAME='finalization_context'");
    if (!contextColumn.length) await connection.query('ALTER TABLE clinical_sandbox_record ADD COLUMN finalization_context JSON NULL AFTER finalize_key');
    const [tables] = await connection.query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE 'clinical_sandbox_%'");
    if (tables.length !== 8) throw new Error('CLINICAL_SCHEMA_INCOMPLETE');
    console.log(`Clinical synthetic sandbox schema verified (${target}): 8 private tables.`);
  } finally { await connection.end(); }
}
if (require.main === module) migrate(process.argv.find(x => x.startsWith('--target='))?.slice(9)).catch(e => {
  console.error('Clinical migration failed:', e.code || 'MIGRATION_ERROR'); process.exitCode = 1;
});
module.exports = { migrate };
