'use strict';

/**
 * Backfill: marca reset_password='Y' a los usuarios de eventos de salud que
 * siguen usando la contraseña por defecto compartida ('bienestarcommunity'),
 * para que el sistema les muestre el diálogo de cambio de contraseña en su
 * primer inicio de sesión (web y app nativa).
 *
 * Alcance: usuarios con alguna health_event_registration cuyo hash bcrypt
 * coincide con la contraseña por defecto y que todavía tienen el flag en 'N'.
 * Los usuarios "matcheados" del import de Jotform que conservaron su propia
 * contraseña NO coinciden con el hash y quedan intactos.
 *
 * Idempotente. Uso:
 *   PW='<password>' node scripts/backfillResetPasswordFlags.js <host> <user> <db> <port> [--dry-run]
 * ej. dev:  PW='...' node scripts/backfillResetPasswordFlags.js localhost root db_community_data 3306 --dry-run
 * ej. prod: PW='...' node scripts/backfillResetPasswordFlags.js database-1.coixqu0wibpz.us-west-1.rds.amazonaws.com admin db_community_data 3306
 */

const mysql = require('mysql2/promise');
const bcryptjs = require('bcryptjs');

const DEFAULT_HEALTH_PASSWORD = 'bienestarcommunity';

async function main() {
  const [host, user, database, port] = process.argv.slice(2);
  const dryRun = process.argv.includes('--dry-run');
  const password = process.env.PW;
  if (!host || !user || !database || !port || !password) {
    console.error('Uso: PW=... node scripts/backfillResetPasswordFlags.js <host> <user> <db> <port> [--dry-run]');
    process.exit(1);
  }

  const connection = await mysql.createConnection({ host, user, password, database, port: Number(port) });
  try {
    const [rows] = await connection.query(
      `SELECT DISTINCT u.id, u.username, u.password, u.reset_password, r.source
       FROM user u
       INNER JOIN health_event_registration r ON r.user_id = u.id
       WHERE u.reset_password = 'N' AND u.password IS NOT NULL`
    );
    console.log(`Candidatos (usuarios con registro de health event y flag N): ${rows.length}`);

    const toFlag = [];
    for (const row of rows) {
      let matches = false;
      try {
        matches = await bcryptjs.compare(DEFAULT_HEALTH_PASSWORD, row.password);
      } catch (e) {
        matches = false;
      }
      if (matches) {
        toFlag.push(row);
      }
    }

    console.log(`Con contraseña por defecto (a marcar reset_password='Y'): ${toFlag.length}`);
    toFlag.slice(0, 10).forEach(r => console.log(`  - user ${r.id} (${r.username}) source=${r.source}`));
    if (toFlag.length > 10) console.log(`  ... y ${toFlag.length - 10} más`);

    if (dryRun) {
      console.log('DRY-RUN: no se modificó nada.');
      return;
    }

    if (toFlag.length) {
      const ids = toFlag.map(r => r.id);
      const [result] = await connection.query(
        'UPDATE user SET reset_password = "Y" WHERE id IN (?)', [ids]
      );
      console.log(`Actualizados: ${result.affectedRows}`);
    } else {
      console.log('Nada para actualizar.');
    }
  } finally {
    await connection.end();
  }
}

main().catch((error) => {
  console.error('Error:', error.message);
  process.exit(1);
});
