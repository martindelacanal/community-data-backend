/**
 * DEV-ONLY cleanup: removes the Jotform-imported Banning beneficiaries from the
 * develop database, keeping exactly ONE as a test user. Also removes E2E test
 * artifacts (accounts with @test.local emails).
 *
 *  - Import-created users (password 'bienestarcommunity'): user row deleted ->
 *    all health-event data cascades (registration, dates, answers, appointments,
 *    scans, pins). Falls back to soft-delete if some non-cascading table
 *    references the user.
 *  - Pre-existing users the import matched: only their Banning registration is
 *    deleted; the account is left untouched.
 *
 * Run:  PW='<password>' node scripts/cleanupDevBanningImports.js
 */
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');

const password = process.env.PW;
if (!password) {
  console.error('Usage: PW=<dev password> node scripts/cleanupDevBanningImports.js');
  process.exit(1);
}

(async () => {
  // Hard guard: this script must never touch anything but the local dev DB.
  const c = await mysql.createConnection({ host: 'localhost', user: 'root', password, database: 'db_community_data' });
  const log = (...args) => console.log('[cleanup]', ...args);

  const [events] = await c.query('SELECT id FROM health_event WHERE slug = ? LIMIT 1', ['banning']);
  if (!events.length) {
    log('banning event not found — nothing to do');
    await c.end();
    return;
  }
  const eventId = events[0].id;

  const [rows] = await c.query(
    `SELECT r.id AS registration_id, u.id AS user_id, u.username, u.firstname, u.lastname, u.password
     FROM health_event_registration r
     INNER JOIN user u ON u.id = r.user_id
     WHERE r.health_event_id = ? AND r.source = 'import_jotform'
     ORDER BY r.id ASC`, [eventId]);
  log(`import_jotform registrations found: ${rows.length}`);

  let keeper = null;
  const importCreated = [];
  const matchedExisting = [];
  for (const row of rows) {
    const createdByImport = await bcrypt.compare('bienestarcommunity', row.password || '');
    if (createdByImport && !keeper) {
      keeper = row;
      continue;
    }
    (createdByImport ? importCreated : matchedExisting).push(row);
  }
  if (keeper) {
    log(`KEEPING test user: ${keeper.firstname} ${keeper.lastname} (username=${keeper.username}, user_id=${keeper.user_id})`);
  } else {
    log('WARN: no import-created user found to keep');
  }

  const softDeleted = [];
  const deleteUser = async (userId) => {
    try {
      await c.query('DELETE FROM client_user WHERE user_id = ?', [userId]);
      await c.query('DELETE FROM user WHERE id = ?', [userId]);
      return true;
    } catch (error) {
      await c.query('UPDATE user SET deleted = "Y", enabled = "N" WHERE id = ?', [userId]);
      softDeleted.push(userId);
      return false;
    }
  };

  // Matched pre-existing accounts: drop only the Banning registration.
  for (const row of matchedExisting) {
    await c.query('DELETE FROM health_event_registration WHERE id = ?', [row.registration_id]);
  }
  log(`registrations removed for matched existing users: ${matchedExisting.length} (accounts untouched)`);

  // Import-created accounts: drop the whole user (health data cascades).
  for (const row of importCreated) {
    await deleteUser(row.user_id);
  }
  log(`import-created users deleted: ${importCreated.length}`);

  // E2E artifacts (this and previous sessions): @test.local accounts.
  const [testUsers] = await c.query(
    "SELECT id, username, email FROM user WHERE email LIKE '%@test.local'");
  for (const testUser of testUsers) {
    await deleteUser(testUser.id);
  }
  log(`e2e test users deleted: ${testUsers.length}`);

  if (softDeleted.length) {
    log(`soft-deleted instead (still referenced elsewhere): ${softDeleted.join(', ')}`);
  }

  const [remaining] = await c.query(
    `SELECT COUNT(*) AS n FROM health_event_registration WHERE health_event_id = ? AND registration_role = 'beneficiary'`, [eventId]);
  log(`beneficiary registrations remaining on banning (dev): ${remaining[0].n}`);

  await c.end();
  log('done.');
})().catch(err => {
  console.error('[cleanup] FAILED:', err.message);
  process.exit(1);
});
