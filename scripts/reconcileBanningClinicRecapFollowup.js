/*
 * Follow-up reconciliation for the Banning clinic recap (2026-08-17).
 *
 * This second, deliberately case-specific run fixes two production facts that
 * were found while reviewing the handwritten food list and Aly Lo's survey
 * case:
 *   1. Merge Aly Lo's duplicate imported event registration/account into the
 *      older account whose email is the one asserted by the client. Remove the
 *      duplicate Entry scan made 29 seconds after the same volunteer scanned
 *      the canonical account.
 *   2. Add the missing Food Distribution record for Salvador Ramos. The phone
 *      in the handwritten list is 714-737-3436 (not 714-732-3936), and the
 *      other four people on that list already have Food records.
 *
 * Every identity and precondition is asserted under row locks. A dry run uses
 * the same DML and rolls the transaction back. Applied changes are recorded in
 * health_event_reconciliation_audit and in a local, Git-ignored snapshot.
 *
 * Usage (from BACKEND/):
 *   node scripts/reconcileBanningClinicRecapFollowup.js --dry-run
 *   node scripts/reconcileBanningClinicRecapFollowup.js --apply \
 *     --confirm-production=BANNING-2026-08-17-FOLLOWUP
 */
'use strict';

const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

const BACKEND_ROOT = path.resolve(__dirname, '..');
const ENV_PATH = path.join(BACKEND_ROOT, '.env');
const LOG_DIR = path.join(BACKEND_ROOT, 'logs');
const APPLY = process.argv.includes('--apply');
const EXPLICIT_DRY_RUN = process.argv.includes('--dry-run');
const DRY_RUN = EXPLICIT_DRY_RUN || !APPLY;
const PROD_CONFIRMATION = '--confirm-production=BANNING-2026-08-17-FOLLOWUP';
const RUN_KEY = 'banning-clinic-recap-2026-08-17-v2';

const ALY_TARGET_USER_ID = 56719;
const ALY_SOURCE_USER_ID = 57620;
const ALY_TARGET_REGISTRATION_ID = 440;
const ALY_SOURCE_REGISTRATION_ID = 37;
const ALY_DUPLICATE_ENTRY_SCAN_ID = 10;
const SALVADOR_USER_ID = 58878;
const SALVADOR_REGISTRATION_ID = 409;
const FOOD_VOLUNTEER_USER_ID = 58665;
const SALVADOR_FOOD_AT = '2026-08-09 20:49:30.000';

if ((APPLY && EXPLICIT_DRY_RUN) ||
    process.argv.slice(2).some(arg => !['--apply', '--dry-run', PROD_CONFIRMATION].includes(arg))) {
  console.error('Usage: node scripts/reconcileBanningClinicRecapFollowup.js <--dry-run|--apply>');
  process.exit(1);
}
if (APPLY && !process.argv.includes(PROD_CONFIRMATION)) {
  console.error(`Production apply refused. Add ${PROD_CONFIRMATION}`);
  process.exit(1);
}

function normalizeEnvValue(value) {
  return String(value == null ? '' : value).trim().replace(/^(['"])(.*)\1$/, '$2').trim();
}

function readProductionConfig() {
  const lines = fs.readFileSync(ENV_PATH, 'utf8').split(/\r?\n/);
  const values = {};
  let inside = false;
  let found = false;
  for (const line of lines) {
    if (!inside) {
      inside = /^\s*#\s*PRODUCTION DATABASE\s*$/i.test(line);
      continue;
    }
    const match = line.match(/^\s*#\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i);
    if (match) {
      values[match[1].toUpperCase()] = normalizeEnvValue(match[2]);
      found = true;
      continue;
    }
    if (found && line.trim() === '') break;
  }
  for (const key of ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT']) {
    if (!values[key]) throw new Error(`Missing ${key} in the production database block`);
  }
  return {
    host: values.DB_HOST,
    user: values.DB_USER,
    password: values.DB_PASSWORD,
    database: values.DB_DATABASE,
    port: Number(values.DB_PORT),
    connectTimeout: 30_000,
    dateStrings: true,
    decimalNumbers: true
  };
}

const SENSITIVE_KEY = /(?:password|passwd|secret|token|credential|api[_-]?key|private[_-]?key|salt|otp|auth[_-]?code)/i;

function sanitize(value) {
  if (value == null || typeof value !== 'object') return value;
  if (Buffer.isBuffer(value)) return `[binary:${value.length} bytes]`;
  if (Array.isArray(value)) return value.map(sanitize);
  const clean = {};
  for (const [key, child] of Object.entries(value)) {
    if (!SENSITIVE_KEY.test(key)) clean[key] = sanitize(child);
  }
  return clean;
}

function asJson(value) {
  return value == null ? null : JSON.stringify(sanitize(value));
}

async function tableExists(connection, tableName) {
  const [[row]] = await connection.query(
    'SELECT COUNT(*) AS total FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?',
    [tableName]
  );
  return Number(row.total) > 0;
}

async function foreignKeyReferenceCounts(connection, referencedTable, referencedId) {
  const [references] = await connection.query(
    `SELECT TABLE_NAME, COLUMN_NAME
       FROM information_schema.KEY_COLUMN_USAGE
      WHERE REFERENCED_TABLE_SCHEMA = DATABASE()
        AND REFERENCED_TABLE_NAME = ? AND REFERENCED_COLUMN_NAME = 'id'
      ORDER BY TABLE_NAME, COLUMN_NAME`, [referencedTable]
  );
  const counts = {};
  for (const reference of references) {
    const tableName = String(reference.TABLE_NAME);
    const columnName = String(reference.COLUMN_NAME);
    if (!/^[A-Za-z0-9_]+$/.test(tableName) || !/^[A-Za-z0-9_]+$/.test(columnName)) {
      throw new Error(`Unsafe schema identifier: ${tableName}.${columnName}`);
    }
    const [[row]] = await connection.query(
      `SELECT COUNT(*) AS total FROM \`${tableName}\` WHERE \`${columnName}\` = ?`, [referencedId]
    );
    counts[`${tableName}.${columnName}`] = Number(row.total);
  }
  return counts;
}

async function userIdLikeReferenceCounts(connection, userId) {
  const [references] = await connection.query(
    `SELECT c.TABLE_NAME, c.COLUMN_NAME
       FROM information_schema.COLUMNS c
       INNER JOIN information_schema.TABLES t
         ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
      WHERE c.TABLE_SCHEMA = DATABASE() AND t.TABLE_TYPE = 'BASE TABLE'
        AND c.DATA_TYPE IN ('tinyint','smallint','mediumint','int','bigint','decimal','numeric')
        AND c.COLUMN_NAME REGEXP '(^|_)user_id$'
      ORDER BY c.TABLE_NAME, c.COLUMN_NAME`
  );
  const counts = {};
  for (const reference of references) {
    const tableName = String(reference.TABLE_NAME);
    const columnName = String(reference.COLUMN_NAME);
    if (!/^[A-Za-z0-9_]+$/.test(tableName) || !/^[A-Za-z0-9_]+$/.test(columnName)) {
      throw new Error(`Unsafe schema identifier: ${tableName}.${columnName}`);
    }
    const [[row]] = await connection.query(
      `SELECT COUNT(*) AS total FROM \`${tableName}\` WHERE \`${columnName}\` = ?`, [userId]
    );
    counts[`${tableName}.${columnName}`] = Number(row.total);
  }
  return counts;
}

async function captureState(connection, eventId) {
  const userIds = [ALY_TARGET_USER_ID, ALY_SOURCE_USER_ID, SALVADOR_USER_ID];
  const [users] = await connection.query(
    `SELECT id, username, email, firstname, lastname, date_of_birth, phone,
            enabled, deleted, deleted_at, client_id, location_id, creation_date, modification_date
       FROM user WHERE id IN (?) ORDER BY id`, [userIds]
  );
  const [registrations] = await connection.query(
    `SELECT * FROM health_event_registration
      WHERE health_event_id = ? AND user_id IN (?) ORDER BY id`, [eventId, userIds]
  );
  const registrationIds = registrations.map(row => row.id);
  const [dates] = registrationIds.length
    ? await connection.query(
      'SELECT * FROM health_event_registration_date WHERE registration_id IN (?) ORDER BY registration_id, id',
      [registrationIds]
    ) : [[]];
  const [appointments] = registrationIds.length
    ? await connection.query(
      `SELECT a.*, sl.service_key, sl.slot_date,
              TIME_FORMAT(sl.start_time, '%H:%i') AS start_time
         FROM health_event_appointment a
         INNER JOIN health_event_slot sl ON sl.id = a.slot_id
        WHERE a.registration_id IN (?) ORDER BY a.registration_id, a.id`, [registrationIds]
    ) : [[]];
  const [answers] = registrationIds.length
    ? await connection.query(
      'SELECT * FROM health_event_answer WHERE registration_id IN (?) ORDER BY registration_id, id',
      [registrationIds]
    ) : [[]];
  const answerIds = answers.map(row => row.id);
  const [answerOptions] = answerIds.length
    ? await connection.query(
      'SELECT * FROM health_event_answer_option WHERE answer_id IN (?) ORDER BY answer_id, option_id',
      [answerIds]
    ) : [[]];
  const [scans] = await connection.query(
    `SELECT s.*, st.name_en AS stand_name
       FROM health_event_scan s
       INNER JOIN health_event_stand st ON st.id = s.stand_id
      WHERE s.health_event_id = ? AND s.scanned_user_id IN (?)
      ORDER BY s.scanned_at, s.id`, [eventId, userIds]
  );
  const [memberships] = await connection.query(
    'SELECT * FROM client_user WHERE user_id IN (?) ORDER BY user_id, client_id', [userIds]
  );
  return { users, registrations, dates, appointments, answers, answerOptions, scans, memberships };
}

async function main() {
  const config = readProductionConfig();
  if (!String(config.host).includes('database-1.')) {
    throw new Error('Production safety check failed: unexpected database host');
  }

  const connection = await mysql.createConnection(config);
  const operations = [];
  let committed = false;
  let preparedPath = null;

  const record = async ({ actionKey, actionType, targetTable, targetId, before, after, note }) => {
    const operation = sanitize({ actionKey, actionType, targetTable, targetId, before, after, note });
    operations.push(operation);
    if (APPLY) {
      await connection.query(
        `INSERT INTO health_event_reconciliation_audit
           (run_key, action_key, action_type, target_table, target_id, before_json, after_json, note)
         VALUES (?,?,?,?,?,?,?,?)`,
        [RUN_KEY, actionKey, actionType, targetTable, String(targetId),
          asJson(before), asJson(after), note]
      );
    }
  };

  try {
    const [[identity]] = await connection.query(
      'SELECT DATABASE() AS db, @@hostname AS db_host, @@session.time_zone AS session_timezone'
    );
    console.log(`[followup] target=production mode=${DRY_RUN ? 'DRY-RUN (rollback)' : 'APPLY'}`);
    console.log(`[followup] database=${identity.db} host=${identity.db_host} timezone=${identity.session_timezone}`);

    if (!(await tableExists(connection, 'health_event_reconciliation_audit'))) {
      throw new Error('health_event_reconciliation_audit is missing; the v1 reconciliation must exist first');
    }
    const [[priorRun]] = await connection.query(
      'SELECT COUNT(*) AS total FROM health_event_reconciliation_audit WHERE run_key = ?', [RUN_KEY]
    );
    if (Number(priorRun.total) !== 0) {
      throw new Error(`${RUN_KEY} already has persisted audit rows; refusing to run it again`);
    }

    await connection.beginTransaction();

    const [[event]] = await connection.query(
      `SELECT id, slug, name_en, start_date, end_date, timezone
         FROM health_event WHERE slug = 'banning' LIMIT 1 FOR UPDATE`
    );
    if (!event || Number(event.id) !== 1 || String(event.start_date).slice(0, 10) !== '2026-08-08' ||
        String(event.end_date).slice(0, 10) !== '2026-08-09' || event.timezone !== 'America/Los_Angeles') {
      throw new Error('The expected production Banning event was not found');
    }

    const [alyUsers] = await connection.query(
      `SELECT id, username, email, firstname, lastname, date_of_birth, phone,
              enabled, deleted, deleted_at, client_id, location_id, creation_date, modification_date
         FROM user WHERE id IN (?,?) ORDER BY id FOR UPDATE`,
      [ALY_TARGET_USER_ID, ALY_SOURCE_USER_ID]
    );
    if (alyUsers.length !== 2) throw new Error('Expected both Aly user records');
    const alyTargetUser = alyUsers.find(row => Number(row.id) === ALY_TARGET_USER_ID);
    const alySourceUser = alyUsers.find(row => Number(row.id) === ALY_SOURCE_USER_ID);
    for (const row of alyUsers) {
      const normalizedPhone = String(row.phone || '').replace(/\D/g, '');
      if (String(row.firstname).trim().toLowerCase() !== 'aly' ||
          String(row.lastname).trim().toLowerCase() !== 'lo' ||
          String(row.date_of_birth).slice(0, 10) !== '1956-12-02' || normalizedPhone !== '9092192394' ||
          row.enabled !== 'Y' || row.deleted !== 'N') {
        throw new Error(`Aly identity/state mismatch for user #${row.id}`);
      }
    }
    if (String(alyTargetUser.email).trim().toLowerCase() !== 'ply830@hotmail.com' || alySourceUser.email != null) {
      throw new Error('Aly canonical/source email assertion failed');
    }

    const [alyRegistrations] = await connection.query(
      `SELECT * FROM health_event_registration
        WHERE health_event_id = ? AND user_id IN (?,?) ORDER BY id FOR UPDATE`,
      [event.id, ALY_TARGET_USER_ID, ALY_SOURCE_USER_ID]
    );
    if (alyRegistrations.length !== 2) throw new Error('Expected two Aly event registrations');
    const targetRegistration = alyRegistrations.find(row => Number(row.id) === ALY_TARGET_REGISTRATION_ID);
    const sourceRegistration = alyRegistrations.find(row => Number(row.id) === ALY_SOURCE_REGISTRATION_ID);
    if (!targetRegistration || Number(targetRegistration.user_id) !== ALY_TARGET_USER_ID ||
        targetRegistration.registration_role !== 'beneficiary' || targetRegistration.status !== 'registered' ||
        targetRegistration.source !== 'walkin' || targetRegistration.contact_email != null ||
        !sourceRegistration || Number(sourceRegistration.user_id) !== ALY_SOURCE_USER_ID ||
        sourceRegistration.registration_role !== 'beneficiary' || sourceRegistration.status !== 'registered' ||
        sourceRegistration.source !== 'import_jotform' || sourceRegistration.external_ref !== 'jotform:38' ||
        String(sourceRegistration.contact_email).trim().toLowerCase() !== 'keu951@gmail.com' ||
        String(sourceRegistration.submitted_at) !== '2026-07-23 00:00:00') {
      throw new Error('Aly registration assertion failed');
    }

    const [sourceDates] = await connection.query(
      'SELECT * FROM health_event_registration_date WHERE registration_id = ? ORDER BY id FOR UPDATE',
      [ALY_SOURCE_REGISTRATION_ID]
    );
    const [targetDates] = await connection.query(
      'SELECT * FROM health_event_registration_date WHERE registration_id = ? ORDER BY id FOR UPDATE',
      [ALY_TARGET_REGISTRATION_ID]
    );
    const [sourceAppointments] = await connection.query(
      `SELECT a.*, sl.service_key, sl.slot_date, TIME_FORMAT(sl.start_time, '%H:%i') AS start_time
         FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
        WHERE a.registration_id = ? ORDER BY a.id FOR UPDATE`, [ALY_SOURCE_REGISTRATION_ID]
    );
    const [targetAppointments] = await connection.query(
      'SELECT * FROM health_event_appointment WHERE registration_id = ? ORDER BY id FOR UPDATE',
      [ALY_TARGET_REGISTRATION_ID]
    );
    const [sourceAnswers] = await connection.query(
      'SELECT * FROM health_event_answer WHERE registration_id = ? ORDER BY id FOR UPDATE',
      [ALY_SOURCE_REGISTRATION_ID]
    );
    const [targetAnswers] = await connection.query(
      'SELECT * FROM health_event_answer WHERE registration_id = ? ORDER BY id FOR UPDATE',
      [ALY_TARGET_REGISTRATION_ID]
    );
    const sourceAnswerIds = sourceAnswers.map(row => row.id);
    const [sourceAnswerOptions] = sourceAnswerIds.length
      ? await connection.query(
        'SELECT * FROM health_event_answer_option WHERE answer_id IN (?) ORDER BY answer_id, option_id FOR UPDATE',
        [sourceAnswerIds]
      ) : [[]];
    const overlappingQuestions = sourceAnswers.filter(source =>
      targetAnswers.some(target => Number(target.question_id) === Number(source.question_id))
    );
    if (sourceDates.length !== 1 || String(sourceDates[0].event_date).slice(0, 10) !== '2026-08-08' ||
        sourceDates[0].priority_service !== 'dental' || targetDates.length !== 0 ||
        sourceAppointments.length !== 1 || Number(sourceAppointments[0].id) !== 38 ||
        sourceAppointments[0].service_key !== 'dental' ||
        String(sourceAppointments[0].slot_date).slice(0, 10) !== '2026-08-08' ||
        sourceAppointments[0].start_time !== '08:00' || sourceAppointments[0].status !== 'booked' ||
        targetAppointments.length !== 0 || sourceAnswers.length !== 16 || targetAnswers.length !== 22 ||
        overlappingQuestions.length !== 0) {
      throw new Error('Aly child-row preconditions changed; refusing a lossy merge');
    }

    const [sourceScans] = await connection.query(
      `SELECT s.*, st.name_en AS stand_name
         FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
        WHERE s.health_event_id = ? AND
          (s.registration_id = ? OR s.scanned_user_id = ? OR s.volunteer_user_id = ?)
        ORDER BY s.id FOR UPDATE`,
      [event.id, ALY_SOURCE_REGISTRATION_ID, ALY_SOURCE_USER_ID, ALY_SOURCE_USER_ID]
    );
    if (sourceScans.length !== 1 || Number(sourceScans[0].id) !== ALY_DUPLICATE_ENTRY_SCAN_ID ||
        sourceScans[0].stand_name !== 'Entry Check-in' || sourceScans[0].scan_type !== 'checkin' ||
        Number(sourceScans[0].registration_id) !== ALY_SOURCE_REGISTRATION_ID) {
      throw new Error('Aly source scan set changed');
    }
    const [[canonicalEntry]] = await connection.query(
      `SELECT s.*, st.name_en AS stand_name
         FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
        WHERE s.id = 9 AND s.health_event_id = ? FOR UPDATE`, [event.id]
    );
    if (!canonicalEntry || Number(canonicalEntry.scanned_user_id) !== ALY_TARGET_USER_ID ||
        canonicalEntry.stand_name !== 'Entry Check-in' || canonicalEntry.scan_type !== 'checkin' ||
        Number(canonicalEntry.volunteer_user_id) !== Number(sourceScans[0].volunteer_user_id) ||
        Math.abs(new Date(sourceScans[0].scanned_at).getTime() - new Date(canonicalEntry.scanned_at).getTime()) > 30_000) {
      throw new Error('Aly duplicate Entry evidence changed');
    }

    const beforeSnapshot = await captureState(connection, event.id);

    await connection.query(
      'UPDATE health_event_registration_date SET registration_id = ? WHERE registration_id = ?',
      [ALY_TARGET_REGISTRATION_ID, ALY_SOURCE_REGISTRATION_ID]
    );
    await connection.query(
      'UPDATE health_event_appointment SET registration_id = ? WHERE registration_id = ?',
      [ALY_TARGET_REGISTRATION_ID, ALY_SOURCE_REGISTRATION_ID]
    );
    await connection.query(
      'UPDATE health_event_answer SET registration_id = ? WHERE registration_id = ?',
      [ALY_TARGET_REGISTRATION_ID, ALY_SOURCE_REGISTRATION_ID]
    );
    await connection.query(
      'UPDATE health_event_scan SET registration_id = ?, scanned_user_id = ? WHERE id = ?',
      [ALY_TARGET_REGISTRATION_ID, ALY_TARGET_USER_ID, ALY_DUPLICATE_ENTRY_SCAN_ID]
    );
    const registrationReferenceCounts = await foreignKeyReferenceCounts(
      connection, 'health_event_registration', ALY_SOURCE_REGISTRATION_ID
    );
    if (Object.values(registrationReferenceCounts).some(total => total !== 0)) {
      throw new Error(`References remain on Aly source registration: ${JSON.stringify(registrationReferenceCounts)}`);
    }
    await connection.query('DELETE FROM health_event_registration WHERE id = ?', [ALY_SOURCE_REGISTRATION_ID]);
    await connection.query(
      `UPDATE health_event_registration
          SET source = ?, external_ref = ?, submitted_at = ?, contact_email = NULL
        WHERE id = ?`,
      [sourceRegistration.source, sourceRegistration.external_ref,
        sourceRegistration.submitted_at, ALY_TARGET_REGISTRATION_ID]
    );
    const [[targetRegistrationAfter]] = await connection.query(
      'SELECT * FROM health_event_registration WHERE id = ?', [ALY_TARGET_REGISTRATION_ID]
    );
    await record({
      actionKey: 'banning-followup-merge-aly-registration-37-into-440',
      actionType: 'merge-registration',
      targetTable: 'health_event_registration',
      targetId: `${ALY_SOURCE_REGISTRATION_ID}->${ALY_TARGET_REGISTRATION_ID}`,
      before: {
        source: { registration: sourceRegistration, dates: sourceDates, appointments: sourceAppointments,
          answers: sourceAnswers, answerOptions: sourceAnswerOptions, scans: sourceScans },
        target: { registration: targetRegistration, dates: targetDates,
          appointments: targetAppointments, answers: targetAnswers }
      },
      after: { registration: targetRegistrationAfter, registrationReferenceCounts,
        user_id: ALY_TARGET_USER_ID },
      note: 'Exact Aly Lo duplicate; combine non-conflicting data on the asserted account and preserve Jotform provenance without the unasserted contact email.'
    });

    const [[scanAnswerCount]] = await connection.query(
      'SELECT COUNT(*) AS total FROM health_event_scan_answer WHERE scan_id = ? FOR UPDATE',
      [ALY_DUPLICATE_ENTRY_SCAN_ID]
    );
    const [[pairedScanCount]] = await connection.query(
      'SELECT COUNT(*) AS total FROM health_event_scan WHERE paired_scan_id = ? FOR UPDATE',
      [ALY_DUPLICATE_ENTRY_SCAN_ID]
    );
    if (Number(scanAnswerCount.total) !== 0 || Number(pairedScanCount.total) !== 0) {
      throw new Error('Aly duplicate Entry scan acquired dependent data');
    }
    const [[duplicateEntryBefore]] = await connection.query(
      'SELECT * FROM health_event_scan WHERE id = ? FOR UPDATE', [ALY_DUPLICATE_ENTRY_SCAN_ID]
    );
    if (!duplicateEntryBefore || Number(duplicateEntryBefore.scanned_user_id) !== ALY_TARGET_USER_ID ||
        Number(duplicateEntryBefore.registration_id) !== ALY_TARGET_REGISTRATION_ID) {
      throw new Error('Aly duplicate Entry scan was not reassigned as expected');
    }
    await connection.query('DELETE FROM health_event_scan WHERE id = ?', [ALY_DUPLICATE_ENTRY_SCAN_ID]);
    await record({
      actionKey: 'banning-followup-remove-aly-duplicate-entry-scan-10',
      actionType: 'delete-audited',
      targetTable: 'health_event_scan',
      targetId: ALY_DUPLICATE_ENTRY_SCAN_ID,
      before: duplicateEntryBefore,
      after: null,
      note: 'Duplicate Entry scan made by the same volunteer 29 seconds after scan #9 under Aly Lo duplicate credentials.'
    });

    const [sourceMemberships] = await connection.query(
      'SELECT * FROM client_user WHERE user_id = ? ORDER BY client_id FOR UPDATE', [ALY_SOURCE_USER_ID]
    );
    const [targetMembershipsBefore] = await connection.query(
      'SELECT * FROM client_user WHERE user_id = ? ORDER BY client_id FOR UPDATE', [ALY_TARGET_USER_ID]
    );
    if (sourceMemberships.length !== 1 || Number(sourceMemberships[0].client_id) !== 5 ||
        targetMembershipsBefore.some(row => Number(row.client_id) === 5)) {
      throw new Error('Aly membership merge preconditions changed');
    }
    for (const membership of sourceMemberships) {
      await connection.query(
        `INSERT INTO client_user(client_id, user_id, checked, creation_date)
         VALUES (?,?,?,?)`,
        [membership.client_id, ALY_TARGET_USER_ID, membership.checked, membership.creation_date]
      );
    }
    await connection.query('DELETE FROM client_user WHERE user_id = ?', [ALY_SOURCE_USER_ID]);

    // Cover both declared FKs and legacy user-id columns that predate FK
    // constraints. This makes a newly added reference fail closed rather than
    // silently leaving history on the disabled duplicate.
    const sourceReferenceCounts = await userIdLikeReferenceCounts(connection, ALY_SOURCE_USER_ID);
    if (Object.values(sourceReferenceCounts).some(total => total !== 0)) {
      throw new Error(`Unexpected global references on Aly source: ${JSON.stringify(sourceReferenceCounts)}`);
    }
    const [[sourceHealthReferences]] = await connection.query(
      `SELECT
         (SELECT COUNT(*) FROM health_event_registration WHERE user_id = ?) AS registrations,
         (SELECT COUNT(*) FROM health_event_scan
           WHERE scanned_user_id = ? OR volunteer_user_id = ?) AS scans`,
      [ALY_SOURCE_USER_ID, ALY_SOURCE_USER_ID, ALY_SOURCE_USER_ID]
    );
    if (Number(sourceHealthReferences.registrations) !== 0 || Number(sourceHealthReferences.scans) !== 0) {
      throw new Error('Health-event references remain on the Aly source account');
    }
    const userForeignKeyCounts = await foreignKeyReferenceCounts(connection, 'user', ALY_SOURCE_USER_ID);
    if (Object.values(userForeignKeyCounts).some(total => total !== 0)) {
      throw new Error(`Foreign-key references remain on Aly source user: ${JSON.stringify(userForeignKeyCounts)}`);
    }
    await connection.query(
      `UPDATE user SET enabled = 'N', deleted = 'Y', deleted_at = COALESCE(deleted_at, NOW()),
                       reset_password = 'N'
        WHERE id = ?`, [ALY_SOURCE_USER_ID]
    );
    const [[alySourceAfter]] = await connection.query(
      `SELECT id, username, email, firstname, lastname, date_of_birth, phone,
              enabled, deleted, deleted_at, client_id, location_id, creation_date, modification_date
         FROM user WHERE id = ?`, [ALY_SOURCE_USER_ID]
    );
    await record({
      actionKey: 'banning-followup-merge-aly-user-57620-into-56719',
      actionType: 'merge-user',
      targetTable: 'user',
      targetId: `${ALY_SOURCE_USER_ID}->${ALY_TARGET_USER_ID}`,
      before: { sourceUser: alySourceUser, sourceMemberships, targetMemberships: targetMembershipsBefore,
        sourceReferenceCounts, userForeignKeyCounts },
      after: { sourceUser: alySourceAfter, mergedIntoUserId: ALY_TARGET_USER_ID },
      note: 'Disable the duplicate Aly login after its event data and client membership were preserved on the asserted account.'
    });

    const [salvadorPhoneMatches] = await connection.query(
      `SELECT id FROM user
        WHERE REGEXP_REPLACE(COALESCE(phone,''), '[^0-9]', '') = '7147373436'
        ORDER BY id FOR UPDATE`
    );
    if (salvadorPhoneMatches.length !== 1 || Number(salvadorPhoneMatches[0].id) !== SALVADOR_USER_ID) {
      throw new Error(`Salvador phone is not globally unique: ${JSON.stringify(salvadorPhoneMatches)}`);
    }
    const [[salvador]] = await connection.query(
      `SELECT u.id, u.firstname, u.lastname, u.date_of_birth, u.phone, u.email,
              u.enabled, u.deleted, r.id AS registration_id, r.status AS registration_status
         FROM user u INNER JOIN health_event_registration r
           ON r.user_id = u.id AND r.health_event_id = ? AND r.registration_role = 'beneficiary'
        WHERE u.id = ? AND r.id = ? FOR UPDATE`,
      [event.id, SALVADOR_USER_ID, SALVADOR_REGISTRATION_ID]
    );
    if (!salvador || String(salvador.firstname).trim().toLowerCase() !== 'salvador' ||
        String(salvador.lastname).trim().toLowerCase() !== 'ramos' ||
        String(salvador.date_of_birth).slice(0, 10) !== '1980-09-15' ||
        String(salvador.phone).replace(/\D/g, '') !== '7147373436' ||
        salvador.enabled !== 'Y' || salvador.deleted !== 'N' ||
        salvador.registration_status !== 'registered') {
      throw new Error('Salvador identity/registration assertion failed');
    }
    const [[foodStand]] = await connection.query(
      `SELECT id, has_checkout FROM health_event_stand
        WHERE health_event_id = ? AND name_en = 'Food Distribution' LIMIT 1 FOR UPDATE`, [event.id]
    );
    if (!foodStand || foodStand.has_checkout !== 'N') throw new Error('Food Distribution stand assertion failed');
    const [salvadorFoodBefore] = await connection.query(
      `SELECT s.id FROM health_event_scan s
        WHERE s.health_event_id = ? AND s.scanned_user_id = ? AND s.stand_id = ? FOR UPDATE`,
      [event.id, SALVADOR_USER_ID, foodStand.id]
    );
    if (salvadorFoodBefore.length !== 0) throw new Error('Salvador already has a Food Distribution scan');
    const [[foodVolunteer]] = await connection.query(
      'SELECT id, firstname, lastname, enabled, deleted FROM user WHERE id = ? FOR UPDATE',
      [FOOD_VOLUNTEER_USER_ID]
    );
    if (!foodVolunteer || String(foodVolunteer.firstname).trim().toLowerCase() !== 'joel' ||
        String(foodVolunteer.lastname).trim().toLowerCase() !== 'wagness' ||
        foodVolunteer.enabled !== 'Y' || foodVolunteer.deleted !== 'N') {
      throw new Error('Food-list volunteer assertion failed');
    }
    const [[foodVolunteerRegistration]] = await connection.query(
      `SELECT COUNT(*) AS total FROM health_event_registration
        WHERE id = 379 AND health_event_id = ? AND user_id = ?
          AND registration_role = 'volunteer' AND status = 'registered'`,
      [event.id, FOOD_VOLUNTEER_USER_ID]
    );
    const [[foodVolunteerAssignment]] = await connection.query(
      `SELECT COUNT(*) AS total FROM health_event_volunteer_assignment
        WHERE health_event_id = ? AND user_id = ? AND stand_id = ?`,
      [event.id, FOOD_VOLUNTEER_USER_ID, foodStand.id]
    );
    if (Number(foodVolunteerRegistration.total) !== 1 || Number(foodVolunteerAssignment.total) < 1) {
      throw new Error('Food-list volunteer registration/assignment assertion failed');
    }
    const [foodListRows] = await connection.query(
      `SELECT u.id, REGEXP_REPLACE(COALESCE(u.phone,''), '[^0-9]', '') AS phone,
              SUM(CASE WHEN s.id IS NOT NULL THEN 1 ELSE 0 END) AS food_scans
         FROM user u
         INNER JOIN health_event_registration r
           ON r.user_id = u.id AND r.health_event_id = ? AND r.registration_role = 'beneficiary'
         LEFT JOIN health_event_scan s
           ON s.health_event_id = r.health_event_id AND s.scanned_user_id = u.id AND s.stand_id = ?
        WHERE u.id IN (59041,58535,57718,44350,58878)
        GROUP BY u.id, phone ORDER BY u.id FOR UPDATE`, [event.id, foodStand.id]
    );
    const expectedFoodList = new Map([
      [59041, ['9517284798', 1]],
      [58535, ['9515934128', 1]],
      [57718, ['4247812288', 1]],
      [44350, ['9515637515', 1]],
      [58878, ['7147373436', 0]]
    ]);
    if (foodListRows.length !== expectedFoodList.size || foodListRows.some(row => {
      const expected = expectedFoodList.get(Number(row.id));
      return !expected || row.phone !== expected[0] || Number(row.food_scans) !== expected[1];
    })) {
      throw new Error(`Handwritten Food list no longer matches the audited state: ${JSON.stringify(foodListRows)}`);
    }
    const [salvadorFoodResult] = await connection.query(
      `INSERT INTO health_event_scan
         (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
          volunteer_user_id, scan_type, paired_scan_id, scanned_at)
       VALUES (?,?,NULL,?,?,?,'checkin',NULL,?)`,
      [event.id, foodStand.id, SALVADOR_REGISTRATION_ID, SALVADOR_USER_ID,
        FOOD_VOLUNTEER_USER_ID, SALVADOR_FOOD_AT]
    );
    const [[salvadorFoodAfter]] = await connection.query(
      `SELECT s.*, st.name_en AS stand_name
         FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
        WHERE s.id = ?`, [salvadorFoodResult.insertId]
    );
    await record({
      actionKey: 'banning-followup-add-salvador-food-distribution',
      actionType: 'insert-manual',
      targetTable: 'health_event_scan',
      targetId: salvadorFoodResult.insertId,
      before: { foodScans: salvadorFoodBefore, handwrittenListState: foodListRows },
      after: salvadorFoodAfter,
      note: 'Administrative reconstruction from the handwritten Food list; phone identity is exact, while timestamp and volunteer are inferred from the adjacent list scans and are not QR telemetry.'
    });

    // Postconditions: one canonical Aly registration, no source references,
    // all non-conflicting registration data retained, and one Salvador Food row.
    const [[alyPost]] = await connection.query(
      `SELECT
         (SELECT COUNT(*) FROM health_event_registration
           WHERE health_event_id = ? AND user_id = ?) AS target_registrations,
         (SELECT COUNT(*) FROM health_event_registration WHERE id = ?) AS source_registration,
         (SELECT COUNT(*) FROM health_event_registration_date
           WHERE id = ? AND registration_id = ?) AS moved_date,
         (SELECT COUNT(*) FROM health_event_appointment
           WHERE id = ? AND registration_id = ? AND status = 'booked') AS moved_appointment,
         (SELECT COUNT(*) FROM health_event_answer WHERE registration_id = ?) AS combined_answers,
         (SELECT COUNT(*) FROM health_event_scan WHERE id = ?) AS duplicate_scan,
         (SELECT COUNT(*) FROM health_event_scan
           WHERE health_event_id = ? AND scanned_user_id = ?) AS canonical_scans`,
      [event.id, ALY_TARGET_USER_ID, ALY_SOURCE_REGISTRATION_ID,
        sourceDates[0].id, ALY_TARGET_REGISTRATION_ID,
        sourceAppointments[0].id, ALY_TARGET_REGISTRATION_ID,
        ALY_TARGET_REGISTRATION_ID, ALY_DUPLICATE_ENTRY_SCAN_ID,
        event.id, ALY_TARGET_USER_ID]
    );
    if (Number(alyPost.target_registrations) !== 1 || Number(alyPost.source_registration) !== 0 ||
        Number(alyPost.moved_date) !== 1 || Number(alyPost.moved_appointment) !== 1 ||
        Number(alyPost.combined_answers) !== 38 || Number(alyPost.duplicate_scan) !== 0 ||
        Number(alyPost.canonical_scans) !== 4) {
      throw new Error(`Aly merge postcondition failed: ${JSON.stringify(alyPost)}`);
    }
    const [[alyRegistrationPost]] = await connection.query(
      `SELECT source, external_ref, contact_email, submitted_at
         FROM health_event_registration WHERE id = ?`, [ALY_TARGET_REGISTRATION_ID]
    );
    if (!alyRegistrationPost || alyRegistrationPost.source !== 'import_jotform' ||
        alyRegistrationPost.external_ref !== 'jotform:38' || alyRegistrationPost.contact_email != null ||
        String(alyRegistrationPost.submitted_at) !== String(sourceRegistration.submitted_at)) {
      throw new Error(`Aly registration provenance postcondition failed: ${JSON.stringify(alyRegistrationPost)}`);
    }
    const [[alySourcePost]] = await connection.query(
      `SELECT enabled, deleted,
              (SELECT COUNT(*) FROM client_user WHERE user_id = u.id) AS memberships,
              (SELECT COUNT(*) FROM health_event_registration WHERE user_id = u.id) AS registrations,
              (SELECT COUNT(*) FROM health_event_scan
                WHERE scanned_user_id = u.id OR volunteer_user_id = u.id) AS scans
         FROM user u WHERE u.id = ?`, [ALY_SOURCE_USER_ID]
    );
    if (!alySourcePost || alySourcePost.enabled !== 'N' || alySourcePost.deleted !== 'Y' ||
        Number(alySourcePost.memberships) !== 0 || Number(alySourcePost.registrations) !== 0 ||
        Number(alySourcePost.scans) !== 0) {
      throw new Error(`Aly source-user postcondition failed: ${JSON.stringify(alySourcePost)}`);
    }
    const [[salvadorFoodPost]] = await connection.query(
      `SELECT COUNT(*) AS total,
              MAX(s.volunteer_user_id) AS volunteer_user_id,
              MAX(s.scanned_at) AS scanned_at,
              SUM((SELECT COUNT(*) FROM health_event_scan_answer sa WHERE sa.scan_id = s.id)) AS answer_count
         FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
        WHERE s.health_event_id = ? AND s.scanned_user_id = ? AND s.registration_id = ?
          AND st.name_en = 'Food Distribution' AND s.scan_type = 'checkin'
          AND s.service_id IS NULL AND s.paired_scan_id IS NULL`,
      [event.id, SALVADOR_USER_ID, SALVADOR_REGISTRATION_ID]
    );
    if (Number(salvadorFoodPost.total) !== 1 ||
        Number(salvadorFoodPost.volunteer_user_id) !== FOOD_VOLUNTEER_USER_ID ||
        String(salvadorFoodPost.scanned_at) !== SALVADOR_FOOD_AT ||
        Number(salvadorFoodPost.answer_count) !== 0) {
      throw new Error(`Salvador Food postcondition failed: ${JSON.stringify(salvadorFoodPost)}`);
    }
    const [salvadorOriginalScans] = await connection.query(
      `SELECT s.id, s.health_event_id, s.registration_id, s.scanned_user_id,
              st.name_en AS stand_name, s.scan_type, s.paired_scan_id,
              (SELECT GROUP_CONCAT(DISTINCT qo.name_en ORDER BY qo.name_en)
                 FROM health_event_scan_answer sa
                 INNER JOIN health_event_question q ON q.id = sa.question_id
                 INNER JOIN health_event_scan_answer_option sao ON sao.scan_answer_id = sa.id
                 INNER JOIN health_event_question_option qo ON qo.id = sao.option_id
                WHERE sa.scan_id = s.id AND q.name_en = 'Service status') AS service_status
         FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
        WHERE s.id IN (870,883,1139) ORDER BY s.id`
    );
    if (salvadorOriginalScans.length !== 3 ||
        salvadorOriginalScans.some(scan => Number(scan.health_event_id) !== Number(event.id) ||
          Number(scan.registration_id) !== SALVADOR_REGISTRATION_ID ||
          Number(scan.scanned_user_id) !== SALVADOR_USER_ID) ||
        Number(salvadorOriginalScans[0].id) !== 870 ||
        salvadorOriginalScans[0].stand_name !== 'Medical Checks' ||
        salvadorOriginalScans[0].scan_type !== 'checkin' || salvadorOriginalScans[0].paired_scan_id != null ||
        Number(salvadorOriginalScans[1].id) !== 883 ||
        salvadorOriginalScans[1].stand_name !== 'Vision' ||
        salvadorOriginalScans[1].scan_type !== 'checkin' || salvadorOriginalScans[1].paired_scan_id != null ||
        Number(salvadorOriginalScans[2].id) !== 1139 ||
        salvadorOriginalScans[2].stand_name !== 'Vision' ||
        salvadorOriginalScans[2].scan_type !== 'checkout' ||
        Number(salvadorOriginalScans[2].paired_scan_id) !== 883 ||
        salvadorOriginalScans[2].service_status !== 'Completed') {
      throw new Error(`Salvador original-scan postcondition failed: ${JSON.stringify(salvadorOriginalScans)}`);
    }
    const [[salvadorScanCount]] = await connection.query(
      `SELECT COUNT(*) AS total FROM health_event_scan
        WHERE health_event_id = ? AND scanned_user_id = ?`, [event.id, SALVADOR_USER_ID]
    );
    if (Number(salvadorScanCount.total) !== 4) {
      throw new Error(`Salvador scan-count postcondition failed: ${JSON.stringify(salvadorScanCount)}`);
    }
    const [brokenScans] = await connection.query(
      `SELECT s.id FROM health_event_scan s
       LEFT JOIN health_event_registration r ON r.id = s.registration_id
       WHERE s.health_event_id = ? AND s.registration_id IS NOT NULL
         AND (r.id IS NULL OR r.user_id <> s.scanned_user_id)`, [event.id]
    );
    if (brokenScans.length) throw new Error(`Registration/user mismatches remain: ${JSON.stringify(brokenScans)}`);
    const [brokenPairs] = await connection.query(
      `SELECT out_scan.id, out_scan.paired_scan_id
         FROM health_event_scan out_scan
         LEFT JOIN health_event_scan in_scan ON in_scan.id = out_scan.paired_scan_id
        WHERE out_scan.health_event_id = ? AND out_scan.scan_type = 'checkout'
          AND (in_scan.id IS NULL OR in_scan.scan_type <> 'checkin'
               OR in_scan.health_event_id <> out_scan.health_event_id
               OR in_scan.stand_id <> out_scan.stand_id
               OR in_scan.scanned_user_id <> out_scan.scanned_user_id
               OR NOT (in_scan.registration_id <=> out_scan.registration_id)
               OR NOT (in_scan.service_id <=> out_scan.service_id)
               OR out_scan.scanned_at < in_scan.scanned_at)`, [event.id]
    );
    if (brokenPairs.length) throw new Error(`Broken scan pairs remain: ${JSON.stringify(brokenPairs)}`);

    const afterSnapshot = await captureState(connection, event.id);
    const snapshot = sanitize({
      runKey: RUN_KEY,
      mode: DRY_RUN ? 'dry-run-rolled-back' : 'prepared-for-commit',
      database: { name: identity.db, host: identity.db_host, sessionTimezone: identity.session_timezone },
      event,
      generatedAt: new Date().toISOString(),
      operations,
      before: beforeSnapshot,
      after: afterSnapshot
    });
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');

    if (DRY_RUN) {
      await connection.rollback();
      const dryRunPath = path.join(LOG_DIR, `${RUN_KEY}-production-dry-run-${stamp}.json`);
      fs.writeFileSync(dryRunPath, JSON.stringify(snapshot, null, 2));
      console.log(`[followup] ${operations.length} audited changes simulated and rolled back.`);
      console.log(`[followup] snapshot=${dryRunPath}`);
    } else {
      preparedPath = path.join(LOG_DIR, `${RUN_KEY}-production-prepared-${stamp}.json`);
      fs.writeFileSync(preparedPath, JSON.stringify(snapshot, null, 2));
      await connection.commit();
      committed = true;

      const appliedPath = path.join(LOG_DIR, `${RUN_KEY}-production-applied-${stamp}.json`);
      try {
        fs.writeFileSync(appliedPath, JSON.stringify({ ...snapshot, mode: 'applied' }, null, 2));
        fs.unlinkSync(preparedPath);
        preparedPath = null;
        console.log(`[followup] snapshot=${appliedPath}`);
      } catch (snapshotError) {
        console.warn(`[followup] COMMITTED, but final local snapshot failed: ${snapshotError.message}`);
        console.warn(`[followup] prepared snapshot remains at ${preparedPath}`);
      }
      console.log(`[followup] ${operations.length} audited changes committed.`);
    }
    console.log('[followup] Aly registrations=1; Aly duplicate Entry removed; Salvador Food=1; invariant mismatches=0; broken pairs=0');
  } catch (error) {
    if (!committed) {
      try { await connection.rollback(); } catch (_) { /* no-op */ }
    }
    throw error;
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error('[followup] FAILED:', error.stack || error.message || error);
  process.exit(1);
});
