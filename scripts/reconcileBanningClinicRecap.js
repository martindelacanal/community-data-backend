/*
 * Reconciles the post-event Banning clinic recap received on 2026-08-13.
 *
 * The script is deliberately event- and case-specific.  It resolves people by
 * their asserted identity, locks every row it changes, records before/after
 * JSON in health_event_reconciliation_audit and writes a local snapshot under
 * BACKEND/logs (ignored by Git).  A dry run executes the exact same DML and
 * rolls the transaction back.
 *
 * Usage (from BACKEND/):
 *   node scripts/reconcileBanningClinicRecap.js production --dry-run
 *   node scripts/reconcileBanningClinicRecap.js production --apply \
 *     --confirm-production=BANNING-2026-08-RECAP
 *
 * Development can be inspected with the same command by replacing production
 * with development.  It is expected not to contain the production-only rows.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

const BACKEND_ROOT = path.resolve(__dirname, '..');
const ENV_PATH = path.join(BACKEND_ROOT, '.env');
const LOG_DIR = path.join(BACKEND_ROOT, 'logs');
const TARGET = String(process.argv[2] || '').toLowerCase();
const APPLY = process.argv.includes('--apply');
const SANITIZE_EXISTING_AUDIT = process.argv.includes('--sanitize-existing-audit');
const DRY_RUN = process.argv.includes('--dry-run') || (!APPLY && !SANITIZE_EXISTING_AUDIT);
const PROD_CONFIRMATION = '--confirm-production=BANNING-2026-08-RECAP';
const PROD_SANITIZE_CONFIRMATION = '--confirm-audit-sanitization=BANNING-2026-08-RECAP';
const RUN_KEY = 'banning-clinic-recap-2026-08-13-v1';
const DEFERRED_CASES = [
  'Aly Lo: the inaccurate response lives in the external Google Form; no database answer is changed.',
  'Stephanie/Angelina: shared-QR service attribution is ambiguous, so scan #117 and all service scans stay unchanged.',
  'Contreras family: food is already recorded for Jose and Fabiola; no per-child food scans are inferred.',
  'Contreras family: the late Jose/Fabiola service pairs remain until the clinic confirms which exact scans were volunteer errors.',
  'Jesus Rodriguez and George Alishak: no attendance is inferred from registrations alone.',
  'Michael Alishak: the appointment date remains unchanged because the recap and admin screenshot conflict.'
];

if (!['development', 'production'].includes(TARGET) ||
    [APPLY, process.argv.includes('--dry-run'), SANITIZE_EXISTING_AUDIT].filter(Boolean).length > 1) {
  console.error('Usage: node scripts/reconcileBanningClinicRecap.js <development|production> <--dry-run|--apply|--sanitize-existing-audit>');
  process.exit(1);
}
if (TARGET === 'production' && APPLY && !process.argv.includes(PROD_CONFIRMATION)) {
  console.error(`Production apply refused. Add ${PROD_CONFIRMATION}`);
  process.exit(1);
}
if (TARGET === 'production' && SANITIZE_EXISTING_AUDIT && !process.argv.includes(PROD_SANITIZE_CONFIRMATION)) {
  console.error(`Production audit sanitization refused. Add ${PROD_SANITIZE_CONFIRMATION}`);
  process.exit(1);
}

function normalizeEnvValue(value) {
  return String(value == null ? '' : value).trim().replace(/^(['"])(.*)\1$/, '$2').trim();
}

function readDatabaseBlock(envText, heading, commented) {
  const lines = String(envText).split(/\r?\n/);
  const headingPattern = new RegExp(`^\\s*#\\s*${heading}\\s*$`, 'i');
  const assignmentPattern = commented
    ? /^\s*#\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i
    : /^\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i;
  const values = {};
  let inside = false;
  let found = false;
  for (const line of lines) {
    if (!inside) {
      inside = headingPattern.test(line);
      continue;
    }
    const match = line.match(assignmentPattern);
    if (match) {
      values[match[1].toUpperCase()] = normalizeEnvValue(match[2]);
      found = true;
      continue;
    }
    if (found && line.trim() === '') break;
  }
  return values;
}

function readDatabaseConfig(target) {
  const envText = fs.readFileSync(ENV_PATH, 'utf8');
  const values = readDatabaseBlock(
    envText,
    target === 'production' ? 'PRODUCTION DATABASE' : 'DEVELOPMENT DATABASE',
    target === 'production'
  );
  for (const key of ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT']) {
    if (!values[key]) throw new Error(`Missing ${key} in the ${target} database block`);
  }
  const production = readDatabaseBlock(envText, 'PRODUCTION DATABASE', true);
  const sameAsProduction = ['DB_HOST', 'DB_USER', 'DB_DATABASE', 'DB_PORT']
    .every(key => normalizeEnvValue(values[key]).toLowerCase() === normalizeEnvValue(production[key]).toLowerCase());
  if (target === 'production' && !sameAsProduction) throw new Error('Production block does not match itself');
  if (target === 'development' && sameAsProduction) throw new Error('Development target unexpectedly matches production');
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

function asJson(value) {
  return value == null ? null : JSON.stringify(value);
}

const SENSITIVE_AUDIT_KEY = /(?:password|passwd|secret|token|credential|api[_-]?key|private[_-]?key|salt|otp|auth[_-]?code)/i;

function sanitizeAuditValue(value) {
  if (value == null || typeof value !== 'object') return value;
  if (Buffer.isBuffer(value)) return `[binary:${value.length} bytes]`;
  if (Array.isArray(value)) return value.map(sanitizeAuditValue);
  const clean = {};
  for (const [key, child] of Object.entries(value)) {
    if (SENSITIVE_AUDIT_KEY.test(key)) continue;
    clean[key] = sanitizeAuditValue(child);
  }
  return clean;
}

function sanitizeLocalSnapshots() {
  if (!fs.existsSync(LOG_DIR)) return 0;
  const names = fs.readdirSync(LOG_DIR).filter(name =>
    name.startsWith(`${RUN_KEY}-production-`) && name.endsWith('.json')
  );
  let changed = 0;
  for (const name of names) {
    const filePath = path.join(LOG_DIR, name);
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const serializedBefore = JSON.stringify(parsed);
    const serializedAfter = JSON.stringify(sanitizeAuditValue(parsed), null, 2);
    if (serializedBefore !== JSON.stringify(JSON.parse(serializedAfter))) {
      fs.writeFileSync(filePath, serializedAfter);
      changed += 1;
    }
    if (SENSITIVE_AUDIT_KEY.test('')) SENSITIVE_AUDIT_KEY.lastIndex = 0;
    const sensitiveKeyPattern = /"(?:password|passwd|secret|token|credential|api[_-]?key|private[_-]?key|salt|otp|auth[_-]?code)"\s*:/i;
    if (sensitiveKeyPattern.test(fs.readFileSync(filePath, 'utf8'))) {
      throw new Error(`Sensitive audit key remains in local snapshot ${name}`);
    }
  }
  return changed;
}

async function sanitizePersistedAudit(connection) {
  const localFilesSanitized = sanitizeLocalSnapshots();
  await connection.beginTransaction();
  try {
    const [rows] = await connection.query(
      `SELECT id FROM health_event_reconciliation_audit
        WHERE run_key = ? AND action_type = 'merge-user'
          AND JSON_CONTAINS_PATH(before_json, 'one', '$.user.password', '$.user.reset_password')
        ORDER BY id FOR UPDATE`, [RUN_KEY]
    );
    if (rows.length !== 4) {
      throw new Error(`Expected exactly 4 sensitive merge-user audit rows, found ${rows.length}`);
    }
    const [result] = await connection.query(
      `UPDATE health_event_reconciliation_audit
          SET before_json = JSON_REMOVE(before_json, '$.user.password', '$.user.reset_password')
        WHERE run_key = ? AND action_type = 'merge-user'
          AND JSON_CONTAINS_PATH(before_json, 'one', '$.user.password', '$.user.reset_password')`,
      [RUN_KEY]
    );
    if (Number(result.affectedRows) !== 4) {
      throw new Error(`Expected to sanitize 4 audit rows, changed ${result.affectedRows}`);
    }
    const sensitiveKeyRegex = '"(password|passwd|secret|token|credential|api[_-]?key|private[_-]?key|salt|otp|auth[_-]?code)"[[:space:]]*:';
    const [[remaining]] = await connection.query(
      `SELECT COUNT(*) AS total FROM health_event_reconciliation_audit
        WHERE run_key = ? AND
          (CAST(before_json AS CHAR) REGEXP ? OR CAST(after_json AS CHAR) REGEXP ?)`,
      [RUN_KEY, sensitiveKeyRegex, sensitiveKeyRegex]
    );
    if (Number(remaining.total) !== 0) {
      throw new Error(`${remaining.total} persisted audit rows still contain sensitive keys`);
    }
    await connection.commit();
    console.log(`[recap] sanitized persisted audit rows=${result.affectedRows}; local snapshots=${localFilesSanitized}; residual sensitive rows=0`);
  } catch (error) {
    await connection.rollback();
    throw error;
  }
}

function printable(value) {
  return JSON.stringify(value, null, 2);
}

async function tableExists(connection, tableName) {
  const [[row]] = await connection.query(
    'SELECT COUNT(*) AS total FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?',
    [tableName]
  );
  return Number(row.total) > 0;
}

async function ensureAuditTable(connection) {
  await connection.query(`
    CREATE TABLE IF NOT EXISTS health_event_reconciliation_audit (
      id BIGINT NOT NULL AUTO_INCREMENT,
      run_key VARCHAR(100) NOT NULL,
      action_key VARCHAR(160) NOT NULL,
      action_type VARCHAR(40) NOT NULL,
      target_table VARCHAR(80) NOT NULL,
      target_id VARCHAR(160) DEFAULT NULL,
      before_json JSON DEFAULT NULL,
      after_json JSON DEFAULT NULL,
      note VARCHAR(1000) DEFAULT NULL,
      applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      PRIMARY KEY (id),
      UNIQUE KEY uq_health_event_reconciliation_action (action_key),
      KEY idx_health_event_reconciliation_run (run_key, applied_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci
  `);
}

async function main() {
  const databaseConfig = readDatabaseConfig(TARGET);
  const connection = await mysql.createConnection(databaseConfig);
  const operations = [];
  let committed = false;

  const record = async ({ actionKey, actionType, targetTable, targetId, before, after, note }) => {
    const safeBefore = sanitizeAuditValue(before);
    const safeAfter = sanitizeAuditValue(after);
    operations.push({ actionKey, actionType, targetTable, targetId, before: safeBefore, after: safeAfter, note });
    if (!DRY_RUN) {
      await connection.query(
        `INSERT INTO health_event_reconciliation_audit
           (run_key, action_key, action_type, target_table, target_id, before_json, after_json, note)
         VALUES (?,?,?,?,?,?,?,?)`,
        [RUN_KEY, actionKey, actionType, targetTable, targetId == null ? null : String(targetId),
          asJson(safeBefore), asJson(safeAfter), note || null]
      );
    }
  };

  try {
    const [[identity]] = await connection.query(
      'SELECT DATABASE() AS db, @@hostname AS db_host, @@session.time_zone AS session_timezone'
    );
    const modeLabel = SANITIZE_EXISTING_AUDIT ? 'SANITIZE EXISTING AUDIT' : (DRY_RUN ? 'DRY-RUN (rollback)' : 'APPLY');
    console.log(`[recap] target=${TARGET} mode=${modeLabel}`);
    console.log(`[recap] database=${identity.db} host=${identity.db_host} timezone=${identity.session_timezone}`);

    if (TARGET === 'production' && !String(databaseConfig.host).includes('database-1.')) {
      throw new Error('Production safety check failed: unexpected host');
    }
    if (SANITIZE_EXISTING_AUDIT) {
      if (!(await tableExists(connection, 'health_event_reconciliation_audit'))) {
        throw new Error('Audit table does not exist');
      }
      await sanitizePersistedAudit(connection);
      return;
    }
    let priorAuditRows = 0;
    const auditTableAlreadyExists = await tableExists(connection, 'health_event_reconciliation_audit');
    if (auditTableAlreadyExists) {
      const [[previousRun]] = await connection.query(
        'SELECT COUNT(*) AS total FROM health_event_reconciliation_audit WHERE run_key = ?', [RUN_KEY]
      );
      priorAuditRows = Number(previousRun.total);
    }
    console.log(`[recap] persisted audit rows for run=${priorAuditRows}`);
    if (!DRY_RUN) {
      if (!auditTableAlreadyExists) await ensureAuditTable(connection);
      if (priorAuditRows > 0) throw new Error(`${RUN_KEY} already has persisted audit rows; refusing to apply it again`);
    }

    // From this point through every postcondition and snapshot, identities stay
    // locked and all reads/writes describe one atomic database state.
    await connection.beginTransaction();

    const [[event]] = await connection.query(
      `SELECT id, slug, name_en, start_date, end_date, timezone
         FROM health_event WHERE slug = 'banning' LIMIT 1`
    );
    if (!event || String(event.start_date).slice(0, 10) !== '2026-08-08' ||
        String(event.end_date).slice(0, 10) !== '2026-08-09') {
      throw new Error('The expected Banning event was not found');
    }

    const people = {};
    async function resolvePerson(key, { email = null, phone = null, dob, expectedId = null, allowMerged = false }) {
      const params = [event.id];
      const predicates = [];
      if (email) {
        predicates.push('LOWER(TRIM(u.email)) = LOWER(?)');
        params.push(email);
      }
      if (phone) {
        predicates.push("RIGHT(REGEXP_REPLACE(COALESCE(u.phone,''), '[^0-9]', ''), 10) = ?");
        params.push(String(phone).replace(/\D/g, '').slice(-10));
      }
      params.push(dob);
      const [rows] = await connection.query(
        `SELECT u.id, u.username, u.email, u.firstname, u.lastname, u.date_of_birth, u.phone,
                u.enabled, u.deleted, r.id AS registration_id, r.status AS registration_status
           FROM user u
           LEFT JOIN health_event_registration r
             ON r.user_id = u.id AND r.health_event_id = ? AND r.registration_role = 'beneficiary'
          WHERE (${predicates.join(' OR ')}) AND u.date_of_birth = ?
          FOR UPDATE`,
        params
      );
      if (rows.length !== 1) throw new Error(`${key}: expected one person, found ${rows.length}`);
      if (expectedId != null && Number(rows[0].id) !== Number(expectedId)) {
        throw new Error(`${key}: expected user #${expectedId}, found #${rows[0].id}`);
      }
      if (rows[0].registration_id == null && !(allowMerged && rows[0].enabled === 'N' && rows[0].deleted === 'Y')) {
        throw new Error(`${key}: active event registration missing`);
      }
      people[key] = rows[0];
      return rows[0];
    }

    // Every identity is tied to the event, DOB and an exact asserted contact.
    await resolvePerson('rebecca', { email: 'rk-health@nym.hush.com', dob: '1954-05-30', expectedId: 57637 });
    await resolvePerson('tomasa', { email: 'ortizvianey05@gmail.com', dob: '1978-09-20', expectedId: 40690 });
    await resolvePerson('elpidio', { email: 'avila30378@gmail.com', dob: '1958-09-10', expectedId: 57743 });
    await resolvePerson('mark', { email: 'chefmj58@yahoo.com', dob: '1958-07-03', expectedId: 58452 });
    await resolvePerson('roberta', { email: 'bobbieweiss87@gmail.com', dob: '1969-10-15', expectedId: 40554 });
    await resolvePerson('russell', { email: 'russweiss60@gmail.com', dob: '1967-11-04', expectedId: 53134 });
    await resolvePerson('stephanie', { email: 'stephanieaguirre247@yahoo.com', dob: '1995-01-17', expectedId: 56734 });
    await resolvePerson('angelina', { phone: '9512098431', dob: '1940-04-25', expectedId: 57605 });
    await resolvePerson('dianaCanonical', { email: 'disna92551@yahoo.com', dob: '1983-02-04', expectedId: 58470 });
    await resolvePerson('dianaAli', { email: 'diana92551@yahoo.com', dob: '1983-02-24', expectedId: 4839, allowMerged: true });
    await resolvePerson('dianaGmail', { email: 'dianaalishakrose@gmail.com', dob: '1983-02-24', expectedId: 58432, allowMerged: true });
    await resolvePerson('dianaRosess', { email: 'dianaalishakrosess@yahoo.com', dob: '1983-08-05', expectedId: 58981, allowMerged: true });
    await resolvePerson('michaelCanonical', { email: 'michael92551@yahoo.com', dob: '2008-05-01', expectedId: 58927 });
    await resolvePerson('michaelDuplicate', { phone: '7142521388', dob: '2012-05-01', expectedId: 59032, allowMerged: true });
    await resolvePerson('eduardo', { email: 'eddie9948@gmail.com', dob: '1970-06-26', expectedId: 59006 });
    await resolvePerson('blanca', { phone: '5626524530', dob: '1950-10-21', expectedId: 59064 });
    await resolvePerson('thania', { phone: '9097126084', dob: '2013-04-06', expectedId: 58624 });
    await resolvePerson('dulce', { phone: '9097126084', dob: '2017-12-27', expectedId: 58625 });
    await resolvePerson('fabiola', { phone: '9097126084', dob: '1977-07-17', expectedId: 41590 });
    await resolvePerson('jose', { email: 'jos2019contrerasl@gmail.com', dob: '1971-12-22', expectedId: 42710 });
    await resolvePerson('karla', { phone: '9098109395', dob: '1984-03-02', expectedId: 59000 });
    await resolvePerson('amanda', { email: 'olivaresamanda1974@gmail.com', dob: '1974-02-22', expectedId: 58535 });
    await resolvePerson('starris', { phone: '4247812288', dob: '1998-10-14', expectedId: 57718 });
    await resolvePerson('julie', { email: 'myzombiebreath51.50@gmail.com', dob: '1969-02-08', expectedId: 59033 });
    await resolvePerson('rosalina', { email: 'rubyfont@gmail.com', dob: '1983-05-31', expectedId: 59090 });
    await resolvePerson('jesus', { email: 'pyrodriguez87@gmail.com', dob: '1949-03-26', expectedId: 59091 });
    await resolvePerson('george', { phone: '7144950392', dob: '1984-06-05', expectedId: 58925 });

    const relevantUserIds = Array.from(new Set(Object.values(people).map(person => Number(person.id))));

    async function captureSnapshot() {
      const placeholders = relevantUserIds.map(() => '?').join(',');
      const [users] = await connection.query(
        `SELECT id, date_of_birth, enabled, deleted, deleted_at, reset_password,
                client_id, location_id, modification_date
           FROM user WHERE id IN (${placeholders}) ORDER BY id`, relevantUserIds
      );
      const [registrations] = await connection.query(
        `SELECT r.* FROM health_event_registration r
          WHERE r.health_event_id = ? AND r.user_id IN (${placeholders}) ORDER BY r.id`,
        [event.id, ...relevantUserIds]
      );
      const registrationIds = registrations.map(row => row.id);
      const registrationPlaceholders = registrationIds.length ? registrationIds.map(() => '?').join(',') : 'NULL';
      const [dates] = registrationIds.length
        ? await connection.query(`SELECT * FROM health_event_registration_date WHERE registration_id IN (${registrationPlaceholders}) ORDER BY id`, registrationIds)
        : [[]];
      const [appointments] = registrationIds.length
        ? await connection.query(
          `SELECT a.*, sl.service_key, sl.slot_date, TIME_FORMAT(sl.start_time, '%H:%i') AS start_time
             FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
            WHERE a.registration_id IN (${registrationPlaceholders}) ORDER BY a.id`, registrationIds)
        : [[]];
      const [answers] = registrationIds.length
        ? await connection.query(`SELECT * FROM health_event_answer WHERE registration_id IN (${registrationPlaceholders}) ORDER BY id`, registrationIds)
        : [[]];
      const answerIds = answers.map(row => row.id);
      const [answerOptions] = answerIds.length
        ? await connection.query(`SELECT * FROM health_event_answer_option WHERE answer_id IN (${answerIds.map(() => '?').join(',')}) ORDER BY answer_id, option_id`, answerIds)
        : [[]];
      const [scans] = await connection.query(
        `SELECT s.*, st.name_en AS stand_name, ss.name_en AS service_name
           FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
           LEFT JOIN health_event_stand_service ss ON ss.id = s.service_id
          WHERE s.health_event_id = ? AND (s.scanned_user_id IN (${placeholders}) OR s.id IN (1171,1249,1251,1269,1270,1271,1272))
          ORDER BY s.id`, [event.id, ...relevantUserIds]
      );
      const scanIds = scans.map(row => row.id);
      const [scanAnswers] = scanIds.length
        ? await connection.query(`SELECT * FROM health_event_scan_answer WHERE scan_id IN (${scanIds.map(() => '?').join(',')}) ORDER BY id`, scanIds)
        : [[]];
      const scanAnswerIds = scanAnswers.map(row => row.id);
      const [scanAnswerOptions] = scanAnswerIds.length
        ? await connection.query(`SELECT * FROM health_event_scan_answer_option WHERE scan_answer_id IN (${scanAnswerIds.map(() => '?').join(',')}) ORDER BY scan_answer_id, option_id`, scanAnswerIds)
        : [[]];
      return { users, registrations, dates, appointments, answers, answerOptions, scans, scanAnswers, scanAnswerOptions };
    }

    const beforeSnapshot = await captureSnapshot();

    async function cancelAppointment(actionKey, person, serviceKey, slotDate, startTime) {
      const [rows] = await connection.query(
        `SELECT a.id, a.status, a.registration_id, sl.service_key, sl.slot_date,
                TIME_FORMAT(sl.start_time, '%H:%i') AS start_time
           FROM health_event_appointment a
           INNER JOIN health_event_slot sl ON sl.id = a.slot_id
          WHERE a.registration_id = ? AND sl.health_event_id = ? AND sl.service_key = ?
            AND sl.slot_date = ? AND TIME_FORMAT(sl.start_time, '%H:%i') = ?
          FOR UPDATE`,
        [person.registration_id, event.id, serviceKey, slotDate, startTime]
      );
      if (rows.length !== 1) throw new Error(`${actionKey}: expected one appointment, found ${rows.length}`);
      const before = rows[0];
      if (before.status !== 'cancelled') {
        await connection.query(`UPDATE health_event_appointment SET status = 'cancelled' WHERE id = ?`, [before.id]);
        await record({ actionKey, actionType: 'update', targetTable: 'health_event_appointment', targetId: before.id,
          before, after: { ...before, status: 'cancelled' }, note: 'Participant notified the clinic that they would not attend.' });
      }
    }

    await cancelAppointment('cancel-rebecca-dental-20260808-1200', people.rebecca, 'dental', '2026-08-08', '12:00');
    await cancelAppointment('cancel-tomasa-dental-20260809-0800', people.tomasa, 'dental', '2026-08-09', '08:00');
    await cancelAppointment('cancel-elpidio-dental-20260808-1000', people.elpidio, 'dental', '2026-08-08', '10:00');
    await cancelAppointment('cancel-mark-vision-20260809-1300', people.mark, 'vision', '2026-08-09', '13:00');
    await cancelAppointment('cancel-roberta-vision-20260809-1100', people.roberta, 'vision', '2026-08-09', '11:00');
    await cancelAppointment('cancel-russell-vision-20260809-1100', people.russell, 'vision', '2026-08-09', '11:00');

    async function correctDateOfBirth(actionKey, person, expectedBefore, correctedDate, note) {
      const [rows] = await connection.query(
        'SELECT id, date_of_birth FROM user WHERE id = ? FOR UPDATE', [person.id]
      );
      if (rows.length !== 1) throw new Error(`${actionKey}: user missing`);
      const before = rows[0];
      if (String(before.date_of_birth).slice(0, 10) === correctedDate) return;
      if (String(before.date_of_birth).slice(0, 10) !== expectedBefore) {
        throw new Error(`${actionKey}: unexpected existing DOB ${before.date_of_birth}`);
      }
      await connection.query('UPDATE user SET date_of_birth = ? WHERE id = ?', [correctedDate, person.id]);
      await record({ actionKey, actionType: 'update', targetTable: 'user', targetId: person.id,
        before, after: { ...before, date_of_birth: correctedDate }, note });
      person.date_of_birth = correctedDate;
    }

    await correctDateOfBirth('correct-jose-contreras-date-of-birth', people.jose,
      '1971-12-21', '1971-12-22', 'The recap explicitly provides 12/22/1971; production stored 12/21/1971.');

    async function moveRegistration(actionKey, sourcePerson, targetPerson, note) {
      if (sourcePerson.registration_id == null) return;
      if (Number(sourcePerson.registration_id) === Number(targetPerson.registration_id)) return;
      const [sourceRows] = await connection.query(
        'SELECT * FROM health_event_registration WHERE id = ? FOR UPDATE', [sourcePerson.registration_id]
      );
      const [targetRows] = await connection.query(
        'SELECT * FROM health_event_registration WHERE id = ? FOR UPDATE', [targetPerson.registration_id]
      );
      // Idempotent re-runs resolve the source person before this point only while
      // the source registration still exists, so a missing row is always unsafe.
      if (sourceRows.length !== 1 || targetRows.length !== 1) {
        throw new Error(`${actionKey}: source/target registration missing`);
      }
      const sourceRegistration = sourceRows[0];
      const targetRegistration = targetRows[0];

      const [targetDatesBefore] = await connection.query(
        'SELECT * FROM health_event_registration_date WHERE registration_id = ? ORDER BY id FOR UPDATE',
        [targetRegistration.id]
      );
      const [targetAppointmentsBefore] = await connection.query(
        'SELECT * FROM health_event_appointment WHERE registration_id = ? ORDER BY id FOR UPDATE',
        [targetRegistration.id]
      );
      const [targetAnswersBefore] = await connection.query(
        'SELECT * FROM health_event_answer WHERE registration_id = ? ORDER BY id FOR UPDATE',
        [targetRegistration.id]
      );
      const targetAnswerIds = targetAnswersBefore.map(answer => answer.id);
      const [targetAnswerOptionsBefore] = targetAnswerIds.length
        ? await connection.query(
          `SELECT * FROM health_event_answer_option
            WHERE answer_id IN (${targetAnswerIds.map(() => '?').join(',')})
            ORDER BY answer_id, option_id FOR UPDATE`, targetAnswerIds
        )
        : [[]];

      const [sourceDates] = await connection.query(
        'SELECT * FROM health_event_registration_date WHERE registration_id = ? ORDER BY id FOR UPDATE',
        [sourceRegistration.id]
      );
      for (const dateRow of sourceDates) {
        const [existing] = await connection.query(
          'SELECT * FROM health_event_registration_date WHERE registration_id = ? AND event_date = ? LIMIT 1 FOR UPDATE',
          [targetRegistration.id, dateRow.event_date]
        );
        if (!existing.length) {
          await connection.query(
            'UPDATE health_event_registration_date SET registration_id = ? WHERE id = ?',
            [targetRegistration.id, dateRow.id]
          );
        } else {
          if (!existing[0].priority_service && dateRow.priority_service) {
            await connection.query(
              'UPDATE health_event_registration_date SET priority_service = ? WHERE id = ?',
              [dateRow.priority_service, existing[0].id]
            );
          }
          await connection.query('DELETE FROM health_event_registration_date WHERE id = ?', [dateRow.id]);
        }
      }

      const [sourceAppointments] = await connection.query(
        'SELECT * FROM health_event_appointment WHERE registration_id = ? ORDER BY id FOR UPDATE',
        [sourceRegistration.id]
      );
      for (const appointment of sourceAppointments) {
        const [existing] = await connection.query(
          'SELECT * FROM health_event_appointment WHERE registration_id = ? AND slot_id = ? LIMIT 1 FOR UPDATE',
          [targetRegistration.id, appointment.slot_id]
        );
        if (!existing.length) {
          await connection.query('UPDATE health_event_appointment SET registration_id = ? WHERE id = ?',
            [targetRegistration.id, appointment.id]);
        } else {
          await connection.query('DELETE FROM health_event_appointment WHERE id = ?', [appointment.id]);
        }
      }

      const [sourceAnswers] = await connection.query(
        'SELECT * FROM health_event_answer WHERE registration_id = ? ORDER BY id FOR UPDATE',
        [sourceRegistration.id]
      );
      const sourceAnswerIds = sourceAnswers.map(answer => answer.id);
      const [sourceAnswerOptions] = sourceAnswerIds.length
        ? await connection.query(
          `SELECT * FROM health_event_answer_option
            WHERE answer_id IN (${sourceAnswerIds.map(() => '?').join(',')})
            ORDER BY answer_id, option_id FOR UPDATE`, sourceAnswerIds
        )
        : [[]];
      for (const answer of sourceAnswers) {
        const [existing] = await connection.query(
          'SELECT id FROM health_event_answer WHERE registration_id = ? AND question_id = ? LIMIT 1 FOR UPDATE',
          [targetRegistration.id, answer.question_id]
        );
        if (!existing.length) {
          await connection.query('UPDATE health_event_answer SET registration_id = ? WHERE id = ?',
            [targetRegistration.id, answer.id]);
        } else {
          // The asserted canonical account wins response conflicts; the full
          // discarded row remains in the pre-snapshot and audit entry.
          await connection.query('DELETE FROM health_event_answer WHERE id = ?', [answer.id]);
        }
      }

      const [sourceScans] = await connection.query(
        `SELECT * FROM health_event_scan
          WHERE health_event_id = ? AND (registration_id = ? OR scanned_user_id = ?)
          ORDER BY id FOR UPDATE`,
        [event.id, sourceRegistration.id, sourceRegistration.user_id]
      );
      for (const scan of sourceScans) {
        if (scan.registration_id != null && Number(scan.registration_id) !== Number(sourceRegistration.id)) {
          throw new Error(`${actionKey}: scan #${scan.id} belongs to another registration`);
        }
      }
      await connection.query(
        `UPDATE health_event_scan SET registration_id = ?, scanned_user_id = ?
          WHERE health_event_id = ? AND (registration_id = ? OR scanned_user_id = ?)`,
        [targetRegistration.id, targetRegistration.user_id, event.id, sourceRegistration.id, sourceRegistration.user_id]
      );
      await connection.query('DELETE FROM health_event_registration WHERE id = ?', [sourceRegistration.id]);

      await record({
        actionKey,
        actionType: 'merge-registration',
        targetTable: 'health_event_registration',
        targetId: `${sourceRegistration.id}->${targetRegistration.id}`,
        before: {
          source: { registration: sourceRegistration, dates: sourceDates, appointments: sourceAppointments,
            answers: sourceAnswers, answerOptions: sourceAnswerOptions, scanIds: sourceScans.map(row => row.id) },
          target: { registration: targetRegistration, dates: targetDatesBefore, appointments: targetAppointmentsBefore,
            answers: targetAnswersBefore, answerOptions: targetAnswerOptionsBefore }
        },
        after: { targetRegistrationId: targetRegistration.id, targetUserId: targetRegistration.user_id },
        note
      });
    }

    async function moveGlobalUserReferences(actionKey, sourcePerson, targetPerson, note) {
      const sourceUserId = Number(sourcePerson.id);
      const targetUserId = Number(targetPerson.id);
      const [sourceRows] = await connection.query(
        'SELECT id, enabled, deleted, deleted_at, reset_password FROM user WHERE id = ? FOR UPDATE', [sourceUserId]
      );
      const [targetRows] = await connection.query(
        'SELECT id, enabled, deleted, deleted_at FROM user WHERE id = ? FOR UPDATE', [targetUserId]
      );
      if (sourceRows.length !== 1 || targetRows.length !== 1) throw new Error(`${actionKey}: user missing`);

      const moved = {};
      const movedRows = {};
      const [memberships] = await connection.query(
        'SELECT client_id, user_id, checked, creation_date FROM client_user WHERE user_id = ? FOR UPDATE', [sourceUserId]
      );
      movedRows.client_user = memberships;
      for (const membership of memberships) {
        await connection.query(
          `INSERT INTO client_user(client_id, user_id, checked, creation_date)
           VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE checked = IF(checked = 'Y' OR VALUES(checked) = 'Y', 'Y', 'N')`,
          [membership.client_id, targetUserId, membership.checked, membership.creation_date]
        );
      }
      await connection.query('DELETE FROM client_user WHERE user_id = ?', [sourceUserId]);
      moved.client_user = memberships.length;

      const simpleReferences = [
        ['beneficiary_answer_history', 'user_id'],
        ['beneficiary_log', 'user_id'],
        ['delivery_beneficiary', 'receiving_user_id'],
        ['interaction_events', 'user_id'],
        ['interaction_sessions', 'user_id'],
        ['user_email_report', 'user_id'],
        ['user_question', 'user_id']
      ];
      for (const [tableName, columnName] of simpleReferences) {
        if (!(await tableExists(connection, tableName))) continue;
        const [primaryKeyRows] = await connection.query(
          `SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'
            ORDER BY ORDINAL_POSITION`, [tableName]
        );
        const auditColumns = Array.from(new Set([
          ...primaryKeyRows.map(row => row.COLUMN_NAME), columnName
        ])).map(column => `\`${column}\``).join(', ');
        const [referenceRows] = await connection.query(
          `SELECT ${auditColumns} FROM \`${tableName}\` WHERE \`${columnName}\` = ? FOR UPDATE`, [sourceUserId]
        );
        movedRows[`${tableName}.${columnName}`] = referenceRows;
        const [result] = await connection.query(
          `UPDATE \`${tableName}\` SET \`${columnName}\` = ? WHERE \`${columnName}\` = ?`,
          [targetUserId, sourceUserId]
        );
        moved[`${tableName}.${columnName}`] = result.affectedRows;
      }

      const [remainingRegistrations] = await connection.query(
        'SELECT id FROM health_event_registration WHERE user_id = ? LIMIT 1', [sourceUserId]
      );
      const [remainingScans] = await connection.query(
        'SELECT id FROM health_event_scan WHERE scanned_user_id = ? OR volunteer_user_id = ? LIMIT 1',
        [sourceUserId, sourceUserId]
      );
      if (remainingRegistrations.length || remainingScans.length) {
        throw new Error(`${actionKey}: health-event references remain for source user #${sourceUserId}`);
      }

      await connection.query(
        `UPDATE user SET enabled = 'N', deleted = 'Y', deleted_at = COALESCE(deleted_at, NOW()),
                         reset_password = 'N'
          WHERE id = ?`, [sourceUserId]
      );
      const [[afterUser]] = await connection.query(
        'SELECT id, enabled, deleted, deleted_at FROM user WHERE id = ?', [sourceUserId]
      );
      await record({ actionKey, actionType: 'merge-user', targetTable: 'user',
        targetId: `${sourceUserId}->${targetUserId}`, before: { user: sourceRows[0], movedRows },
        after: { source: afterUser, mergedIntoUserId: targetUserId, moved }, note });
    }

    // The event form stored on Diana's older account explicitly says she was
    // registering someone else, identifies the participant as a minor/male and
    // its Saturday scans provide Michael's otherwise missing vision visit.
    await moveRegistration('merge-event-diana-ali-registration-into-michael', people.dianaAli, people.michaelCanonical,
      'Registration content and the recap identify this Saturday vision activity as Michael, while the global account belongs to Diana.');
    await moveGlobalUserReferences('merge-user-diana-ali-into-diana-canonical', people.dianaAli, people.dianaCanonical,
      'Consolidate Diana global account history into the asserted canonical account.');

    await moveRegistration('merge-event-diana-gmail-into-canonical', people.dianaGmail, people.dianaCanonical,
      'Consolidate duplicate Diana event registration; canonical answers win conflicts.');
    await moveGlobalUserReferences('merge-user-diana-gmail-into-canonical', people.dianaGmail, people.dianaCanonical,
      'Disable duplicate Diana login after moving owned records.');

    await moveRegistration('merge-event-diana-rosess-into-canonical', people.dianaRosess, people.dianaCanonical,
      'Consolidate Saturday Diana services into the asserted canonical account.');
    await moveGlobalUserReferences('merge-user-diana-rosess-into-canonical', people.dianaRosess, people.dianaCanonical,
      'Disable duplicate Diana login after moving owned records.');

    await moveRegistration('merge-event-michael-duplicate-into-canonical', people.michaelDuplicate, people.michaelCanonical,
      'The recap explicitly identifies the Michael account with email as canonical.');
    await moveGlobalUserReferences('merge-user-michael-duplicate-into-canonical', people.michaelDuplicate, people.michaelCanonical,
      'Disable duplicate Michael login after moving owned records.');

    async function getScan(scanId) {
      const [rows] = await connection.query(
        `SELECT s.*, st.name_en AS stand_name
           FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
          WHERE s.id = ? AND s.health_event_id = ? FOR UPDATE`, [scanId, event.id]
      );
      return rows[0] || null;
    }

    async function reassignScan(actionKey, scanId, expectedSourceUserId, targetPerson, expectedStandName, note) {
      const before = await getScan(scanId);
      if (!before) throw new Error(`${actionKey}: scan #${scanId} missing`);
      if (Number(before.scanned_user_id) === Number(targetPerson.id) &&
          Number(before.registration_id) === Number(targetPerson.registration_id)) return;
      if (Number(before.scanned_user_id) !== Number(expectedSourceUserId) || before.stand_name !== expectedStandName) {
        throw new Error(`${actionKey}: scan #${scanId} no longer matches its expected source`);
      }
      await connection.query(
        'UPDATE health_event_scan SET scanned_user_id = ?, registration_id = ? WHERE id = ?',
        [targetPerson.id, targetPerson.registration_id, scanId]
      );
      const after = await getScan(scanId);
      await record({ actionKey, actionType: 'reassign', targetTable: 'health_event_scan', targetId: scanId,
        before, after, note });
    }

    // The recap confirms that Stephanie's QR was shared, but it does not identify
    // which concrete scans belong to Angelina.  Preserve scan #117 and all other
    // service scans until the clinic supplies that attribution.

    async function deleteScans(actionKey, scanIds, expectedUserId, note) {
      const placeholders = scanIds.map(() => '?').join(',');
      const [rows] = await connection.query(
        `SELECT s.*, st.name_en AS stand_name
           FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
          WHERE s.health_event_id = ? AND s.id IN (${placeholders}) ORDER BY s.id FOR UPDATE`,
        [event.id, ...scanIds]
      );
      if (!rows.length) return;
      if (rows.length !== scanIds.length || rows.some(row => Number(row.scanned_user_id) !== Number(expectedUserId))) {
        throw new Error(`${actionKey}: scan set no longer matches the expected participant`);
      }
      const [externalPairs] = await connection.query(
        `SELECT id, paired_scan_id FROM health_event_scan
          WHERE paired_scan_id IN (${placeholders}) AND id NOT IN (${placeholders})`,
        [...scanIds, ...scanIds]
      );
      if (externalPairs.length) throw new Error(`${actionKey}: an external checkout references this scan set`);
      const [scanAnswers] = await connection.query(
        `SELECT sa.*, (SELECT GROUP_CONCAT(option_id ORDER BY option_id)
                        FROM health_event_scan_answer_option sao WHERE sao.scan_answer_id = sa.id) AS option_ids
           FROM health_event_scan_answer sa WHERE sa.scan_id IN (${placeholders}) ORDER BY sa.id`, scanIds
      );
      await connection.query(`DELETE FROM health_event_scan WHERE id IN (${placeholders})`, scanIds);
      await record({ actionKey, actionType: 'delete-audited', targetTable: 'health_event_scan',
        targetId: scanIds.join(','), before: { scans: rows, answers: scanAnswers }, after: null, note });
    }

    // Immediate, fully-closed duplicate toggles preceded the longer completed
    // dental visits for both Diana and Michael.  Removing these pairs leaves one
    // dental service per person, as stated in the recap.
    await deleteScans('remove-duplicate-diana-dental-pair-650-651', [650, 651], people.dianaCanonical.id,
      'Duplicate 20-second dental toggle; later completed pair 808/827 is retained.');
    await deleteScans('remove-duplicate-michael-dental-pair-663-674', [663, 674], people.michaelCanonical.id,
      'Duplicate short dental toggle; later completed pair 809/831 is retained.');

    // Jose/Fabiola's late service pairs resemble the volunteer batch, but the
    // recap also says their family may have checked out correctly.  Do not erase
    // those visits until the clinic identifies the exact erroneous scans.

    const [[foodStand]] = await connection.query(
      `SELECT id FROM health_event_stand
        WHERE health_event_id = ? AND name_en = 'Food Distribution' LIMIT 1`, [event.id]
    );
    if (!foodStand) throw new Error('Food Distribution stand missing');

    async function reclassifyAsFood(actionKey, scanId, expectedPerson, expectedStand, note) {
      const before = await getScan(scanId);
      if (!before) throw new Error(`${actionKey}: scan #${scanId} missing`);
      if (Number(before.scanned_user_id) !== Number(expectedPerson.id) || before.scan_type !== 'checkin') {
        throw new Error(`${actionKey}: participant/type mismatch`);
      }
      if (Number(before.stand_id) === Number(foodStand.id) && before.service_id == null) return;
      if (before.stand_name !== expectedStand || before.paired_scan_id != null) {
        throw new Error(`${actionKey}: expected an open ${expectedStand} check-in`);
      }
      const [[dependent]] = await connection.query(
        'SELECT COUNT(*) AS total FROM health_event_scan WHERE paired_scan_id = ?', [scanId]
      );
      const [[answers]] = await connection.query(
        'SELECT COUNT(*) AS total FROM health_event_scan_answer WHERE scan_id = ?', [scanId]
      );
      if (Number(dependent.total) || Number(answers.total)) throw new Error(`${actionKey}: scan has dependent data`);
      await connection.query(
        'UPDATE health_event_scan SET stand_id = ?, service_id = NULL WHERE id = ?', [foodStand.id, scanId]
      );
      const after = await getScan(scanId);
      await record({ actionKey, actionType: 'reclassify', targetTable: 'health_event_scan', targetId: scanId,
        before, after, note });
    }

    await reclassifyAsFood('reclassify-karla-scan-1163-to-food', 1163, people.karla, 'Vision',
      'The recap identifies the 13:42 second Vision check-in as Food Distribution.');
    await reclassifyAsFood('reclassify-amanda-scan-1173-to-food', 1173, people.amanda, 'Dental',
      'Open Dental check-in from the food-distribution batch; completed dental pair 802/1063 is retained.');
    await reclassifyAsFood('reclassify-starris-scan-1171-to-food', 1171, people.starris, 'Dental',
      'Exact handwritten phone match in the same volunteer batch; the prior real Vision checkout is retained.');

    async function findCheckoutQuestion(standId, questionName) {
      const [rows] = await connection.query(
        `SELECT q.id FROM health_event_question q
         INNER JOIN health_event_form f ON f.id = q.form_id
         WHERE f.health_event_id = ? AND f.audience = 'checkout' AND f.stand_id = ?
           AND q.name_en = ? LIMIT 1`, [event.id, standId, questionName]
      );
      if (rows.length !== 1) throw new Error(`Checkout question not found: ${standId}/${questionName}`);
      return rows[0].id;
    }

    async function setTextAnswer(actionKey, scanId, standId, questionName, text, note) {
      const questionId = await findCheckoutQuestion(standId, questionName);
      const [existing] = await connection.query(
        'SELECT * FROM health_event_scan_answer WHERE scan_id = ? AND question_id = ? LIMIT 1 FOR UPDATE',
        [scanId, questionId]
      );
      if (existing.length && existing[0].answer_text === text) return existing[0].id;
      let answerId;
      if (existing.length) {
        answerId = existing[0].id;
        await connection.query(
          'UPDATE health_event_scan_answer SET answer_text = ?, answer_number = NULL WHERE id = ?', [text, answerId]
        );
      } else {
        const [result] = await connection.query(
          'INSERT INTO health_event_scan_answer(scan_id, question_id, answer_text) VALUES (?,?,?)',
          [scanId, questionId, text]
        );
        answerId = result.insertId;
      }
      await record({ actionKey, actionType: existing.length ? 'update' : 'insert',
        targetTable: 'health_event_scan_answer', targetId: answerId, before: existing[0] || null,
        after: { id: answerId, scan_id: scanId, question_id: questionId, answer_text: text }, note });
      return answerId;
    }

    async function setStatusAnswer(actionKey, scanId, standId, statusName, note) {
      const questionId = await findCheckoutQuestion(standId, 'Service status');
      const [options] = await connection.query(
        `SELECT o.id FROM health_event_question_option o
          WHERE o.question_id = ? AND o.name_en = ? LIMIT 1`, [questionId, statusName]
      );
      if (options.length !== 1) throw new Error(`Status option not found: ${statusName}`);
      const optionId = options[0].id;
      const [existing] = await connection.query(
        'SELECT * FROM health_event_scan_answer WHERE scan_id = ? AND question_id = ? LIMIT 1 FOR UPDATE',
        [scanId, questionId]
      );
      let answerId;
      if (existing.length) {
        answerId = existing[0].id;
      } else {
        const [result] = await connection.query(
          'INSERT INTO health_event_scan_answer(scan_id, question_id) VALUES (?,?)', [scanId, questionId]
        );
        answerId = result.insertId;
      }
      const [currentOptions] = await connection.query(
        'SELECT option_id FROM health_event_scan_answer_option WHERE scan_answer_id = ? ORDER BY option_id FOR UPDATE',
        [answerId]
      );
      if (currentOptions.length === 1 && Number(currentOptions[0].option_id) === Number(optionId)) return answerId;
      await connection.query('DELETE FROM health_event_scan_answer_option WHERE scan_answer_id = ?', [answerId]);
      await connection.query(
        'INSERT INTO health_event_scan_answer_option(scan_answer_id, option_id) VALUES (?,?)', [answerId, optionId]
      );
      await record({ actionKey, actionType: existing.length ? 'update' : 'insert',
        targetTable: 'health_event_scan_answer', targetId: answerId,
        before: { answer: existing[0] || null, options: currentOptions },
        after: { id: answerId, scan_id: scanId, question_id: questionId, option_id: optionId, status: statusName }, note });
      return answerId;
    }

    const eduardoVisionCheckout = await getScan(508);
    if (!eduardoVisionCheckout || Number(eduardoVisionCheckout.scanned_user_id) !== Number(people.eduardo.id) ||
        eduardoVisionCheckout.stand_name !== 'Vision' || eduardoVisionCheckout.scan_type !== 'checkout') {
      throw new Error('Eduardo Vision checkout #508 no longer matches');
    }
    await setTextAnswer('eduardo-vision-regular-reading-glasses-note', 508, eduardoVisionCheckout.stand_id,
      'Notes', 'Received regular reading glasses.', 'Explicit treatment detail from the clinic recap.');

    async function ensureCheckin(actionKey, person, standName, scannedAt, volunteerUserId, note) {
      const [standRows] = await connection.query(
        'SELECT id, has_checkout FROM health_event_stand WHERE health_event_id = ? AND name_en = ? LIMIT 1',
        [event.id, standName]
      );
      if (standRows.length !== 1) throw new Error(`${actionKey}: stand ${standName} missing`);
      const stand = standRows[0];
      const [existing] = await connection.query(
        `SELECT * FROM health_event_scan WHERE health_event_id = ? AND scanned_user_id = ?
          AND stand_id = ? AND scan_type = 'checkin' AND scanned_at = ? LIMIT 1 FOR UPDATE`,
        [event.id, person.id, stand.id, scannedAt]
      );
      if (existing.length) return { scan: existing[0], stand };
      const [result] = await connection.query(
        `INSERT INTO health_event_scan
           (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
            volunteer_user_id, scan_type, paired_scan_id, scanned_at)
         VALUES (?,?,NULL,?,?,?,'checkin',NULL,?)`,
        [event.id, stand.id, person.registration_id, person.id, volunteerUserId, scannedAt]
      );
      const scan = await getScan(result.insertId);
      await record({ actionKey, actionType: 'insert-manual', targetTable: 'health_event_scan',
        targetId: result.insertId, before: null, after: scan, note });
      return { scan, stand };
    }

    async function ensureCheckout(actionKey, person, checkinId, scannedAt, volunteerUserId, statusName, note, notesText = null) {
      const checkin = await getScan(checkinId);
      if (!checkin || Number(checkin.scanned_user_id) !== Number(person.id) || checkin.scan_type !== 'checkin') {
        throw new Error(`${actionKey}: check-in #${checkinId} mismatch`);
      }
      const [existing] = await connection.query(
        `SELECT * FROM health_event_scan WHERE paired_scan_id = ? AND scan_type = 'checkout'
          ORDER BY id LIMIT 1 FOR UPDATE`, [checkinId]
      );
      let checkout;
      if (existing.length) {
        checkout = existing[0];
      } else {
        const [result] = await connection.query(
          `INSERT INTO health_event_scan
             (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
              volunteer_user_id, scan_type, paired_scan_id, scanned_at)
           VALUES (?,?,?,?,?,?,'checkout',?,?)`,
          [event.id, checkin.stand_id, checkin.service_id, person.registration_id, person.id,
            volunteerUserId, checkinId, scannedAt]
        );
        checkout = await getScan(result.insertId);
        await record({ actionKey, actionType: 'insert-manual', targetTable: 'health_event_scan',
          targetId: result.insertId, before: null, after: checkout, note });
      }
      await setStatusAnswer(`${actionKey}-status-${statusName.toLowerCase().replace(/\s+/g, '-')}`,
        checkout.id, checkin.stand_id, statusName, note);
      if (notesText) {
        await setTextAnswer(`${actionKey}-notes`, checkout.id, checkin.stand_id, 'Notes', notesText, note);
      }
      return checkout;
    }

    // Michael's Saturday vision check-in was preserved from Diana's shared
    // account.  The recap confirms treatment, so close that existing visit.
    const michaelVisionCheckout = await ensureCheckout('michael-saturday-vision-checkout', people.michaelCanonical, 123,
      '2026-08-08 19:45:36.000', 58932, 'Completed',
      'Administrative completion of Michael vision visit confirmed by the recap.');

    // Blanca: close the existing medical visit as a referral and record the
    // food bag.  No dental scan is created because the dentist did not treat her.
    const blancaMedicalCheckout = await ensureCheckout('blanca-medical-referral-checkout', people.blanca, 965,
      '2026-08-09 19:06:30.000', 59034, 'Referred',
      'Administrative checkout based on clinic recap.',
      'Elevated blood pressure; advised to seek emergency care. Dental treatment was not provided.');
    const blancaFood = await ensureCheckin('blanca-food-distribution', people.blanca, 'Food Distribution',
      '2026-08-09 19:06:45.000', 59031, 'Food bag documented as she left.');

    // Thania's scheduled dental visit ended in an explicit family refusal.
    const thaniaDental = await ensureCheckin('thania-dental-declined-checkin', people.thania, 'Dental',
      '2026-08-09 18:00:00.000', 1, 'Administrative visit at the scheduled 11:00 local slot.');
    const thaniaDentalCheckout = await ensureCheckout('thania-dental-declined-checkout', people.thania, thaniaDental.scan.id,
      '2026-08-09 18:00:30.000', 1, 'Declined treatment',
      'Family declined dental treatment for the participant.');

    // Jose and Fabiola already have Food Distribution scans.  The recap does
    // not establish whether one bag should be counted per household or per
    // participant, so no food visits are invented for Thania or Dulce.

    // Julie and Rosalina were seen by Vision as the clinic was closing.  The
    // sole surviving contemporaneous scan is Rosalina's Vision check-in #1320;
    // deterministic times around that anchor make the administrative additions
    // repeatable without pretending they came from QR device telemetry.
    const rosalinaVision = await getScan(1320);
    if (!rosalinaVision || Number(rosalinaVision.scanned_user_id) !== Number(people.rosalina.id) ||
        rosalinaVision.stand_name !== 'Vision' || rosalinaVision.scan_type !== 'checkin') {
      throw new Error('Rosalina Vision check-in #1320 no longer matches');
    }
    const rosalinaEntry = await ensureCheckin('rosalina-entry-checkin', people.rosalina, 'Entry Check-in',
      '2026-08-09 22:56:30.000', 58939, 'Administrative entry anchored to existing Vision scan #1320.');
    const rosalinaVisionCheckout = await ensureCheckout('rosalina-vision-checkout', people.rosalina, 1320,
      '2026-08-09 23:10:00.000', 58939, 'Completed',
      'Administrative completion requested in the clinic recap.');
    const rosalinaFood = await ensureCheckin('rosalina-food-distribution', people.rosalina, 'Food Distribution',
      '2026-08-09 23:10:30.000', 58939, 'Administrative food-distribution record requested in the recap.');

    const julieEntry = await ensureCheckin('julie-entry-checkin', people.julie, 'Entry Check-in',
      '2026-08-09 22:56:20.000', 58939, 'Administrative entry anchored to the closing-time companion visit.');
    const julieVision = await ensureCheckin('julie-vision-checkin', people.julie, 'Vision',
      '2026-08-09 22:57:10.000', 58939, 'Vision treatment confirmed by the clinic recap.');
    const julieVisionCheckout = await ensureCheckout('julie-vision-checkout', people.julie, julieVision.scan.id,
      '2026-08-09 23:10:10.000', 58939, 'Completed',
      'Administrative completion requested in the clinic recap.');
    const julieFood = await ensureCheckin('julie-food-distribution', people.julie, 'Food Distribution',
      '2026-08-09 23:10:40.000', 58939, 'Administrative food-distribution record requested in the recap.');

    // Explicit non-actions: these assertions make it impossible for a future
    // edit to silently turn uncertainty into attendance.
    for (const [key, person] of [['jesus', people.jesus], ['george', people.george]]) {
      const [[count]] = await connection.query(
        'SELECT COUNT(*) AS total FROM health_event_scan WHERE health_event_id = ? AND scanned_user_id = ?',
        [event.id, person.id]
      );
      if (Number(count.total) !== 0) throw new Error(`${key}: expected zero scans, found ${count.total}`);
    }

    async function assertStandaloneScan(label, scanResult, person, standName) {
      const scan = await getScan(scanResult.scan.id);
      if (!scan || Number(scan.scanned_user_id) !== Number(person.id) ||
          Number(scan.registration_id) !== Number(person.registration_id) ||
          scan.stand_name !== standName || scan.scan_type !== 'checkin' || scan.paired_scan_id != null) {
        throw new Error(`${label}: standalone scan postcondition failed`);
      }
    }

    async function assertCheckoutStatus(label, checkinId, checkoutId, expectedStatus) {
      const [rows] = await connection.query(
        `SELECT in_scan.id AS checkin_id, out_scan.id AS checkout_id,
                in_scan.registration_id AS in_registration_id,
                out_scan.registration_id AS out_registration_id,
                in_scan.service_id AS in_service_id, out_scan.service_id AS out_service_id,
                in_scan.scanned_user_id AS in_user_id, out_scan.scanned_user_id AS out_user_id,
                in_scan.stand_id AS in_stand_id, out_scan.stand_id AS out_stand_id,
                in_scan.scanned_at AS checkin_at, out_scan.scanned_at AS checkout_at,
                (SELECT GROUP_CONCAT(DISTINCT qo.name_en ORDER BY qo.name_en)
                   FROM health_event_scan_answer sa
                   INNER JOIN health_event_question q ON q.id = sa.question_id
                   INNER JOIN health_event_scan_answer_option sao ON sao.scan_answer_id = sa.id
                   INNER JOIN health_event_question_option qo ON qo.id = sao.option_id
                  WHERE sa.scan_id = out_scan.id AND q.name_en = 'Service status') AS service_status
           FROM health_event_scan in_scan
           INNER JOIN health_event_scan out_scan ON out_scan.paired_scan_id = in_scan.id
          WHERE in_scan.id = ? AND out_scan.id = ?`, [checkinId, checkoutId]
      );
      const row = rows[0];
      if (rows.length !== 1 || row.service_status !== expectedStatus ||
          Number(row.in_registration_id) !== Number(row.out_registration_id) ||
          String(row.in_service_id ?? '') !== String(row.out_service_id ?? '') ||
          Number(row.in_user_id) !== Number(row.out_user_id) ||
          Number(row.in_stand_id) !== Number(row.out_stand_id) ||
          String(row.checkout_at) < String(row.checkin_at)) {
        throw new Error(`${label}: paired visit/status postcondition failed: ${printable(rows)}`);
      }
    }

    await assertCheckoutStatus('Michael Vision', 123, michaelVisionCheckout.id, 'Completed');
    await assertCheckoutStatus('Blanca Medical', 965, blancaMedicalCheckout.id, 'Referred');
    await assertCheckoutStatus('Thania Dental', thaniaDental.scan.id, thaniaDentalCheckout.id, 'Declined treatment');
    await assertCheckoutStatus('Rosalina Vision', 1320, rosalinaVisionCheckout.id, 'Completed');
    await assertCheckoutStatus('Julie Vision', julieVision.scan.id, julieVisionCheckout.id, 'Completed');
    await assertStandaloneScan('Blanca Food', blancaFood, people.blanca, 'Food Distribution');
    await assertStandaloneScan('Rosalina Entry', rosalinaEntry, people.rosalina, 'Entry Check-in');
    await assertStandaloneScan('Rosalina Food', rosalinaFood, people.rosalina, 'Food Distribution');
    await assertStandaloneScan('Julie Entry', julieEntry, people.julie, 'Entry Check-in');
    await assertStandaloneScan('Julie Food', julieFood, people.julie, 'Food Distribution');

    const [[eduardoNote]] = await connection.query(
      `SELECT sa.answer_text FROM health_event_scan_answer sa
       INNER JOIN health_event_question q ON q.id = sa.question_id
       WHERE sa.scan_id = 508 AND q.name_en = 'Notes' LIMIT 1`
    );
    if (!eduardoNote || eduardoNote.answer_text !== 'Received regular reading glasses.') {
      throw new Error('Eduardo reading-glasses note postcondition failed');
    }

    const [[joseDob]] = await connection.query('SELECT date_of_birth FROM user WHERE id = ?', [people.jose.id]);
    if (!joseDob || String(joseDob.date_of_birth).slice(0, 10) !== '1971-12-22') {
      throw new Error('Jose date-of-birth correction postcondition failed');
    }

    for (const source of [people.dianaAli, people.dianaGmail, people.dianaRosess, people.michaelDuplicate]) {
      const [[sourceState]] = await connection.query(
        `SELECT u.enabled, u.deleted,
                (SELECT COUNT(*) FROM health_event_registration r WHERE r.user_id = u.id) AS registrations,
                (SELECT COUNT(*) FROM health_event_scan s
                  WHERE s.scanned_user_id = u.id OR s.volunteer_user_id = u.id) AS scans
           FROM user u WHERE u.id = ?`, [source.id]
      );
      if (!sourceState || sourceState.enabled !== 'N' || sourceState.deleted !== 'Y' ||
          Number(sourceState.registrations) !== 0 || Number(sourceState.scans) !== 0) {
        throw new Error(`Merged source user #${source.id} postcondition failed: ${printable(sourceState)}`);
      }
    }

    for (const target of [people.dianaCanonical, people.michaelCanonical]) {
      const [[targetState]] = await connection.query(
        `SELECT COUNT(*) AS registrations FROM health_event_registration
          WHERE health_event_id = ? AND registration_role = 'beneficiary' AND user_id = ?`,
        [event.id, target.id]
      );
      if (Number(targetState.registrations) !== 1) {
        throw new Error(`Canonical user #${target.id} must have exactly one event registration`);
      }
    }

    const [[deletedDuplicates]] = await connection.query(
      'SELECT COUNT(*) AS total FROM health_event_scan WHERE id IN (650,651,663,674)'
    );
    if (Number(deletedDuplicates.total) !== 0) throw new Error('Duplicate Diana/Michael dental pairs remain');

    const [reclassified] = await connection.query(
      `SELECT s.id, s.scanned_user_id, s.scan_type, s.service_id, s.paired_scan_id, st.name_en
         FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
        WHERE s.id IN (1163,1171,1173) ORDER BY s.id`
    );
    if (reclassified.length !== 3 || reclassified.some(row => row.name_en !== 'Food Distribution' ||
        row.scan_type !== 'checkin' || row.service_id != null || row.paired_scan_id != null)) {
      throw new Error(`Food-batch reclassification postcondition failed: ${printable(reclassified)}`);
    }

    const [preservedAmbiguousScans] = await connection.query(
      `SELECT id, scanned_user_id FROM health_event_scan
        WHERE id IN (117,1249,1251,1269,1270,1271,1272) ORDER BY id`
    );
    if (preservedAmbiguousScans.length !== 7 ||
        Number(preservedAmbiguousScans.find(row => Number(row.id) === 117)?.scanned_user_id) !== Number(people.stephanie.id)) {
      throw new Error('A deferred ambiguous scan was unexpectedly changed or removed');
    }

    for (const person of [people.thania, people.dulce]) {
      const beforeFood = beforeSnapshot.scans.filter(scan => Number(scan.scanned_user_id) === Number(person.id) &&
        scan.stand_name === 'Food Distribution').length;
      const [[currentFood]] = await connection.query(
        `SELECT COUNT(*) AS total FROM health_event_scan s
         INNER JOIN health_event_stand st ON st.id = s.stand_id
         WHERE s.health_event_id = ? AND s.scanned_user_id = ? AND st.name_en = 'Food Distribution'`,
        [event.id, person.id]
      );
      if (Number(currentFood.total) !== beforeFood) {
        throw new Error(`Deferred family Food count changed for user #${person.id}`);
      }
    }

    const [[michaelAppointment]] = await connection.query(
      `SELECT COUNT(*) AS total FROM health_event_appointment a
       INNER JOIN health_event_slot sl ON sl.id = a.slot_id
       WHERE a.registration_id = ? AND sl.service_key = 'dental'
         AND sl.slot_date = '2026-08-08' AND TIME_FORMAT(sl.start_time, '%H:%i') = '14:00'`,
      [people.michaelCanonical.registration_id]
    );
    if (Number(michaelAppointment.total) !== 1) {
      throw new Error('Michael conflicting appointment date was unexpectedly changed');
    }

    // Structural postconditions.
    const [cancelledRows] = await connection.query(
      `SELECT a.id, a.status, u.email, sl.service_key, sl.slot_date, TIME_FORMAT(sl.start_time, '%H:%i') AS start_time
         FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
         INNER JOIN health_event_registration r ON r.id = a.registration_id
         INNER JOIN user u ON u.id = r.user_id
        WHERE sl.health_event_id = ? AND LOWER(u.email) IN
          ('rk-health@nym.hush.com','ortizvianey05@gmail.com','avila30378@gmail.com',
           'chefmj58@yahoo.com','bobbieweiss87@gmail.com','russweiss60@gmail.com')`, [event.id]
    );
    if (cancelledRows.length !== 6 || cancelledRows.some(row => row.status !== 'cancelled')) {
      throw new Error('Not all six requested appointments are cancelled');
    }

    const [brokenScans] = await connection.query(
      `SELECT s.id, s.registration_id, s.scanned_user_id, r.user_id AS registration_user_id
         FROM health_event_scan s LEFT JOIN health_event_registration r ON r.id = s.registration_id
        WHERE s.health_event_id = ? AND s.registration_id IS NOT NULL
          AND (r.id IS NULL OR r.user_id <> s.scanned_user_id)`, [event.id]
    );
    if (brokenScans.length) throw new Error(`Registration/user mismatch remains on scans: ${printable(brokenScans)}`);

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
    if (brokenPairs.length) throw new Error(`Broken checkout pair remains: ${printable(brokenPairs)}`);

    const afterSnapshot = await captureSnapshot();
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const snapshotBase = sanitizeAuditValue({
      runKey: RUN_KEY,
      target: TARGET,
      database: { name: identity.db, host: identity.db_host, sessionTimezone: identity.session_timezone },
      event,
      generatedAt: new Date().toISOString(),
      deferredCases: DEFERRED_CASES,
      operations,
      before: beforeSnapshot,
      after: afterSnapshot
    });
    let logPath;

    if (DRY_RUN) {
      await connection.rollback();
      logPath = path.join(LOG_DIR, `${RUN_KEY}-${TARGET}-dry-run-${stamp}.json`);
      fs.writeFileSync(logPath, JSON.stringify({ ...snapshotBase, mode: 'dry-run-rolled-back' }, null, 2));
    } else {
      // Stage the local recovery snapshot before committing.  A filesystem
      // failure therefore still rolls the database back.  The database audit
      // is the durable source of truth; final local-file failures are reported
      // separately and never misreport a committed transaction as rolled back.
      const preparedPath = path.join(LOG_DIR, `${RUN_KEY}-${TARGET}-prepared-${stamp}.json`);
      fs.writeFileSync(preparedPath, JSON.stringify({ ...snapshotBase, mode: 'prepared-for-commit' }, null, 2));
      await connection.commit();
      committed = true;

      const appliedPath = path.join(LOG_DIR, `${RUN_KEY}-${TARGET}-applied-${stamp}.json`);
      try {
        fs.writeFileSync(appliedPath, JSON.stringify({ ...snapshotBase, mode: 'applied' }, null, 2));
        logPath = appliedPath;
        try { fs.unlinkSync(preparedPath); } catch (cleanupError) {
          console.warn(`[recap] COMMITTED, but could not remove prepared snapshot: ${cleanupError.message}`);
        }
      } catch (snapshotError) {
        logPath = preparedPath;
        console.warn(`[recap] COMMITTED, but final local snapshot failed: ${snapshotError.message}`);
        console.warn(`[recap] prepared snapshot remains at ${preparedPath}`);
      }
    }

    console.log(`[recap] ${operations.length} audited changes ${DRY_RUN ? 'simulated and rolled back' : 'committed'}.`);
    console.log(`[recap] snapshot=${logPath}`);
    console.log(`[recap] cancelled appointments=${cancelledRows.length}; invariant mismatches=0; broken pairs=0`);
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
  console.error('[recap] FAILED:', error.stack || error.message || error);
  process.exit(1);
});
