/*
 * Operational backfill for the Banning clinic (2026-08-18).
 *
 * Unlike the two case-specific recap reconciliations (v1/v2), this run closes
 * the systematic operational gaps left by the event volunteers:
 *   1. Add an 'NA (not recorded)' option to each of the three checkout
 *      'Service status' questions so administrative checkouts can be told
 *      apart from real clinical outcomes until the provider lists arrive.
 *   2. Backfill the missing Entry check-in for every participant who has
 *      same-day service activity but was never scanned at the entry stand.
 *   3. Close every still-open service visit (check-in at a has_checkout stand
 *      with no paired checkout) with an administrative checkout at the exact
 *      check-in timestamp and a 'Service status' answer of NA.
 *   4. Add the missing Food Distribution record for every attendee day; the
 *      client confirmed every attendee received a food bag.
 *
 * Everything runs in one transaction with row locks, every change is recorded
 * in health_event_reconciliation_audit and in a local, Git-ignored snapshot
 * under BACKEND/logs.  A dry run executes the exact same DML and rolls the
 * transaction back, printing per-step counts.
 *
 * Usage (from BACKEND/):
 *   node scripts/reconcileBanningClinicOperationalBackfill.js production --dry-run
 *   node scripts/reconcileBanningClinicOperationalBackfill.js production --apply \
 *     --confirm-production=BANNING-2026-08-18-OPERATIONAL
 *
 * Development can be inspected with the same command by replacing production
 * with development.  It is expected not to contain the production-only event.
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
const EXPLICIT_DRY_RUN = process.argv.includes('--dry-run');
const DRY_RUN = EXPLICIT_DRY_RUN || !APPLY;
const PROD_CONFIRMATION = '--confirm-production=BANNING-2026-08-18-OPERATIONAL';
const RUN_KEY = 'banning-clinic-operational-backfill-2026-08-18-v3';

const EVENT_TIMEZONE = 'America/Los_Angeles';
const BACKFILL_VOLUNTEER_USER_ID = 1; // system Administrator
const NA_OPTION_EN = 'NA (not recorded)';
const NA_OPTION_ES = 'NA (sin registrar)';

if (!['development', 'production'].includes(TARGET) || (APPLY && EXPLICIT_DRY_RUN) ||
    process.argv.slice(3).some(arg => !['--apply', '--dry-run', PROD_CONFIRMATION].includes(arg))) {
  console.error('Usage: node scripts/reconcileBanningClinicOperationalBackfill.js <development|production> <--dry-run|--apply>');
  process.exit(1);
}
if (TARGET === 'production' && APPLY && !process.argv.includes(PROD_CONFIRMATION)) {
  console.error(`Production apply refused. Add ${PROD_CONFIRMATION}`);
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

function asJson(value) {
  return value == null ? null : JSON.stringify(value);
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
    console.log(`[operational] target=${TARGET} mode=${DRY_RUN ? 'DRY-RUN (rollback)' : 'APPLY'}`);
    console.log(`[operational] database=${identity.db} host=${identity.db_host} timezone=${identity.session_timezone}`);

    if (TARGET === 'production' && !String(databaseConfig.host).includes('database-1.')) {
      throw new Error('Production safety check failed: unexpected host');
    }

    // Every local-day computation depends on named-timezone conversion, so the
    // server's timezone tables must be loaded before anything else runs.
    const [[timezoneCheck]] = await connection.query(
      `SELECT CONVERT_TZ('2026-08-08 12:00:00', '+00:00', ?) AS converted`, [EVENT_TIMEZONE]
    );
    if (!timezoneCheck || timezoneCheck.converted == null) {
      throw new Error(`CONVERT_TZ returned NULL for ${EVENT_TIMEZONE}; the MySQL timezone tables are not loaded`);
    }

    let priorAuditRows = 0;
    const auditTableAlreadyExists = await tableExists(connection, 'health_event_reconciliation_audit');
    if (auditTableAlreadyExists) {
      const [[previousRun]] = await connection.query(
        'SELECT COUNT(*) AS total FROM health_event_reconciliation_audit WHERE run_key = ?', [RUN_KEY]
      );
      priorAuditRows = Number(previousRun.total);
    }
    console.log(`[operational] persisted audit rows for run=${priorAuditRows}`);
    if (!DRY_RUN) {
      if (!auditTableAlreadyExists) await ensureAuditTable(connection);
      if (priorAuditRows > 0) throw new Error(`${RUN_KEY} already has persisted audit rows; refusing to apply it again`);
    }

    // From this point through every postcondition and snapshot, the event, its
    // stands, its scans and its checkout forms stay locked and all reads/writes
    // describe one atomic database state.
    await connection.beginTransaction();

    const [[event]] = await connection.query(
      `SELECT id, slug, name_en, start_date, end_date, timezone
         FROM health_event WHERE slug = 'banning' LIMIT 1 FOR UPDATE`
    );
    if (!event) {
      throw new Error(`The Banning event (slug 'banning') does not exist in the ${TARGET} database; nothing to backfill`);
    }
    if (String(event.start_date).slice(0, 10) !== '2026-08-08' ||
        String(event.end_date).slice(0, 10) !== '2026-08-09' || event.timezone !== EVENT_TIMEZONE) {
      throw new Error(`The Banning event does not match the asserted 2026-08-08..09 ${EVENT_TIMEZONE} event`);
    }

    // Every inserted scan is attributed to the system Administrator so the
    // administrative origin is unambiguous in the raw data.
    const [[backfillVolunteer]] = await connection.query(
      'SELECT id, role_id, enabled, deleted FROM user WHERE id = ? FOR UPDATE', [BACKFILL_VOLUNTEER_USER_ID]
    );
    if (!backfillVolunteer || backfillVolunteer.enabled !== 'Y' || Number(backfillVolunteer.role_id) !== 1) {
      throw new Error(`User #${BACKFILL_VOLUNTEER_USER_ID} is not the enabled system Administrator: ${printable(backfillVolunteer)}`);
    }

    const [stands] = await connection.query(
      'SELECT id, name_en, is_entry, has_checkout FROM health_event_stand WHERE health_event_id = ? ORDER BY id FOR UPDATE',
      [event.id]
    );
    const entryStands = stands.filter(stand => stand.is_entry === 'Y');
    if (entryStands.length !== 1) throw new Error(`Expected exactly one entry stand, found ${entryStands.length}`);
    const entryStand = entryStands[0];
    const foodStands = stands.filter(stand => stand.name_en === 'Food Distribution');
    if (foodStands.length !== 1 || foodStands[0].has_checkout !== 'N') {
      throw new Error('Food Distribution stand assertion failed');
    }
    const foodStand = foodStands[0];
    const checkoutStandIds = stands.filter(stand => stand.has_checkout === 'Y').map(stand => Number(stand.id));
    if (!checkoutStandIds.length) throw new Error('No has_checkout stands found for the event');

    // Lock every scan of the event once; steps 2-4 and the postconditions all
    // aggregate over this set, so no concurrent scanner may change it mid-run.
    await connection.query('SELECT id FROM health_event_scan WHERE health_event_id = ? FOR UPDATE', [event.id]);

    const [registrations] = await connection.query(
      `SELECT id, user_id FROM health_event_registration
        WHERE health_event_id = ? AND registration_role = 'beneficiary'
        ORDER BY id FOR UPDATE`, [event.id]
    );
    const registrationsByUser = new Map();
    for (const registration of registrations) {
      const list = registrationsByUser.get(Number(registration.user_id)) || [];
      list.push(registration);
      registrationsByUser.set(Number(registration.user_id), list);
    }
    function beneficiaryRegistrationId(userId) {
      const list = registrationsByUser.get(Number(userId)) || [];
      if (list.length !== 1) {
        throw new Error(`User #${userId} must have exactly one beneficiary registration, found ${list.length}`);
      }
      return Number(list[0].id);
    }
    const [scannedUsers] = await connection.query(
      'SELECT DISTINCT scanned_user_id AS user_id FROM health_event_scan WHERE health_event_id = ? ORDER BY scanned_user_id',
      [event.id]
    );
    for (const scannedUser of scannedUsers) {
      if (scannedUser.user_id == null) throw new Error('A scan without scanned_user_id exists for the event');
      beneficiaryRegistrationId(scannedUser.user_id); // throws unless exactly one registration
    }

    // The three checkout 'Service status' questions, one per has_checkout stand.
    const [statusQuestions] = await connection.query(
      `SELECT q.id AS question_id, f.stand_id, st.name_en AS stand_name, st.has_checkout
         FROM health_event_question q
         INNER JOIN health_event_form f ON f.id = q.form_id
         INNER JOIN health_event_stand st ON st.id = f.stand_id
        WHERE f.health_event_id = ? AND f.audience = 'checkout' AND q.name_en = 'Service status'
        ORDER BY q.id FOR UPDATE`, [event.id]
    );
    if (statusQuestions.length !== 3 ||
        statusQuestions.some(question => question.has_checkout !== 'Y') ||
        new Set(statusQuestions.map(question => Number(question.stand_id))).size !== 3 ||
        checkoutStandIds.some(standId => !statusQuestions.some(question => Number(question.stand_id) === standId))) {
      throw new Error(`Expected exactly one checkout 'Service status' question per has_checkout stand: ${printable(statusQuestions)}`);
    }
    const statusQuestionByStand = new Map(statusQuestions.map(question => [Number(question.stand_id), question]));
    const statusQuestionIds = statusQuestions.map(question => Number(question.question_id));

    async function getScan(scanId) {
      const [rows] = await connection.query(
        `SELECT s.*, st.name_en AS stand_name
           FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
          WHERE s.id = ? AND s.health_event_id = ? FOR UPDATE`, [scanId, event.id]
      );
      return rows[0] || null;
    }

    async function captureSnapshot() {
      const [[totals]] = await connection.query(
        `SELECT COUNT(*) AS scans, COUNT(DISTINCT scanned_user_id) AS scanned_users
           FROM health_event_scan WHERE health_event_id = ?`, [event.id]
      );
      const [standDayCounts] = await connection.query(
        `SELECT st.id AS stand_id, st.name_en AS stand_name, s.scan_type,
                DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%Y-%m-%d') AS local_day,
                COUNT(*) AS scans
           FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
          WHERE s.health_event_id = ?
          GROUP BY st.id, st.name_en, s.scan_type, local_day
          ORDER BY st.id, local_day, s.scan_type`, [event.timezone, event.id]
      );
      const [statusQuestionOptions] = await connection.query(
        `SELECT id, question_id, name_en, name_es, sort_order, enabled
           FROM health_event_question_option
          WHERE question_id IN (?) ORDER BY question_id, sort_order, id`, [statusQuestionIds]
      );
      return { totals, standDayCounts, statusQuestionOptions };
    }

    const beforeSnapshot = await captureSnapshot();

    // The three driving queries are shared with the postconditions so that the
    // fixes and their verification can never diverge.
    async function findMissingEntryPairs() {
      const [rows] = await connection.query(
        `SELECT s.scanned_user_id AS user_id,
                DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%Y%m%d') AS day_key,
                DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%Y-%m-%d') AS local_day,
                DATE_FORMAT(MIN(s.scanned_at), '%Y-%m-%d %H:%i:%s.%f') AS first_scan_at,
                SUM(CASE WHEN s.stand_id <> ? THEN 1 ELSE 0 END) AS non_entry_scans,
                SUM(CASE WHEN s.stand_id = ? AND s.scan_type = 'checkin' THEN 1 ELSE 0 END) AS entry_checkins
           FROM health_event_scan s
          WHERE s.health_event_id = ?
          GROUP BY s.scanned_user_id, day_key, local_day
         HAVING non_entry_scans > 0 AND entry_checkins = 0
          ORDER BY s.scanned_user_id, day_key`,
        [event.timezone, event.timezone, entryStand.id, entryStand.id, event.id]
      );
      return rows;
    }

    async function findOpenServiceVisits() {
      const [rows] = await connection.query(
        `SELECT s.id, s.stand_id, st.name_en AS stand_name, s.service_id, s.registration_id, s.scanned_user_id,
                DATE_FORMAT(s.scanned_at, '%Y-%m-%d %H:%i:%s.%f') AS scanned_at_exact
           FROM health_event_scan s
           INNER JOIN health_event_stand st ON st.id = s.stand_id
          WHERE s.health_event_id = ? AND s.scan_type = 'checkin' AND st.has_checkout = 'Y'
            AND NOT EXISTS (SELECT 1 FROM health_event_scan o
                             WHERE o.paired_scan_id = s.id AND o.scan_type = 'checkout')
          ORDER BY s.id`, [event.id]
      );
      return rows;
    }

    async function findMissingFoodPairs() {
      const [rows] = await connection.query(
        `SELECT s.scanned_user_id AS user_id,
                DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%Y%m%d') AS day_key,
                DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%Y-%m-%d') AS local_day,
                DATE_FORMAT(MAX(s.scanned_at), '%Y-%m-%d %H:%i:%s.%f') AS last_scan_at,
                SUM(CASE WHEN s.stand_id = ? AND s.scan_type = 'checkin' THEN 1 ELSE 0 END) AS food_checkins
           FROM health_event_scan s
          WHERE s.health_event_id = ?
          GROUP BY s.scanned_user_id, day_key, local_day
         HAVING food_checkins = 0
          ORDER BY s.scanned_user_id, day_key`,
        [event.timezone, event.timezone, foodStand.id, event.id]
      );
      return rows;
    }

    // STEP 1 — 'NA (not recorded)' option on each checkout 'Service status'
    // question.  An already-existing option is reused without an audit row.
    const naOptionByStand = new Map();
    let naOptionsInserted = 0;
    let naOptionsReused = 0;
    for (const question of statusQuestions) {
      const [existingOptions] = await connection.query(
        `SELECT id, question_id, name_en, name_es, sort_order, enabled
           FROM health_event_question_option
          WHERE question_id = ? AND name_en = ? ORDER BY id FOR UPDATE`,
        [question.question_id, NA_OPTION_EN]
      );
      if (existingOptions.length > 1) {
        throw new Error(`Question #${question.question_id} already has ${existingOptions.length} '${NA_OPTION_EN}' options`);
      }
      if (existingOptions.length === 1) {
        if (existingOptions[0].enabled !== 'Y') {
          throw new Error(`Question #${question.question_id} has a disabled '${NA_OPTION_EN}' option`);
        }
        naOptionByStand.set(Number(question.stand_id), Number(existingOptions[0].id));
        naOptionsReused += 1;
        continue;
      }
      const [[sortRow]] = await connection.query(
        'SELECT COALESCE(MAX(sort_order), 0) AS max_sort FROM health_event_question_option WHERE question_id = ? FOR UPDATE',
        [question.question_id]
      );
      const [result] = await connection.query(
        `INSERT INTO health_event_question_option(question_id, name_en, name_es, is_other, sort_order, enabled)
         VALUES (?,?,?,'N',?,'Y')`,
        [question.question_id, NA_OPTION_EN, NA_OPTION_ES, Number(sortRow.max_sort) + 1]
      );
      const [[insertedOption]] = await connection.query(
        'SELECT id, question_id, name_en, name_es, is_other, sort_order, enabled FROM health_event_question_option WHERE id = ?',
        [result.insertId]
      );
      await record({
        actionKey: `na-option-q${question.question_id}`,
        actionType: 'insert',
        targetTable: 'health_event_question_option',
        targetId: result.insertId,
        before: null,
        after: insertedOption,
        note: `New '${NA_OPTION_EN}' status option for the ${question.stand_name} checkout form; it marks administrative checkouts whose real service outcome is pending the provider lists.`
      });
      naOptionByStand.set(Number(question.stand_id), Number(result.insertId));
      naOptionsInserted += 1;
    }

    // STEP 2 — Entry check-in backfill: any (user, event-local day) with
    // service activity but no Entry scan gets one 60 seconds before that
    // day's earliest scan.
    const missingEntryPairs = await findMissingEntryPairs();
    const entryBackfillsByDay = new Map();
    for (const pair of missingEntryPairs) {
      const registrationId = beneficiaryRegistrationId(pair.user_id);
      const [result] = await connection.query(
        `INSERT INTO health_event_scan
           (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
            volunteer_user_id, scan_type, paired_scan_id, scanned_at)
         VALUES (?,?,NULL,?,?,?,'checkin',NULL, DATE_SUB(?, INTERVAL 60 SECOND))`,
        [event.id, entryStand.id, registrationId, pair.user_id,
          BACKFILL_VOLUNTEER_USER_ID, pair.first_scan_at]
      );
      const scan = await getScan(result.insertId);
      await record({
        actionKey: `entry-backfill-u${pair.user_id}-d${pair.day_key}`,
        actionType: 'insert-manual',
        targetTable: 'health_event_scan',
        targetId: result.insertId,
        before: { missingEntryPair: pair },
        after: scan,
        note: `Administrative Entry check-in inferred from ${pair.non_entry_scans} same-day service scans on ${pair.local_day}; the timestamp is the day's earliest scan minus 60 seconds, not QR telemetry.`
      });
      entryBackfillsByDay.set(pair.local_day, (entryBackfillsByDay.get(pair.local_day) || 0) + 1);
    }

    // STEP 3 — Close every open service visit with an administrative checkout
    // at the exact stored check-in timestamp (fractional seconds preserved by
    // passing the DATE_FORMAT string back, never through a JS Date) and a
    // 'Service status' answer of NA.
    const openServiceVisits = await findOpenServiceVisits();
    const naCheckoutIds = [];
    const naCheckoutsByStand = new Map();
    for (const checkin of openServiceVisits) {
      const question = statusQuestionByStand.get(Number(checkin.stand_id));
      const naOptionId = naOptionByStand.get(Number(checkin.stand_id));
      if (!question || !naOptionId) {
        throw new Error(`No status question/NA option for stand #${checkin.stand_id} (${checkin.stand_name})`);
      }
      const [result] = await connection.query(
        `INSERT INTO health_event_scan
           (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
            volunteer_user_id, scan_type, paired_scan_id, scanned_at)
         VALUES (?,?,?,?,?,?,'checkout',?,?)`,
        [event.id, checkin.stand_id, checkin.service_id, checkin.registration_id, checkin.scanned_user_id,
          BACKFILL_VOLUNTEER_USER_ID, checkin.id, checkin.scanned_at_exact]
      );
      const checkout = await getScan(result.insertId);
      await record({
        actionKey: `na-checkout-s${checkin.id}`,
        actionType: 'insert-manual',
        targetTable: 'health_event_scan',
        targetId: result.insertId,
        before: { openCheckin: checkin },
        after: checkout,
        note: `Administrative checkout closing the open ${checkin.stand_name} visit at the exact check-in timestamp; the real service outcome is pending the provider lists.`
      });

      // v1 setStatusAnswer pattern, collapsed for brand-new checkouts: the
      // scan was inserted in this transaction, so any pre-existing answer is
      // a logic error rather than data to merge.
      const [existingAnswers] = await connection.query(
        'SELECT id FROM health_event_scan_answer WHERE scan_id = ? AND question_id = ? LIMIT 1 FOR UPDATE',
        [checkout.id, question.question_id]
      );
      if (existingAnswers.length) {
        throw new Error(`Freshly inserted checkout #${checkout.id} already has a status answer`);
      }
      const [answerResult] = await connection.query(
        'INSERT INTO health_event_scan_answer(scan_id, question_id) VALUES (?,?)',
        [checkout.id, question.question_id]
      );
      await connection.query(
        'INSERT INTO health_event_scan_answer_option(scan_answer_id, option_id) VALUES (?,?)',
        [answerResult.insertId, naOptionId]
      );
      await record({
        actionKey: `na-checkout-s${checkin.id}-status`,
        actionType: 'insert',
        targetTable: 'health_event_scan_answer',
        targetId: answerResult.insertId,
        before: null,
        after: { id: answerResult.insertId, scan_id: checkout.id, question_id: question.question_id,
          option_id: naOptionId, status: NA_OPTION_EN },
        note: `Service status ${NA_OPTION_EN} on the administrative ${checkin.stand_name} checkout.`
      });
      naCheckoutIds.push(Number(checkout.id));
      naCheckoutsByStand.set(checkin.stand_name, (naCheckoutsByStand.get(checkin.stand_name) || 0) + 1);
    }

    // STEP 4 — Food Distribution backfill: the client confirmed every attendee
    // received a food bag, so every attended (user, event-local day) without a
    // Food check-in gets one 60 seconds after that day's latest scan.  The
    // pairs are computed from the current transaction state, after steps 2-3.
    const missingFoodPairs = await findMissingFoodPairs();
    const foodBackfillsByDay = new Map();
    for (const pair of missingFoodPairs) {
      const registrationId = beneficiaryRegistrationId(pair.user_id);
      const [result] = await connection.query(
        `INSERT INTO health_event_scan
           (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
            volunteer_user_id, scan_type, paired_scan_id, scanned_at)
         VALUES (?,?,NULL,?,?,?,'checkin',NULL, DATE_ADD(?, INTERVAL 60 SECOND))`,
        [event.id, foodStand.id, registrationId, pair.user_id,
          BACKFILL_VOLUNTEER_USER_ID, pair.last_scan_at]
      );
      const scan = await getScan(result.insertId);
      await record({
        actionKey: `food-backfill-u${pair.user_id}-d${pair.day_key}`,
        actionType: 'insert-manual',
        targetTable: 'health_event_scan',
        targetId: result.insertId,
        before: { missingFoodPair: pair },
        after: scan,
        note: `Administrative Food Distribution record for ${pair.local_day}; the client confirmed every attendee received a food bag, and the timestamp is the day's latest scan plus 60 seconds, not QR telemetry.`
      });
      foodBackfillsByDay.set(pair.local_day, (foodBackfillsByDay.get(pair.local_day) || 0) + 1);
    }

    // Postconditions.  Each one must hold on the transaction state or the run
    // (dry or applied) aborts before any commit.
    const remainingOpenVisits = await findOpenServiceVisits();
    if (remainingOpenVisits.length) {
      throw new Error(`Open service visits remain: ${printable(remainingOpenVisits)}`);
    }
    const remainingMissingEntries = await findMissingEntryPairs();
    if (remainingMissingEntries.length) {
      throw new Error(`(user, day) pairs without an Entry check-in remain: ${printable(remainingMissingEntries)}`);
    }
    const remainingMissingFood = await findMissingFoodPairs();
    if (remainingMissingFood.length) {
      throw new Error(`(user, day) pairs without a Food check-in remain: ${printable(remainingMissingFood)}`);
    }

    // v1's structural pair check; equal checkout/check-in timestamps satisfy
    // the >= requirement.
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

    const [brokenScans] = await connection.query(
      `SELECT s.id, s.registration_id, s.scanned_user_id, r.user_id AS registration_user_id
         FROM health_event_scan s LEFT JOIN health_event_registration r ON r.id = s.registration_id
        WHERE s.health_event_id = ? AND s.registration_id IS NOT NULL
          AND (r.id IS NULL OR r.user_id <> s.scanned_user_id)`, [event.id]
    );
    if (brokenScans.length) throw new Error(`Registration/user mismatch remains on scans: ${printable(brokenScans)}`);

    const [naOptionCounts] = await connection.query(
      `SELECT question_id, COUNT(*) AS total
         FROM health_event_question_option
        WHERE question_id IN (?) AND name_en = ?
        GROUP BY question_id`, [statusQuestionIds, NA_OPTION_EN]
    );
    if (naOptionCounts.length !== statusQuestionIds.length ||
        naOptionCounts.some(row => Number(row.total) !== 1)) {
      throw new Error(`Each Service status question must have exactly one '${NA_OPTION_EN}' option: ${printable(naOptionCounts)}`);
    }

    if (naCheckoutIds.length) {
      const [naCheckoutAnswers] = await connection.query(
        `SELECT s.id, s.stand_id,
                (SELECT COUNT(*) FROM health_event_scan_answer sa WHERE sa.scan_id = s.id) AS answer_count,
                (SELECT MIN(sa2.question_id) FROM health_event_scan_answer sa2 WHERE sa2.scan_id = s.id) AS question_id,
                (SELECT GROUP_CONCAT(sao.option_id ORDER BY sao.option_id)
                   FROM health_event_scan_answer sa3
                   INNER JOIN health_event_scan_answer_option sao ON sao.scan_answer_id = sa3.id
                  WHERE sa3.scan_id = s.id) AS option_ids
           FROM health_event_scan s WHERE s.id IN (?) ORDER BY s.id`, [naCheckoutIds]
      );
      const badCheckouts = naCheckoutAnswers.filter(row => {
        const question = statusQuestionByStand.get(Number(row.stand_id));
        const naOptionId = naOptionByStand.get(Number(row.stand_id));
        return Number(row.answer_count) !== 1 ||
          Number(row.question_id) !== Number(question?.question_id) ||
          String(row.option_ids) !== String(naOptionId);
      });
      if (naCheckoutAnswers.length !== naCheckoutIds.length || badCheckouts.length) {
        throw new Error(`NA checkout answer postcondition failed: ${printable(badCheckouts)}`);
      }
    }

    const afterSnapshot = await captureSnapshot();
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const snapshotBase = sanitizeAuditValue({
      runKey: RUN_KEY,
      target: TARGET,
      database: { name: identity.db, host: identity.db_host, sessionTimezone: identity.session_timezone },
      event,
      generatedAt: new Date().toISOString(),
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
          console.warn(`[operational] COMMITTED, but could not remove prepared snapshot: ${cleanupError.message}`);
        }
      } catch (snapshotError) {
        logPath = preparedPath;
        console.warn(`[operational] COMMITTED, but final local snapshot failed: ${snapshotError.message}`);
        console.warn(`[operational] prepared snapshot remains at ${preparedPath}`);
      }
    }

    const perDay = counts => Array.from(counts.entries()).sort()
      .map(([key, total]) => `${key}=${total}`).join(', ') || 'none';
    console.log(`[operational] ${operations.length} audited changes ${DRY_RUN ? 'simulated and rolled back' : 'committed'}.`);
    console.log(`[operational] snapshot=${logPath}`);
    console.log(`[operational] STEP 1 NA options: inserted=${naOptionsInserted} reused=${naOptionsReused} (questions=${statusQuestions.length})`);
    console.log(`[operational] STEP 2 entry backfills: total=${missingEntryPairs.length} (${perDay(entryBackfillsByDay)})`);
    console.log(`[operational] STEP 3 NA checkouts: total=${openServiceVisits.length} (${perDay(naCheckoutsByStand)})`);
    console.log(`[operational] STEP 4 food backfills: total=${missingFoodPairs.length} (${perDay(foodBackfillsByDay)})`);
    console.log('[operational] postconditions: open visits=0; missing entries=0; missing food=0; broken pairs=0; registration mismatches=0; NA options per question=1; NA checkout answers verified');
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
  console.error('[operational] FAILED:', error.stack || error.message || error);
  process.exit(1);
});
