/*
 * Banning clinic: same-day duplicate scans, stand rename and Medical Clearance
 * (2026-08-27, v5).
 *
 * Client decision (Lucia + Marci): every participant went through each stand
 * ONCE per day. There were no second scanning points at Haircuts and no second
 * passes at the medical services, so every additional same-day check-in at the
 * same stand (+ service) is a volunteer/participant error. Only cross-day
 * repeats (food on both days, told to come back tomorrow) stay.
 *
 *   STEP 1  Same-day duplicates: for every (person, stand, service, event-local
 *           day) with more than one check-in keep ONE and delete the others
 *           together with their paired check-out (and its answers).
 *             - stands without check-out: keep the earliest scan of the day;
 *             - stands with check-out: keep the visit whose duration was
 *               measured by a REAL check-out (never an administrative
 *               'NA (not recorded)' one) and is the longest; ties -> earliest.
 *               This discards "scan-scan" visits of a few seconds and the
 *               re-scans on the way out, and keeps the actual service time.
 *   STEP 2  Rename stand "Medical Checks" -> "Primary Care/General Health"
 *           (it confused the team with the medical clearance / vitals station).
 *   STEP 3  Create the "Medical Clearance" stand Marci forgot and rebuild its
 *           check-ins: everyone who received Dental, Vision or Primary Care had
 *           their vitals checked first, so one administrative check-in per
 *           (person, day) is inserted 60 s before that day's first main-service
 *           check-in (after STEP 1).
 *
 * Every mutation is asserted, transactional and recorded in
 * health_event_reconciliation_audit (before_json holds the deleted scan, its
 * check-out and the check-out answers, so the run is reversible).
 *
 * Usage (from BACKEND/):
 *   node scripts/reconcileBanningClinicDedupeAndClearance.js production --dry-run
 *   node scripts/reconcileBanningClinicDedupeAndClearance.js production --apply \
 *     --confirm-production=BANNING-2026-08-27-DEDUPE
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
const DRY_RUN = process.argv.includes('--dry-run');
const PROD_CONFIRMATION = '--confirm-production=BANNING-2026-08-27-DEDUPE';
const RUN_KEY = 'banning-clinic-dedupe-and-clearance-2026-08-27-v5';
const ALLOWED_ARGS = new Set(['--apply', '--dry-run', PROD_CONFIRMATION]);

const EVENT_SLUG = 'banning';
const EVENT_TIMEZONE = 'America/Los_Angeles';
const BACKFILL_VOLUNTEER_USER_ID = 1; // system Administrator (same as v3)
const NA_OPTION_EN = 'NA (not recorded)';

const RENAME = {
  fromEn: 'Medical Checks',
  toEn: 'Primary Care/General Health',
  toEs: 'Atención Primaria/Salud General',
  formToEn: 'Primary Care/General Health — Service checkout',
  formToEs: 'Atención Primaria/Salud General — Cierre de atención'
};
const CLEARANCE = {
  nameEn: 'Medical Clearance',
  nameEs: 'Autorización Médica (signos vitales)',
  icon: 'monitor_heart',
  sortOrder: 2 // right after Entry; every other stand shifts down by one
};
// Main services whose participants went through medical clearance first.
const CLEARANCE_SOURCE_STANDS_EN = ['Dental', 'Vision', RENAME.fromEn];

if (!['development', 'production'].includes(TARGET) || APPLY === DRY_RUN ||
    process.argv.slice(3).some(arg => !ALLOWED_ARGS.has(arg))) {
  console.error('Usage: node scripts/reconcileBanningClinicDedupeAndClearance.js <development|production> <--dry-run|--apply>');
  process.exit(1);
}
if (APPLY && TARGET === 'production' && !process.argv.includes(PROD_CONFIRMATION)) {
  console.error(`Production apply refused. Add ${PROD_CONFIRMATION}`);
  process.exit(1);
}

// ---------------------------------------------------------------------------
// .env parsing (development block uncommented, production block commented)
// ---------------------------------------------------------------------------

function normalize(value) {
  return String(value == null ? '' : value).trim().replace(/^(['"])(.*)\1$/, '$2').trim();
}

function readDatabaseBlock(envText, heading, commented) {
  const lines = envText.split(/\r?\n/);
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
      values[match[1].toUpperCase()] = normalize(match[2]);
      found = true;
      continue;
    }
    if (found && line.trim() === '') break;
    if (found && /^\s*#\s*[A-Z][A-Z ]+\s*$/i.test(line)) break;
  }
  return values;
}

function readDatabaseConfig(target) {
  const envText = fs.readFileSync(ENV_PATH, 'utf8');
  const values = readDatabaseBlock(envText,
    target === 'production' ? 'PRODUCTION DATABASE' : 'DEVELOPMENT DATABASE', target === 'production');
  const production = readDatabaseBlock(envText, 'PRODUCTION DATABASE', true);
  for (const key of ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT']) {
    if (!values[key]) throw new Error(`Missing ${key} in the ${target} database block`);
  }
  const sameHost = values.DB_HOST.toLowerCase() === String(production.DB_HOST || '').toLowerCase();
  if (target === 'production' && !sameHost) throw new Error('Production block does not match itself');
  if (target === 'development' && sameHost) throw new Error('Development target unexpectedly points at production');
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

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

async function main() {
  const connection = await mysql.createConnection(readDatabaseConfig(TARGET));
  const operations = [];
  let committed = false;

  const record = async ({ actionKey, actionType, targetTable, targetId, before, after, note }) => {
    const entry = { actionKey, actionType, targetTable, targetId, before: sanitize(before), after: sanitize(after), note };
    operations.push(entry);
    if (!DRY_RUN) {
      await connection.query(
        `INSERT INTO health_event_reconciliation_audit
           (run_key, action_key, action_type, target_table, target_id, before_json, after_json, note)
         VALUES (?,?,?,?,?,?,?,?)`,
        [RUN_KEY, actionKey, actionType, targetTable, targetId == null ? null : String(targetId),
          asJson(entry.before), asJson(entry.after), note || null]
      );
    }
  };

  try {
    const [[identity]] = await connection.query(
      'SELECT DATABASE() AS db, @@hostname AS db_host, @@session.time_zone AS session_timezone');
    console.log(`[dedupe] target=${TARGET} mode=${DRY_RUN ? 'DRY-RUN (rollback)' : 'APPLY'}`);
    console.log(`[dedupe] database=${identity.db} host=${identity.db_host} timezone=${identity.session_timezone}`);

    const [[tz]] = await connection.query(
      `SELECT CONVERT_TZ('2026-08-08 12:00:00', '+00:00', ?) AS converted`, [EVENT_TIMEZONE]);
    if (!tz || tz.converted == null) {
      throw new Error(`CONVERT_TZ returned NULL for ${EVENT_TIMEZONE}; timezone tables are not loaded`);
    }

    await ensureAuditTable(connection);
    const [[previousRun]] = await connection.query(
      'SELECT COUNT(*) AS total FROM health_event_reconciliation_audit WHERE run_key = ?', [RUN_KEY]);
    if (Number(previousRun.total) > 0) {
      throw new Error(`${RUN_KEY} already has ${previousRun.total} persisted audit rows; refusing to run it again`);
    }

    await connection.beginTransaction();

    const [[event]] = await connection.query(
      'SELECT id, slug, name_en, start_date, end_date, timezone FROM health_event WHERE slug = ? LIMIT 1 FOR UPDATE',
      [EVENT_SLUG]);
    if (!event) throw new Error(`Event '${EVENT_SLUG}' not found in ${TARGET}`);
    if (event.timezone !== EVENT_TIMEZONE) throw new Error(`Unexpected event timezone ${event.timezone}`);

    const [stands] = await connection.query(
      `SELECT id, name_en, name_es, icon, is_entry, has_checkout, sort_order, enabled
         FROM health_event_stand WHERE health_event_id = ? ORDER BY sort_order, id FOR UPDATE`, [event.id]);
    const standByEn = new Map(stands.map(stand => [stand.name_en, stand]));
    for (const name of [...CLEARANCE_SOURCE_STANDS_EN, 'Entry Check-in']) {
      if (!standByEn.has(name)) throw new Error(`Stand '${name}' not found`);
    }
    if (standByEn.has(CLEARANCE.nameEn)) throw new Error(`Stand '${CLEARANCE.nameEn}' already exists`);
    if (standByEn.has(RENAME.toEn)) throw new Error(`Stand '${RENAME.toEn}' already exists`);

    const localDay = column => `DATE(CONVERT_TZ(${column}, @@session.time_zone, '${EVENT_TIMEZONE}'))`;

    const perStandCounts = async () => {
      const [rows] = await connection.query(
        `SELECT st.name_en AS stand, s.service_id,
                SUM(s.scan_type = 'checkin') AS checkins, SUM(s.scan_type = 'checkout') AS checkouts,
                COUNT(DISTINCT s.scanned_user_id) AS people,
                COUNT(DISTINCT CASE WHEN s.scan_type = 'checkin'
                      THEN CONCAT(s.scanned_user_id, '|', ${localDay('s.scanned_at')}) END) AS person_days
           FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
          WHERE s.health_event_id = ?
          GROUP BY st.id, st.name_en, st.sort_order, s.service_id ORDER BY st.sort_order, s.service_id`, [event.id]);
      return rows.map(row => ({ ...row, checkins: Number(row.checkins), checkouts: Number(row.checkouts) }));
    };

    const beforeSnapshot = await perStandCounts();

    // ------------------------------------------------------------------
    // STEP 1 — same-day duplicates
    // ------------------------------------------------------------------
    const [checkins] = await connection.query(
      `SELECT s.id, s.stand_id, s.service_id, s.registration_id, s.scanned_user_id, s.volunteer_user_id,
              s.scan_type, s.paired_scan_id, s.scanned_at, s.creation_date,
              ${localDay('s.scanned_at')} AS local_day,
              st.has_checkout,
              c.id AS checkout_id, c.scanned_at AS checkout_at, c.volunteer_user_id AS checkout_volunteer_user_id,
              c.registration_id AS checkout_registration_id, c.service_id AS checkout_service_id,
              c.creation_date AS checkout_creation_date,
              TIMESTAMPDIFF(SECOND, s.scanned_at, c.scanned_at) AS duration_s,
              EXISTS(SELECT 1 FROM health_event_scan_answer a
                       INNER JOIN health_event_scan_answer_option ao ON ao.scan_answer_id = a.id
                       INNER JOIN health_event_question_option o ON o.id = ao.option_id
                      WHERE a.scan_id = c.id AND o.name_en = ?) AS administrative_checkout
         FROM health_event_scan s
         INNER JOIN health_event_stand st ON st.id = s.stand_id
         LEFT JOIN health_event_scan c ON c.paired_scan_id = s.id AND c.scan_type = 'checkout'
        WHERE s.health_event_id = ? AND s.scan_type = 'checkin'
        ORDER BY s.scanned_user_id, s.stand_id, s.service_id, s.scanned_at, s.id
        FOR UPDATE`, [NA_OPTION_EN, event.id]);

    const groups = new Map();
    for (const scan of checkins) {
      const key = `${scan.scanned_user_id}|${scan.stand_id}|${scan.service_id || 0}|${scan.local_day}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(scan);
    }

    // Rank of a visit at a stand with check-out: a real measured duration wins;
    // administrative NA check-outs and open visits never beat a real one.
    const visitScore = scan => {
      if (scan.checkout_id == null) return -2;
      if (Number(scan.administrative_checkout)) return -1;
      return Math.max(0, Number(scan.duration_s));
    };

    const deletedByStand = new Map();
    let deletedCheckins = 0;
    let deletedCheckouts = 0;
    for (const scans of groups.values()) {
      if (scans.length < 2) continue;
      const hasCheckout = scans[0].has_checkout === 'Y';
      const keep = hasCheckout
        ? [...scans].sort((a, b) => visitScore(b) - visitScore(a) ||
            String(a.scanned_at).localeCompare(String(b.scanned_at)) || a.id - b.id)[0]
        : scans[0]; // earliest of the day
      const standName = stands.find(stand => stand.id === scans[0].stand_id).name_en;
      for (const scan of scans) {
        if (scan.id === keep.id) continue;
        let checkoutAnswers = [];
        if (scan.checkout_id != null) {
          [checkoutAnswers] = await connection.query(
            `SELECT a.*,
                    (SELECT GROUP_CONCAT(ao.option_id ORDER BY ao.option_id)
                       FROM health_event_scan_answer_option ao WHERE ao.scan_answer_id = a.id) AS option_ids
               FROM health_event_scan_answer a WHERE a.scan_id = ?`, [scan.checkout_id]);
          const [checkoutDelete] = await connection.query(
            'DELETE FROM health_event_scan WHERE id = ? AND paired_scan_id = ?', [scan.checkout_id, scan.id]);
          if (checkoutDelete.affectedRows !== 1) throw new Error(`Could not delete check-out ${scan.checkout_id}`);
          deletedCheckouts += 1;
        }
        const [checkinDelete] = await connection.query(
          "DELETE FROM health_event_scan WHERE id = ? AND scan_type = 'checkin'", [scan.id]);
        if (checkinDelete.affectedRows !== 1) throw new Error(`Could not delete check-in ${scan.id}`);
        deletedCheckins += 1;
        deletedByStand.set(standName, (deletedByStand.get(standName) || 0) + 1);

        const { has_checkout, administrative_checkout, duration_s, local_day, ...checkinRow } = scan;
        const keptLabel = hasCheckout
          ? (visitScore(keep) >= 0 ? `real check-out, ${Math.round(visitScore(keep) / 60)} min` : 'no real check-out')
          : 'earliest of the day';
        await record({
          actionKey: `dedupe-scan-${scan.id}`,
          actionType: 'delete-audited',
          targetTable: 'health_event_scan',
          targetId: scan.checkout_id != null ? `${scan.id},${scan.checkout_id}` : String(scan.id),
          before: {
            checkin: checkinRow,
            checkout: scan.checkout_id == null ? null : {
              id: scan.checkout_id, paired_scan_id: scan.id, scanned_at: scan.checkout_at,
              volunteer_user_id: scan.checkout_volunteer_user_id, registration_id: scan.checkout_registration_id,
              service_id: scan.checkout_service_id, creation_date: scan.checkout_creation_date,
              administrative: Boolean(Number(administrative_checkout)), duration_s
            },
            checkout_answers: checkoutAnswers
          },
          after: null,
          note: `Same-day duplicate at ${standName} on ${local_day} (${scans.length} check-ins for this person` +
            `${hasCheckout ? '' : '/service'}); kept scan ${keep.id} (${keptLabel}). ` +
            'Client confirmed every participant passed each stand once per day.'
        });
      }
    }

    // ------------------------------------------------------------------
    // STEP 2 — rename Medical Checks
    // ------------------------------------------------------------------
    const medical = standByEn.get(RENAME.fromEn);
    await connection.query(
      'UPDATE health_event_stand SET name_en = ?, name_es = ? WHERE id = ?', [RENAME.toEn, RENAME.toEs, medical.id]);
    await record({
      actionKey: `rename-stand-${medical.id}`,
      actionType: 'update',
      targetTable: 'health_event_stand',
      targetId: medical.id,
      before: { name_en: medical.name_en, name_es: medical.name_es },
      after: { name_en: RENAME.toEn, name_es: RENAME.toEs },
      note: 'Client request: "Medical check-up" confused the team with the medical clearance (vitals) station; the stand is the primary-care consultation.'
    });
    const [forms] = await connection.query(
      'SELECT id, title_en, title_es FROM health_event_form WHERE health_event_id = ? AND stand_id = ? FOR UPDATE',
      [event.id, medical.id]);
    for (const form of forms) {
      await connection.query('UPDATE health_event_form SET title_en = ?, title_es = ? WHERE id = ?',
        [RENAME.formToEn, RENAME.formToEs, form.id]);
      await record({
        actionKey: `rename-form-${form.id}`,
        actionType: 'update',
        targetTable: 'health_event_form',
        targetId: form.id,
        before: { title_en: form.title_en, title_es: form.title_es },
        after: { title_en: RENAME.formToEn, title_es: RENAME.formToEs },
        note: 'Checkout form title follows the stand rename.'
      });
    }

    // ------------------------------------------------------------------
    // STEP 3 — Medical Clearance stand + administrative check-ins
    // ------------------------------------------------------------------
    for (const stand of stands) {
      if (stand.sort_order >= CLEARANCE.sortOrder) {
        await connection.query('UPDATE health_event_stand SET sort_order = ? WHERE id = ?', [stand.sort_order + 1, stand.id]);
        await record({
          actionKey: `shift-stand-order-${stand.id}`,
          actionType: 'update',
          targetTable: 'health_event_stand',
          targetId: stand.id,
          before: { sort_order: stand.sort_order },
          after: { sort_order: stand.sort_order + 1 },
          note: 'Makes room for Medical Clearance right after Entry.'
        });
      }
    }
    const [standInsert] = await connection.query(
      `INSERT INTO health_event_stand (health_event_id, name_en, name_es, icon, is_entry, has_checkout, sort_order, enabled)
       VALUES (?,?,?,?,'N','N',?,'Y')`,
      [event.id, CLEARANCE.nameEn, CLEARANCE.nameEs, CLEARANCE.icon, CLEARANCE.sortOrder]);
    const clearanceStandId = Number(standInsert.insertId);
    const [[clearanceStand]] = await connection.query('SELECT * FROM health_event_stand WHERE id = ?', [clearanceStandId]);
    await record({
      actionKey: 'insert-stand-medical-clearance',
      actionType: 'insert-manual',
      targetTable: 'health_event_stand',
      targetId: clearanceStandId,
      before: null,
      after: clearanceStand,
      note: 'Vitals station that operated on both days but was never configured; everyone receiving Dental, Vision or Primary Care went through it.'
    });

    const sourceStandIds = CLEARANCE_SOURCE_STANDS_EN.map(name => standByEn.get(name).id);
    const [clearancePairs] = await connection.query(
      `SELECT s.scanned_user_id AS user_id, ${localDay('s.scanned_at')} AS local_day,
              MIN(s.scanned_at) AS first_service_at, COUNT(*) AS service_checkins,
              (SELECT r.id FROM health_event_registration r
                WHERE r.health_event_id = s.health_event_id AND r.user_id = s.scanned_user_id
                  AND r.registration_role = 'beneficiary' AND r.status = 'registered'
                ORDER BY r.id LIMIT 1) AS registration_id
         FROM health_event_scan s
        WHERE s.health_event_id = ? AND s.scan_type = 'checkin' AND s.stand_id IN (?)
        GROUP BY s.health_event_id, s.scanned_user_id, local_day
        ORDER BY local_day, first_service_at`, [event.id, sourceStandIds]);
    const clearanceByDay = new Map();
    for (const pair of clearancePairs) {
      const [result] = await connection.query(
        `INSERT INTO health_event_scan
           (health_event_id, stand_id, service_id, registration_id, scanned_user_id, volunteer_user_id,
            scan_type, paired_scan_id, scanned_at)
         VALUES (?,?,NULL,?,?,?,'checkin',NULL, DATE_SUB(?, INTERVAL 60 SECOND))`,
        [event.id, clearanceStandId, pair.registration_id, pair.user_id, BACKFILL_VOLUNTEER_USER_ID, pair.first_service_at]);
      const [[scan]] = await connection.query('SELECT * FROM health_event_scan WHERE id = ?', [result.insertId]);
      await record({
        actionKey: `clearance-backfill-u${pair.user_id}-d${pair.local_day}`,
        actionType: 'insert-manual',
        targetTable: 'health_event_scan',
        targetId: result.insertId,
        before: { pair },
        after: scan,
        note: `Administrative Medical Clearance check-in inferred from ${pair.service_checkins} Dental/Vision/Primary Care ` +
          `check-in(s) on ${pair.local_day}; timestamp = first main-service check-in minus 60 s, not QR telemetry.`
      });
      clearanceByDay.set(pair.local_day, (clearanceByDay.get(pair.local_day) || 0) + 1);
    }

    // ------------------------------------------------------------------
    // postconditions
    // ------------------------------------------------------------------
    const [[dupCheck]] = await connection.query(
      `SELECT COUNT(*) AS groups_with_duplicates FROM (
         SELECT 1 FROM health_event_scan s
          WHERE s.health_event_id = ? AND s.scan_type = 'checkin'
          GROUP BY s.scanned_user_id, s.stand_id, COALESCE(s.service_id, 0), ${localDay('s.scanned_at')}
         HAVING COUNT(*) > 1) x`, [event.id]);
    if (Number(dupCheck.groups_with_duplicates) !== 0) {
      throw new Error(`Postcondition failed: ${dupCheck.groups_with_duplicates} same-day duplicate groups remain`);
    }
    const [[orphanCheck]] = await connection.query(
      `SELECT SUM(c.scan_type = 'checkout' AND (c.paired_scan_id IS NULL OR p.id IS NULL)) AS orphan_checkouts,
              SUM(p.id IS NOT NULL AND p.scan_type <> 'checkin') AS bad_pairs
         FROM health_event_scan c LEFT JOIN health_event_scan p ON p.id = c.paired_scan_id
        WHERE c.health_event_id = ?`, [event.id]);
    if (Number(orphanCheck.orphan_checkouts) !== 0 || Number(orphanCheck.bad_pairs) !== 0) {
      throw new Error(`Postcondition failed: orphan check-outs=${orphanCheck.orphan_checkouts} bad pairs=${orphanCheck.bad_pairs}`);
    }
    const [[clearanceCheck]] = await connection.query(
      `SELECT COUNT(*) AS clearance_checkins, COUNT(DISTINCT scanned_user_id) AS clearance_people
         FROM health_event_scan WHERE health_event_id = ? AND stand_id = ?`, [event.id, clearanceStandId]);
    if (Number(clearanceCheck.clearance_checkins) !== clearancePairs.length) {
      throw new Error('Postcondition failed: clearance check-ins do not match the inferred pairs');
    }

    const afterSnapshot = await perStandCounts();

    fs.mkdirSync(LOG_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const snapshot = sanitize({
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
      fs.writeFileSync(logPath, JSON.stringify({ ...snapshot, mode: 'dry-run-rolled-back' }, null, 2));
    } else {
      const preparedPath = path.join(LOG_DIR, `${RUN_KEY}-${TARGET}-prepared-${stamp}.json`);
      fs.writeFileSync(preparedPath, JSON.stringify({ ...snapshot, mode: 'prepared-for-commit' }, null, 2));
      await connection.commit();
      committed = true;
      logPath = path.join(LOG_DIR, `${RUN_KEY}-${TARGET}-applied-${stamp}.json`);
      try {
        fs.writeFileSync(logPath, JSON.stringify({ ...snapshot, mode: 'applied' }, null, 2));
        try { fs.unlinkSync(preparedPath); } catch (_) { /* keep both */ }
      } catch (snapshotError) {
        logPath = preparedPath;
        console.warn(`[dedupe] COMMITTED, but final snapshot failed: ${snapshotError.message}`);
      }
    }

    const perKey = counts => Array.from(counts.entries()).sort().map(([k, v]) => `${k}=${v}`).join(', ') || 'none';
    console.log(`[dedupe] ${operations.length} audited changes ${DRY_RUN ? 'simulated and rolled back' : 'committed'}.`);
    console.log(`[dedupe] snapshot=${logPath}`);
    console.log(`[dedupe] STEP 1 deleted check-ins=${deletedCheckins} (+${deletedCheckouts} paired check-outs): ${perKey(deletedByStand)}`);
    console.log(`[dedupe] STEP 2 renamed stand ${medical.id} '${RENAME.fromEn}' -> '${RENAME.toEn}' (+${forms.length} form title(s))`);
    console.log(`[dedupe] STEP 3 clearance stand id=${clearanceStandId}; check-ins=${clearancePairs.length} people=${clearanceCheck.clearance_people} (${perKey(clearanceByDay)})`);
    console.log('[dedupe] per-stand counts (after):');
    for (const row of afterSnapshot) {
      const svc = row.service_id ? `/svc${row.service_id}` : '';
      console.log(`  ${row.stand}${svc}: checkins=${row.checkins} checkouts=${row.checkouts} people=${row.people} person_days=${row.person_days}`);
    }
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
  console.error(`[dedupe] FAILED: ${error.message}`);
  process.exit(1);
});
