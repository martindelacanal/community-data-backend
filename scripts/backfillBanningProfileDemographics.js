/*
 * Banning clinic: complete app-profile demographics from the event form
 * (2026-08-27, v6).
 *
 * The Jotform import and the web event registration created accounts without
 * user.gender_id / user.ethnicity_id: those answers were stored as event form
 * answers ("What is your current gender identity?" / "Ethnicity") instead. The
 * client asked to copy them into the profile so the app-wide metrics and the
 * "Gender / Ethnicity" export columns cover everyone.
 *
 * Rules:
 *   - only beneficiary registrations of the Banning event;
 *   - only fields that are NULL today (an existing profile value is never
 *     overwritten, even when it disagrees with the form);
 *   - one mapping table from form option -> app catalogue, resolved by NAME
 *     (no hardcoded ids); free-text / "other" answers land in user.other_ethnicity;
 *   - every update is recorded in health_event_reconciliation_audit with the
 *     previous values, so the run is reversible.
 *
 * Usage (from BACKEND/):
 *   node scripts/backfillBanningProfileDemographics.js production --dry-run
 *   node scripts/backfillBanningProfileDemographics.js production --apply \
 *     --confirm-production=BANNING-2026-08-27-PROFILE
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
const PROD_CONFIRMATION = '--confirm-production=BANNING-2026-08-27-PROFILE';
const RUN_KEY = 'banning-clinic-profile-demographics-2026-08-27-v6';
const ALLOWED_ARGS = new Set(['--apply', '--dry-run', PROD_CONFIRMATION]);

const EVENT_SLUG = 'banning';
const GENDER_QUESTION_EN = 'What is your current gender identity?';
const ETHNICITY_QUESTION_EN = 'Ethnicity';
const OTHER_ETHNICITY_MAX = 45; // user.other_ethnicity VARCHAR(45)

// Form option (name_en) -> app catalogue name. The question asked for the
// CURRENT gender identity, so trans women/men map to the identity they stated.
const GENDER_MAP = {
  'Female': 'Female',
  'Male': 'Male',
  'Transgender Female (Trans Woman)': 'Female',
  'Transgender Male (Trans Man)': 'Male',
  'Non-binary': 'Other',
  'Gender non-conforming / Queer': 'Other',
  'A different gender identity (please specify)': 'Other',
  'Decline to state / Prefer not to respond': 'Prefer not to say'
};
// Form option -> { catalogue name, free text kept in user.other_ethnicity }.
const ETHNICITY_MAP = {
  'Hispanic/Latino': { name: 'Hispanic or Latino' },
  'Black/African American': { name: 'Black or African American' },
  'White': { name: 'White' },
  'Asian': { name: 'Asian' },
  'Native Hawaiian/Pacific Islander': { name: 'Native Hawaiian or Other Pacific Islander' },
  'American Indian/Alaska Native': { name: 'American Indian or Alaska Native' },
  'Middle Eastern/North African': { name: 'Others', otherText: 'Middle Eastern/North African' },
  'Two or more races': { name: 'Mixed' },
  'Other': { name: 'Others', useAnswerText: true },
  'Prefer not to answer': { name: 'Prefer not to say' }
};

if (!['development', 'production'].includes(TARGET) || APPLY === DRY_RUN ||
    process.argv.slice(3).some(arg => !ALLOWED_ARGS.has(arg))) {
  console.error('Usage: node scripts/backfillBanningProfileDemographics.js <development|production> <--dry-run|--apply>');
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
    console.log(`[profile] target=${TARGET} mode=${DRY_RUN ? 'DRY-RUN (rollback)' : 'APPLY'}`);
    console.log(`[profile] database=${identity.db} host=${identity.db_host}`);

    await ensureAuditTable(connection);
    const [[previousRun]] = await connection.query(
      'SELECT COUNT(*) AS total FROM health_event_reconciliation_audit WHERE run_key = ?', [RUN_KEY]);
    if (Number(previousRun.total) > 0) {
      throw new Error(`${RUN_KEY} already has ${previousRun.total} persisted audit rows; refusing to run it again`);
    }

    await connection.beginTransaction();

    const [[event]] = await connection.query(
      'SELECT id, slug, name_en FROM health_event WHERE slug = ? LIMIT 1 FOR UPDATE', [EVENT_SLUG]);
    if (!event) throw new Error(`Event '${EVENT_SLUG}' not found in ${TARGET}`);

    // Catalogues by name (ids differ between dev and prod).
    const [genders] = await connection.query('SELECT id, name FROM gender');
    const [ethnicities] = await connection.query('SELECT id, name FROM ethnicity');
    const genderIdByName = new Map(genders.map(row => [row.name, Number(row.id)]));
    const ethnicityIdByName = new Map(ethnicities.map(row => [row.name, Number(row.id)]));
    for (const name of new Set(Object.values(GENDER_MAP))) {
      if (!genderIdByName.has(name)) throw new Error(`gender catalogue has no '${name}'`);
    }
    for (const { name } of Object.values(ETHNICITY_MAP)) {
      if (!ethnicityIdByName.has(name)) throw new Error(`ethnicity catalogue has no '${name}'`);
    }

    // Event questions by text (beneficiary forms only).
    const [questions] = await connection.query(
      `SELECT q.id, q.name_en FROM health_event_question q
         INNER JOIN health_event_form f ON f.id = q.form_id
        WHERE f.health_event_id = ? AND f.audience = 'beneficiary' AND q.name_en IN (?, ?)`,
      [event.id, GENDER_QUESTION_EN, ETHNICITY_QUESTION_EN]);
    const genderQuestion = questions.find(q => q.name_en === GENDER_QUESTION_EN);
    const ethnicityQuestion = questions.find(q => q.name_en === ETHNICITY_QUESTION_EN);
    if (!genderQuestion || !ethnicityQuestion) throw new Error('Gender / Ethnicity questions not found in the beneficiary form');
    if (questions.length !== 2) throw new Error('Ambiguous gender/ethnicity questions');

    // Every form option must be covered by the mapping (fail fast on unknown labels).
    const [options] = await connection.query(
      'SELECT id, question_id, name_en, is_other FROM health_event_question_option WHERE question_id IN (?, ?)',
      [genderQuestion.id, ethnicityQuestion.id]);
    for (const option of options) {
      const map = Number(option.question_id) === Number(genderQuestion.id) ? GENDER_MAP : ETHNICITY_MAP;
      if (!(option.name_en in map)) throw new Error(`No mapping for form option '${option.name_en}' (question ${option.question_id})`);
    }
    const optionById = new Map(options.map(option => [Number(option.id), option]));

    // Candidates: Banning beneficiaries whose profile misses gender or ethnicity.
    const [candidates] = await connection.query(
      `SELECT r.id AS registration_id, r.source, r.status, u.id AS user_id,
              u.gender_id, u.ethnicity_id, u.other_ethnicity,
              (SELECT ao.option_id FROM health_event_answer a
                 INNER JOIN health_event_answer_option ao ON ao.answer_id = a.id
                WHERE a.registration_id = r.id AND a.question_id = ? LIMIT 1) AS gender_option_id,
              (SELECT COUNT(*) FROM health_event_answer a
                 INNER JOIN health_event_answer_option ao ON ao.answer_id = a.id
                WHERE a.registration_id = r.id AND a.question_id = ?) AS gender_option_count,
              (SELECT ao.option_id FROM health_event_answer a
                 INNER JOIN health_event_answer_option ao ON ao.answer_id = a.id
                WHERE a.registration_id = r.id AND a.question_id = ? LIMIT 1) AS ethnicity_option_id,
              (SELECT COUNT(*) FROM health_event_answer a
                 INNER JOIN health_event_answer_option ao ON ao.answer_id = a.id
                WHERE a.registration_id = r.id AND a.question_id = ?) AS ethnicity_option_count,
              (SELECT a.other_text FROM health_event_answer a
                WHERE a.registration_id = r.id AND a.question_id = ? LIMIT 1) AS ethnicity_other_text
         FROM health_event_registration r
         INNER JOIN user u ON u.id = r.user_id
        WHERE r.health_event_id = ? AND r.registration_role = 'beneficiary' AND u.deleted = 'N'
          AND (u.gender_id IS NULL OR u.ethnicity_id IS NULL)
        ORDER BY r.id
        FOR UPDATE OF u`,
      [genderQuestion.id, genderQuestion.id, ethnicityQuestion.id, ethnicityQuestion.id, ethnicityQuestion.id, event.id]);

    const seenUsers = new Set();
    const summary = { candidates: candidates.length, updatedUsers: 0, genderSet: 0, ethnicitySet: 0, otherTextSet: 0, skippedNoAnswer: 0 };
    const genderTally = new Map();
    const ethnicityTally = new Map();

    for (const row of candidates) {
      if (seenUsers.has(row.user_id)) continue; // one registration per user in this event, but stay safe
      seenUsers.add(row.user_id);
      if (Number(row.gender_option_count) > 1 || Number(row.ethnicity_option_count) > 1) {
        throw new Error(`Registration ${row.registration_id} has more than one gender/ethnicity option`);
      }

      const after = { gender_id: row.gender_id, ethnicity_id: row.ethnicity_id, other_ethnicity: row.other_ethnicity };
      const applied = [];

      if (row.gender_id == null && row.gender_option_id != null) {
        const option = optionById.get(Number(row.gender_option_id));
        const catalogueName = GENDER_MAP[option.name_en];
        after.gender_id = genderIdByName.get(catalogueName);
        applied.push(`gender '${option.name_en}' -> ${catalogueName}`);
        genderTally.set(catalogueName, (genderTally.get(catalogueName) || 0) + 1);
      }
      if (row.ethnicity_id == null && row.ethnicity_option_id != null) {
        const option = optionById.get(Number(row.ethnicity_option_id));
        const mapping = ETHNICITY_MAP[option.name_en];
        after.ethnicity_id = ethnicityIdByName.get(mapping.name);
        applied.push(`ethnicity '${option.name_en}' -> ${mapping.name}`);
        ethnicityTally.set(mapping.name, (ethnicityTally.get(mapping.name) || 0) + 1);
        const otherText = mapping.useAnswerText ? normalize(row.ethnicity_other_text) : (mapping.otherText || '');
        if (otherText && !row.other_ethnicity) {
          after.other_ethnicity = otherText.slice(0, OTHER_ETHNICITY_MAX);
          applied.push(`other_ethnicity '${after.other_ethnicity}'`);
        }
      }

      if (!applied.length) {
        summary.skippedNoAnswer += 1;
        continue;
      }

      const [update] = await connection.query(
        `UPDATE user SET gender_id = ?, ethnicity_id = ?, other_ethnicity = ? WHERE id = ? AND deleted = 'N'`,
        [after.gender_id, after.ethnicity_id, after.other_ethnicity, row.user_id]);
      if (update.affectedRows !== 1) throw new Error(`Could not update user ${row.user_id}`);
      summary.updatedUsers += 1;
      if (after.gender_id !== row.gender_id) summary.genderSet += 1;
      if (after.ethnicity_id !== row.ethnicity_id) summary.ethnicitySet += 1;
      if (after.other_ethnicity !== row.other_ethnicity) summary.otherTextSet += 1;

      await record({
        actionKey: `profile-demographics-u${row.user_id}`,
        actionType: 'update',
        targetTable: 'user',
        targetId: row.user_id,
        before: { gender_id: row.gender_id, ethnicity_id: row.ethnicity_id, other_ethnicity: row.other_ethnicity },
        after,
        note: `Copied from the Banning event form (registration ${row.registration_id}, source ${row.source}): ${applied.join('; ')}. Only empty profile fields were filled.`
      });
    }

    // Postcondition: no Banning beneficiary with an answer is left without a profile value.
    const [[remaining]] = await connection.query(
      `SELECT SUM(u.gender_id IS NULL AND EXISTS(SELECT 1 FROM health_event_answer a
               INNER JOIN health_event_answer_option ao ON ao.answer_id = a.id
               WHERE a.registration_id = r.id AND a.question_id = ?)) AS gender_missing,
              SUM(u.ethnicity_id IS NULL AND EXISTS(SELECT 1 FROM health_event_answer a
               INNER JOIN health_event_answer_option ao ON ao.answer_id = a.id
               WHERE a.registration_id = r.id AND a.question_id = ?)) AS ethnicity_missing
         FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
        WHERE r.health_event_id = ? AND r.registration_role = 'beneficiary' AND u.deleted = 'N'`,
      [genderQuestion.id, ethnicityQuestion.id, event.id]);
    if (Number(remaining.gender_missing) !== 0 || Number(remaining.ethnicity_missing) !== 0) {
      throw new Error(`Postcondition failed: gender_missing=${remaining.gender_missing} ethnicity_missing=${remaining.ethnicity_missing}`);
    }

    fs.mkdirSync(LOG_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const snapshot = sanitize({
      runKey: RUN_KEY,
      target: TARGET,
      database: { name: identity.db, host: identity.db_host },
      event,
      generatedAt: new Date().toISOString(),
      summary,
      genderTally: Object.fromEntries(genderTally),
      ethnicityTally: Object.fromEntries(ethnicityTally),
      operations
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
        console.warn(`[profile] COMMITTED, but final snapshot failed: ${snapshotError.message}`);
      }
    }

    const tally = map => Array.from(map.entries()).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}=${v}`).join(', ') || 'none';
    console.log(`[profile] ${operations.length} audited changes ${DRY_RUN ? 'simulated and rolled back' : 'committed'}.`);
    console.log(`[profile] snapshot=${logPath}`);
    console.log(`[profile] candidates=${summary.candidates} updated=${summary.updatedUsers} gender_set=${summary.genderSet} ethnicity_set=${summary.ethnicitySet} other_text_set=${summary.otherTextSet} skipped_no_answer=${summary.skippedNoAnswer}`);
    console.log(`[profile] gender: ${tally(genderTally)}`);
    console.log(`[profile] ethnicity: ${tally(ethnicityTally)}`);
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
  console.error(`[profile] FAILED: ${error.message}`);
  process.exit(1);
});
