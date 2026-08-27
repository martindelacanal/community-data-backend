/*
 * Health events: question -> participant-profile mapping (2026-08-28).
 * Idempotent; run on dev AND prod.
 *
 *  1. health_event_question_option.profile_option_id INT NULL — the app
 *     catalogue row (gender / ethnicity / language id) a form option stands for.
 *     The question itself is mapped through the existing maps_to column with
 *     the new values profile_gender | profile_ethnicity |
 *     profile_second_ethnicity | profile_language | profile_zipcode
 *     (api/services/healthEventProfileSync.js).
 *  2. Banning's beneficiary form is mapped as the reference example: the
 *     "What is your current gender identity?" and "Ethnicity" questions get
 *     maps_to + per-option catalogue ids (same mapping as the v6 backfill).
 *     Only unmapped questions/options are touched.
 *
 * Usage (from BACKEND/):
 *   node scripts/2026-08-28_healthEventQuestionProfileMapping.js <development|production> --dry-run
 *   node scripts/2026-08-28_healthEventQuestionProfileMapping.js <development|production> --apply
 */
'use strict';

const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

const ENV_PATH = path.join(path.resolve(__dirname, '..'), '.env');
const TARGET = String(process.argv[2] || '').toLowerCase();
const APPLY = process.argv.includes('--apply');
const DRY_RUN = process.argv.includes('--dry-run');

const EVENT_SLUG = 'banning';
const GENDER_QUESTION_EN = 'What is your current gender identity?';
const ETHNICITY_QUESTION_EN = 'Ethnicity';
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
const ETHNICITY_MAP = {
  'Hispanic/Latino': 'Hispanic or Latino',
  'Black/African American': 'Black or African American',
  'White': 'White',
  'Asian': 'Asian',
  'Native Hawaiian/Pacific Islander': 'Native Hawaiian or Other Pacific Islander',
  'American Indian/Alaska Native': 'American Indian or Alaska Native',
  'Middle Eastern/North African': 'Others',
  'Two or more races': 'Mixed',
  'Other': 'Others',
  'Prefer not to answer': 'Prefer not to say'
};

if (!['development', 'production'].includes(TARGET) || APPLY === DRY_RUN ||
    process.argv.slice(3).some(arg => !['--apply', '--dry-run'].includes(arg))) {
  console.error('Usage: node scripts/2026-08-28_healthEventQuestionProfileMapping.js <development|production> <--dry-run|--apply>');
  process.exit(1);
}

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
    host: values.DB_HOST, user: values.DB_USER, password: values.DB_PASSWORD,
    database: values.DB_DATABASE, port: Number(values.DB_PORT), connectTimeout: 30_000
  };
}

async function main() {
  const connection = await mysql.createConnection(readDatabaseConfig(TARGET));
  try {
    const [[identity]] = await connection.query('SELECT DATABASE() AS db, @@hostname AS db_host');
    console.log(`[profile-mapping] target=${TARGET} mode=${DRY_RUN ? 'DRY-RUN' : 'APPLY'} database=${identity.db} host=${identity.db_host}`);

    // ---- 1. schema -----------------------------------------------------
    const [columns] = await connection.query(
      `SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'health_event_question_option' AND COLUMN_NAME = 'profile_option_id'`);
    if (!columns.length) {
      console.log('[profile-mapping] would add health_event_question_option.profile_option_id INT NULL');
      if (!DRY_RUN) {
        await connection.query(
          'ALTER TABLE health_event_question_option ADD COLUMN profile_option_id INT DEFAULT NULL AFTER service_key');
        console.log('[profile-mapping] column added');
      }
    } else {
      console.log('[profile-mapping] column already exists');
    }

    // ---- 2. Banning reference mapping ---------------------------------
    const [[event]] = await connection.query('SELECT id FROM health_event WHERE slug = ? LIMIT 1', [EVENT_SLUG]);
    if (!event) {
      console.log(`[profile-mapping] event '${EVENT_SLUG}' not present in ${TARGET}; schema step only`);
      return;
    }
    const [genders] = await connection.query('SELECT id, name FROM gender');
    const [ethnicities] = await connection.query('SELECT id, name FROM ethnicity');
    const genderIdByName = new Map(genders.map(row => [row.name, Number(row.id)]));
    const ethnicityIdByName = new Map(ethnicities.map(row => [row.name, Number(row.id)]));

    const [questions] = await connection.query(
      `SELECT q.id, q.name_en, q.maps_to FROM health_event_question q
         INNER JOIN health_event_form f ON f.id = q.form_id
        WHERE f.health_event_id = ? AND f.audience = 'beneficiary' AND q.name_en IN (?, ?)`,
      [event.id, GENDER_QUESTION_EN, ETHNICITY_QUESTION_EN]);

    for (const question of questions) {
      const isGender = question.name_en === GENDER_QUESTION_EN;
      const mapsTo = isGender ? 'profile_gender' : 'profile_ethnicity';
      const map = isGender ? GENDER_MAP : ETHNICITY_MAP;
      const idByName = isGender ? genderIdByName : ethnicityIdByName;

      if (!question.maps_to) {
        console.log(`[profile-mapping] question ${question.id} '${question.name_en}': maps_to -> ${mapsTo}`);
        if (!DRY_RUN) await connection.query('UPDATE health_event_question SET maps_to = ? WHERE id = ?', [mapsTo, question.id]);
      } else if (question.maps_to !== mapsTo) {
        console.log(`[profile-mapping] question ${question.id} already maps to '${question.maps_to}'; left untouched`);
        continue;
      }

      const [options] = await connection.query(
        'SELECT id, name_en, profile_option_id FROM health_event_question_option WHERE question_id = ?', [question.id])
        .catch(() => [[]]); // column missing in dry-run before the ALTER
      let mapped = 0;
      for (const option of options) {
        if (option.profile_option_id != null) continue;
        const catalogueName = map[option.name_en];
        const catalogueId = catalogueName ? idByName.get(catalogueName) : null;
        if (!catalogueId) {
          console.log(`[profile-mapping]   option ${option.id} '${option.name_en}': no catalogue match, left unmapped`);
          continue;
        }
        mapped += 1;
        if (!DRY_RUN) {
          await connection.query('UPDATE health_event_question_option SET profile_option_id = ? WHERE id = ?', [catalogueId, option.id]);
        }
      }
      console.log(`[profile-mapping]   ${mapped} option(s) ${DRY_RUN ? 'would be' : ''} mapped to the ${isGender ? 'gender' : 'ethnicity'} catalogue`);
    }
    console.log(`[profile-mapping] done (${DRY_RUN ? 'nothing written' : 'applied'})`);
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error(`[profile-mapping] FAILED: ${error.message}`);
  process.exit(1);
});
