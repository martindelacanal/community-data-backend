/*
 * Adds the per-registration switch used to reopen a required survey after an
 * event has ended. Idempotent and intentionally schema-only: case-specific
 * activation belongs in an audited reconciliation script.
 *
 * Usage (from BACKEND/):
 *   node scripts/2026-08-23_healthEventPostEventSurvey.js development --dry-run
 *   node scripts/2026-08-23_healthEventPostEventSurvey.js development --apply
 *   node scripts/2026-08-23_healthEventPostEventSurvey.js production --apply \
 *     --confirm-production=POST-EVENT-SURVEY-2026-08-23
 */
'use strict';

const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

const ENV_PATH = path.resolve(__dirname, '..', '.env');
const TARGET = String(process.argv[2] || '').toLowerCase();
const APPLY = process.argv.includes('--apply');
const DRY_RUN = process.argv.includes('--dry-run');
const PROD_CONFIRMATION = '--confirm-production=POST-EVENT-SURVEY-2026-08-23';
const ALLOWED_ARGS = new Set(['--apply', '--dry-run', PROD_CONFIRMATION]);

if (!['development', 'production'].includes(TARGET) || APPLY === DRY_RUN ||
    process.argv.slice(3).some(arg => !ALLOWED_ARGS.has(arg))) {
  console.error('Usage: node scripts/2026-08-23_healthEventPostEventSurvey.js <development|production> <--dry-run|--apply>');
  process.exit(1);
}
if (TARGET === 'production' && APPLY && !process.argv.includes(PROD_CONFIRMATION)) {
  console.error(`Production apply refused. Add ${PROD_CONFIRMATION}`);
  process.exit(1);
}

function normalize(value) {
  return String(value == null ? '' : value).trim().replace(/^(['"])(.*)\1$/, '$2').trim();
}

function readBlock(text, heading, commented) {
  const assignment = commented
    ? /^\s*#\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i
    : /^\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i;
  const values = {};
  let inside = false;
  let found = false;
  for (const line of String(text).split(/\r?\n/)) {
    if (!inside) {
      inside = new RegExp(`^\\s*#\\s*${heading}\\s*$`, 'i').test(line);
      continue;
    }
    const match = line.match(assignment);
    if (match) {
      values[match[1].toUpperCase()] = normalize(match[2]);
      found = true;
      continue;
    }
    if (found && line.trim() === '') break;
  }
  return values;
}

function databaseConfig(target) {
  const text = fs.readFileSync(ENV_PATH, 'utf8');
  const values = readBlock(
    text,
    target === 'production' ? 'PRODUCTION DATABASE' : 'DEVELOPMENT DATABASE',
    target === 'production'
  );
  for (const key of ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT']) {
    if (!values[key]) throw new Error(`Missing ${key} in ${target} database block`);
  }
  const production = readBlock(text, 'PRODUCTION DATABASE', true);
  const sameAsProduction = ['DB_HOST', 'DB_USER', 'DB_DATABASE', 'DB_PORT']
    .every(key => normalize(values[key]).toLowerCase() === normalize(production[key]).toLowerCase());
  if (target === 'production' && !sameAsProduction) throw new Error('Production target mismatch');
  if (target === 'development' && sameAsProduction) throw new Error('Development target matches production');
  return {
    host: values.DB_HOST,
    user: values.DB_USER,
    password: values.DB_PASSWORD,
    database: values.DB_DATABASE,
    port: Number(values.DB_PORT),
    connectTimeout: 30_000
  };
}

async function main() {
  const config = databaseConfig(TARGET);
  const connection = await mysql.createConnection(config);
  try {
    const [[identity]] = await connection.query(
      'SELECT DATABASE() AS db, @@hostname AS db_host'
    );
    console.log(`[post-event-survey] target=${TARGET} mode=${DRY_RUN ? 'DRY-RUN' : 'APPLY'}`);
    console.log(`[post-event-survey] database=${identity.db} host=${identity.db_host}`);
    if (TARGET === 'production' && !String(config.host).includes('database-1.')) {
      throw new Error('Production safety check failed: unexpected host');
    }

    const [tables] = await connection.query(
      `SELECT TABLE_NAME FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'health_event_registration'`
    );
    if (tables.length !== 1) throw new Error('health_event_registration does not exist');

    const [before] = await connection.query(
      `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
         FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'health_event_registration'
          AND COLUMN_NAME = 'post_event_survey_open'`
    );
    if (!before.length) {
      if (DRY_RUN) {
        console.log('[post-event-survey] would add post_event_survey_open CHAR(1) NOT NULL DEFAULT N');
        return;
      }
      await connection.query(
        `ALTER TABLE health_event_registration
           ADD COLUMN post_event_survey_open CHAR(1) COLLATE utf8mb4_spanish_ci
           NOT NULL DEFAULT 'N' AFTER status`
      );
      console.log('[post-event-survey] column added');
    } else {
      console.log('[post-event-survey] column already exists');
    }

    const [[column]] = await connection.query(
      `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
         FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'health_event_registration'
          AND COLUMN_NAME = 'post_event_survey_open'`
    );
    if (!column || String(column.COLUMN_TYPE).toLowerCase() !== 'char(1)' ||
        column.IS_NULLABLE !== 'NO' || column.COLUMN_DEFAULT !== 'N') {
      throw new Error(`Unexpected column definition: ${JSON.stringify(column)}`);
    }
    const [[invalid]] = await connection.query(
      `SELECT COUNT(*) AS total FROM health_event_registration
        WHERE post_event_survey_open NOT IN ('Y','N')`
    );
    if (Number(invalid.total) !== 0) throw new Error('Invalid post_event_survey_open values found');
    console.log('[post-event-survey] verified');
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error('[post-event-survey] FAILED:', error.message);
  process.exit(1);
});
