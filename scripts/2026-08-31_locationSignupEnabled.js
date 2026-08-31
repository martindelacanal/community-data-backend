/*
 * Locations: sign-up visibility flag (2026-08-31). Idempotent; run on dev AND prod.
 *
 *  1. location.signup_enabled CHAR(1) NOT NULL DEFAULT 'Y' — when 'N', the
 *     location stays fully functional for admins, metrics, filters and health
 *     events, but beneficiaries can no longer pick it: it is excluded from the
 *     public sign-up list (/register/locations) and from the beneficiary
 *     branch of /locations (location picker, map/QR, notification settings).
 *  2. "Banning: Nicolet Middle School" is set to signup_enabled='N': it was
 *     created for the Banning health clinic and will not be a Bienestar food
 *     distribution point, yet 19 people chose it as their location after the
 *     event because it kept showing up in the sign-up list.
 *
 * Usage (from BACKEND/):
 *   node scripts/2026-08-31_locationSignupEnabled.js <development|production> --dry-run
 *   node scripts/2026-08-31_locationSignupEnabled.js <development|production> --apply
 */
'use strict';

const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

const ENV_PATH = path.join(path.resolve(__dirname, '..'), '.env');
const TARGET = String(process.argv[2] || '').toLowerCase();
const APPLY = process.argv.includes('--apply');
const DRY_RUN = process.argv.includes('--dry-run');

const HIDDEN_LOCATIONS = [
  { organization: 'Nicolet Middle School', community_city: 'Banning: Nicolet Middle School' }
];

if (!['development', 'production'].includes(TARGET) || APPLY === DRY_RUN ||
    process.argv.slice(3).some(arg => !['--apply', '--dry-run'].includes(arg))) {
  console.error('Usage: node scripts/2026-08-31_locationSignupEnabled.js <development|production> <--dry-run|--apply>');
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
    console.log(`[signup-enabled] target=${TARGET} mode=${DRY_RUN ? 'DRY-RUN' : 'APPLY'} database=${identity.db} host=${identity.db_host}`);

    const [columns] = await connection.query(
      `SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'location' AND COLUMN_NAME = 'signup_enabled'`);
    if (!columns.length) {
      console.log("[signup-enabled] would add location.signup_enabled CHAR(1) NOT NULL DEFAULT 'Y'");
      if (!DRY_RUN) {
        await connection.query(
          `ALTER TABLE location ADD COLUMN signup_enabled CHAR(1) COLLATE utf8mb4_spanish_ci NOT NULL DEFAULT 'Y' AFTER enabled`);
        console.log('[signup-enabled] column added');
      }
    } else {
      console.log('[signup-enabled] column already exists');
    }

    for (const target of HIDDEN_LOCATIONS) {
      const [rows] = await connection.query(
        'SELECT id, organization, community_city FROM location WHERE organization = ? AND community_city = ?',
        [target.organization, target.community_city]);
      if (rows.length !== 1) {
        console.log(`[signup-enabled] '${target.community_city}' matched ${rows.length} rows in ${TARGET}; skipped`);
        continue;
      }
      console.log(`[signup-enabled] location ${rows[0].id} '${rows[0].community_city}': signup_enabled -> N`);
      if (!DRY_RUN) {
        await connection.query('UPDATE location SET signup_enabled = ? WHERE id = ?', ['N', rows[0].id]);
      }
    }
    console.log(`[signup-enabled] done (${DRY_RUN ? 'nothing written' : 'applied'})`);
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error(`[signup-enabled] FAILED: ${error.message}`);
  process.exit(1);
});
