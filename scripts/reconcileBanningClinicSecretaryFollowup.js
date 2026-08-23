/*
 * Banning clinic secretary follow-up (2026-08-23).
 *
 * Applies the client's clarified family/QR mapping, moves Michael Alishak's
 * appointment to the Sunday slot shared with his mother, and reopens only Aly
 * Lo's event survey after removing its inaccurate answers. Every mutation is
 * asserted, transactional and recorded in health_event_reconciliation_audit.
 *
 * Usage (from BACKEND/):
 *   node scripts/reconcileBanningClinicSecretaryFollowup.js production --dry-run
 *   node scripts/reconcileBanningClinicSecretaryFollowup.js production --apply \
 *     --confirm-production=BANNING-2026-08-23-SECRETARY
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
const PROD_CONFIRMATION = '--confirm-production=BANNING-2026-08-23-SECRETARY';
const RUN_KEY = 'banning-clinic-secretary-followup-2026-08-23-v4';
const ALLOWED_ARGS = new Set(['--apply', '--dry-run', PROD_CONFIRMATION]);

const EVENT_ID = 1;
const SYSTEM_USER_ID = 1;
const ALY = { userId: 56719, registrationId: 440, surveyFormId: 6 };
const CONTRERAS = {
  jose: { userId: 42710, registrationId: 339 },
  fabiola: { userId: 41590, registrationId: 338 },
  thania: { userId: 58624, registrationId: 336 },
  dulce: { userId: 58625, registrationId: 337 }
};
const STEPHANIE = { userId: 56734, registrationId: 21 };
const ANGELINA = { userId: 57605, registrationId: 20 };
const MICHAEL = { userId: 58927, registrationId: 420, appointmentId: 639 };
const FAMILY_ENTRY_AT = '2026-08-09 17:48:49.633';

const ENTRY_BACKFILLS = [1993, 2019, 2020];
const CONTRERAS_VISION_KEEP = [875, 911, 915, 1042, 1061, 1088, 1090, 1091];
const CONTRERAS_VISION_DELETE = [1043, 1044, 1081, 1085, 1249, 1251, 1269, 1271];
const CHILD_FOOD_DELETE = [2233, 2234];
const STEPH_ANGELINA_ENTRY = [116, 117, 2003];
const STEPH_ANGELINA_DENTAL = [136, 190, 298, 301, 2086, 2088];
const EXPECTED_ALY_SURVEY_QUESTIONS = [
  30, 31, 32, 33, 34, 35, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
  49, 50, 51, 67
];

if (TARGET !== 'production' || APPLY === DRY_RUN ||
    process.argv.slice(3).some(arg => !ALLOWED_ARGS.has(arg))) {
  console.error('Usage: node scripts/reconcileBanningClinicSecretaryFollowup.js production <--dry-run|--apply>');
  process.exit(1);
}
if (APPLY && !process.argv.includes(PROD_CONFIRMATION)) {
  console.error(`Production apply refused. Add ${PROD_CONFIRMATION}`);
  process.exit(1);
}

function normalize(value) {
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
      values[match[1].toUpperCase()] = normalize(match[2]);
      found = true;
      continue;
    }
    if (found && line.trim() === '') break;
  }
  for (const key of ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT']) {
    if (!values[key]) throw new Error(`Missing ${key} in production database block`);
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

function sameIds(rows, expected, field = 'id') {
  const actual = rows.map(row => Number(row[field])).sort((a, b) => a - b);
  const wanted = [...expected].map(Number).sort((a, b) => a - b);
  return JSON.stringify(actual) === JSON.stringify(wanted);
}

function rowById(rows, id) {
  return rows.find(row => Number(row.id) === Number(id));
}

async function tableExists(connection, tableName) {
  const [[row]] = await connection.query(
    `SELECT COUNT(*) AS total FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
    [tableName]
  );
  return Number(row.total) === 1;
}

async function columnExists(connection, tableName, columnName) {
  const [[row]] = await connection.query(
    `SELECT COUNT(*) AS total FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [tableName, columnName]
  );
  return Number(row.total) === 1;
}

async function scanBundle(connection, scanIds = null, userIds = null, lock = false) {
  const filters = ['s.health_event_id = ?'];
  const params = [EVENT_ID];
  if (scanIds && scanIds.length) {
    filters.push('s.id IN (?)');
    params.push(scanIds);
  }
  if (userIds && userIds.length) {
    filters.push('s.scanned_user_id IN (?)');
    params.push(userIds);
  }
  const [scans] = await connection.query(
    `SELECT s.*, st.name_en AS stand_name
       FROM health_event_scan s
       INNER JOIN health_event_stand st ON st.id = s.stand_id
      WHERE ${filters.join(' AND ')}
      ORDER BY s.scanned_at, s.id${lock ? ' FOR UPDATE' : ''}`,
    params
  );
  const ids = scans.map(row => row.id);
  const [answers] = ids.length
    ? await connection.query(
      `SELECT sa.*, q.name_en AS question_name
         FROM health_event_scan_answer sa
         INNER JOIN health_event_question q ON q.id = sa.question_id
        WHERE sa.scan_id IN (?) ORDER BY sa.scan_id, sa.id${lock ? ' FOR UPDATE' : ''}`,
      [ids]
    ) : [[]];
  const answerIds = answers.map(row => row.id);
  const [options] = answerIds.length
    ? await connection.query(
      `SELECT sao.*, qo.name_en AS option_name
         FROM health_event_scan_answer_option sao
         INNER JOIN health_event_question_option qo ON qo.id = sao.option_id
        WHERE sao.scan_answer_id IN (?) ORDER BY sao.scan_answer_id, sao.option_id${lock ? ' FOR UPDATE' : ''}`,
      [answerIds]
    ) : [[]];
  return { scans, answers, options };
}

function selectedOptions(bundle, scanId, questionName) {
  const answerIds = new Set(bundle.answers
    .filter(row => Number(row.scan_id) === Number(scanId) && row.question_name === questionName)
    .map(row => Number(row.id)));
  return bundle.options
    .filter(row => answerIds.has(Number(row.scan_answer_id)))
    .map(row => row.option_name)
    .sort();
}

async function insertStatusAnswer(connection, scanId, questionId, optionId) {
  const [existing] = await connection.query(
    'SELECT id FROM health_event_scan_answer WHERE scan_id = ? AND question_id = ? FOR UPDATE',
    [scanId, questionId]
  );
  if (existing.length) throw new Error(`Scan #${scanId} already has a status answer`);
  const [inserted] = await connection.query(
    `INSERT INTO health_event_scan_answer(scan_id, question_id, answer_text, answer_number)
     VALUES (?,?,NULL,NULL)`,
    [scanId, questionId]
  );
  await connection.query(
    'INSERT INTO health_event_scan_answer_option(scan_answer_id, option_id) VALUES (?,?)',
    [inserted.insertId, optionId]
  );
  return inserted.insertId;
}

async function deleteScans(connection, scanIds) {
  const [externalPairs] = await connection.query(
    `SELECT id, paired_scan_id FROM health_event_scan
      WHERE paired_scan_id IN (?) AND id NOT IN (?) FOR UPDATE`,
    [scanIds, scanIds]
  );
  if (externalPairs.length) {
    throw new Error(`Scans targeted for deletion have external pairs: ${JSON.stringify(externalPairs)}`);
  }
  const [result] = await connection.query('DELETE FROM health_event_scan WHERE id IN (?)', [scanIds]);
  if (Number(result.affectedRows) !== scanIds.length) {
    throw new Error(`Expected to delete ${scanIds.length} scans, deleted ${result.affectedRows}`);
  }
}

async function captureState(connection) {
  const userIds = [
    ...Object.values(CONTRERAS).map(person => person.userId),
    STEPHANIE.userId, ANGELINA.userId, MICHAEL.userId, ALY.userId
  ];
  const [users] = await connection.query(
    `SELECT id, firstname, lastname, email, phone, date_of_birth, enabled, deleted
       FROM user WHERE id IN (?) ORDER BY id`,
    [userIds]
  );
  const [registrations] = await connection.query(
    `SELECT * FROM health_event_registration
      WHERE health_event_id = ? AND user_id IN (?) ORDER BY id`,
    [EVENT_ID, userIds]
  );
  const registrationIds = registrations.map(row => row.id);
  const [dates] = await connection.query(
    `SELECT * FROM health_event_registration_date
      WHERE registration_id IN (?) ORDER BY registration_id, event_date`,
    [registrationIds]
  );
  const [appointments] = await connection.query(
    `SELECT a.*, sl.service_key, sl.slot_date,
            TIME_FORMAT(sl.start_time, '%H:%i') AS start_time,
            sl.capacity,
            (SELECT COUNT(*) FROM health_event_appointment ax
              WHERE ax.slot_id = sl.id AND ax.status = 'booked') AS booked
       FROM health_event_appointment a
       INNER JOIN health_event_slot sl ON sl.id = a.slot_id
      WHERE a.registration_id IN (?) ORDER BY a.registration_id, a.id`,
    [registrationIds]
  );
  const scans = await scanBundle(connection, null, userIds, false);
  const [alyAnswers] = await connection.query(
    `SELECT a.*, q.form_id FROM health_event_answer a
      INNER JOIN health_event_question q ON q.id = a.question_id
      WHERE a.registration_id = ? ORDER BY q.form_id, a.question_id`,
    [ALY.registrationId]
  );
  const answerIds = alyAnswers.map(row => row.id);
  const [alyAnswerOptions] = answerIds.length
    ? await connection.query(
      'SELECT * FROM health_event_answer_option WHERE answer_id IN (?) ORDER BY answer_id, option_id',
      [answerIds]
    ) : [[]];
  return { users, registrations, dates, appointments, scans, alyAnswers, alyAnswerOptions };
}

async function pendingRequired(connection, registrationId) {
  const [questions] = await connection.query(
    `SELECT q.id, q.form_id, q.question_type, q.required,
            q.depends_on_question_id, q.depends_on_option_id
       FROM health_event_form f
       INNER JOIN health_event_question q ON q.form_id = f.id AND q.enabled = 'Y'
      WHERE f.health_event_id = ? AND f.audience = 'beneficiary'
        AND f.enabled = 'Y' AND f.required_before_qr = 'Y'
      ORDER BY f.section_order, q.sort_order, q.id`,
    [EVENT_ID]
  );
  const [answers] = await connection.query(
    `SELECT a.question_id, ao.option_id
       FROM health_event_answer a
       LEFT JOIN health_event_answer_option ao ON ao.answer_id = a.id
      WHERE a.registration_id = ?`,
    [registrationId]
  );
  const answered = new Set(answers.map(row => Number(row.question_id)));
  const selected = new Map();
  for (const row of answers) {
    const questionId = Number(row.question_id);
    if (!selected.has(questionId)) selected.set(questionId, new Set());
    if (row.option_id != null) selected.get(questionId).add(Number(row.option_id));
  }
  const byId = new Map(questions.map(question => [Number(question.id), question]));
  const visible = new Set();
  const isVisible = (question, seen = new Set()) => {
    const id = Number(question.id);
    if (visible.has(id)) return true;
    if (question.depends_on_question_id == null) return true;
    if (seen.has(id)) return false;
    seen.add(id);
    const parent = byId.get(Number(question.depends_on_question_id));
    if (!parent || !isVisible(parent, seen)) return false;
    const parentSelected = selected.get(Number(question.depends_on_question_id));
    return !!(parentSelected && question.depends_on_option_id != null &&
      parentSelected.has(Number(question.depends_on_option_id)));
  };
  for (const question of questions) {
    if (isVisible(question)) visible.add(Number(question.id));
  }
  const pending = questions.filter(question =>
    question.question_type !== 'appointment' && question.question_type !== 'notice' &&
    question.required === 'Y' && visible.has(Number(question.id)) &&
    !answered.has(Number(question.id))
  );
  return { total: pending.length, formIds: [...new Set(pending.map(row => Number(row.form_id)))], pending };
}

async function main() {
  const config = readProductionConfig();
  if (!String(config.host).includes('database-1.')) {
    throw new Error('Production safety check failed: unexpected database host');
  }
  const connection = await mysql.createConnection(config);
  const operations = [];
  let committed = false;

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
    console.log(`[secretary] target=production mode=${DRY_RUN ? 'DRY-RUN (rollback)' : 'APPLY'}`);
    console.log(`[secretary] database=${identity.db} host=${identity.db_host} timezone=${identity.session_timezone}`);

    if (!(await tableExists(connection, 'health_event_reconciliation_audit'))) {
      throw new Error('health_event_reconciliation_audit is missing');
    }
    if (!(await columnExists(connection, 'health_event_registration', 'post_event_survey_open'))) {
      throw new Error('post_event_survey_open is missing; run the 2026-08-23 migration first');
    }
    const [[priorRun]] = await connection.query(
      'SELECT COUNT(*) AS total FROM health_event_reconciliation_audit WHERE run_key = ?',
      [RUN_KEY]
    );
    if (Number(priorRun.total) !== 0) {
      throw new Error(`${RUN_KEY} already has persisted audit rows; refusing to run again`);
    }

    await connection.beginTransaction();
    const [[event]] = await connection.query(
      `SELECT id, slug, start_date, end_date, timezone FROM health_event
        WHERE id = ? AND slug = 'banning' FOR UPDATE`,
      [EVENT_ID]
    );
    if (!event || String(event.start_date) !== '2026-08-08' ||
        String(event.end_date) !== '2026-08-09' || event.timezone !== 'America/Los_Angeles') {
      throw new Error('Banning event identity/date assertion failed');
    }
    const [[systemUser]] = await connection.query(
      'SELECT id, role_id, enabled, deleted FROM user WHERE id = ? FOR UPDATE',
      [SYSTEM_USER_ID]
    );
    if (!systemUser || Number(systemUser.role_id) !== 1 || systemUser.enabled !== 'Y' || systemUser.deleted !== 'N') {
      throw new Error('System administrator assertion failed');
    }

    const [people] = await connection.query(
      `SELECT u.id, u.firstname, u.lastname, u.email, u.phone, u.date_of_birth,
              u.enabled, u.deleted, r.id AS registration_id, r.status,
              r.post_event_survey_open
         FROM user u
         INNER JOIN health_event_registration r
           ON r.user_id = u.id AND r.health_event_id = ? AND r.registration_role = 'beneficiary'
        WHERE u.id IN (?) ORDER BY u.id FOR UPDATE`,
      [EVENT_ID, [
        ...Object.values(CONTRERAS).map(person => person.userId),
        STEPHANIE.userId, ANGELINA.userId, MICHAEL.userId, ALY.userId
      ]]
    );
    if (people.length !== 8 || people.some(person => person.status !== 'registered' ||
        person.enabled !== 'Y' || person.deleted !== 'N')) {
      throw new Error('Participant registration/account assertion failed');
    }
    const expectedRegistrations = new Map([
      ...Object.values(CONTRERAS).map(person => [person.userId, person.registrationId]),
      [STEPHANIE.userId, STEPHANIE.registrationId],
      [ANGELINA.userId, ANGELINA.registrationId],
      [MICHAEL.userId, MICHAEL.registrationId],
      [ALY.userId, ALY.registrationId]
    ]);
    if (people.some(person => Number(person.registration_id) !== expectedRegistrations.get(Number(person.id)))) {
      throw new Error(`Registration IDs changed: ${JSON.stringify(people)}`);
    }
    const alyPerson = people.find(person => Number(person.id) === ALY.userId);
    if (!alyPerson || String(alyPerson.email).trim().toLowerCase() !== 'ply830@hotmail.com' ||
        String(alyPerson.date_of_birth) !== '1956-12-02' ||
        String(alyPerson.phone).replace(/\D/g, '') !== '9092192394' ||
        alyPerson.post_event_survey_open !== 'N') {
      throw new Error('Aly identity/survey flag assertion failed');
    }

    const [stands] = await connection.query(
      `SELECT id, name_en, has_checkout FROM health_event_stand
        WHERE health_event_id = ? ORDER BY id FOR UPDATE`,
      [EVENT_ID]
    );
    const standByName = new Map(stands.map(row => [row.name_en, row]));
    for (const name of ['Entry Check-in', 'Dental', 'Vision', 'Medical Checks', 'Resource Table', 'Food Distribution']) {
      if (!standByName.has(name)) throw new Error(`Missing stand ${name}`);
    }
    const dentalStand = standByName.get('Dental');
    const visionStand = standByName.get('Vision');
    const medicalStand = standByName.get('Medical Checks');

    const [statusOptions] = await connection.query(
      `SELECT st.name_en AS stand_name, q.id AS question_id, qo.id AS option_id, qo.name_en AS option_name
         FROM health_event_form f
         INNER JOIN health_event_stand st ON st.id = f.stand_id
         INNER JOIN health_event_question q ON q.form_id = f.id AND q.name_en = 'Service status'
         INNER JOIN health_event_question_option qo ON qo.question_id = q.id
        WHERE f.health_event_id = ? AND f.audience = 'checkout'
          AND st.name_en IN ('Dental','Vision','Medical Checks')
          AND qo.name_en IN ('Completed','Declined treatment')
        ORDER BY st.id, qo.id FOR UPDATE`,
      [EVENT_ID]
    );
    const statusKey = new Map(statusOptions.map(row => [
      `${row.stand_name}:${row.option_name}`,
      { questionId: Number(row.question_id), optionId: Number(row.option_id) }
    ]));
    for (const key of ['Dental:Declined treatment', 'Vision:Completed', 'Medical Checks:Completed']) {
      if (!statusKey.has(key)) throw new Error(`Missing checkout option ${key}`);
    }

    const beforeSnapshot = await captureState(connection);

    // 1. All four Contreras family members entered together under José's QR.
    const entryBefore = await scanBundle(connection, [823, ...ENTRY_BACKFILLS], null, true);
    if (!sameIds(entryBefore.scans, [823, ...ENTRY_BACKFILLS]) ||
        rowById(entryBefore.scans, 823).stand_name !== 'Entry Check-in' ||
        String(rowById(entryBefore.scans, 823).scanned_at) !== FAMILY_ENTRY_AT ||
        ENTRY_BACKFILLS.some(id => Number(rowById(entryBefore.scans, id).volunteer_user_id) !== SYSTEM_USER_ID)) {
      throw new Error('Contreras Entry preconditions changed');
    }
    await connection.query(
      'UPDATE health_event_scan SET scanned_at = ? WHERE id IN (?)',
      [FAMILY_ENTRY_AT, ENTRY_BACKFILLS]
    );
    const entryAfter = await scanBundle(connection, [823, ...ENTRY_BACKFILLS], null, false);
    await record({
      actionKey: 'banning-secretary-normalize-contreras-family-entry',
      actionType: 'update-timestamps',
      targetTable: 'health_event_scan',
      targetId: ENTRY_BACKFILLS.join(','),
      before: entryBefore,
      after: entryAfter,
      note: 'Family entered together using José QR; administrative Entry rows share the confirmed family arrival while retaining system user attribution.'
    });

    // 2. Keep the two long consecutive José-QR Vision visits as José + Thania.
    const visionKeepBefore = await scanBundle(connection, CONTRERAS_VISION_KEEP, null, true);
    if (!sameIds(visionKeepBefore.scans, CONTRERAS_VISION_KEEP)) {
      throw new Error('Contreras Vision keep-set changed');
    }
    const joseVisionIn = rowById(visionKeepBefore.scans, 875);
    const joseVisionOut = rowById(visionKeepBefore.scans, 911);
    const thaniaVisionIn = rowById(visionKeepBefore.scans, 915);
    const thaniaVisionOut = rowById(visionKeepBefore.scans, 1042);
    if (Number(joseVisionIn.scanned_user_id) !== CONTRERAS.jose.userId ||
        Number(joseVisionOut.paired_scan_id) !== 875 ||
        Number(thaniaVisionIn.scanned_user_id) !== CONTRERAS.jose.userId ||
        Number(thaniaVisionOut.paired_scan_id) !== 915 ||
        joseVisionIn.stand_name !== 'Vision' || thaniaVisionIn.stand_name !== 'Vision' ||
        selectedOptions(visionKeepBefore, 911, 'Service status').length !== 0 ||
        JSON.stringify(selectedOptions(visionKeepBefore, 1042, 'Service status')) !== JSON.stringify(['Completed'])) {
      throw new Error('Contreras Vision pair/status preconditions changed');
    }
    await connection.query(
      'UPDATE health_event_scan SET registration_id = ?, scanned_user_id = ? WHERE id IN (?,?)',
      [CONTRERAS.thania.registrationId, CONTRERAS.thania.userId, 915, 1042]
    );
    const visionCompleted = statusKey.get('Vision:Completed');
    await insertStatusAnswer(connection, 911, visionCompleted.questionId, visionCompleted.optionId);
    const visionKeepAfter = await scanBundle(connection, CONTRERAS_VISION_KEEP, null, false);
    await record({
      actionKey: 'banning-secretary-map-contreras-vision-visits',
      actionType: 'reassign-and-complete',
      targetTable: 'health_event_scan',
      targetId: '875/911,915/1042',
      before: visionKeepBefore,
      after: visionKeepAfter,
      note: 'First long José-QR visit remains José; the immediately following long visit is Thania. Both are Completed.'
    });

    const visionDeleteBefore = await scanBundle(connection, CONTRERAS_VISION_DELETE, null, true);
    if (!sameIds(visionDeleteBefore.scans, CONTRERAS_VISION_DELETE)) {
      throw new Error('Contreras duplicate Vision set changed');
    }
    const duplicateVisionPairs = [
      [1043, 1044, CONTRERAS.jose],
      [1081, 1085, CONTRERAS.jose],
      [1249, 1251, CONTRERAS.jose],
      [1269, 1271, CONTRERAS.fabiola]
    ];
    for (const [checkinId, checkoutId, person] of duplicateVisionPairs) {
      const checkin = rowById(visionDeleteBefore.scans, checkinId);
      const checkout = rowById(visionDeleteBefore.scans, checkoutId);
      if (!checkin || !checkout || checkin.stand_name !== 'Vision' ||
          checkout.stand_name !== 'Vision' || checkin.scan_type !== 'checkin' ||
          checkout.scan_type !== 'checkout' || Number(checkout.paired_scan_id) !== checkinId ||
          Number(checkin.scanned_user_id) !== person.userId ||
          Number(checkout.scanned_user_id) !== person.userId ||
          Number(checkin.registration_id) !== person.registrationId ||
          Number(checkout.registration_id) !== person.registrationId) {
        throw new Error(`Duplicate Vision pair precondition changed: ${checkinId}/${checkoutId}`);
      }
    }
    await deleteScans(connection, CONTRERAS_VISION_DELETE);
    await record({
      actionKey: 'banning-secretary-remove-contreras-vision-retoggles',
      actionType: 'delete-audited',
      targetTable: 'health_event_scan',
      targetId: CONTRERAS_VISION_DELETE.join(','),
      before: visionDeleteBefore,
      after: null,
      note: 'Remove two rapid retoggle pairs and the later manual duplicate batch, leaving one completed Vision visit per family member.'
    });

    // 3. Food is recorded for the two parents only; Medical belongs to Fabiola.
    const familyServicesBefore = await scanBundle(
      connection,
      [...CHILD_FOOD_DELETE, 1266, 1268, 1270, 1272, 1376, 1377],
      null,
      true
    );
    if (!sameIds(familyServicesBefore.scans, [...CHILD_FOOD_DELETE, 1266, 1268, 1270, 1272, 1376, 1377]) ||
        JSON.stringify(selectedOptions(familyServicesBefore, 1377, 'Service status')) !==
          JSON.stringify(['Declined treatment']) ||
        selectedOptions(familyServicesBefore, 1272, 'Service status').length !== 0) {
      throw new Error('Contreras Food/Medical/Dental preconditions changed');
    }
    await deleteScans(connection, CHILD_FOOD_DELETE);
    const medicalCompleted = statusKey.get('Medical Checks:Completed');
    await insertStatusAnswer(connection, 1272, medicalCompleted.questionId, medicalCompleted.optionId);
    const [fabiolaDentalIn] = await connection.query(
      `INSERT INTO health_event_scan
         (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
          volunteer_user_id, scan_type, paired_scan_id, scanned_at)
       VALUES (?,?,NULL,?,?,?,'checkin',NULL,?)`,
      [EVENT_ID, dentalStand.id, CONTRERAS.fabiola.registrationId,
        CONTRERAS.fabiola.userId, SYSTEM_USER_ID, '2026-08-09 18:00:00.000']
    );
    const [fabiolaDentalOut] = await connection.query(
      `INSERT INTO health_event_scan
         (health_event_id, stand_id, service_id, registration_id, scanned_user_id,
          volunteer_user_id, scan_type, paired_scan_id, scanned_at)
       VALUES (?,?,NULL,?,?,?,'checkout',?,?)`,
      [EVENT_ID, dentalStand.id, CONTRERAS.fabiola.registrationId,
        CONTRERAS.fabiola.userId, SYSTEM_USER_ID, fabiolaDentalIn.insertId,
        '2026-08-09 18:00:30.000']
    );
    const dentalDeclined = statusKey.get('Dental:Declined treatment');
    await insertStatusAnswer(
      connection,
      fabiolaDentalOut.insertId,
      dentalDeclined.questionId,
      dentalDeclined.optionId
    );
    const familyServicesAfter = await scanBundle(
      connection,
      [1266, 1268, 1270, 1272, 1376, 1377, fabiolaDentalIn.insertId, fabiolaDentalOut.insertId],
      null,
      false
    );
    await record({
      actionKey: 'banning-secretary-correct-contreras-food-medical-dental',
      actionType: 'delete-update-insert',
      targetTable: 'health_event_scan',
      targetId: `2233,2234,1272,${fabiolaDentalIn.insertId},${fabiolaDentalOut.insertId}`,
      before: familyServicesBefore,
      after: familyServicesAfter,
      note: 'Only José and Fabiola received family food bags; Fabiola completed Medical and declined Dental, as separately confirmed.'
    });

    // 4. The second shared Entry scan belongs to Angelina; remove its v3 backfill.
    const entryQrBefore = await scanBundle(connection, STEPH_ANGELINA_ENTRY, null, true);
    if (!sameIds(entryQrBefore.scans, STEPH_ANGELINA_ENTRY) ||
        Number(rowById(entryQrBefore.scans, 116).scanned_user_id) !== STEPHANIE.userId ||
        Number(rowById(entryQrBefore.scans, 117).scanned_user_id) !== STEPHANIE.userId ||
        Number(rowById(entryQrBefore.scans, 2003).scanned_user_id) !== ANGELINA.userId) {
      throw new Error('Stephanie/Angelina Entry preconditions changed');
    }
    await connection.query(
      'UPDATE health_event_scan SET registration_id = ?, scanned_user_id = ? WHERE id = 117',
      [ANGELINA.registrationId, ANGELINA.userId]
    );
    await deleteScans(connection, [2003]);
    const entryQrAfter = await scanBundle(connection, [116, 117], null, false);
    await record({
      actionKey: 'banning-secretary-reassign-angelina-entry',
      actionType: 'reassign-and-delete-backfill',
      targetTable: 'health_event_scan',
      targetId: '117,2003',
      before: entryQrBefore,
      after: entryQrAfter,
      note: 'The second Entry use of Stephanie QR was Angelina; replace the later administrative Entry backfill with the real timestamp.'
    });

    // 5. Rebuild the shared Dental toggle sequence as one real visit each.
    const dentalQrBefore = await scanBundle(connection, STEPH_ANGELINA_DENTAL, null, true);
    if (!sameIds(dentalQrBefore.scans, STEPH_ANGELINA_DENTAL) ||
        Number(rowById(dentalQrBefore.scans, 190).paired_scan_id) !== 136 ||
        rowById(dentalQrBefore.scans, 298).scan_type !== 'checkin' ||
        rowById(dentalQrBefore.scans, 301).scan_type !== 'checkin' ||
        Number(rowById(dentalQrBefore.scans, 2086).paired_scan_id) !== 298 ||
        Number(rowById(dentalQrBefore.scans, 2088).paired_scan_id) !== 301 ||
        JSON.stringify(selectedOptions(dentalQrBefore, 2086, 'Service status')) !==
          JSON.stringify(['NA (not recorded)']) ||
        JSON.stringify(selectedOptions(dentalQrBefore, 2088, 'Service status')) !==
          JSON.stringify(['NA (not recorded)'])) {
      throw new Error('Stephanie/Angelina Dental toggle preconditions changed');
    }
    const [targetAnswerConflicts] = await connection.query(
      `SELECT scan_id, question_id FROM health_event_scan_answer
        WHERE scan_id IN (298,301) AND question_id = ? FOR UPDATE`,
      [dentalDeclined.questionId]
    );
    if (targetAnswerConflicts.length) throw new Error('Dental target scans already have a Service status');
    await connection.query('UPDATE health_event_scan_answer SET scan_id = 298 WHERE scan_id = 2086');
    await connection.query('UPDATE health_event_scan_answer SET scan_id = 301 WHERE scan_id = 2088');
    await deleteScans(connection, [2086, 2088]);
    await connection.query(
      `UPDATE health_event_scan
          SET registration_id = ?, scanned_user_id = ?, scan_type = 'checkin', paired_scan_id = NULL
        WHERE id = 190`,
      [ANGELINA.registrationId, ANGELINA.userId]
    );
    await connection.query(
      `UPDATE health_event_scan SET scan_type = 'checkout', paired_scan_id = 136 WHERE id = 298`
    );
    await connection.query(
      `UPDATE health_event_scan SET scan_type = 'checkout', paired_scan_id = 190 WHERE id = 301`
    );
    const dentalQrAfter = await scanBundle(connection, [136, 190, 298, 301], null, false);
    await record({
      actionKey: 'banning-secretary-rebuild-stephanie-angelina-dental-visits',
      actionType: 'reassign-and-repair-pairs',
      targetTable: 'health_event_scan',
      targetId: '136/298,190/301',
      before: dentalQrBefore,
      after: dentalQrAfter,
      note: 'Second early Dental toggle was Angelina check-in; later individual QR scans become the corresponding real check-outs. Preserve NA status without synthetic zero-duration rows.'
    });

    // 6. Michael followed his mother: Vision Saturday, Dental Sunday at 11.
    const [[michaelAppointmentBefore]] = await connection.query(
      `SELECT a.*, sl.service_key, sl.slot_date,
              TIME_FORMAT(sl.start_time, '%H:%i') AS start_time
         FROM health_event_appointment a
         INNER JOIN health_event_slot sl ON sl.id = a.slot_id
        WHERE a.id = ? AND a.registration_id = ? FOR UPDATE`,
      [MICHAEL.appointmentId, MICHAEL.registrationId]
    );
    const [[sundaySlot]] = await connection.query(
      `SELECT sl.id, sl.service_key, sl.slot_date,
              TIME_FORMAT(sl.start_time, '%H:%i') AS start_time, sl.capacity,
              (SELECT COUNT(*) FROM health_event_appointment a
                WHERE a.slot_id = sl.id AND a.status = 'booked') AS booked
         FROM health_event_slot sl WHERE sl.id = 12 FOR UPDATE`
    );
    const [michaelDatesBefore] = await connection.query(
      'SELECT * FROM health_event_registration_date WHERE registration_id = ? ORDER BY event_date FOR UPDATE',
      [MICHAEL.registrationId]
    );
    if (!michaelAppointmentBefore || Number(michaelAppointmentBefore.slot_id) !== 7 ||
        String(michaelAppointmentBefore.slot_date) !== '2026-08-08' ||
        michaelAppointmentBefore.start_time !== '14:00' || michaelAppointmentBefore.status !== 'booked' ||
        !sundaySlot || sundaySlot.service_key !== 'dental' ||
        String(sundaySlot.slot_date) !== '2026-08-09' || sundaySlot.start_time !== '11:00' ||
        Number(sundaySlot.booked) !== 19 || Number(sundaySlot.capacity) !== 20 ||
        michaelDatesBefore.length !== 2) {
      throw new Error('Michael appointment/date preconditions changed');
    }
    await connection.query(
      'UPDATE health_event_appointment SET slot_id = 12 WHERE id = ?',
      [MICHAEL.appointmentId]
    );
    await connection.query(
      `UPDATE health_event_registration_date SET priority_service = 'vision'
        WHERE registration_id = ? AND event_date = '2026-08-08'`,
      [MICHAEL.registrationId]
    );
    const [[michaelAppointmentAfter]] = await connection.query(
      `SELECT a.*, sl.service_key, sl.slot_date,
              TIME_FORMAT(sl.start_time, '%H:%i') AS start_time,
              (SELECT COUNT(*) FROM health_event_appointment ax
                WHERE ax.slot_id = sl.id AND ax.status = 'booked') AS booked,
              sl.capacity
         FROM health_event_appointment a
         INNER JOIN health_event_slot sl ON sl.id = a.slot_id
        WHERE a.id = ?`,
      [MICHAEL.appointmentId]
    );
    const [michaelDatesAfter] = await connection.query(
      'SELECT * FROM health_event_registration_date WHERE registration_id = ? ORDER BY event_date',
      [MICHAEL.registrationId]
    );
    await record({
      actionKey: 'banning-secretary-move-michael-dental-to-sunday',
      actionType: 'update-appointment',
      targetTable: 'health_event_appointment',
      targetId: MICHAEL.appointmentId,
      before: { appointment: michaelAppointmentBefore, dates: michaelDatesBefore },
      after: { appointment: michaelAppointmentAfter, dates: michaelDatesAfter },
      note: 'Move Michael to the Sunday 11:00 Dental slot shared with his mother; Saturday priority becomes Vision. Raw registration answers remain historical.'
    });

    // 7. Remove only Aly's inaccurate survey answers and explicitly reopen it.
    const [alyAnswersBefore] = await connection.query(
      `SELECT a.*, q.form_id, q.name_en
         FROM health_event_answer a
         INNER JOIN health_event_question q ON q.id = a.question_id
        WHERE a.registration_id = ? ORDER BY q.form_id, a.question_id FOR UPDATE`,
      [ALY.registrationId]
    );
    const surveyAnswers = alyAnswersBefore.filter(row => Number(row.form_id) === ALY.surveyFormId);
    const preservedAnswers = alyAnswersBefore.filter(row => Number(row.form_id) !== ALY.surveyFormId);
    const surveyAnswerIds = surveyAnswers.map(row => row.id);
    const [surveyOptions] = await connection.query(
      'SELECT * FROM health_event_answer_option WHERE answer_id IN (?) ORDER BY answer_id, option_id FOR UPDATE',
      [surveyAnswerIds]
    );
    if (surveyAnswers.length !== 22 || preservedAnswers.length !== 16 ||
        !sameIds(surveyAnswers, EXPECTED_ALY_SURVEY_QUESTIONS, 'question_id') ||
        surveyAnswers.some(row => row.source !== 'beneficiary-home')) {
      throw new Error('Aly survey answer preconditions changed');
    }
    await connection.query(
      `DELETE a FROM health_event_answer a
        INNER JOIN health_event_question q ON q.id = a.question_id
       WHERE a.registration_id = ? AND q.form_id = ?`,
      [ALY.registrationId, ALY.surveyFormId]
    );
    await connection.query(
      `UPDATE health_event_registration SET post_event_survey_open = 'Y' WHERE id = ?`,
      [ALY.registrationId]
    );
    const [alyAnswersAfter] = await connection.query(
      `SELECT a.*, q.form_id, q.name_en
         FROM health_event_answer a
         INNER JOIN health_event_question q ON q.id = a.question_id
        WHERE a.registration_id = ? ORDER BY q.form_id, a.question_id`,
      [ALY.registrationId]
    );
    const pendingAfterReset = await pendingRequired(connection, ALY.registrationId);
    await record({
      actionKey: 'banning-secretary-reset-and-reopen-aly-survey',
      actionType: 'delete-answers-and-reopen',
      targetTable: 'health_event_answer',
      targetId: ALY.registrationId,
      before: { answers: surveyAnswers, answerOptions: surveyOptions, preservedAnswerCount: preservedAnswers.length },
      after: { answers: alyAnswersAfter, pending: pendingAfterReset, post_event_survey_open: 'Y' },
      note: 'Delete only the 22 inaccurate internal survey answers; preserve 16 registration answers and reopen the ended event only for Aly.'
    });

    // Global and case-specific postconditions.
    const selectedUsers = [
      ...Object.values(CONTRERAS).map(person => person.userId),
      STEPHANIE.userId, ANGELINA.userId
    ];
    const finalScans = await scanBundle(connection, null, selectedUsers, false);
    const scansFor = (userId, standName, type = null) => finalScans.scans.filter(scan =>
      Number(scan.scanned_user_id) === Number(userId) && scan.stand_name === standName &&
      (!type || scan.scan_type === type)
    );
    const assertVisit = (person, standName, expectedStatus) => {
      const checkins = scansFor(person.userId, standName, 'checkin');
      const checkouts = scansFor(person.userId, standName, 'checkout');
      if (checkins.length !== 1 || checkouts.length !== 1 ||
          Number(checkouts[0].paired_scan_id) !== Number(checkins[0].id) ||
          Number(checkins[0].registration_id) !== person.registrationId ||
          Number(checkouts[0].registration_id) !== person.registrationId ||
          JSON.stringify(selectedOptions(finalScans, checkouts[0].id, 'Service status')) !==
            JSON.stringify([expectedStatus])) {
        throw new Error(`Visit postcondition failed for user ${person.userId} / ${standName}`);
      }
    };
    for (const person of Object.values(CONTRERAS)) {
      if (scansFor(person.userId, 'Entry Check-in', 'checkin').length !== 1) {
        throw new Error(`Entry postcondition failed for user ${person.userId}`);
      }
      assertVisit(person, 'Vision', 'Completed');
    }
    assertVisit(CONTRERAS.fabiola, 'Medical Checks', 'Completed');
    assertVisit(CONTRERAS.fabiola, 'Dental', 'Declined treatment');
    assertVisit(CONTRERAS.thania, 'Dental', 'Declined treatment');
    if (scansFor(CONTRERAS.jose.userId, 'Food Distribution', 'checkin').length !== 1 ||
        scansFor(CONTRERAS.fabiola.userId, 'Food Distribution', 'checkin').length !== 1 ||
        scansFor(CONTRERAS.thania.userId, 'Food Distribution').length !== 0 ||
        scansFor(CONTRERAS.dulce.userId, 'Food Distribution').length !== 0 ||
        scansFor(CONTRERAS.jose.userId, 'Resource Table', 'checkin').length !== 1) {
      throw new Error('Contreras Food/Resource postcondition failed');
    }
    for (const person of [STEPHANIE, ANGELINA]) {
      if (scansFor(person.userId, 'Entry Check-in', 'checkin').length !== 1) {
        throw new Error(`QR Entry postcondition failed for user ${person.userId}`);
      }
      assertVisit(person, 'Dental', 'NA (not recorded)');
    }

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
               OR out_scan.scanned_at < in_scan.scanned_at)`,
      [EVENT_ID]
    );
    const [registrationMismatches] = await connection.query(
      `SELECT s.id FROM health_event_scan s
       LEFT JOIN health_event_registration r ON r.id = s.registration_id
       WHERE s.health_event_id = ? AND s.registration_id IS NOT NULL
         AND (r.id IS NULL OR r.user_id <> s.scanned_user_id)`,
      [EVENT_ID]
    );
    if (brokenPairs.length || registrationMismatches.length) {
      throw new Error(`Global scan postconditions failed: ${JSON.stringify({ brokenPairs, registrationMismatches })}`);
    }
    if (String(michaelAppointmentAfter.slot_date) !== '2026-08-09' ||
        michaelAppointmentAfter.start_time !== '11:00' ||
        Number(michaelAppointmentAfter.booked) !== Number(michaelAppointmentAfter.capacity) ||
        michaelDatesAfter.find(row => String(row.event_date) === '2026-08-08').priority_service !== 'vision' ||
        michaelDatesAfter.find(row => String(row.event_date) === '2026-08-09').priority_service !== 'dental') {
      throw new Error('Michael final appointment/date assertion failed');
    }
    const [[alyPost]] = await connection.query(
      `SELECT r.post_event_survey_open,
              (SELECT COUNT(*) FROM health_event_answer a
                INNER JOIN health_event_question q ON q.id = a.question_id
                WHERE a.registration_id = r.id AND q.form_id = ?) AS survey_answers,
              (SELECT COUNT(*) FROM health_event_answer a
                INNER JOIN health_event_question q ON q.id = a.question_id
                WHERE a.registration_id = r.id AND q.form_id <> ?) AS preserved_answers
         FROM health_event_registration r WHERE r.id = ?`,
      [ALY.surveyFormId, ALY.surveyFormId, ALY.registrationId]
    );
    if (!alyPost || alyPost.post_event_survey_open !== 'Y' ||
        Number(alyPost.survey_answers) !== 0 || Number(alyPost.preserved_answers) !== 16 ||
        pendingAfterReset.total !== 20 || JSON.stringify(pendingAfterReset.formIds) !== JSON.stringify([6])) {
      throw new Error(`Aly reset postcondition failed: ${JSON.stringify({ alyPost, pendingAfterReset })}`);
    }

    const afterSnapshot = await captureState(connection);
    const snapshot = sanitize({
      runKey: RUN_KEY,
      mode: DRY_RUN ? 'dry-run-rolled-back' : 'prepared-for-commit',
      generatedAt: new Date().toISOString(),
      database: identity,
      event,
      operations,
      postconditions: {
        brokenPairs: brokenPairs.length,
        registrationMismatches: registrationMismatches.length,
        alyPendingRequired: pendingAfterReset.total,
        michaelSundaySlotBooked: Number(michaelAppointmentAfter.booked),
        michaelSundaySlotCapacity: Number(michaelAppointmentAfter.capacity)
      },
      before: beforeSnapshot,
      after: afterSnapshot
    });
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const snapshotPath = path.join(
      LOG_DIR,
      `${RUN_KEY}-production-${DRY_RUN ? 'dry-run' : 'apply'}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`
    );
    fs.writeFileSync(snapshotPath, JSON.stringify(snapshot, null, 2), 'utf8');

    if (DRY_RUN) {
      await connection.rollback();
    } else {
      await connection.commit();
      committed = true;
    }
    console.log(`[secretary] ${operations.length} audited action(s) ${DRY_RUN ? 'simulated and rolled back' : 'committed'}`);
    console.log(`[secretary] snapshot=${snapshotPath}`);
    console.log('[secretary] postconditions: Contreras/QR mappings exact; broken pairs=0; registration mismatches=0');
    console.log(`[secretary] Aly pending=${pendingAfterReset.total}; Michael Sunday slot=${michaelAppointmentAfter.booked}/${michaelAppointmentAfter.capacity}`);
  } catch (error) {
    if (!committed) {
      try { await connection.rollback(); } catch (rollbackError) { /* noop */ }
    }
    throw error;
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error('[secretary] FAILED:', error.stack || error.message);
  process.exit(1);
});
