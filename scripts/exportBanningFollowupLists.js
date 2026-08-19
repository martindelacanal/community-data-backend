/*
 * Categorized follow-up lists for the Banning clinic (2026-08-08/09).
 *
 * The client asked for the patients who checked in but have no other scans, or
 * who are missing a check-out scan, separated into vision appointments /
 * dental appointments / medical check-up / unknown. This script is STRICTLY
 * READ-ONLY (SELECT queries only) and materializes two situations:
 *   S1 'Missing check-out': every check-in at a checkout-enabled stand
 *      (Dental, Vision, Medical Checks) whose check-out is absent or only
 *      carries the administrative 'NA (not recorded)' status placeholder.
 *      The category is the stand itself.
 *   S2 'Checked in, no clinical services': users with an Entry check-in and
 *      zero scans at any service stand (Food Distribution and Entry do not
 *      count as services). The category follows their booked appointment,
 *      else their survey answers, else 'Unknown'.
 *
 * Outputs (deliverables/banning-clinic-2026-08/): one styled workbook with a
 * 'Read me' sheet plus the four category sheets, and one UTF-8-BOM CSV per
 * category so Excel opens accents correctly.
 *
 * Usage (from BACKEND/):
 *   node scripts/exportBanningFollowupLists.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const XLSX = require('xlsx-js-style');

const BACKEND_ROOT = path.resolve(__dirname, '..');
const ENV_PATH = path.join(BACKEND_ROOT, '.env');
const OUTPUT_DIR = path.resolve(BACKEND_ROOT, '..', 'deliverables', 'banning-clinic-2026-08');

const EVENT_ID = 1;
const EVENT_SLUG = 'banning';
const EVENT_TIMEZONE = 'America/Los_Angeles';
const NA_STATUS = 'NA (not recorded)';
const SURVEY_QUESTION_ID = 30; // 'What service did you come in today to receive?'
const INTEREST_QUESTION_ID = 20; // 'Which services are you interested in receiving?'
const SCREENING_OPTION_ID = 81; // 'General health screening / clinical service'

const SITUATION_MISSING_CHECKOUT = 'Missing check-out';
const SITUATION_ENTRY_ONLY = 'Checked in, no clinical services';

const SHEET_DENTAL = 'Dental appointments';
const SHEET_VISION = 'Vision appointments';
const SHEET_MEDICAL = 'Medical check-up';
const SHEET_UNKNOWN = 'Unknown';
const CATEGORY_SHEETS = [SHEET_DENTAL, SHEET_VISION, SHEET_MEDICAL, SHEET_UNKNOWN];
const CSV_FILE_BY_SHEET = {
  [SHEET_DENTAL]: 'dental-appointments.csv',
  [SHEET_VISION]: 'vision-appointments.csv',
  [SHEET_MEDICAL]: 'medical-check-up.csv',
  [SHEET_UNKNOWN]: 'unknown.csv'
};
const SHEET_BY_CHECKOUT_STAND = {
  Dental: SHEET_DENTAL,
  Vision: SHEET_VISION,
  'Medical Checks': SHEET_MEDICAL
};

const HEADERS = [
  'Situation', 'First name', 'Last name', 'Email', 'Phone', 'Date of birth',
  'Day attended', 'Check-in time', 'Booked appointment', 'Priority service answer',
  'Survey services wanted', 'Also on other list', 'Notes'
];

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

function dateOnly(value) {
  return value == null ? '' : String(value).slice(0, 10);
}

function csvCell(value) {
  if (value == null) return '';
  const text = String(value);
  // Excel treats a leading =, +, - or @ as a formula; the leading quote keeps
  // phone numbers and notes as plain text.
  const escaped = /^[=+\-@]/.test(text) ? `'${text}` : text;
  return /[",\n\r]/.test(escaped) ? `"${escaped.replace(/"/g, '""')}"` : escaped;
}

/** Comma-delimited + BOM: opens with correct accents in Excel without a wizard. */
function rowsToCsv(rows) {
  const lines = [HEADERS.map(csvCell).join(',')];
  for (const row of rows) {
    lines.push(HEADERS.map(header => csvCell(row.values[header])).join(','));
  }
  return '\ufeff' + lines.join('\r\n') + '\r\n';
}

const HEADER_STYLE = {
  font: { bold: true, color: { rgb: 'FFFFFF' } },
  fill: { fgColor: { rgb: '11B3D1' } },
  alignment: { vertical: 'center', wrapText: false }
};
const READ_ME_TITLE_STYLE = { font: { bold: true, sz: 14 } };
const READ_ME_HEADING_STYLE = { font: { bold: true } };

function categorySheet(rows) {
  const aoa = [HEADERS, ...rows.map(row => HEADERS.map(header => row.values[header]))];
  const sheet = XLSX.utils.aoa_to_sheet(aoa);
  for (let column = 0; column < HEADERS.length; column += 1) {
    const address = XLSX.utils.encode_cell({ r: 0, c: column });
    if (sheet[address]) sheet[address].s = HEADER_STYLE;
  }
  sheet['!cols'] = [
    { wch: 30 }, { wch: 14 }, { wch: 16 }, { wch: 30 }, { wch: 14 }, { wch: 12 },
    { wch: 22 }, { wch: 14 }, { wch: 24 }, { wch: 22 }, { wch: 26 }, { wch: 24 }, { wch: 20 }
  ];
  if (rows.length) {
    sheet['!autofilter'] = {
      ref: XLSX.utils.encode_range({ r: 0, c: 0 }, { r: rows.length, c: HEADERS.length - 1 })
    };
  }
  return sheet;
}

function readMeSheet(generatedOn) {
  const lines = [
    ['Banning Community Health Clinic 2026 — follow-up lists', READ_ME_TITLE_STYLE],
    [''],
    [`Generated on ${generatedOn} from the production database (read-only). Event: Banning, 2026-08-08 and 2026-08-09.`],
    ['All days and times are event-local (America/Los_Angeles).'],
    [''],
    ['The two situations listed', READ_ME_HEADING_STYLE],
    [`1. '${SITUATION_MISSING_CHECKOUT}': the patient checked in at Dental, Vision or Medical Checks, but no check-out`],
    ['   was recorded for that visit, so the outcome of the service is unknown. A check-out whose Service status is'],
    [`   '${NA_STATUS}' counts as missing too: that status is an administrative placeholder added after the`],
    ['   event to close open visits, not something a volunteer recorded on site.'],
    [`2. '${SITUATION_ENTRY_ONLY}': the patient checked in at the entrance but has no scan at any`],
    ['   service stand (Dental, Vision, Medical Checks, Haircuts, Resource Table, Portraits). Food Distribution and'],
    ['   the entrance itself do not count as clinical services.'],
    [''],
    ['How each person was assigned to a sheet', READ_ME_HEADING_STYLE],
    [`For '${SITUATION_MISSING_CHECKOUT}' rows, the sheet is the stand where the check-out is missing:`],
    ['   Dental -> Dental appointments, Vision -> Vision appointments, Medical Checks -> Medical check-up.'],
    [`For '${SITUATION_ENTRY_ONLY}' rows, the most specific signal wins, in order:`],
    ['   1. A booked dental appointment puts them on Dental appointments; a booked vision appointment puts them on'],
    ["      Vision appointments. Someone with both appears on both sheets, flagged in 'Also on other list'."],
    ["   2. Otherwise, the priority service they chose when registering (dental or vision) decides the sheet."],
    ["   3. Otherwise, their day-of survey answer decides: 'Dental' and/or 'Vision' put them on those sheets, and"],
    ["      'Medical' (or registration interest in 'General health screening / clinical service') on Medical check-up."],
    ['   4. People who never indicated what service they wanted go on Unknown.'],
    [''],
    ['Reading the columns', READ_ME_HEADING_STYLE],
    [`'Day attended' and 'Check-in time' refer to the service check-in for '${SITUATION_MISSING_CHECKOUT}' rows and to`],
    [`   the entrance check-in for '${SITUATION_ENTRY_ONLY}' rows (one day/time per day attended).`],
    ["'Booked appointment' lists every booked dental/vision appointment slot the person had."],
    ["'Priority service answer' is the service they marked as their priority when registering."],
    ["'Survey services wanted' lists what they answered to 'What service did you come in today to receive?'."],
    ["'Also on other list' flags people who appear on more than one sheet, so they are contacted only once."],
    ["'Notes' is left empty for the outreach team."],
    [''],
    ['Each sheet is sorted by day attended, then by last name. The same rows are also delivered as one CSV file'],
    ['per sheet (UTF-8) for mail-merge tools.']
  ];
  const sheet = XLSX.utils.aoa_to_sheet(lines.map(([text]) => [text]));
  for (let row = 0; row < lines.length; row += 1) {
    const style = lines[row][1];
    const address = XLSX.utils.encode_cell({ r: row, c: 0 });
    if (style && sheet[address]) sheet[address].s = style;
  }
  sheet['!cols'] = [{ wch: 118 }];
  return sheet;
}

/** Bulk per-user context: booked appointments, priority answers, survey answers. */
async function loadUserContext(connection, userIds) {
  const context = new Map();
  for (const userId of userIds) {
    context.set(Number(userId), {
      user: null, appointments: [], priorities: [], surveyServices: [], wantsScreening: false
    });
  }
  if (!userIds.length) return context;

  const [users] = await connection.query(
    `SELECT id, firstname, lastname, email, phone, date_of_birth
       FROM user WHERE id IN (?) ORDER BY id`, [userIds]
  );
  for (const user of users) context.get(Number(user.id)).user = user;

  const [appointments] = await connection.query(
    `SELECT r.user_id, sl.service_key, sl.slot_date,
            TIME_FORMAT(sl.start_time, '%H:%i') AS start_time
       FROM health_event_appointment a
       INNER JOIN health_event_slot sl ON sl.id = a.slot_id
       INNER JOIN health_event_registration r ON r.id = a.registration_id
      WHERE r.health_event_id = ? AND r.user_id IN (?) AND a.status = 'booked'
      ORDER BY r.user_id, sl.slot_date, sl.start_time`, [EVENT_ID, userIds]
  );
  for (const appointment of appointments) {
    context.get(Number(appointment.user_id)).appointments.push(appointment);
  }

  const [priorities] = await connection.query(
    `SELECT r.user_id, d.priority_service
       FROM health_event_registration_date d
       INNER JOIN health_event_registration r ON r.id = d.registration_id
      WHERE r.health_event_id = ? AND r.user_id IN (?) AND d.priority_service IS NOT NULL
      ORDER BY r.user_id, d.event_date`, [EVENT_ID, userIds]
  );
  for (const priority of priorities) {
    const entry = context.get(Number(priority.user_id));
    if (!entry.priorities.includes(priority.priority_service)) {
      entry.priorities.push(priority.priority_service);
    }
  }

  const [surveyAnswers] = await connection.query(
    `SELECT r.user_id, qo.name_en
       FROM health_event_answer an
       INNER JOIN health_event_registration r ON r.id = an.registration_id
       INNER JOIN health_event_answer_option ao ON ao.answer_id = an.id
       INNER JOIN health_event_question_option qo ON qo.id = ao.option_id
      WHERE r.health_event_id = ? AND an.question_id = ? AND r.user_id IN (?)
      ORDER BY r.user_id, qo.id`, [EVENT_ID, SURVEY_QUESTION_ID, userIds]
  );
  for (const answer of surveyAnswers) {
    context.get(Number(answer.user_id)).surveyServices.push(answer.name_en);
  }

  const [screeningAnswers] = await connection.query(
    `SELECT DISTINCT r.user_id
       FROM health_event_answer an
       INNER JOIN health_event_registration r ON r.id = an.registration_id
       INNER JOIN health_event_answer_option ao ON ao.answer_id = an.id
      WHERE r.health_event_id = ? AND an.question_id = ? AND ao.option_id = ?
        AND r.user_id IN (?)`, [EVENT_ID, INTEREST_QUESTION_ID, SCREENING_OPTION_ID, userIds]
  );
  for (const answer of screeningAnswers) {
    context.get(Number(answer.user_id)).wantsScreening = true;
  }
  return context;
}

function buildRow({ situation, sheet, userId, context, day, sortDay, time }) {
  const entry = context.get(Number(userId));
  const user = entry.user || {};
  return {
    sheet,
    situation,
    userId: Number(userId),
    sortDay,
    sortLastName: String(user.lastname || '').trim().toLowerCase(),
    sortFirstName: String(user.firstname || '').trim().toLowerCase(),
    values: {
      Situation: situation,
      'First name': user.firstname || '',
      'Last name': user.lastname || '',
      Email: user.email || '',
      Phone: user.phone || '',
      'Date of birth': dateOnly(user.date_of_birth),
      'Day attended': day,
      'Check-in time': time,
      'Booked appointment': entry.appointments
        .map(a => `${a.service_key} ${dateOnly(a.slot_date)} ${a.start_time}`).join(', '),
      'Priority service answer': entry.priorities.join(', '),
      'Survey services wanted': entry.surveyServices.join(', '),
      'Also on other list': '',
      Notes: ''
    }
  };
}

async function main() {
  const config = readProductionConfig();
  if (!String(config.host).includes('database-1.')) {
    throw new Error('Production safety check failed: unexpected database host');
  }

  const connection = await mysql.createConnection(config);
  try {
    // Belt and braces for a read-only script: any accidental write would fail.
    await connection.query('SET SESSION TRANSACTION READ ONLY');
    await connection.beginTransaction();

    const [[identity]] = await connection.query('SELECT DATABASE() AS db, @@hostname AS db_host');
    console.log(`[followup-lists] target=production database=${identity.db} host=${identity.db_host}`);

    const [[event]] = await connection.query(
      'SELECT id, slug, timezone, start_date, end_date FROM health_event WHERE id = ?', [EVENT_ID]
    );
    if (!event || event.slug !== EVENT_SLUG || event.timezone !== EVENT_TIMEZONE ||
        dateOnly(event.start_date) !== '2026-08-08' || dateOnly(event.end_date) !== '2026-08-09') {
      throw new Error('The expected production Banning event was not found');
    }

    // Resolve the stands from the database instead of hardcoding ids, and
    // assert the layout this export was written against.
    const [stands] = await connection.query(
      'SELECT id, name_en, is_entry, has_checkout FROM health_event_stand WHERE health_event_id = ? ORDER BY id',
      [EVENT_ID]
    );
    const entryStand = stands.find(stand => stand.is_entry === 'Y');
    const checkoutStands = stands.filter(stand => stand.has_checkout === 'Y');
    const foodStand = stands.find(stand => stand.name_en === 'Food Distribution');
    if (!entryStand || !foodStand || checkoutStands.length !== 3 ||
        checkoutStands.some(stand => !SHEET_BY_CHECKOUT_STAND[stand.name_en])) {
      throw new Error(`Unexpected stand layout: ${JSON.stringify(stands)}`);
    }
    // Every stand except the entrance and Food Distribution counts as a service.
    const serviceStandIds = stands
      .filter(stand => stand.id !== entryStand.id && stand.id !== foodStand.id)
      .map(stand => stand.id);
    const checkoutStandIds = checkoutStands.map(stand => stand.id);

    const [[optionCheck]] = await connection.query(
      `SELECT
         (SELECT name_en FROM health_event_question WHERE id = ?) AS survey_question,
         (SELECT name_en FROM health_event_question WHERE id = ?) AS interest_question,
         (SELECT name_en FROM health_event_question_option WHERE id = ?) AS screening_option`,
      [SURVEY_QUESTION_ID, INTEREST_QUESTION_ID, SCREENING_OPTION_ID]
    );
    if (!String(optionCheck.survey_question || '').startsWith('What service did you come in today to receive?') ||
        optionCheck.interest_question !== 'Which services are you interested in receiving?' ||
        optionCheck.screening_option !== 'General health screening / clinical service') {
      throw new Error(`Survey question/option assertion failed: ${JSON.stringify(optionCheck)}`);
    }

    // S1: check-ins at checkout-enabled stands, with their paired check-out and
    // its Service status (if any). NA-status check-outs are administrative
    // placeholders and count as missing, whether or not they exist yet.
    const [serviceCheckins] = await connection.query(
      `SELECT s.id AS scan_id, st.name_en AS stand_name, s.scanned_user_id AS user_id,
              DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%Y-%m-%d') AS day_local,
              DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%H:%i') AS time_local,
              co.id AS checkout_scan_id,
              (SELECT qo.name_en
                 FROM health_event_scan_answer sa
                 INNER JOIN health_event_question q ON q.id = sa.question_id
                   AND q.name_en = 'Service status'
                 INNER JOIN health_event_scan_answer_option sao ON sao.scan_answer_id = sa.id
                 INNER JOIN health_event_question_option qo ON qo.id = sao.option_id
                WHERE sa.scan_id = co.id LIMIT 1) AS checkout_status
         FROM health_event_scan s
         INNER JOIN health_event_stand st ON st.id = s.stand_id
         LEFT JOIN health_event_scan co ON co.paired_scan_id = s.id AND co.scan_type = 'checkout'
        WHERE s.health_event_id = ? AND s.scan_type = 'checkin' AND s.stand_id IN (?)
        ORDER BY s.scanned_at, s.id`,
      [EVENT_TIMEZONE, EVENT_TIMEZONE, EVENT_ID, checkoutStandIds]
    );
    const missingCheckouts = serviceCheckins.filter(scan =>
      scan.checkout_scan_id == null || scan.checkout_status === NA_STATUS
    );

    // S2: Entry check-ins of users with zero scans at any service stand.
    const [entryOnlyScans] = await connection.query(
      `SELECT s.scanned_user_id AS user_id,
              DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%Y-%m-%d') AS day_local,
              DATE_FORMAT(CONVERT_TZ(s.scanned_at, '+00:00', ?), '%H:%i') AS time_local
         FROM health_event_scan s
        WHERE s.health_event_id = ? AND s.stand_id = ? AND s.scan_type = 'checkin'
          AND NOT EXISTS (
            SELECT 1 FROM health_event_scan o
             WHERE o.health_event_id = s.health_event_id
               AND o.scanned_user_id = s.scanned_user_id AND o.stand_id IN (?))
        ORDER BY s.scanned_user_id, s.scanned_at, s.id`,
      [EVENT_TIMEZONE, EVENT_TIMEZONE, EVENT_ID, entryStand.id, serviceStandIds]
    );
    const entryOnlyByUser = new Map();
    for (const scan of entryOnlyScans) {
      const userId = Number(scan.user_id);
      if (!entryOnlyByUser.has(userId)) entryOnlyByUser.set(userId, new Map());
      const days = entryOnlyByUser.get(userId);
      // Keep the first (earliest) entrance check-in of each event-local day.
      if (!days.has(scan.day_local)) days.set(scan.day_local, scan.time_local);
    }

    const userIds = [...new Set([
      ...missingCheckouts.map(scan => Number(scan.user_id)),
      ...entryOnlyByUser.keys()
    ])].sort((a, b) => a - b);
    const context = await loadUserContext(connection, userIds);
    const missingUser = userIds.find(userId => !context.get(userId).user);
    if (missingUser != null) throw new Error(`User #${missingUser} not found`);

    const rows = [];
    for (const scan of missingCheckouts) {
      rows.push(buildRow({
        situation: SITUATION_MISSING_CHECKOUT,
        sheet: SHEET_BY_CHECKOUT_STAND[scan.stand_name],
        userId: scan.user_id,
        context,
        day: scan.day_local,
        sortDay: scan.day_local,
        time: scan.time_local
      }));
    }
    for (const [userId, days] of entryOnlyByUser) {
      const entry = context.get(userId);
      // Most specific signal wins: a booked appointment, else the single-choice
      // priority answer, else the day-of survey. 'Unknown' is reserved for
      // people who never indicated what service they wanted.
      const hasDental = entry.appointments.some(a => a.service_key === 'dental');
      const hasVision = entry.appointments.some(a => a.service_key === 'vision');
      const sheets = [];
      if (hasDental) sheets.push(SHEET_DENTAL);
      if (hasVision) sheets.push(SHEET_VISION);
      if (!sheets.length) {
        if (entry.priorities.includes('dental')) sheets.push(SHEET_DENTAL);
        if (entry.priorities.includes('vision')) sheets.push(SHEET_VISION);
      }
      if (!sheets.length) {
        if (entry.surveyServices.includes('Dental')) sheets.push(SHEET_DENTAL);
        if (entry.surveyServices.includes('Vision')) sheets.push(SHEET_VISION);
        if (entry.surveyServices.includes('Medical') || entry.wantsScreening) sheets.push(SHEET_MEDICAL);
      }
      if (!sheets.length) sheets.push(SHEET_UNKNOWN);
      const dayList = [...days.keys()].sort();
      for (const sheet of sheets) {
        rows.push(buildRow({
          situation: SITUATION_ENTRY_ONLY,
          sheet,
          userId,
          context,
          day: dayList.join(', '),
          sortDay: dayList[0],
          time: dayList.map(day => days.get(day)).join(', ')
        }));
      }
    }

    // Flag people who appear on more than one sheet so they are contacted once.
    const sheetsByUser = new Map();
    for (const row of rows) {
      if (!sheetsByUser.has(row.userId)) sheetsByUser.set(row.userId, new Set());
      sheetsByUser.get(row.userId).add(row.sheet);
    }
    for (const row of rows) {
      const others = [...sheetsByUser.get(row.userId)].filter(sheet => sheet !== row.sheet);
      row.values['Also on other list'] = others.map(sheet => `also ${sheet}`).join(', ');
    }

    const rowsBySheet = new Map(CATEGORY_SHEETS.map(sheet => [sheet, []]));
    for (const row of rows) rowsBySheet.get(row.sheet).push(row);
    for (const sheetRows of rowsBySheet.values()) {
      sheetRows.sort((a, b) =>
        a.sortDay.localeCompare(b.sortDay) ||
        a.sortLastName.localeCompare(b.sortLastName) ||
        a.sortFirstName.localeCompare(b.sortFirstName));
    }

    // Local calendar date (toISOString would already be tomorrow in the evening).
    const now = new Date();
    const generatedOn = new Date(now.getTime() - now.getTimezoneOffset() * 60_000)
      .toISOString().slice(0, 10);
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });

    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, readMeSheet(generatedOn), 'Read me');
    for (const sheet of CATEGORY_SHEETS) {
      XLSX.utils.book_append_sheet(workbook, categorySheet(rowsBySheet.get(sheet)), sheet);
    }
    const workbookPath = path.join(OUTPUT_DIR, 'banning-followup-lists.xlsx');
    fs.writeFileSync(workbookPath, XLSX.write(workbook, { bookType: 'xlsx', type: 'buffer' }));

    const csvPaths = [];
    for (const sheet of CATEGORY_SHEETS) {
      const csvPath = path.join(OUTPUT_DIR, CSV_FILE_BY_SHEET[sheet]);
      fs.writeFileSync(csvPath, rowsToCsv(rowsBySheet.get(sheet)));
      csvPaths.push(csvPath);
    }

    console.log(`[followup-lists] generated=${generatedOn} rows=${rows.length} people=${userIds.length}`);
    for (const sheet of CATEGORY_SHEETS) {
      const sheetRows = rowsBySheet.get(sheet);
      const missing = sheetRows.filter(row => row.situation === SITUATION_MISSING_CHECKOUT).length;
      const entryOnly = sheetRows.filter(row => row.situation === SITUATION_ENTRY_ONLY).length;
      console.log(`[followup-lists] ${sheet}: ${sheetRows.length} rows ` +
        `('${SITUATION_MISSING_CHECKOUT}'=${missing}, '${SITUATION_ENTRY_ONLY}'=${entryOnly})`);
    }
    console.log(`[followup-lists] workbook=${workbookPath}`);
    for (const csvPath of csvPaths) console.log(`[followup-lists] csv=${csvPath}`);

    await connection.rollback();
  } catch (error) {
    try { await connection.rollback(); } catch (_) { /* no-op */ }
    throw error;
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error('[followup-lists] FAILED:', error.stack || error.message || error);
  process.exit(1);
});
