/**
 * Health event analytics & raw-data exports.
 *
 * The admin "Metrics & exports" tab needs the same numbers on screen and in the
 * downloaded files, sliced by day / time / stand / service / volunteer. To keep
 * those two views from ever disagreeing, this module loads the event ONCE into
 * memory (a single event's scans are bounded by physical throughput — a two-day
 * clinic produces ~1.3k rows), applies the filters in JS, and derives both the
 * dashboard aggregates and every export table from that one snapshot.
 *
 * Timezone: the server runs in UTC while an event lives in its own timezone, so
 * grouping by DATE(scanned_at) splits an event day in the middle of the
 * afternoon. Every timestamp is converted to the event's wall clock in SQL and
 * carried around as a plain 'YYYY-MM-DD HH:MM:SS' string.
 */

const mysqlConnection = require('../connection/connection');
const XLSX = require('xlsx-js-style');

const DEFAULT_TIMEZONE = 'America/Los_Angeles';

/** Export datasets offered by the tab (order = sheet order in the workbook). */
const DATASETS = [
  'summary',
  'stand_summary',
  'attendance',
  'participants',
  'participant_services',
  'visits',
  'scan_log',
  'volunteers',
  'volunteer_scans',
  'stand_hourly',
  'checkout_answers'
];

const DATASET_SET = new Set(DATASETS);

// Sheet names must be <= 31 chars and free of []:*?/\ — keep them short.
const SHEET_NAMES = {
  summary: 'Summary',
  stand_summary: 'By stand',
  attendance: 'Attendance by day',
  participants: 'Participants',
  participant_services: 'Participant x service',
  visits: 'Visits',
  scan_log: 'Scan log',
  volunteers: 'Volunteers',
  volunteer_scans: 'Volunteer x service',
  stand_hourly: 'Hourly',
  checkout_answers: 'Checkout answers'
};

// ---------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------

/**
 * Event-local wall clock as a plain string. COALESCE keeps dev databases that
 * never loaded the MySQL timezone tables working (CONVERT_TZ returns NULL
 * there), falling back to the stored UTC value.
 */
function localExpr(column) {
  return `DATE_FORMAT(COALESCE(CONVERT_TZ(${column}, @@session.time_zone, ?), ${column}), '%Y-%m-%d %H:%i:%s')`;
}

function toDateOnly(value) {
  if (!value) return '';
  if (typeof value === 'string') return value.slice(0, 10);
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

function parseIdList(raw) {
  if (raw == null || raw === '') return [];
  return String(raw)
    .split(',')
    .map(part => part.trim())
    .filter(part => part !== '')
    .map(part => (part === 'none' ? 'none' : Number.parseInt(part, 10)))
    .filter(part => part === 'none' || Number.isInteger(part));
}

function parseTime(raw) {
  const match = /^([01]?\d|2[0-3]):([0-5]\d)$/.exec(String(raw || '').trim());
  return match ? `${match[1].padStart(2, '0')}:${match[2]}` : null;
}

function parseDate(raw) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(raw || '').trim());
  return match ? match[0] : null;
}

/** Query string → normalized filter object (also echoed back to the client). */
function parseAnalyticsFilters(query = {}) {
  const scanType = query.scan_type === 'checkin' || query.scan_type === 'checkout' ? query.scan_type : null;
  return {
    from: parseDate(query.from),
    to: parseDate(query.to),
    time_from: parseTime(query.time_from),
    time_to: parseTime(query.time_to),
    stand_ids: parseIdList(query.stand_ids).filter(id => id !== 'none'),
    service_ids: parseIdList(query.service_ids),
    volunteer_ids: parseIdList(query.volunteer_ids).filter(id => id !== 'none'),
    scan_type: scanType,
    sources: String(query.sources || '').split(',').map(s => s.trim()).filter(Boolean),
    search: String(query.search || '').trim(),
    only_attendees: query.only_attendees === '1' || query.only_attendees === 'true'
  };
}

function matchesSearch(scan, needle) {
  return (
    (scan.person_name || '').toLowerCase().includes(needle) ||
    (scan.person_email || '').toLowerCase().includes(needle) ||
    (scan.person_phone || '').includes(needle) ||
    (scan.volunteer_name || '').toLowerCase().includes(needle)
  );
}

function filterScans(scans, filters) {
  const standSet = new Set(filters.stand_ids);
  const volunteerSet = new Set(filters.volunteer_ids);
  const sourceSet = new Set(filters.sources);
  // 'none' selects scans recorded without a service (every stand except the
  // Resource Table works that way), so it cannot be a plain id comparison.
  const serviceSet = new Set(filters.service_ids.filter(id => id !== 'none'));
  const allowNoService = filters.service_ids.includes('none');
  const needle = filters.search.toLowerCase();

  return scans.filter(scan => {
    if (filters.from && scan.day < filters.from) return false;
    if (filters.to && scan.day > filters.to) return false;
    if (filters.time_from && scan.time < filters.time_from) return false;
    if (filters.time_to && scan.time > filters.time_to) return false;
    if (standSet.size && !standSet.has(scan.stand_id)) return false;
    if (serviceSet.size || allowNoService) {
      const ok = scan.service_id == null ? allowNoService : serviceSet.has(scan.service_id);
      if (!ok) return false;
    }
    if (volunteerSet.size && !volunteerSet.has(scan.volunteer_user_id)) return false;
    if (filters.scan_type && scan.scan_type !== filters.scan_type) return false;
    if (sourceSet.size && !sourceSet.has(scan.source || '')) return false;
    if (needle && !matchesSearch(scan, needle)) return false;
    return true;
  });
}

// ---------------------------------------------------------------------------
// Snapshot loading
// ---------------------------------------------------------------------------

function fullName(firstname, lastname) {
  return `${firstname || ''} ${lastname || ''}`.replace(/\s+/g, ' ').trim();
}

async function loadEvent(eventId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT he.id, he.slug, he.name_en, he.name_es, he.start_date, he.end_date, he.timezone,
            l.community_city AS location_name, c.name AS client_name
     FROM health_event he
     LEFT JOIN location l ON l.id = he.location_id
     LEFT JOIN client c ON c.id = he.client_id
     WHERE he.id = ? LIMIT 1`, [eventId]);
  return rows.length ? rows[0] : null;
}

async function loadScans(eventId, timezone) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT s.id AS scan_id, s.scan_type, s.paired_scan_id,
            ${localExpr('s.scanned_at')} AS scanned_at_local,
            DATE_FORMAT(s.scanned_at, '%Y-%m-%d %H:%i:%s') AS scanned_at_utc,
            s.stand_id, st.name_en AS stand_en, st.name_es AS stand_es,
            st.is_entry, st.has_checkout, st.sort_order AS stand_order,
            s.service_id, ss.name_en AS service_en, ss.name_es AS service_es, ss.sort_order AS service_order,
            s.registration_id, r.source, r.submitted_at,
            s.scanned_user_id, bu.firstname, bu.lastname, bu.email, bu.phone,
            s.volunteer_user_id, vu.firstname AS volunteer_firstname, vu.lastname AS volunteer_lastname,
            vu.username AS volunteer_username, vu.email AS volunteer_email
     FROM health_event_scan s
     INNER JOIN health_event_stand st ON st.id = s.stand_id
     LEFT JOIN health_event_stand_service ss ON ss.id = s.service_id
     LEFT JOIN health_event_registration r ON r.id = s.registration_id
     INNER JOIN user bu ON bu.id = s.scanned_user_id
     LEFT JOIN user vu ON vu.id = s.volunteer_user_id
     WHERE s.health_event_id = ?
     ORDER BY s.scanned_at ASC, s.id ASC`, [timezone, eventId]);

  return rows.map(row => {
    const local = row.scanned_at_local || '';
    return {
      scan_id: Number(row.scan_id),
      scan_type: row.scan_type,
      paired_scan_id: row.paired_scan_id != null ? Number(row.paired_scan_id) : null,
      local,
      day: local.slice(0, 10),
      time: local.slice(11, 16),
      hour: Number.parseInt(local.slice(11, 13), 10) || 0,
      scanned_at_utc: row.scanned_at_utc,
      stand_id: row.stand_id,
      stand_en: row.stand_en,
      stand_es: row.stand_es,
      is_entry: row.is_entry === 'Y',
      has_checkout: row.has_checkout === 'Y',
      stand_order: row.stand_order,
      service_id: row.service_id,
      service_en: row.service_en,
      service_es: row.service_es,
      service_order: row.service_order,
      registration_id: row.registration_id,
      source: row.source,
      user_id: row.scanned_user_id,
      person_name: fullName(row.firstname, row.lastname),
      person_firstname: row.firstname || '',
      person_lastname: row.lastname || '',
      person_email: row.email || '',
      person_phone: row.phone || '',
      volunteer_user_id: row.volunteer_user_id,
      volunteer_name: fullName(row.volunteer_firstname, row.volunteer_lastname),
      volunteer_username: row.volunteer_username || '',
      volunteer_email: row.volunteer_email || ''
    };
  });
}

async function loadStands(eventId) {
  const [stands] = await mysqlConnection.promise().query(
    `SELECT id, name_en, name_es, is_entry, has_checkout, sort_order, enabled
     FROM health_event_stand WHERE health_event_id = ? ORDER BY sort_order, id`, [eventId]);
  const [services] = await mysqlConnection.promise().query(
    `SELECT ss.id, ss.stand_id, ss.name_en, ss.name_es, ss.sort_order, ss.enabled
     FROM health_event_stand_service ss
     INNER JOIN health_event_stand st ON st.id = ss.stand_id
     WHERE st.health_event_id = ? ORDER BY ss.stand_id, ss.sort_order, ss.id`, [eventId]);
  return { stands, services };
}

async function loadRegistrations(eventId, timezone, eventStartDate) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT r.id AS registration_id, r.registration_role, r.status, r.source, r.contact_email,
            ${localExpr('r.submitted_at')} AS submitted_at_local,
            u.id AS user_id, u.firstname, u.lastname, u.email, u.username, u.phone, u.zipcode,
            u.household_size, u.enabled AS user_enabled, u.language AS app_language,
            DATE_FORMAT(u.date_of_birth, '%Y-%m-%d') AS date_of_birth,
            TIMESTAMPDIFF(YEAR, u.date_of_birth, ?) AS age,
            g.name AS gender, eth.name AS ethnicity, u.other_ethnicity,
            eth2.name AS second_ethnicity, lang.name AS preferred_language, u.other_language,
            (SELECT GROUP_CONCAT(DATE_FORMAT(d.event_date, '%Y-%m-%d') ORDER BY d.event_date SEPARATOR ', ')
               FROM health_event_registration_date d WHERE d.registration_id = r.id) AS signed_up_days,
            (SELECT GROUP_CONCAT(DISTINCT d.priority_service ORDER BY d.priority_service SEPARATOR ', ')
               FROM health_event_registration_date d WHERE d.registration_id = r.id) AS priority_services,
            (SELECT GROUP_CONCAT(DISTINCT DATE_FORMAT(sl.slot_date, '%Y-%m-%d')
                    ORDER BY DATE_FORMAT(sl.slot_date, '%Y-%m-%d') SEPARATOR ', ')
               FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
               WHERE a.registration_id = r.id AND a.status = 'booked') AS booked_appointment_days,
            (SELECT GROUP_CONCAT(DISTINCT DATE_FORMAT(sl.slot_date, '%Y-%m-%d')
                    ORDER BY DATE_FORMAT(sl.slot_date, '%Y-%m-%d') SEPARATOR ', ')
               FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
               WHERE a.registration_id = r.id AND a.status = 'cancelled') AS cancelled_appointment_days,
            (SELECT GROUP_CONCAT(CONCAT(sl.service_key, ' ', DATE_FORMAT(sl.slot_date, '%Y-%m-%d'), ' ',
                    TIME_FORMAT(sl.start_time, '%H:%i'), ' [', a.status, ']')
                    ORDER BY sl.slot_date, sl.start_time, a.id SEPARATOR ' | ')
               FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
               WHERE a.registration_id = r.id) AS appointments
     FROM health_event_registration r
     INNER JOIN user u ON u.id = r.user_id
     LEFT JOIN gender g ON g.id = u.gender_id
     LEFT JOIN ethnicity eth ON eth.id = u.ethnicity_id
     LEFT JOIN ethnicity eth2 ON eth2.id = u.second_ethnicity_id
     LEFT JOIN language lang ON lang.id = u.language_id
     WHERE r.health_event_id = ?
     ORDER BY r.id ASC`, [timezone, eventStartDate, eventId]);
  return rows;
}

async function loadQuestions(eventId) {
  const [questions] = await mysqlConnection.promise().query(
    `SELECT q.id, q.name_en, q.name_es, q.question_type, q.sort_order, f.audience, f.section_order,
            f.title_en AS form_en, f.title_es AS form_es
     FROM health_event_question q
     INNER JOIN health_event_form f ON f.id = q.form_id
     WHERE f.health_event_id = ? AND f.audience IN ('beneficiary','volunteer') AND q.question_type <> 'notice'
     ORDER BY f.audience, f.section_order, q.sort_order, q.id`, [eventId]);
  return questions;
}

/**
 * Registration answers, with the selected options stitched in JS rather than by
 * GROUP_CONCAT: a long multi-select answer would silently hit
 * group_concat_max_len (1024 bytes by default) and lose values, and the two
 * correlated subqueries cost ~900ms on a 16k-answer event.
 */
async function loadRegistrationAnswers(eventId) {
  const [answers] = await mysqlConnection.promise().query(
    `SELECT a.id, a.registration_id, a.question_id, a.answer_text, a.answer_number, a.other_text,
            DATE_FORMAT(a.answer_date, '%Y-%m-%d') AS answer_date
     FROM health_event_answer a
     INNER JOIN health_event_registration r ON r.id = a.registration_id
     WHERE r.health_event_id = ?`, [eventId]);

  const [options] = await mysqlConnection.promise().query(
    `SELECT ao.answer_id, o.name_en, o.name_es
     FROM health_event_answer_option ao
     INNER JOIN health_event_question_option o ON o.id = ao.option_id
     INNER JOIN health_event_answer a ON a.id = ao.answer_id
     INNER JOIN health_event_registration r ON r.id = a.registration_id
     WHERE r.health_event_id = ?
     ORDER BY ao.answer_id, o.sort_order, o.id`, [eventId]);

  const optionsByAnswer = new Map();
  for (const option of options) {
    if (!optionsByAnswer.has(option.answer_id)) optionsByAnswer.set(option.answer_id, { en: [], es: [] });
    const bucket = optionsByAnswer.get(option.answer_id);
    bucket.en.push(option.name_en);
    bucket.es.push(option.name_es);
  }
  for (const answer of answers) {
    const bucket = optionsByAnswer.get(answer.id);
    answer.options_en = bucket ? bucket.en.join(' | ') : null;
    answer.options_es = bucket ? bucket.es.join(' | ') : null;
  }
  return answers;
}

async function loadCheckoutAnswers(eventId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT sa.id AS scan_answer_id, sa.scan_id, sa.question_id, sa.answer_text, sa.answer_number,
            q.name_en AS question_en, q.name_es AS question_es, q.question_type, q.sort_order, f.stand_id
     FROM health_event_scan_answer sa
     INNER JOIN health_event_scan s ON s.id = sa.scan_id
     INNER JOIN health_event_question q ON q.id = sa.question_id
     INNER JOIN health_event_form f ON f.id = q.form_id
     WHERE s.health_event_id = ?
     ORDER BY sa.scan_id, q.sort_order, sa.question_id`, [eventId]);

  const [options] = await mysqlConnection.promise().query(
    `SELECT sao.scan_answer_id, o.name_en, o.name_es
     FROM health_event_scan_answer_option sao
     INNER JOIN health_event_question_option o ON o.id = sao.option_id
     INNER JOIN health_event_scan_answer sa ON sa.id = sao.scan_answer_id
     INNER JOIN health_event_scan s ON s.id = sa.scan_id
     WHERE s.health_event_id = ?
     ORDER BY sao.scan_answer_id, o.sort_order, o.id`, [eventId]);

  const optionsByAnswer = new Map();
  for (const option of options) {
    if (!optionsByAnswer.has(option.scan_answer_id)) optionsByAnswer.set(option.scan_answer_id, { en: [], es: [] });
    const bucket = optionsByAnswer.get(option.scan_answer_id);
    bucket.en.push(option.name_en);
    bucket.es.push(option.name_es);
  }
  for (const row of rows) {
    const bucket = optionsByAnswer.get(row.scan_answer_id);
    row.options_en = bucket ? bucket.en.join(' | ') : null;
    row.options_es = bucket ? bucket.es.join(' | ') : null;
  }
  return rows;
}

/**
 * One snapshot of everything the tab needs, before any filter is applied.
 * `withAnswers` pulls the registration form answers (16k+ rows on a real
 * event) — only the Participants export needs them, so the dashboard and the
 * scan log skip that cost.
 */
async function loadSnapshot(eventId, { withAnswers = false } = {}) {
  const event = await loadEvent(eventId);
  if (!event) return null;
  const timezone = event.timezone || DEFAULT_TIMEZONE;
  const startDate = toDateOnly(event.start_date);

  const [scans, standsData, registrations, questions, checkoutAnswers, answers] = await Promise.all([
    loadScans(eventId, timezone),
    loadStands(eventId),
    loadRegistrations(eventId, timezone, startDate || null),
    loadQuestions(eventId),
    loadCheckoutAnswers(eventId),
    withAnswers ? loadRegistrationAnswers(eventId) : Promise.resolve([])
  ]);

  return {
    event: {
      id: event.id,
      slug: event.slug,
      name_en: event.name_en,
      name_es: event.name_es,
      timezone,
      start_date: startDate,
      end_date: toDateOnly(event.end_date),
      location_name: event.location_name || '',
      client_name: event.client_name || ''
    },
    scans,
    stands: standsData.stands,
    services: standsData.services,
    registrations,
    questions,
    answers,
    checkoutAnswers
  };
}

// ---------------------------------------------------------------------------
// Derived structures
// ---------------------------------------------------------------------------

function pickLang(row, field, lang) {
  const value = lang === 'es' ? row[`${field}_es`] : row[`${field}_en`];
  return value || row[`${field}_en`] || row[`${field}_es`] || '';
}

/** Stable ordered list of stand/service "columns" used by the wide exports. */
function buildColumnCatalog(snapshot, lang) {
  const servicesByStand = new Map();
  for (const service of snapshot.services) {
    if (!servicesByStand.has(service.stand_id)) servicesByStand.set(service.stand_id, []);
    servicesByStand.get(service.stand_id).push(service);
  }
  const columns = [];
  for (const stand of snapshot.stands) {
    const standName = pickLang(stand, 'name', lang);
    columns.push({ key: `stand_${stand.id}`, stand_id: stand.id, service_id: null, label: standName });
    for (const service of servicesByStand.get(stand.id) || []) {
      columns.push({
        key: `svc_${service.id}`,
        stand_id: stand.id,
        service_id: service.id,
        label: `${standName} · ${pickLang(service, 'name', lang)}`
      });
    }
  }
  return columns;
}

/**
 * Pairs each check-in with its check-out (the scan rows point backwards through
 * paired_scan_id) and returns one "visit" per check-in: the unit the client
 * thinks in — this person, at this service, from this time to that time.
 */
function buildVisits(scans, checkoutAnswersByScan) {
  const checkoutByCheckin = new Map();
  for (const scan of scans) {
    if (scan.scan_type === 'checkout' && scan.paired_scan_id != null) {
      checkoutByCheckin.set(scan.paired_scan_id, scan);
    }
  }

  const visits = [];
  for (const scan of scans) {
    if (scan.scan_type !== 'checkin') continue;
    // Both ends come from the already-filtered set, so a visit is only "closed"
    // when its check-out also survived the filters (e.g. a time-of-day window
    // that cuts the afternoon leaves the last visits legitimately open).
    const paired = checkoutByCheckin.get(scan.scan_id) || null;
    let minutes = null;
    if (paired) {
      const start = Date.parse(`${scan.local.replace(' ', 'T')}Z`);
      const end = Date.parse(`${paired.local.replace(' ', 'T')}Z`);
      if (!Number.isNaN(start) && !Number.isNaN(end) && end >= start) {
        minutes = Math.round(((end - start) / 60000) * 10) / 10;
      }
    }
    visits.push({
      checkin: scan,
      checkout: paired,
      minutes,
      answers: paired ? (checkoutAnswersByScan.get(paired.scan_id) || []) : []
    });
  }
  return visits;
}

/** Whole minutes between two event-local 'YYYY-MM-DD HH:MM:SS' strings. */
function minutesBetween(fromLocal, toLocal) {
  const start = Date.parse(`${String(fromLocal).replace(' ', 'T')}Z`);
  const end = Date.parse(`${String(toLocal).replace(' ', 'T')}Z`);
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return 0;
  return Math.round((end - start) / 60000);
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  const value = sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  return Math.round(value * 10) / 10;
}

function increment(map, key, seed) {
  if (!map.has(key)) map.set(key, seed());
  return map.get(key);
}

/**
 * Answer row → display text. The precedence deliberately matches
 * answerToCsvText() in routes/healthEvents.js and answerDisplay() in the admin
 * UI, so the same question never reads differently across two exports.
 */
function answerValue(row, lang) {
  if (row.answer_text != null && row.answer_text !== '') return String(row.answer_text);
  if (row.answer_number != null) return String(row.answer_number);
  if (row.answer_date) return String(row.answer_date);
  const options = lang === 'es' ? (row.options_es || row.options_en) : (row.options_en || row.options_es);
  if (options) return row.other_text ? `${options} (${row.other_text})` : options;
  return row.other_text || '';
}

// ---------------------------------------------------------------------------
// Dashboard aggregates
// ---------------------------------------------------------------------------

function buildAnalytics(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const checkoutAnswersByScan = new Map();
  for (const row of snapshot.checkoutAnswers) {
    if (!checkoutAnswersByScan.has(row.scan_id)) checkoutAnswersByScan.set(row.scan_id, []);
    checkoutAnswersByScan.get(row.scan_id).push(row);
  }
  const visits = buildVisits(scans, checkoutAnswersByScan);

  const beneficiaryRegs = snapshot.registrations.filter(r => r.registration_role === 'beneficiary' && r.status === 'registered');
  const volunteerRegs = snapshot.registrations.filter(r => r.registration_role === 'volunteer' && r.status === 'registered');

  // ---- days present in the data (event-local) ----
  const dayKeys = Array.from(new Set(scans.map(scan => scan.day))).filter(Boolean).sort();

  // ---- KPIs ----
  const attendedUsers = new Set(scans.map(scan => scan.user_id));
  const scanningVolunteers = new Set(scans.map(scan => scan.volunteer_user_id).filter(id => id != null));
  const standsPerUser = new Map();
  for (const scan of scans) {
    increment(standsPerUser, scan.user_id, () => new Set()).add(scan.stand_id);
  }
  const dwellMinutes = visits.filter(v => v.minutes != null).map(v => v.minutes);
  const openVisits = visits.filter(v => !v.checkout && v.checkin.has_checkout).length;

  const kpis = {
    registered_beneficiaries: beneficiaryRegs.length,
    registered_volunteers: volunteerRegs.length,
    attended_beneficiaries: attendedUsers.size,
    // Registered people with no scan in the CURRENT selection — a true no-show
    // only when no stand/service/volunteer filter narrows the set, which is why
    // the UI labels it "registered without scans here".
    registered_without_scans: Math.max(
      beneficiaryRegs.length - beneficiaryRegs.filter(r => attendedUsers.has(r.user_id)).length, 0),
    walkins: beneficiaryRegs.filter(r => r.source === 'walkin').length,
    total_scans: scans.length,
    checkins: scans.filter(scan => scan.scan_type === 'checkin').length,
    checkouts: scans.filter(scan => scan.scan_type === 'checkout').length,
    scanning_volunteers: scanningVolunteers.size,
    // Entry check-ins are admissions, not services: counting them would inflate
    // "services delivered" by one per person per day.
    service_visits: visits.filter(visit => !visit.checkin.is_entry).length,
    avg_stands_per_attendee: attendedUsers.size
      ? Math.round((Array.from(standsPerUser.values()).reduce((sum, set) => sum + set.size, 0) / attendedUsers.size) * 100) / 100
      : 0,
    avg_service_minutes: dwellMinutes.length ? Math.round((dwellMinutes.reduce((a, b) => a + b, 0) / dwellMinutes.length) * 10) / 10 : null,
    median_service_minutes: median(dwellMinutes),
    open_visits: openVisits,
    days: dayKeys.length
  };

  // ---- stand × service × day × type ----
  const standServiceMap = new Map();
  for (const scan of scans) {
    const key = `${scan.stand_id}|${scan.service_id || 0}|${scan.day}|${scan.scan_type}`;
    const bucket = increment(standServiceMap, key, () => ({
      stand_id: scan.stand_id,
      stand_name: pickLang(scan, 'stand', lang),
      service_id: scan.service_id,
      service_name: scan.service_id ? pickLang(scan, 'service', lang) : '',
      day: scan.day,
      scan_type: scan.scan_type,
      total: 0,
      people: new Set(),
      volunteers: new Set(),
      stand_order: scan.stand_order,
      service_order: scan.service_order || 0
    }));
    bucket.total += 1;
    bucket.people.add(scan.user_id);
    if (scan.volunteer_user_id != null) bucket.volunteers.add(scan.volunteer_user_id);
  }
  const by_stand_service = Array.from(standServiceMap.values())
    .map(row => ({
      stand_id: row.stand_id,
      stand_name: row.stand_name,
      service_id: row.service_id,
      service_name: row.service_name,
      day: row.day,
      scan_type: row.scan_type,
      total: row.total,
      unique_people: row.people.size,
      volunteers: row.volunteers.size
    }))
    .sort((a, b) => a.stand_name.localeCompare(b.stand_name) || a.service_name.localeCompare(b.service_name) ||
      a.day.localeCompare(b.day) || a.scan_type.localeCompare(b.scan_type));

  // ---- stand rollup (one row per stand/service across all days) ----
  const standRollupMap = new Map();
  for (const scan of scans) {
    const key = `${scan.stand_id}|${scan.service_id || 0}`;
    const bucket = increment(standRollupMap, key, () => ({
      stand_id: scan.stand_id,
      stand_name: pickLang(scan, 'stand', lang),
      service_id: scan.service_id,
      service_name: scan.service_id ? pickLang(scan, 'service', lang) : '',
      stand_order: scan.stand_order,
      service_order: scan.service_order || 0,
      checkins: 0,
      checkouts: 0,
      people: new Set(),
      volunteers: new Set(),
      per_day: {}
    }));
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    bucket.people.add(scan.user_id);
    if (scan.volunteer_user_id != null) bucket.volunteers.add(scan.volunteer_user_id);
    if (!bucket.per_day[scan.day]) bucket.per_day[scan.day] = { checkins: 0, checkouts: 0, people: new Set() };
    if (scan.scan_type === 'checkin') bucket.per_day[scan.day].checkins += 1; else bucket.per_day[scan.day].checkouts += 1;
    bucket.per_day[scan.day].people.add(scan.user_id);
  }

  const dwellByKey = new Map();
  for (const visit of visits) {
    const key = `${visit.checkin.stand_id}|${visit.checkin.service_id || 0}`;
    const bucket = increment(dwellByKey, key, () => ({ minutes: [], open: 0 }));
    if (visit.minutes != null) bucket.minutes.push(visit.minutes);
    else if (visit.checkin.has_checkout) bucket.open += 1;
  }

  const by_stand = Array.from(standRollupMap.entries())
    .map(([key, row]) => {
      const dwell = dwellByKey.get(key) || { minutes: [], open: 0 };
      return {
        stand_id: row.stand_id,
        stand_name: row.stand_name,
        service_id: row.service_id,
        service_name: row.service_name,
        checkins: row.checkins,
        checkouts: row.checkouts,
        total: row.checkins + row.checkouts,
        unique_people: row.people.size,
        volunteers: row.volunteers.size,
        avg_minutes: dwell.minutes.length
          ? Math.round((dwell.minutes.reduce((a, b) => a + b, 0) / dwell.minutes.length) * 10) / 10 : null,
        median_minutes: median(dwell.minutes),
        open_visits: dwell.open,
        per_day: dayKeys.map(day => {
          const cell = row.per_day[day];
          return {
            day,
            checkins: cell ? cell.checkins : 0,
            checkouts: cell ? cell.checkouts : 0,
            unique_people: cell ? cell.people.size : 0
          };
        }),
        share_of_attendees: attendedUsers.size ? Math.round((row.people.size / attendedUsers.size) * 1000) / 10 : 0,
        stand_order: row.stand_order,
        service_order: row.service_order
      };
    })
    .sort((a, b) => (a.stand_order - b.stand_order) || (a.service_order - b.service_order) ||
      a.service_name.localeCompare(b.service_name));

  // ---- volunteers ----
  const volunteerMap = new Map();
  for (const scan of scans) {
    if (scan.volunteer_user_id == null) continue;
    const bucket = increment(volunteerMap, scan.volunteer_user_id, () => ({
      volunteer_user_id: scan.volunteer_user_id,
      name: scan.volunteer_name,
      username: scan.volunteer_username,
      email: scan.volunteer_email,
      checkins: 0,
      checkouts: 0,
      people: new Set(),
      stands: new Set(),
      services: new Set(),
      days: new Set(),
      first_scan: scan.local,
      last_scan: scan.local,
      per_day: {}
    }));
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    bucket.people.add(scan.user_id);
    bucket.stands.add(scan.stand_id);
    if (scan.service_id) bucket.services.add(scan.service_id);
    bucket.days.add(scan.day);
    if (scan.local < bucket.first_scan) bucket.first_scan = scan.local;
    if (scan.local > bucket.last_scan) bucket.last_scan = scan.local;
    if (!bucket.per_day[scan.day]) bucket.per_day[scan.day] = { total: 0, first: scan.local, last: scan.local };
    const dayBucket = bucket.per_day[scan.day];
    dayBucket.total += 1;
    if (scan.local < dayBucket.first) dayBucket.first = scan.local;
    if (scan.local > dayBucket.last) dayBucket.last = scan.local;
  }

  const by_volunteer = Array.from(volunteerMap.values())
    .map(row => ({
      volunteer_user_id: row.volunteer_user_id,
      name: row.name,
      username: row.username,
      email: row.email,
      total: row.checkins + row.checkouts,
      checkins: row.checkins,
      checkouts: row.checkouts,
      unique_people: row.people.size,
      stands: row.stands.size,
      services: row.services.size,
      days: row.days.size,
      first_scan: row.first_scan,
      last_scan: row.last_scan,
      // Summed per day, never first-to-last across the whole event: a volunteer
      // who worked both days would otherwise be credited with the night between
      // them. This is the honest answer to "how long was I at the table" — the
      // assignment sessions are not, since volunteers rarely close them.
      active_minutes: Object.values(row.per_day).reduce((sum, day) => sum + minutesBetween(day.first, day.last), 0),
      per_day: dayKeys.map(day => ({ day, total: row.per_day[day] ? row.per_day[day].total : 0 }))
    }))
    .sort((a, b) => b.total - a.total || a.name.localeCompare(b.name));

  // ---- volunteer × stand × service (drill-down) ----
  const volunteerStandMap = new Map();
  for (const scan of scans) {
    if (scan.volunteer_user_id == null) continue;
    const key = `${scan.volunteer_user_id}|${scan.stand_id}|${scan.service_id || 0}`;
    const bucket = increment(volunteerStandMap, key, () => ({
      volunteer_user_id: scan.volunteer_user_id,
      volunteer_name: scan.volunteer_name,
      stand_id: scan.stand_id,
      stand_name: pickLang(scan, 'stand', lang),
      service_id: scan.service_id,
      service_name: scan.service_id ? pickLang(scan, 'service', lang) : '',
      checkins: 0,
      checkouts: 0,
      people: new Set(),
      first_scan: scan.local,
      last_scan: scan.local
    }));
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    bucket.people.add(scan.user_id);
    if (scan.local < bucket.first_scan) bucket.first_scan = scan.local;
    if (scan.local > bucket.last_scan) bucket.last_scan = scan.local;
  }
  const by_volunteer_stand = Array.from(volunteerStandMap.values())
    .map(row => ({
      volunteer_user_id: row.volunteer_user_id,
      volunteer_name: row.volunteer_name,
      stand_id: row.stand_id,
      stand_name: row.stand_name,
      service_id: row.service_id,
      service_name: row.service_name,
      total: row.checkins + row.checkouts,
      checkins: row.checkins,
      checkouts: row.checkouts,
      unique_people: row.people.size,
      first_scan: row.first_scan,
      last_scan: row.last_scan
    }))
    .sort((a, b) => b.total - a.total);

  // ---- hourly flow ----
  const hourMap = new Map();
  for (const scan of scans) {
    const key = `${scan.day}|${scan.hour}`;
    const bucket = increment(hourMap, key, () => ({
      day: scan.day, hour: scan.hour, checkins: 0, checkouts: 0, people: new Set()
    }));
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    bucket.people.add(scan.user_id);
  }
  const by_hour = Array.from(hourMap.values())
    .map(row => ({
      day: row.day,
      hour: row.hour,
      label: `${String(row.hour).padStart(2, '0')}:00`,
      checkins: row.checkins,
      checkouts: row.checkouts,
      total: row.checkins + row.checkouts,
      unique_people: row.people.size
    }))
    .sort((a, b) => a.day.localeCompare(b.day) || a.hour - b.hour);

  // ---- stands visited per person (distribution) ----
  const distributionMap = new Map();
  for (const set of standsPerUser.values()) {
    distributionMap.set(set.size, (distributionMap.get(set.size) || 0) + 1);
  }
  const stands_per_person = Array.from(distributionMap.entries())
    .map(([stands_visited, people]) => ({ stands_visited, people }))
    .sort((a, b) => a.stands_visited - b.stands_visited);

  // ---- checkout answers (option/value breakdown per stand & question) ----
  const answerMap = new Map();
  const standNameById = new Map(snapshot.stands.map(stand => [stand.id, pickLang(stand, 'name', lang)]));
  for (const visit of visits) {
    for (const row of visit.answers) {
      if (row.question_type === 'text' || row.question_type === 'number') continue;
      const value = answerValue(row, lang) || '—';
      const key = `${row.stand_id || visit.checkin.stand_id}|${row.question_id}|${value}`;
      const bucket = increment(answerMap, key, () => ({
        stand_id: row.stand_id || visit.checkin.stand_id,
        stand_name: standNameById.get(row.stand_id || visit.checkin.stand_id) || '',
        question_id: row.question_id,
        question: lang === 'es' ? (row.question_es || row.question_en) : (row.question_en || row.question_es),
        value,
        total: 0,
        people: new Set()
      }));
      bucket.total += 1;
      bucket.people.add(visit.checkin.user_id);
    }
  }
  const checkout_answers = Array.from(answerMap.values())
    .map(row => ({
      stand_id: row.stand_id,
      stand_name: row.stand_name,
      question_id: row.question_id,
      question: row.question,
      value: row.value,
      total: row.total,
      unique_people: row.people.size
    }))
    .sort((a, b) => a.stand_name.localeCompare(b.stand_name) || a.question_id - b.question_id || b.total - a.total);

  // ---- per-day attendance vs sign-ups ----
  const signedUpByDay = new Map();
  const cancelledOnlyByDay = new Map();
  const parseDays = value => new Set(
    String(value || '').split(',').map(day => day.trim()).filter(Boolean)
  );
  for (const registration of beneficiaryRegs) {
    const signedUpDays = parseDays(registration.signed_up_days);
    const bookedDays = parseDays(registration.booked_appointment_days);
    const cancelledDays = parseDays(registration.cancelled_appointment_days);

    // Appointment dates are also sign-up dates. Usually registration_date has
    // the same rows, but using the appointments as a fallback keeps historical
    // imports and administrative corrections from disappearing from attendance.
    for (const day of new Set([...signedUpDays, ...bookedDays, ...cancelledDays])) {
      increment(signedUpByDay, day, () => new Set()).add(registration.user_id);
    }
    // A cancellation only excuses a no-show when the person has no remaining
    // booked appointment that day. Cancelling one of two appointments must not
    // hide a genuine no-show for the other one.
    for (const day of cancelledDays) {
      if (!bookedDays.has(day)) {
        increment(cancelledOnlyByDay, day, () => new Set()).add(registration.user_id);
      }
    }
  }
  const attendedByDay = new Map();
  for (const scan of scans) {
    increment(attendedByDay, scan.day, () => new Set()).add(scan.user_id);
  }
  const allDays = Array.from(new Set([
    ...signedUpByDay.keys(), ...attendedByDay.keys(), ...cancelledOnlyByDay.keys()
  ])).sort();
  const seenBefore = new Set();
  const attendance_by_day = allDays.map(day => {
    const signedUp = signedUpByDay.get(day) || new Set();
    const attended = attendedByDay.get(day) || new Set();
    const cancelledOnly = cancelledOnlyByDay.get(day) || new Set();
    let newPeople = 0;
    for (const userId of attended) {
      if (!seenBefore.has(userId)) { newPeople += 1; seenBefore.add(userId); }
    }
    let matched = 0;
    let cancelled = 0;
    let noShow = 0;
    for (const userId of signedUp) {
      if (attended.has(userId)) matched += 1;
      else if (cancelledOnly.has(userId)) cancelled += 1;
      else noShow += 1;
    }
    const expectedAfterCancellations = matched + noShow;
    return {
      day,
      signed_up: signedUp.size,
      attended: attended.size,
      first_time_that_day: newPeople,
      signed_up_and_attended: matched,
      walk_in_or_other_day: attended.size - matched,
      cancelled,
      no_show: noShow,
      show_rate: expectedAfterCancellations
        ? Math.round((matched / expectedAfterCancellations) * 1000) / 10
        : null
    };
  });

  // ---- filter option catalogues (always the full lists, never the filtered set) ----
  const options = {
    days: Array.from(new Set(snapshot.scans.map(scan => scan.day))).filter(Boolean).sort(),
    stands: snapshot.stands.map(stand => ({
      id: stand.id, name: pickLang(stand, 'name', lang), enabled: stand.enabled
    })),
    services: snapshot.services.map(service => ({
      id: service.id,
      stand_id: service.stand_id,
      name: pickLang(service, 'name', lang),
      enabled: service.enabled
    })),
    volunteers: Array.from(
      snapshot.scans.reduce((map, scan) => {
        if (scan.volunteer_user_id != null && !map.has(scan.volunteer_user_id)) {
          map.set(scan.volunteer_user_id, { id: scan.volunteer_user_id, name: scan.volunteer_name });
        }
        return map;
      }, new Map()).values()
    ).sort((a, b) => a.name.localeCompare(b.name)),
    sources: Array.from(new Set(snapshot.registrations.map(r => r.source).filter(Boolean))).sort()
  };

  return {
    event: snapshot.event,
    filters,
    filters_label: describeFilters(snapshot, filters, lang),
    days: dayKeys,
    kpis,
    by_stand,
    by_stand_service,
    by_volunteer,
    by_volunteer_stand,
    by_hour,
    stands_per_person,
    checkout_answers,
    attendance_by_day,
    options,
    generated_at: new Date().toISOString()
  };
}

// ---------------------------------------------------------------------------
// Paged raw scan log (on-screen table)
// ---------------------------------------------------------------------------

function buildScanLog(snapshot, filters, lang, page, pageSize) {
  const checkoutAnswersByScan = new Map();
  for (const row of snapshot.checkoutAnswers) {
    if (!checkoutAnswersByScan.has(row.scan_id)) checkoutAnswersByScan.set(row.scan_id, []);
    checkoutAnswersByScan.get(row.scan_id).push(row);
  }
  const scans = filterScans(snapshot.scans, filters);
  // Newest first: during and right after an event this is a live feed.
  const ordered = [...scans].reverse();
  const total = ordered.length;
  const start = (page - 1) * pageSize;
  const rows = ordered.slice(start, start + pageSize).map(scan => ({
    scan_id: scan.scan_id,
    scan_type: scan.scan_type,
    scanned_at: scan.local,
    day: scan.day,
    time: scan.time,
    stand_name: pickLang(scan, 'stand', lang),
    service_name: scan.service_id ? pickLang(scan, 'service', lang) : '',
    person_name: scan.person_name,
    person_phone: scan.person_phone,
    person_email: scan.person_email,
    user_id: scan.user_id,
    registration_id: scan.registration_id,
    source: scan.source || '',
    volunteer_name: scan.volunteer_name,
    volunteer_user_id: scan.volunteer_user_id,
    checkout_answers: (checkoutAnswersByScan.get(scan.scan_id) || []).map(row => ({
      question: lang === 'es' ? (row.question_es || row.question_en) : (row.question_en || row.question_es),
      value: answerValue(row, lang)
    }))
  }));
  return { total, page, pageSize, rows };
}

// ---------------------------------------------------------------------------
// Export tables
// ---------------------------------------------------------------------------

function yesNo(value, lang) {
  if (lang === 'es') return value ? 'Sí' : 'No';
  return value ? 'Yes' : 'No';
}

function label(lang, en, es) {
  return lang === 'es' ? es : en;
}

function buildSummaryTable(analytics, lang) {
  const k = analytics.kpis;
  const rows = [
    [label(lang, 'Event', 'Evento'), lang === 'es' ? (analytics.event.name_es || analytics.event.name_en) : analytics.event.name_en],
    [label(lang, 'Timezone', 'Zona horaria'), analytics.event.timezone],
    [label(lang, 'Filters applied', 'Filtros aplicados'), analytics.filters_label],
    [label(lang, 'Registered participants', 'Participantes registrados'), k.registered_beneficiaries],
    [label(lang, 'Registered volunteers', 'Voluntarios registrados'), k.registered_volunteers],
    [label(lang, 'Participants scanned at least once', 'Participantes con al menos un escaneo'), k.attended_beneficiaries],
    [label(lang, 'Registered with no scan in this selection', 'Registrados sin escaneos en esta selección'), k.registered_without_scans],
    [label(lang, 'Walk-ins registered on site', 'Walk-ins registrados en el lugar'), k.walkins],
    [label(lang, 'Total scans', 'Escaneos totales'), k.total_scans],
    [label(lang, 'Check-ins', 'Check-ins'), k.checkins],
    [label(lang, 'Check-outs', 'Check-outs'), k.checkouts],
    [label(lang, 'Service visits (entry desk excluded)', 'Visitas de servicio (sin la entrada)'), k.service_visits],
    [label(lang, 'Volunteers who scanned', 'Voluntarios que escanearon'), k.scanning_volunteers],
    [label(lang, 'Average stands per participant', 'Promedio de puestos por participante'), k.avg_stands_per_attendee],
    [label(lang, 'Average service minutes', 'Minutos promedio de atención'), k.avg_service_minutes],
    [label(lang, 'Median service minutes', 'Minutos medianos de atención'), k.median_service_minutes],
    [label(lang, 'Visits without check-out', 'Visitas sin check-out'), k.open_visits],
    [label(lang, 'Event days with activity', 'Días con actividad'), k.days],
    [label(lang, 'Generated at (UTC)', 'Generado el (UTC)'), analytics.generated_at]
  ];
  return {
    key: 'summary',
    title: label(lang, 'Summary', 'Resumen'),
    headers: [label(lang, 'Metric', 'Métrica'), label(lang, 'Value', 'Valor')],
    rows: rows.map(([name, value]) => [name, value == null ? '' : value])
  };
}

/** Human-readable filter recap, stamped on every export so files stay traceable. */
function describeFilters(snapshot, filters, lang) {
  const standNames = new Map(snapshot.stands.map(s => [s.id, pickLang(s, 'name', lang)]));
  const serviceNames = new Map(snapshot.services.map(s => [s.id, pickLang(s, 'name', lang)]));
  const volunteerNames = new Map();
  for (const scan of snapshot.scans) {
    if (scan.volunteer_user_id != null) volunteerNames.set(scan.volunteer_user_id, scan.volunteer_name);
  }
  const names = (ids, lookup) => ids
    .map(id => (id === 'none' ? label(lang, '(no service)', '(sin servicio)') : lookup.get(id) || `#${id}`))
    .join(', ');

  const parts = [];
  if (filters.from) parts.push(`${label(lang, 'from', 'desde')} ${filters.from}`);
  if (filters.to) parts.push(`${label(lang, 'to', 'hasta')} ${filters.to}`);
  if (filters.time_from) parts.push(`${label(lang, 'from', 'desde')} ${filters.time_from}`);
  if (filters.time_to) parts.push(`${label(lang, 'until', 'hasta')} ${filters.time_to}`);
  if (filters.stand_ids.length) parts.push(`${label(lang, 'stands', 'puestos')}: ${names(filters.stand_ids, standNames)}`);
  if (filters.service_ids.length) parts.push(`${label(lang, 'services', 'servicios')}: ${names(filters.service_ids, serviceNames)}`);
  if (filters.volunteer_ids.length) parts.push(`${label(lang, 'volunteers', 'voluntarios')}: ${names(filters.volunteer_ids, volunteerNames)}`);
  if (filters.scan_type) parts.push(`${label(lang, 'type', 'tipo')}: ${filters.scan_type}`);
  if (filters.sources.length) parts.push(`${label(lang, 'source', 'origen')}: ${filters.sources.join(', ')}`);
  if (filters.search) parts.push(`${label(lang, 'search', 'búsqueda')}: ${filters.search}`);
  if (filters.only_attendees) parts.push(label(lang, 'attendees only', 'solo quienes asistieron'));
  return parts.length ? parts.join(' · ') : label(lang, 'None (whole event)', 'Ninguno (evento completo)');
}

function buildStandSummaryTable(analytics, lang) {
  const headers = [
    label(lang, 'Stand', 'Puesto'), label(lang, 'Service / partner', 'Servicio / organización'),
    label(lang, 'Check-ins', 'Check-ins'), label(lang, 'Check-outs', 'Check-outs'),
    label(lang, 'Total scans', 'Escaneos totales'),
    label(lang, 'People served (unique)', 'Personas atendidas (únicas)'),
    label(lang, '% of all attendees', '% de todos los asistentes'),
    label(lang, 'Volunteers who scanned here', 'Voluntarios que escanearon acá'),
    label(lang, 'Average minutes', 'Minutos promedio'),
    label(lang, 'Median minutes', 'Minutos medianos'),
    label(lang, 'Visits without check-out', 'Visitas sin check-out'),
    ...analytics.days.flatMap(day => [
      `${day} · ${label(lang, 'check-ins', 'check-ins')}`,
      `${day} · ${label(lang, 'people', 'personas')}`
    ])
  ];
  const rows = analytics.by_stand.map(row => [
    row.stand_name, row.service_name, row.checkins, row.checkouts, row.total,
    row.unique_people, row.share_of_attendees, row.volunteers,
    row.avg_minutes == null ? '' : row.avg_minutes,
    row.median_minutes == null ? '' : row.median_minutes,
    row.open_visits,
    ...row.per_day.flatMap(cell => [cell.checkins, cell.unique_people])
  ]);
  return { key: 'stand_summary', title: label(lang, 'By stand', 'Por puesto'), headers, rows };
}

function buildAttendanceTable(analytics, lang) {
  const headers = [
    label(lang, 'Day', 'Día'),
    label(lang, 'Signed up for this day', 'Anotados para este día'),
    label(lang, 'Attended (scanned)', 'Asistieron (escaneados)'),
    label(lang, 'Signed up and attended', 'Anotados que asistieron'),
    label(lang, 'Show rate %', '% de asistencia'),
    label(lang, 'No-show', 'No asistieron'),
    label(lang, 'Cancelled', 'Cancelaron'),
    label(lang, 'Attended without signing up for this day', 'Asistieron sin anotarse para este día'),
    label(lang, 'First day on site', 'Primer día en el lugar')
  ];
  const rows = analytics.attendance_by_day.map(row => [
    row.day, row.signed_up, row.attended, row.signed_up_and_attended,
    row.show_rate == null ? '' : row.show_rate,
    row.no_show, row.cancelled, row.walk_in_or_other_day, row.first_time_that_day
  ]);
  return { key: 'attendance', title: label(lang, 'Attendance by day', 'Asistencia por día'), headers, rows };
}

function buildScanLogTable(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const answersByScan = new Map();
  for (const row of snapshot.checkoutAnswers) {
    if (!answersByScan.has(row.scan_id)) answersByScan.set(row.scan_id, []);
    answersByScan.get(row.scan_id).push(row);
  }
  const headers = [
    'Scan ID', label(lang, 'Type', 'Tipo'), label(lang, 'Day', 'Día'), label(lang, 'Time', 'Hora'),
    label(lang, 'Local date & time', 'Fecha y hora local'), label(lang, 'UTC date & time', 'Fecha y hora UTC'),
    label(lang, 'Stand', 'Puesto'), label(lang, 'Service / partner', 'Servicio / organización'),
    label(lang, 'Participant', 'Participante'), 'User ID', label(lang, 'Phone', 'Teléfono'),
    label(lang, 'Email', 'Correo'), label(lang, 'Registration source', 'Origen del registro'),
    label(lang, 'Scanned by (volunteer)', 'Escaneado por (voluntario)'), 'Volunteer ID',
    label(lang, 'Volunteer username', 'Usuario del voluntario'),
    label(lang, 'Paired scan ID', 'ID de escaneo emparejado'),
    label(lang, 'Checkout answers', 'Respuestas de checkout')
  ];
  const rows = scans.map(scan => [
    scan.scan_id,
    scan.scan_type,
    scan.day,
    scan.time,
    scan.local,
    scan.scanned_at_utc,
    pickLang(scan, 'stand', lang),
    scan.service_id ? pickLang(scan, 'service', lang) : '',
    scan.person_name,
    scan.user_id,
    scan.person_phone,
    scan.person_email,
    scan.source || '',
    scan.volunteer_name,
    scan.volunteer_user_id == null ? '' : scan.volunteer_user_id,
    scan.volunteer_username,
    scan.paired_scan_id == null ? '' : scan.paired_scan_id,
    (answersByScan.get(scan.scan_id) || [])
      .map(row => `${lang === 'es' ? (row.question_es || row.question_en) : (row.question_en || row.question_es)}: ${answerValue(row, lang)}`)
      .join(' | ')
  ]);
  return { key: 'scan_log', title: label(lang, 'Scan log', 'Registro de escaneos'), headers, rows };
}

function buildVisitsTable(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const answersByScan = new Map();
  for (const row of snapshot.checkoutAnswers) {
    if (!answersByScan.has(row.scan_id)) answersByScan.set(row.scan_id, []);
    answersByScan.get(row.scan_id).push(row);
  }
  const visits = buildVisits(scans, answersByScan);

  // One column per distinct checkout question so the sheet stays pivotable.
  const questionColumns = [];
  const seenQuestions = new Set();
  for (const row of snapshot.checkoutAnswers) {
    if (seenQuestions.has(row.question_id)) continue;
    seenQuestions.add(row.question_id);
    questionColumns.push({
      id: row.question_id,
      stand_id: row.stand_id,
      title: lang === 'es' ? (row.question_es || row.question_en) : (row.question_en || row.question_es)
    });
  }

  const headers = [
    label(lang, 'Check-in scan ID', 'ID de escaneo (check-in)'),
    label(lang, 'Day', 'Día'), label(lang, 'Stand', 'Puesto'),
    label(lang, 'Service / partner', 'Servicio / organización'),
    label(lang, 'Participant', 'Participante'), 'User ID',
    label(lang, 'Phone', 'Teléfono'),
    label(lang, 'Check-in time', 'Hora de check-in'),
    label(lang, 'Check-out time', 'Hora de check-out'),
    label(lang, 'Minutes at the stand', 'Minutos en el puesto'),
    label(lang, 'Closed with check-out', 'Cerrado con check-out'),
    label(lang, 'Checked in by', 'Check-in hecho por'),
    label(lang, 'Checked out by', 'Check-out hecho por'),
    ...questionColumns.map(q => q.title)
  ];

  const rows = visits.map(visit => {
    const answers = new Map(visit.answers.map(row => [row.question_id, answerValue(row, lang)]));
    return [
      visit.checkin.scan_id,
      visit.checkin.day,
      pickLang(visit.checkin, 'stand', lang),
      visit.checkin.service_id ? pickLang(visit.checkin, 'service', lang) : '',
      visit.checkin.person_name,
      visit.checkin.user_id,
      visit.checkin.person_phone,
      visit.checkin.time,
      visit.checkout ? visit.checkout.time : '',
      visit.minutes == null ? '' : visit.minutes,
      yesNo(!!visit.checkout, lang),
      visit.checkin.volunteer_name,
      visit.checkout ? visit.checkout.volunteer_name : '',
      ...questionColumns.map(q => answers.get(q.id) || '')
    ];
  });
  return { key: 'visits', title: label(lang, 'Visits', 'Visitas'), headers, rows };
}

function buildParticipantsTable(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const columns = buildColumnCatalog(snapshot, lang);
  const questions = snapshot.questions.filter(q => q.audience === 'beneficiary');

  const answersByRegistration = new Map();
  for (const answer of snapshot.answers) {
    if (!answersByRegistration.has(answer.registration_id)) answersByRegistration.set(answer.registration_id, new Map());
    answersByRegistration.get(answer.registration_id).set(answer.question_id, answer);
  }

  // scan aggregates per user
  const perUser = new Map();
  for (const scan of scans) {
    const bucket = increment(perUser, scan.user_id, () => ({
      total: 0, checkins: 0, checkouts: 0, stands: new Set(), days: new Set(),
      first: scan.local, last: scan.local, cells: new Map(), volunteers: new Set()
    }));
    bucket.total += 1;
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    bucket.stands.add(scan.stand_id);
    bucket.days.add(scan.day);
    if (scan.local < bucket.first) bucket.first = scan.local;
    if (scan.local > bucket.last) bucket.last = scan.local;
    if (scan.volunteer_name) bucket.volunteers.add(scan.volunteer_name);
    if (scan.scan_type === 'checkin') {
      const standKey = `stand_${scan.stand_id}`;
      bucket.cells.set(standKey, (bucket.cells.get(standKey) || 0) + 1);
      if (scan.service_id) {
        const serviceKey = `svc_${scan.service_id}`;
        bucket.cells.set(serviceKey, (bucket.cells.get(serviceKey) || 0) + 1);
      }
    }
  }

  const headers = [
    'Registration ID', 'User ID',
    label(lang, 'First name', 'Nombre'), label(lang, 'Last name', 'Apellido'),
    label(lang, 'Email', 'Correo'), label(lang, 'Phone', 'Teléfono'),
    label(lang, 'Username', 'Usuario'),
    label(lang, 'Date of birth', 'Fecha de nacimiento'), label(lang, 'Age at the event', 'Edad en el evento'),
    label(lang, 'Zipcode', 'Código postal'), label(lang, 'Household size', 'Tamaño del hogar'),
    label(lang, 'Gender', 'Género'), label(lang, 'Ethnicity', 'Etnia'),
    label(lang, 'Other ethnicity', 'Otra etnia'), label(lang, 'Second ethnicity', 'Segunda etnia'),
    label(lang, 'Preferred language', 'Idioma preferido'), label(lang, 'Other language', 'Otro idioma'),
    label(lang, 'Registration source', 'Origen del registro'), label(lang, 'Registered at', 'Registrado el'),
    label(lang, 'Signed up for days', 'Días para los que se anotó'),
    label(lang, 'Priority services', 'Servicios prioritarios'),
    label(lang, 'Appointments', 'Turnos'),
    label(lang, 'Attended', 'Asistió'),
    label(lang, 'Total scans', 'Escaneos totales'),
    label(lang, 'Check-ins', 'Check-ins'), label(lang, 'Check-outs', 'Check-outs'),
    label(lang, 'Stands visited', 'Puestos visitados'),
    label(lang, 'Days attended', 'Días asistidos'),
    label(lang, 'First scan', 'Primer escaneo'), label(lang, 'Last scan', 'Último escaneo'),
    label(lang, 'Minutes on site (first → last scan)', 'Minutos en el lugar (primer → último escaneo)'),
    label(lang, 'Volunteers who scanned them', 'Voluntarios que lo escanearon'),
    ...columns.map(column => `${label(lang, 'Check-ins', 'Check-ins')} · ${column.label}`),
    ...questions.map(question => (lang === 'es' ? (question.name_es || question.name_en) : (question.name_en || question.name_es)))
  ];

  const rows = [];
  for (const registration of snapshot.registrations) {
    if (registration.registration_role !== 'beneficiary') continue;
    const stats = perUser.get(registration.user_id) || null;
    if (filters.only_attendees && !stats) continue;
    const registrationAnswers = answersByRegistration.get(registration.registration_id);
    const onSiteMinutes = stats ? minutesBetween(stats.first, stats.last) : '';
    rows.push([
      registration.registration_id,
      registration.user_id,
      registration.firstname || '',
      registration.lastname || '',
      registration.email || registration.contact_email || '',
      registration.phone || '',
      registration.username || '',
      registration.date_of_birth || '',
      registration.age == null ? '' : registration.age,
      registration.zipcode || '',
      registration.household_size == null ? '' : registration.household_size,
      registration.gender || '',
      registration.ethnicity || '',
      registration.other_ethnicity || '',
      registration.second_ethnicity || '',
      registration.preferred_language || '',
      registration.other_language || '',
      registration.source || '',
      registration.submitted_at_local || '',
      registration.signed_up_days || '',
      registration.priority_services || '',
      registration.appointments || '',
      yesNo(!!stats, lang),
      stats ? stats.total : 0,
      stats ? stats.checkins : 0,
      stats ? stats.checkouts : 0,
      stats ? stats.stands.size : 0,
      stats ? stats.days.size : 0,
      stats ? stats.first : '',
      stats ? stats.last : '',
      onSiteMinutes,
      stats ? Array.from(stats.volunteers).join(' | ') : '',
      ...columns.map(column => (stats ? (stats.cells.get(column.key) || 0) : 0)),
      ...questions.map(question => {
        const answer = registrationAnswers ? registrationAnswers.get(question.id) : null;
        return answer ? answerValue(answer, lang) : '';
      })
    ]);
  }
  return { key: 'participants', title: label(lang, 'Participants', 'Participantes'), headers, rows };
}

function buildParticipantServicesTable(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const map = new Map();
  for (const scan of scans) {
    const key = `${scan.user_id}|${scan.stand_id}|${scan.service_id || 0}`;
    const bucket = increment(map, key, () => ({
      user_id: scan.user_id,
      name: scan.person_name,
      phone: scan.person_phone,
      stand: pickLang(scan, 'stand', lang),
      service: scan.service_id ? pickLang(scan, 'service', lang) : '',
      days: new Set(),
      checkins: 0,
      checkouts: 0,
      first: scan.local,
      last: scan.local,
      volunteers: new Set()
    }));
    bucket.days.add(scan.day);
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    if (scan.local < bucket.first) bucket.first = scan.local;
    if (scan.local > bucket.last) bucket.last = scan.local;
    if (scan.volunteer_name) bucket.volunteers.add(scan.volunteer_name);
  }
  const headers = [
    'User ID', label(lang, 'Participant', 'Participante'), label(lang, 'Phone', 'Teléfono'),
    label(lang, 'Stand', 'Puesto'), label(lang, 'Service / partner', 'Servicio / organización'),
    label(lang, 'Days', 'Días'), label(lang, 'Check-ins', 'Check-ins'), label(lang, 'Check-outs', 'Check-outs'),
    label(lang, 'First scan', 'Primer escaneo'), label(lang, 'Last scan', 'Último escaneo'),
    label(lang, 'Volunteers', 'Voluntarios')
  ];
  const rows = Array.from(map.values())
    .sort((a, b) => a.name.localeCompare(b.name) || a.stand.localeCompare(b.stand) || a.service.localeCompare(b.service))
    .map(row => [
      row.user_id, row.name, row.phone, row.stand, row.service,
      Array.from(row.days).sort().join(', '),
      row.checkins, row.checkouts, row.first, row.last,
      Array.from(row.volunteers).join(' | ')
    ]);
  return { key: 'participant_services', title: label(lang, 'Participant x service', 'Participante x servicio'), headers, rows };
}

function buildVolunteersTable(snapshot, analytics, filters, lang) {
  const columns = buildColumnCatalog(snapshot, lang);
  const scans = filterScans(snapshot.scans, filters);

  const cellsByVolunteer = new Map();
  for (const scan of scans) {
    if (scan.volunteer_user_id == null) continue;
    const cells = increment(cellsByVolunteer, scan.volunteer_user_id, () => new Map());
    const standKey = `stand_${scan.stand_id}`;
    cells.set(standKey, (cells.get(standKey) || 0) + 1);
    if (scan.service_id) {
      const serviceKey = `svc_${scan.service_id}`;
      cells.set(serviceKey, (cells.get(serviceKey) || 0) + 1);
    }
  }

  const statsById = new Map(analytics.by_volunteer.map(row => [row.volunteer_user_id, row]));
  const volunteerRegs = snapshot.registrations.filter(r => r.registration_role === 'volunteer');

  const headers = [
    'User ID', label(lang, 'Volunteer', 'Voluntario'), label(lang, 'Username', 'Usuario'),
    label(lang, 'Email', 'Correo'), label(lang, 'Phone', 'Teléfono'),
    label(lang, 'Account approved', 'Cuenta aprobada'),
    label(lang, 'Registered at', 'Registrado el'),
    label(lang, 'Total scans', 'Escaneos totales'),
    label(lang, 'Check-ins', 'Check-ins'), label(lang, 'Check-outs', 'Check-outs'),
    label(lang, 'People scanned (unique)', 'Personas escaneadas (únicas)'),
    label(lang, 'Stands worked', 'Puestos donde trabajó'),
    label(lang, 'Services worked', 'Servicios donde trabajó'),
    label(lang, 'Days worked', 'Días trabajados'),
    label(lang, 'First scan', 'Primer escaneo'), label(lang, 'Last scan', 'Último escaneo'),
    label(lang, 'Active minutes (sum per day)', 'Minutos activos (suma por día)'),
    ...columns.map(column => `${label(lang, 'Scans', 'Escaneos')} · ${column.label}`)
  ];

  const rowsById = new Map();
  for (const registration of volunteerRegs) {
    rowsById.set(registration.user_id, {
      user_id: registration.user_id,
      name: fullName(registration.firstname, registration.lastname),
      username: registration.username || '',
      email: registration.email || registration.contact_email || '',
      phone: registration.phone || '',
      approved: registration.user_enabled === 'Y',
      registered_at: registration.submitted_at_local || ''
    });
  }
  // Admins and ops managers also scan without being registered as volunteers.
  for (const stats of analytics.by_volunteer) {
    if (rowsById.has(stats.volunteer_user_id)) continue;
    rowsById.set(stats.volunteer_user_id, {
      user_id: stats.volunteer_user_id,
      name: stats.name,
      username: stats.username,
      email: stats.email,
      phone: '',
      approved: true,
      registered_at: ''
    });
  }

  const rows = Array.from(rowsById.values())
    .map(person => {
      const stats = statsById.get(person.user_id);
      const cells = cellsByVolunteer.get(person.user_id) || new Map();
      return {
        sortKey: stats ? stats.total : -1,
        name: person.name,
        values: [
          person.user_id, person.name, person.username, person.email, person.phone,
          yesNo(person.approved, lang), person.registered_at,
          stats ? stats.total : 0,
          stats ? stats.checkins : 0,
          stats ? stats.checkouts : 0,
          stats ? stats.unique_people : 0,
          stats ? stats.stands : 0,
          stats ? stats.services : 0,
          stats ? stats.days : 0,
          stats ? stats.first_scan : '',
          stats ? stats.last_scan : '',
          stats ? stats.active_minutes : '',
          ...columns.map(column => cells.get(column.key) || 0)
        ]
      };
    })
    .sort((a, b) => b.sortKey - a.sortKey || a.name.localeCompare(b.name))
    .map(row => row.values);

  return { key: 'volunteers', title: label(lang, 'Volunteers', 'Voluntarios'), headers, rows };
}

function buildVolunteerScansTable(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const map = new Map();
  for (const scan of scans) {
    if (scan.volunteer_user_id == null) continue;
    const key = `${scan.volunteer_user_id}|${scan.stand_id}|${scan.service_id || 0}|${scan.day}`;
    const bucket = increment(map, key, () => ({
      volunteer_user_id: scan.volunteer_user_id,
      volunteer_name: scan.volunteer_name,
      stand: pickLang(scan, 'stand', lang),
      service: scan.service_id ? pickLang(scan, 'service', lang) : '',
      day: scan.day,
      checkins: 0,
      checkouts: 0,
      people: new Set(),
      first: scan.local,
      last: scan.local
    }));
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    bucket.people.add(scan.user_id);
    if (scan.local < bucket.first) bucket.first = scan.local;
    if (scan.local > bucket.last) bucket.last = scan.local;
  }
  const headers = [
    'User ID', label(lang, 'Volunteer', 'Voluntario'), label(lang, 'Day', 'Día'),
    label(lang, 'Stand', 'Puesto'), label(lang, 'Service / partner', 'Servicio / organización'),
    label(lang, 'Total scans', 'Escaneos totales'),
    label(lang, 'Check-ins', 'Check-ins'), label(lang, 'Check-outs', 'Check-outs'),
    label(lang, 'People scanned (unique)', 'Personas escaneadas (únicas)'),
    label(lang, 'First scan', 'Primer escaneo'), label(lang, 'Last scan', 'Último escaneo')
  ];
  const rows = Array.from(map.values())
    .sort((a, b) => a.volunteer_name.localeCompare(b.volunteer_name) || a.day.localeCompare(b.day) ||
      a.stand.localeCompare(b.stand) || a.service.localeCompare(b.service))
    .map(row => [
      row.volunteer_user_id, row.volunteer_name, row.day, row.stand, row.service,
      row.checkins + row.checkouts, row.checkins, row.checkouts, row.people.size, row.first, row.last
    ]);
  return { key: 'volunteer_scans', title: label(lang, 'Volunteer x service', 'Voluntario x servicio'), headers, rows };
}

function buildStandHourlyTable(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const map = new Map();
  for (const scan of scans) {
    const key = `${scan.day}|${scan.hour}|${scan.stand_id}|${scan.service_id || 0}`;
    const bucket = increment(map, key, () => ({
      day: scan.day,
      hour: scan.hour,
      stand: pickLang(scan, 'stand', lang),
      service: scan.service_id ? pickLang(scan, 'service', lang) : '',
      checkins: 0,
      checkouts: 0,
      people: new Set(),
      volunteers: new Set()
    }));
    if (scan.scan_type === 'checkin') bucket.checkins += 1; else bucket.checkouts += 1;
    bucket.people.add(scan.user_id);
    if (scan.volunteer_user_id != null) bucket.volunteers.add(scan.volunteer_user_id);
  }
  const headers = [
    label(lang, 'Day', 'Día'), label(lang, 'Hour', 'Hora'),
    label(lang, 'Stand', 'Puesto'), label(lang, 'Service / partner', 'Servicio / organización'),
    label(lang, 'Total scans', 'Escaneos totales'),
    label(lang, 'Check-ins', 'Check-ins'), label(lang, 'Check-outs', 'Check-outs'),
    label(lang, 'People (unique)', 'Personas (únicas)'),
    label(lang, 'Volunteers on duty', 'Voluntarios activos')
  ];
  const rows = Array.from(map.values())
    .sort((a, b) => a.day.localeCompare(b.day) || a.hour - b.hour || a.stand.localeCompare(b.stand))
    .map(row => [
      row.day, `${String(row.hour).padStart(2, '0')}:00`, row.stand, row.service,
      row.checkins + row.checkouts, row.checkins, row.checkouts, row.people.size, row.volunteers.size
    ]);
  return { key: 'stand_hourly', title: label(lang, 'Hourly', 'Por hora'), headers, rows };
}

function buildCheckoutAnswersTable(snapshot, filters, lang) {
  const scans = filterScans(snapshot.scans, filters);
  const scanById = new Map(scans.map(scan => [scan.scan_id, scan]));
  const headers = [
    label(lang, 'Scan ID', 'ID de escaneo'), label(lang, 'Day', 'Día'), label(lang, 'Time', 'Hora'),
    label(lang, 'Stand', 'Puesto'), label(lang, 'Participant', 'Participante'), 'User ID',
    label(lang, 'Question', 'Pregunta'), label(lang, 'Answer', 'Respuesta'),
    label(lang, 'Recorded by', 'Registrado por')
  ];
  const rows = [];
  for (const row of snapshot.checkoutAnswers) {
    const scan = scanById.get(row.scan_id);
    if (!scan) continue;
    rows.push([
      scan.scan_id, scan.day, scan.time, pickLang(scan, 'stand', lang),
      scan.person_name, scan.user_id,
      lang === 'es' ? (row.question_es || row.question_en) : (row.question_en || row.question_es),
      answerValue(row, lang),
      scan.volunteer_name
    ]);
  }
  return { key: 'checkout_answers', title: label(lang, 'Checkout answers', 'Respuestas de checkout'), headers, rows };
}

/** Builds one export table. `analytics` is only needed by a few datasets. */
function buildTable(dataset, snapshot, analytics, filters, lang) {
  switch (dataset) {
    case 'summary': return buildSummaryTable(analytics, lang);
    case 'stand_summary': return buildStandSummaryTable(analytics, lang);
    case 'attendance': return buildAttendanceTable(analytics, lang);
    case 'participants': return buildParticipantsTable(snapshot, filters, lang);
    case 'participant_services': return buildParticipantServicesTable(snapshot, filters, lang);
    case 'visits': return buildVisitsTable(snapshot, filters, lang);
    case 'scan_log': return buildScanLogTable(snapshot, filters, lang);
    case 'volunteers': return buildVolunteersTable(snapshot, analytics, filters, lang);
    case 'volunteer_scans': return buildVolunteerScansTable(snapshot, filters, lang);
    case 'stand_hourly': return buildStandHourlyTable(snapshot, filters, lang);
    case 'checkout_answers': return buildCheckoutAnswersTable(snapshot, filters, lang);
    default: return null;
  }
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

function csvCell(value) {
  if (value == null) return '';
  const text = String(value);
  // Excel treats a leading =, +, - or @ as a formula; the leading quote keeps
  // phone numbers and notes as plain text.
  const escaped = /^[=+\-@]/.test(text) ? `'${text}` : text;
  return /[";\n\r]/.test(escaped) ? `"${escaped.replace(/"/g, '""')}"` : escaped;
}

/** Semicolon-delimited + BOM: the format Excel opens correctly without a wizard. */
function tableToCsv(table) {
  const lines = [table.headers.map(csvCell).join(';')];
  for (const row of table.rows) {
    lines.push(row.map(csvCell).join(';'));
  }
  return '\ufeff' + lines.join('\r\n') + '\r\n';
}

const HEADER_STYLE = {
  font: { bold: true, color: { rgb: 'FFFFFF' } },
  fill: { fgColor: { rgb: '11B3D1' } },
  alignment: { vertical: 'center', wrapText: false }
};

function tableToSheet(table) {
  const aoa = [table.headers, ...table.rows];
  const sheet = XLSX.utils.aoa_to_sheet(aoa);
  for (let column = 0; column < table.headers.length; column += 1) {
    const address = XLSX.utils.encode_cell({ r: 0, c: column });
    if (sheet[address]) sheet[address].s = HEADER_STYLE;
  }
  sheet['!cols'] = table.headers.map(header => ({
    wch: Math.min(Math.max(String(header || '').length + 2, 10), 46)
  }));
  // Header row ships pre-filtered — these sheets are meant to be sliced and
  // pivoted. The summary is a key/value list, where a filter button is noise.
  if (table.rows.length && table.key !== 'summary') {
    sheet['!autofilter'] = {
      ref: XLSX.utils.encode_range(
        { r: 0, c: 0 },
        { r: table.rows.length, c: table.headers.length - 1 })
    };
  }
  return sheet;
}

function tablesToWorkbook(tables) {
  const workbook = XLSX.utils.book_new();
  for (const table of tables) {
    XLSX.utils.book_append_sheet(workbook, tableToSheet(table), SHEET_NAMES[table.key] || table.key);
  }
  return XLSX.write(workbook, { bookType: 'xlsx', type: 'buffer' });
}

module.exports = {
  DATASETS,
  DATASET_SET,
  parseAnalyticsFilters,
  loadSnapshot,
  buildAnalytics,
  buildScanLog,
  buildTable,
  tableToCsv,
  tablesToWorkbook
};
