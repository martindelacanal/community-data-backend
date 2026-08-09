/**
 * Health Events module — health clinic events with stands, event-specific forms
 * and QR check-in / check-out flow.
 *
 * Public:      landing by slug, forms by slug, beneficiary + volunteer registration.
 * Beneficiary: my events, pending required questions (gate the QR), save answers.
 * Volunteer:   context (events/stands/services), assignment, scan, checkout answers,
 *              live "attending" list (polling), recent scans.
 * Admin:       events CRUD, landing editor, landing images (S3 + variants),
 *              stands + services reconcile, forms builder reconcile, slots,
 *              registrations browser, volunteer account creation, metrics summary.
 *
 * Every scan is stored with millisecond timestamps for future metrics.
 */
const express = require('express');
const jwt = require('jsonwebtoken');
const bcryptjs = require('bcryptjs');
const crypto = require('crypto');
const multer = require('multer');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const mysqlConnection = require('../connection/connection');
const logger = require('../utils/logger.js');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const { uploadImageWithVariants, deleteS3Objects } = require('../services/imageVariants');
const { createBeneficiaryPin, resolveBeneficiaryPin } = require('../utils/beneficiaryPin');
const { LEGAL_CONSENT_VERSION, isLegalConsentAccepted } = require('../utils/legalConsent');
const {
  isCurrentDailyBeneficiaryQr,
  parseDailyBeneficiaryQr
} = require('../utils/dailyBeneficiaryQr');
const {
  HEALTH_QR_SLIDING_DEBOUNCE_MS,
  SlidingHealthQrDebounce,
  buildHealthScanDebounceKey,
  findRecentHealthScan
} = require('../utils/healthScanGuard');

const router = express.Router();

const bucketName = process.env.BUCKET_NAME;
const s3 = new S3Client({
  credentials: {
    accessKeyId: process.env.ACCESS_KEY,
    secretAccessKey: process.env.SECRET_ACCESS_KEY
  },
  region: process.env.BUCKET_REGION
});

const imageUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024, files: 1 },
  fileFilter: (req, file, cb) => {
    // Keep Health Event originals decodable by every supported WKWebView.
    // Responsive variants remain available to clients that can use them.
    const ok = ['image/jpeg', 'image/png'].includes(String(file.mimetype).toLowerCase());
    cb(ok ? null : new Error('INVALID_IMAGE_TYPE'), ok);
  }
}).single('image');

const QUESTION_TYPES = new Set(['text', 'number', 'single', 'multiple', 'date', 'consent', 'appointment', 'notice']);
const SLUG_REGEX = /^[a-z0-9]([a-z0-9-]{0,78}[a-z0-9])?$/;
const RESERVED_SLUGS = new Set([
  'home', 'login', 'register', 'survey', 'settings', 'calendar', 'contact', 'articles', 'article',
  'privacy', 'terms', 'my-account', 'my-wellbeing', 'who-we-are', 'trusted-resources', 'trusted-resource',
  'map', 'event', 'view', 'edit', 'new', 'table', 'metrics', 'delivery', 'stocker', 'beneficiary',
  'admin', 'system-manual', 'key-figures', 'home-cards', 'mobile-onboarding', 'health-event',
  'health-events', 'health-volunteer', 'api', 'assets', 'search-results', 'system'
]);
const DUPLICATE_SCAN_WINDOW_SECONDS = HEALTH_QR_SLIDING_DEBOUNCE_MS / 1000;
const healthQrSlidingDebounce = new SlidingHealthQrDebounce();
const healthQrSlidingDebounceCleanup = setInterval(
  () => healthQrSlidingDebounce.pruneExpired(),
  60 * 1000
);
healthQrSlidingDebounceCleanup.unref();

// Shared default password for every health-event account the system hands out
// (self-registrations, admin-created volunteers, password resets). Matches the
// Jotform import default so on-site staff can always tell people one password.
// Accounts created with it get reset_password='Y' → first-login change prompt.
const DEFAULT_HEALTH_PASSWORD = 'bienestarcommunity';

// ============ AUTH MIDDLEWARE ============

function getBearerToken(req) {
  if (!req.headers.authorization || !req.headers.authorization.startsWith('Bearer ')) {
    return null;
  }
  const token = req.headers.authorization.slice(7).trim();
  if (!token || token === 'null' || token === 'undefined') {
    return null;
  }
  return token;
}

function getOptionalAuthUser(req) {
  try {
    const token = getBearerToken(req);
    if (!token) return null;
    const authData = jwt.verify(token, process.env.JWT_SECRET);
    return JSON.parse(authData.data);
  } catch (error) {
    return null;
  }
}

function verifyToken(req, res, next) {
  const token = getBearerToken(req);
  if (!token) {
    return res.status(401).json({ error: 'UNAUTHORIZED', message: 'Authentication required' });
  }
  jwt.verify(token, process.env.JWT_SECRET, (error, authData) => {
    if (error) {
      return res.status(403).json({ error: 'FORBIDDEN', message: 'Invalid token' });
    }
    req.data = authData;
    next();
  });
}

function requireRoles(...roles) {
  return (req, res, next) => {
    try {
      const user = JSON.parse(req.data.data);
      if (!roles.includes(user.role)) {
        return res.status(403).json({ error: 'FORBIDDEN', message: 'Insufficient permissions' });
      }
      req.currentUser = user;
      next();
    } catch (error) {
      logger.error('healthEvents requireRoles error: ' + error.message);
      return res.status(403).json({ error: 'FORBIDDEN', message: 'Insufficient permissions' });
    }
  };
}

// Roles con permisos administrativos plenos sobre el módulo de health events
// (opsmanager tiene paridad total con admin aquí).
const ADMIN_ROLES = ['admin', 'opsmanager'];
const requireAdmin = requireRoles(...ADMIN_ROLES);
const requireBeneficiary = requireRoles('beneficiary');
const requireVolunteer = requireRoles('eventvolunteer', ...ADMIN_ROLES);

// ============ HELPERS ============

const signedUrlCache = new Map();
const SIGNED_URL_CACHE_MS = 30 * 60 * 1000;

async function signImageUrl(s3Key, expiresIn = 3600) {
  if (!s3Key) return null;
  const cacheKey = `${s3Key}_${expiresIn}`;
  const cached = signedUrlCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.url;
  }
  try {
    const url = await getSignedUrl(s3, new GetObjectCommand({ Bucket: bucketName, Key: s3Key }), { expiresIn });
    signedUrlCache.set(cacheKey, { url, expiresAt: Date.now() + SIGNED_URL_CACHE_MS });
    return url;
  } catch (error) {
    logger.error('healthEvents signImageUrl error: ' + error.message);
    return null;
  }
}

async function attachImageUrls(images) {
  return Promise.all(images.map(async (img) => ({
    id: img.id,
    section_key: img.section_key,
    alt_en: img.alt_en,
    alt_es: img.alt_es,
    link_url: img.link_url || null,
    display_order: img.display_order,
    enabled: img.enabled,
    url: await signImageUrl(img.s3_key),
    url_small: await signImageUrl(img.s3_key_small),
    url_medium: await signImageUrl(img.s3_key_medium)
  })));
}

/** Current wall-clock in the event timezone as 'YYYY-MM-DD HH:mm:ss' (lexicographically comparable). */
function nowInTimezone(timeZone) {
  try {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: timeZone || 'America/Los_Angeles',
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    }).formatToParts(new Date());
    const get = (type) => (parts.find(p => p.type === type) || {}).value || '00';
    const hour = get('hour') === '24' ? '00' : get('hour');
    return `${get('year')}-${get('month')}-${get('day')} ${hour}:${get('minute')}:${get('second')}`;
  } catch (error) {
    return new Date().toISOString().slice(0, 19).replace('T', ' ');
  }
}

function toSqlDateTimeString(value) {
  if (!value) return null;
  if (value instanceof Date) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())} ` +
      `${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}`;
  }
  return String(value).replace('T', ' ').slice(0, 19);
}

function toSqlDateString(value) {
  if (!value) return null;
  if (value instanceof Date) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`;
  }
  return String(value).slice(0, 10);
}

function isRegistrationOpen(event) {
  const now = nowInTimezone(event.timezone);
  const opens = toSqlDateTimeString(event.registration_opens_at);
  const closes = toSqlDateTimeString(event.registration_closes_at);
  if (opens && now < opens) return false;
  if (closes && now > closes) return false;
  return event.enabled === 'Y' && !hasEventEnded(event);
}

/** True once the event's last day is over in the event's own timezone. */
function hasEventEnded(event) {
  const endDate = toSqlDateString(event.end_date);
  if (!endDate) return false;
  const today = nowInTimezone(event.timezone).slice(0, 10);
  return today > endDate;
}

function safeParseJson(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (error) {
    return fallback;
  }
}

async function getEventBySlug(slug) {
  const [rows] = await mysqlConnection.promise().query(
    'SELECT he.*, l.organization, l.community_city, l.address \
     FROM health_event he INNER JOIN location l ON l.id = he.location_id \
     WHERE he.slug = ? LIMIT 1', [slug]);
  return rows.length ? rows[0] : null;
}

async function getEventById(id) {
  const [rows] = await mysqlConnection.promise().query(
    'SELECT he.*, l.organization, l.community_city, l.address \
     FROM health_event he INNER JOIN location l ON l.id = he.location_id \
     WHERE he.id = ? LIMIT 1', [id]);
  return rows.length ? rows[0] : null;
}

function publicEventShape(event) {
  return {
    id: event.id,
    slug: event.slug,
    name_en: event.name_en,
    name_es: event.name_es,
    client_id: event.client_id,
    location: {
      id: event.location_id,
      organization: event.organization,
      community_city: event.community_city,
      address: event.address
    },
    start_date: toSqlDateString(event.start_date),
    end_date: toSqlDateString(event.end_date),
    start_time: event.start_time,
    end_time: event.end_time,
    timezone: event.timezone,
    registration_open: isRegistrationOpen(event),
    event_ended: hasEventEnded(event),
    registration_opens_at: toSqlDateTimeString(event.registration_opens_at),
    registration_closes_at: toSqlDateTimeString(event.registration_closes_at)
  };
}

/** Load forms (+questions +options) for an event/audience. standId only for 'checkout'. */
async function fetchForms(eventId, audience, standId = null) {
  const params = [eventId, audience];
  let standFilter = '';
  if (audience === 'checkout' && standId) {
    standFilter = ' AND f.stand_id = ?';
    params.push(standId);
  }
  const [forms] = await mysqlConnection.promise().query(
    `SELECT f.id, f.audience, f.stand_id, f.title_en, f.title_es, f.intro_en, f.intro_es,
            f.section_order, f.required_before_qr, f.enabled
     FROM health_event_form f
     WHERE f.health_event_id = ? AND f.audience = ? AND f.enabled = 'Y'${standFilter}
     ORDER BY f.section_order ASC, f.id ASC`, params);
  if (!forms.length) return [];

  const formIds = forms.map(f => f.id);
  const [questions] = await mysqlConnection.promise().query(
    `SELECT q.* FROM health_event_question q
     WHERE q.form_id IN (${formIds.map(() => '?').join(',')}) AND q.enabled = 'Y'
     ORDER BY q.sort_order ASC, q.id ASC`, formIds);

  const questionIds = questions.map(q => q.id);
  let options = [];
  if (questionIds.length) {
    const [optionRows] = await mysqlConnection.promise().query(
      `SELECT o.* FROM health_event_question_option o
       WHERE o.question_id IN (${questionIds.map(() => '?').join(',')}) AND o.enabled = 'Y'
       ORDER BY o.sort_order ASC, o.id ASC`, questionIds);
    options = optionRows;
  }

  const optionsByQuestion = new Map();
  for (const option of options) {
    if (!optionsByQuestion.has(option.question_id)) optionsByQuestion.set(option.question_id, []);
    optionsByQuestion.get(option.question_id).push({
      id: option.id,
      name_en: option.name_en,
      name_es: option.name_es,
      is_other: option.is_other,
      event_date: toSqlDateString(option.event_date),
      service_key: option.service_key,
      sort_order: option.sort_order
    });
  }

  const questionShape = (q) => ({
    id: q.id,
    form_id: q.form_id,
    question_type: q.question_type,
    name_en: q.name_en,
    name_es: q.name_es,
    help_en: q.help_en,
    help_es: q.help_es,
    required: q.required,
    allow_other: q.allow_other,
    maps_to: q.maps_to,
    config_json: safeParseJson(q.config_json, null),
    depends_on_question_id: q.depends_on_question_id,
    depends_on_option_id: q.depends_on_option_id,
    sort_order: q.sort_order,
    options: optionsByQuestion.get(q.id) || []
  });

  return forms.map(f => ({
    id: f.id,
    audience: f.audience,
    stand_id: f.stand_id,
    title_en: f.title_en,
    title_es: f.title_es,
    intro_en: f.intro_en,
    intro_es: f.intro_es,
    section_order: f.section_order,
    required_before_qr: f.required_before_qr,
    questions: questions.filter(q => q.form_id === f.id).map(questionShape)
  }));
}

// includeDisabled: el admin necesita ver TAMBIÉN los horarios deshabilitados
// (si se ocultan, "desaparecen" de la pantalla y parecen borrados — incidente
// 06-ago-2026); el formulario público sigue recibiendo solo los habilitados.
async function fetchSlotsWithBooked(eventId, includeDisabled = false) {
  const [slots] = await mysqlConnection.promise().query(
    `SELECT s.id, s.service_key, s.slot_date, TIME_FORMAT(s.start_time, '%H:%i') AS start_time,
            TIME_FORMAT(s.end_time, '%H:%i') AS end_time, s.capacity, s.enabled,
            (SELECT COUNT(*) FROM health_event_appointment a
             WHERE a.slot_id = s.id AND a.status = 'booked') AS booked
     FROM health_event_slot s
     WHERE s.health_event_id = ? ${includeDisabled ? '' : 'AND s.enabled = "Y"'}
     ORDER BY s.slot_date ASC, s.start_time ASC`, [eventId]);
  return slots.map(s => ({ ...s, slot_date: toSqlDateString(s.slot_date) }));
}

/**
 * Compute which questions are "visible" given the current answers
 * (option-dependency chain, same semantics as the existing survey).
 * answersByQuestion: Map<question_id, Set<option_id>> (only option-based answers matter)
 */
function computeVisibleQuestionIds(allQuestions, answersByQuestion) {
  const byId = new Map(allQuestions.map(q => [q.id, q]));
  const visible = new Set();
  const isVisible = (q, seen = new Set()) => {
    if (visible.has(q.id)) return true;
    if (!q.depends_on_question_id) return true;
    if (seen.has(q.id)) return false;
    seen.add(q.id);
    const parent = byId.get(q.depends_on_question_id);
    if (!parent) return true;
    if (!isVisible(parent, seen)) return false;
    const selected = answersByQuestion.get(q.depends_on_question_id);
    return !!(selected && q.depends_on_option_id && selected.has(q.depends_on_option_id));
  };
  for (const q of allQuestions) {
    if (isVisible(q)) visible.add(q.id);
  }
  return visible;
}

function buildAnswersByQuestionFromSubmission(items) {
  const map = new Map();
  for (const item of items || []) {
    const selected = new Set();
    if (Array.isArray(item.answer)) {
      for (const v of item.answer) {
        const n = Number.parseInt(v, 10);
        if (Number.isInteger(n)) selected.add(n);
      }
    } else if (item.answer != null && (item.question_type === 'single')) {
      const n = Number.parseInt(item.answer, 10);
      if (Number.isInteger(n)) selected.add(n);
    }
    map.set(item.question_id, selected);
  }
  return map;
}

function hasMeaningfulAnswer(questionType, answer) {
  if (answer === undefined || answer === null) return false;
  switch (questionType) {
    case 'text':
    case 'date':
      return String(answer).trim() !== '';
    case 'number':
      return String(answer).trim() !== '' && !Number.isNaN(Number(answer));
    case 'single':
      return Number.isInteger(Number.parseInt(answer, 10));
    case 'multiple':
      return Array.isArray(answer) && answer.length > 0;
    case 'consent':
      return answer === true || answer === 1 || answer === '1' || answer === 'Y';
    default:
      return false;
  }
}

/**
 * Upsert answers for a registration inside a transaction.
 * items: [{question_id, answer, other_text}] — answer typed per question_type.
 * questionsById: Map of allowed question rows.
 */
async function upsertAnswers(connection, registrationId, items, questionsById, source) {
  let saved = 0;
  for (const item of items || []) {
    const question = questionsById.get(item.question_id);
    if (!question) continue;
    if (question.question_type === 'appointment') continue; // stored via health_event_appointment
    if (!hasMeaningfulAnswer(question.question_type, item.answer)) continue;

    let answerText = null;
    let answerNumber = null;
    let answerDate = null;
    let optionIds = [];

    switch (question.question_type) {
      case 'text':
        answerText = String(item.answer).trim().slice(0, 5000);
        break;
      case 'number':
        answerNumber = Number(item.answer);
        break;
      case 'date':
        answerDate = toSqlDateString(item.answer);
        break;
      case 'consent':
        answerNumber = 1;
        break;
      case 'single':
        optionIds = [Number.parseInt(item.answer, 10)];
        break;
      case 'multiple':
        optionIds = item.answer.map(v => Number.parseInt(v, 10)).filter(Number.isInteger);
        break;
      default:
        continue;
    }

    if (optionIds.length) {
      const validOptionIds = new Set((question.options || []).map(o => o.id));
      optionIds = optionIds.filter(id => validOptionIds.has(id));
      if (!optionIds.length) continue;
    }

    const otherText = item.other_text != null ? String(item.other_text).trim().slice(0, 500) : null;

    const [existing] = await connection.query(
      'SELECT id FROM health_event_answer WHERE registration_id = ? AND question_id = ? LIMIT 1',
      [registrationId, item.question_id]);

    let answerId;
    if (existing.length) {
      answerId = existing[0].id;
      await connection.query(
        'UPDATE health_event_answer SET answer_text = ?, answer_number = ?, answer_date = ?, other_text = ?, source = ? WHERE id = ?',
        [answerText, answerNumber, answerDate, otherText, source, answerId]);
      await connection.query('DELETE FROM health_event_answer_option WHERE answer_id = ?', [answerId]);
    } else {
      const [inserted] = await connection.query(
        'INSERT INTO health_event_answer(registration_id, question_id, answer_text, answer_number, answer_date, other_text, source) VALUES (?,?,?,?,?,?,?)',
        [registrationId, item.question_id, answerText, answerNumber, answerDate, otherText, source]);
      answerId = inserted.insertId;
    }

    for (const optionId of optionIds) {
      await connection.query(
        'INSERT IGNORE INTO health_event_answer_option(answer_id, option_id) VALUES (?,?)',
        [answerId, optionId]);
    }
    saved++;
  }
  return saved;
}

/** Derive attendance dates + priority services from submitted answers and sync registration_date rows. */
async function syncRegistrationDates(connection, registrationId, items, questionsById) {
  const dates = new Map(); // 'YYYY-MM-DD' -> priority_service|null
  for (const item of items || []) {
    const question = questionsById.get(item.question_id);
    if (!question || question.question_type !== 'single') continue;
    const optionId = Number.parseInt(item.answer, 10);
    const option = (question.options || []).find(o => o.id === optionId);
    if (!option) continue;
    if (question.maps_to === 'attend_date' && option.event_date) {
      const date = toSqlDateString(option.event_date);
      if (!dates.has(date)) dates.set(date, null);
    }
    if (question.maps_to === 'priority_service') {
      const config = safeParseJson(question.config_json, {});
      const date = toSqlDateString(config && config.event_date);
      if (date) {
        dates.set(date, option.service_key || option.name_en);
      }
    }
  }
  for (const [date, priority] of dates.entries()) {
    const [existing] = await connection.query(
      'SELECT id FROM health_event_registration_date WHERE registration_id = ? AND event_date = ? LIMIT 1',
      [registrationId, date]);
    if (existing.length) {
      if (priority) {
        await connection.query('UPDATE health_event_registration_date SET priority_service = ? WHERE id = ?',
          [priority, existing[0].id]);
      }
    } else {
      await connection.query(
        'INSERT INTO health_event_registration_date(registration_id, event_date, priority_service) VALUES (?,?,?)',
        [registrationId, date, priority]);
    }
  }
}

async function bookAppointments(connection, registrationId, appointments) {
  for (const appointment of appointments || []) {
    const slotId = Number.parseInt(appointment.slot_id, 10);
    if (!Number.isInteger(slotId)) continue;
    const [slotRows] = await connection.query(
      'SELECT id, capacity FROM health_event_slot WHERE id = ? AND enabled = "Y" LIMIT 1 FOR UPDATE', [slotId]);
    if (!slotRows.length) {
      const err = new Error('SLOT_NOT_FOUND');
      err.code = 'SLOT_NOT_FOUND';
      err.slot_id = slotId;
      throw err;
    }
    await connection.query(
      'INSERT INTO health_event_appointment(registration_id, slot_id) VALUES (?,?) \
       ON DUPLICATE KEY UPDATE status = "booked"', [registrationId, slotId]);
    if (slotRows[0].capacity != null) {
      const [countRows] = await connection.query(
        'SELECT COUNT(*) AS booked FROM health_event_appointment WHERE slot_id = ? AND status = "booked"', [slotId]);
      if (countRows[0].booked > slotRows[0].capacity) {
        const err = new Error('SLOT_FULL');
        err.code = 'SLOT_FULL';
        err.slot_id = slotId;
        throw err;
      }
    }
  }
}

/** Build the same signin token the /signin endpoint issues, for auto-login after registration. */
async function buildSigninToken(userId) {
  const [rows] = await mysqlConnection.promise().query(
    'SELECT user.id, user.firstname, user.username, user.email, user.client_id AS client_id, \
            role.name AS role, user.language AS language, user.enabled AS enabled \
     FROM user INNER JOIN role ON role.id = user.role_id WHERE user.id = ? LIMIT 1', [userId]);
  if (!rows.length) return null;
  const data = JSON.stringify(rows[0]);
  return new Promise((resolve, reject) => {
    jwt.sign({ data }, process.env.JWT_SECRET, { expiresIn: '6h' }, (err, token) => {
      if (err) reject(err); else resolve(token);
    });
  });
}

function normalizeForUsername(text) {
  return String(text || '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase().replace(/[^a-z0-9]+/g, '.')
    .replace(/^\.+|\.+$/g, '').replace(/\.{2,}/g, '.');
}

async function generateUniqueUsername(connection, firstname, lastname) {
  let base = normalizeForUsername(`${firstname}.${lastname}`).slice(0, 40);
  if (!base) base = 'volunteer';
  let candidate = base;
  let suffix = 1;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const [rows] = await connection.query('SELECT id FROM user WHERE username = ? LIMIT 1', [candidate]);
    if (!rows.length) return candidate;
    suffix++;
    candidate = `${base.slice(0, 40 - String(suffix).length)}${suffix}`;
  }
}

function generateReadablePassword() {
  const consonants = 'bcdfghjkmnpqrstvwz';
  const vowels = 'aeiou';
  let word = '';
  for (let i = 0; i < 3; i++) {
    word += consonants[crypto.randomInt(consonants.length)];
    word += vowels[crypto.randomInt(vowels.length)];
  }
  return `${word}${crypto.randomInt(1000, 9999)}`;
}

async function findClientIdForLocation(connection, locationId) {
  const [rows] = await connection.query('SELECT client_id FROM client_location WHERE location_id = ?', [locationId]);
  return rows.length ? rows[0].client_id : null;
}

async function createHealthEventUser(connection, {
  username, passwordHash, email, roleId, firstName, lastName, dateOfBirth, phone,
  zipcode, locationId, uiLanguage, enabled = 'Y', resetPassword = 'N'
}) {
  const clientId = await findClientIdForLocation(connection, locationId);
  const [inserted] = await connection.query(
    'INSERT INTO user(username, password, email, role_id, client_id, firstname, lastname, date_of_birth, phone, \
       zipcode, first_location_id, location_id, household_size, language, legal_consent_accepted, \
       legal_consent_accepted_at, legal_consent_version, enabled, reset_password) \
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    [username || null, passwordHash, email || null, roleId, clientId, firstName || null, lastName || null,
      dateOfBirth || null, phone || null, zipcode || null, locationId, locationId, 1,
      uiLanguage === 'es' ? 'es' : 'en', 1, new Date(), LEGAL_CONSENT_VERSION, enabled === 'N' ? 'N' : 'Y',
      resetPassword === 'Y' ? 'Y' : 'N']);
  const userId = inserted.insertId;
  if (clientId) {
    await connection.query('INSERT IGNORE INTO client_user(client_id, user_id) VALUES (?,?)', [clientId, userId]);
  }
  return userId;
}

function splitFullName(fullName) {
  const parts = String(fullName || '').trim().split(/\s+/);
  if (parts.length <= 1) return { firstName: parts[0] || null, lastName: null };
  return { firstName: parts.slice(0, -1).join(' '), lastName: parts[parts.length - 1] };
}

/** Registration + user + dates + appointments + answers, shaped for the registration emails. */
async function loadRegistrationEmailData(registrationId) {
  const [regRows] = await mysqlConnection.promise().query(
    `SELECT r.id, r.health_event_id, r.registration_role, r.status, r.source, r.contact_email, r.submitted_at,
            u.id AS user_id, u.firstname, u.lastname, u.email, u.username, u.phone, u.date_of_birth, u.zipcode,
            u.language AS user_language
     FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
     WHERE r.id = ? LIMIT 1`, [registrationId]);
  if (!regRows.length) return null;
  const registration = regRows[0];
  const [dates] = await mysqlConnection.promise().query(
    'SELECT event_date, priority_service FROM health_event_registration_date WHERE registration_id = ? ORDER BY event_date',
    [registrationId]);
  const [appointments] = await mysqlConnection.promise().query(
    `SELECT sl.service_key, sl.slot_date, TIME_FORMAT(sl.start_time, '%H:%i') AS start_time,
            TIME_FORMAT(sl.end_time, '%H:%i') AS end_time, a.status
     FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
     WHERE a.registration_id = ? AND a.status = 'booked' ORDER BY sl.slot_date, sl.start_time`, [registrationId]);
  const [answers] = await mysqlConnection.promise().query(
    `SELECT a.question_id, q.name_en AS question_en, q.name_es AS question_es, q.question_type,
            a.answer_text, a.answer_number, a.answer_date, a.other_text,
            (SELECT GROUP_CONCAT(o.name_en ORDER BY o.sort_order SEPARATOR ' | ')
               FROM health_event_answer_option ao INNER JOIN health_event_question_option o ON o.id = ao.option_id
               WHERE ao.answer_id = a.id) AS options_en,
            (SELECT GROUP_CONCAT(o.name_es ORDER BY o.sort_order SEPARATOR ' | ')
               FROM health_event_answer_option ao INNER JOIN health_event_question_option o ON o.id = ao.option_id
               WHERE ao.answer_id = a.id) AS options_es
     FROM health_event_answer a INNER JOIN health_event_question q ON q.id = a.question_id
     WHERE a.registration_id = ?
     ORDER BY q.form_id, q.sort_order`, [registrationId]);
  return {
    registration: {
      id: registration.id,
      source: registration.source,
      contact_email: registration.contact_email,
      submitted_at: registration.submitted_at
    },
    user: {
      firstname: registration.firstname,
      lastname: registration.lastname,
      email: registration.email,
      username: registration.username,
      phone: registration.phone,
      date_of_birth: toSqlDateString(registration.date_of_birth),
      zipcode: registration.zipcode,
      language: registration.user_language === 'es' ? 'es' : 'en'
    },
    dates: dates.map(d => ({ event_date: toSqlDateString(d.event_date), priority_service: d.priority_service })),
    appointments: appointments.map(a => ({ ...a, slot_date: toSqlDateString(a.slot_date) })),
    // mysql2 returns DATE columns as JS Date objects; emails need 'YYYY-MM-DD'.
    answers: answers.map(a => ({ ...a, answer_date: a.answer_date != null ? toSqlDateString(a.answer_date) : null }))
  };
}

/**
 * Post-commit registration emails, fired and forgotten (never blocks the response):
 *  - notification to the per-event recipient list for the audience
 *    (health_event_notification_recipient, independent from the global
 *    food-distribution volunteer list), and
 *  - optional confirmation to the registrant (confirmation = {to, language, firstname}).
 */
function dispatchRegistrationEmails(event, registrationId, audience, confirmation = null) {
  (async () => {
    const emailModule = require('../email/email');
    const [recipients] = await mysqlConnection.promise().query(
      'SELECT email, language FROM health_event_notification_recipient \
       WHERE health_event_id = ? AND audience = ? AND enabled = "Y"',
      [event.id, audience]);
    const wantsConfirmation = !!(confirmation && confirmation.to);
    if (!recipients.length && !wantsConfirmation) return;

    const data = await loadRegistrationEmailData(registrationId);
    if (!data) return;
    const locationName = [event.organization, event.community_city].filter(Boolean).join(' — ');

    if (recipients.length && typeof emailModule.sendHealthEventRegistrationNotification === 'function') {
      await emailModule.sendHealthEventRegistrationNotification({
        audience,
        eventNameEn: event.name_en,
        eventNameEs: event.name_es,
        locationName,
        source: data.registration.source,
        submittedOn: toSqlDateTimeString(data.registration.submitted_at),
        contactEmail: data.registration.contact_email,
        user: data.user,
        dates: data.dates,
        appointments: data.appointments,
        answers: data.answers
      }, recipients);
    }

    // Strict single-address validation: the address comes from a public form,
    // so anything not matching (including comma-separated lists) is dropped.
    const confirmationTo = wantsConfirmation ? String(confirmation.to).trim() : '';
    if (wantsConfirmation && confirmationTo.length <= 255 && NOTIFICATION_EMAIL_REGEX.test(confirmationTo) &&
        typeof emailModule.sendHealthEventBeneficiaryConfirmation === 'function') {
      const language = confirmation.language || data.user.language;
      await emailModule.sendHealthEventBeneficiaryConfirmation({
        to: confirmationTo,
        language: language === 'es' ? 'es' : 'en',
        firstname: confirmation.firstname || data.user.firstname,
        eventNameEn: event.name_en,
        eventNameEs: event.name_es,
        locationName,
        address: event.address || null,
        startTime: event.start_time || null,
        endTime: event.end_time || null,
        eventStartDate: toSqlDateString(event.start_date),
        eventEndDate: toSqlDateString(event.end_date),
        dates: data.dates,
        appointments: data.appointments,
        credentials: confirmation.credentials || null
      });
    }
  })().catch((error) => {
    logger.error('healthEvents dispatchRegistrationEmails error: ' + error.message);
  });
}

// =====================================================================
// PUBLIC ENDPOINTS
// =====================================================================

/**
 * Public-home payload: events flagged by the admin as visible on the public
 * home (cards linking to their landings) + the newest promoted event's dialog.
 * Unauthenticated + 60s in-memory cache (high-traffic page).
 */
const publicHomeCache = { at: 0, data: null };
const PUBLIC_HOME_CACHE_MS = 60 * 1000;

router.get('/health-events/public-home', async (req, res) => {
  try {
    if (publicHomeCache.data && Date.now() - publicHomeCache.at < PUBLIC_HOME_CACHE_MS) {
      return res.status(200).json(publicHomeCache.data);
    }
    // CURDATE() corre en hora del SERVIDOR (UTC): un evento en Los Ángeles
    // desaparecería ~7 horas antes de terminar su último día. El SQL trae un
    // día de margen y el corte exacto se decide por zona horaria del evento.
    const [eventRows] = await mysqlConnection.promise().query(
      `SELECT he.*, l.organization, l.community_city, l.address
       FROM health_event he INNER JOIN location l ON l.id = he.location_id
       WHERE he.enabled = 'Y' AND he.landing_enabled = 'Y' AND he.public_home_visible = 'Y'
         AND he.end_date >= (CURDATE() - INTERVAL 1 DAY)
       ORDER BY he.start_date ASC`);
    const events = eventRows.filter(event => !hasEventEnded(event));

    const list = [];
    for (const event of events) {
      const [heroRows] = await mysqlConnection.promise().query(
        `SELECT * FROM health_event_image WHERE health_event_id = ? AND section_key = 'hero' AND enabled = 'Y'
         ORDER BY display_order ASC, id ASC LIMIT 1`, [event.id]);
      const hero = heroRows.length ? (await attachImageUrls(heroRows))[0] : null;
      const landing = safeParseJson(event.landing_json, {}) || {};
      const heroText = landing.hero || {};
      list.push({
        ...publicEventShape(event),
        subtitle_en: heroText.subtitle_en || null,
        subtitle_es: heroText.subtitle_es || null,
        tagline_en: heroText.tagline_en || null,
        tagline_es: heroText.tagline_es || null,
        hero_image: hero ? { url: hero.url, url_small: hero.url_small, url_medium: hero.url_medium } : null
      });
    }

    // Promo dialog: only the NEWEST promoted event (highest id = last added).
    let promo = null;
    const promoted = events.filter(e => e.promo_dialog_enabled === 'Y');
    if (promoted.length) {
      const latest = promoted.reduce((a, b) => (b.id > a.id ? b : a));
      const promoJson = safeParseJson(latest.promo_json, {}) || {};
      let imageUrl = null;
      const [imageRows] = await mysqlConnection.promise().query(
        `SELECT * FROM health_event_image WHERE health_event_id = ? AND section_key = 'promo_dialog' AND enabled = 'Y'
         ORDER BY display_order ASC, id DESC LIMIT 1`, [latest.id]);
      if (imageRows.length) {
        imageUrl = await signImageUrl(imageRows[0].s3_key_medium || imageRows[0].s3_key);
      }
      promo = {
        event_id: latest.id,
        slug: latest.slug,
        version: Number(latest.promo_dialog_version) || 1,
        name_en: latest.name_en,
        name_es: latest.name_es,
        text_en: promoJson.text_en || null,
        text_es: promoJson.text_es || null,
        image_url: imageUrl,
        link_url: promoJson.link_url || null
      };
    }

    const data = { events: list, promo };
    publicHomeCache.at = Date.now();
    publicHomeCache.data = data;
    res.status(200).json(data);
  } catch (error) {
    logger.error('GET /health-events/public-home error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.get('/health-events/landing/:slug', async (req, res) => {
  try {
    const event = await getEventBySlug(String(req.params.slug).toLowerCase());
    if (!event || event.enabled !== 'Y' || event.landing_enabled !== 'Y') {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const [images] = await mysqlConnection.promise().query(
      'SELECT * FROM health_event_image WHERE health_event_id = ? AND enabled = "Y" \
       ORDER BY section_key ASC, display_order ASC, id ASC', [event.id]);
    res.status(200).json({
      event: { ...publicEventShape(event), landing_json: safeParseJson(event.landing_json, null) },
      images: await attachImageUrls(images)
    });
  } catch (error) {
    logger.error('GET /health-events/landing error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.get('/health-events/:slug/forms', async (req, res) => {
  try {
    const audience = req.query.audience === 'volunteer' ? 'volunteer' : 'beneficiary';
    const event = await getEventBySlug(String(req.params.slug).toLowerCase());
    if (!event || event.enabled !== 'Y') {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const open = isRegistrationOpen(event);
    const forms = open ? await fetchForms(event.id, audience) : [];
    const slots = open && audience === 'beneficiary' ? await fetchSlotsWithBooked(event.id) : [];
    res.status(200).json({ event: publicEventShape(event), forms, slots });
  } catch (error) {
    logger.error('GET /health-events/:slug/forms error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.post('/health-events/:slug/register', async (req, res) => {
  let connection;
  try {
    const event = await getEventBySlug(String(req.params.slug).toLowerCase());
    if (!event || event.enabled !== 'Y') {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    if (hasEventEnded(event)) {
      return res.status(410).json({ error: 'EVENT_ENDED' });
    }
    if (!isRegistrationOpen(event)) {
      return res.status(410).json({
        error: 'REGISTRATION_CLOSED',
        registration_closes_at: toSqlDateTimeString(event.registration_closes_at)
      });
    }

    const authUser = getOptionalAuthUser(req);
    const account = req.body.account || {};
    const answers = Array.isArray(req.body.answers) ? req.body.answers : [];
    const appointments = Array.isArray(req.body.appointments) ? req.body.appointments : [];

    // Beneficiario logueado que YA está registrado (p. ej. importado de Jotform
    // que vuelve a llenar el formulario para completar la encuesta): en vez de
    // rechazar con 409 y descartar todo lo que respondió (incidente Banning
    // 08-ago-2026), sus respuestas se absorben en la registración existente.
    let existingRegistrationId = null;
    if (authUser && authUser.role === 'beneficiary') {
      const [alreadyRegistered] = await mysqlConnection.promise().query(
        'SELECT id FROM health_event_registration WHERE health_event_id = ? AND user_id = ? AND registration_role = "beneficiary" LIMIT 1',
        [event.id, authUser.id]);
      if (alreadyRegistered.length) {
        existingRegistrationId = alreadyRegistered[0].id;
      }
    }

    const forms = await fetchForms(event.id, 'beneficiary');
    const allQuestions = forms.flatMap(f => f.questions);
    const questionsById = new Map(allQuestions.map(q => [q.id, q]));

    // Server-side required validation over reachable questions. For an existing
    // registration the stored answers also count: visibility and "answered" are
    // evaluated over the union of stored + submitted.
    const answersByQuestion = buildAnswersByQuestionFromSubmission(
      answers.map(a => ({ ...a, question_type: (questionsById.get(a.question_id) || {}).question_type })));
    const answeredIds = new Set(answers
      .filter(a => {
        const q = questionsById.get(a.question_id);
        return q && (q.question_type === 'appointment' || hasMeaningfulAnswer(q.question_type, a.answer));
      })
      .map(a => a.question_id));
    if (existingRegistrationId) {
      const storedOptions = await fetchExistingAnswerOptions(existingRegistrationId);
      const storedAnswered = await fetchAnsweredQuestionIds(existingRegistrationId);
      for (const questionId of storedAnswered) {
        answeredIds.add(questionId);
      }
      for (const [questionId, optionIds] of storedOptions.entries()) {
        if (!answersByQuestion.has(questionId)) {
          answersByQuestion.set(questionId, optionIds);
        }
      }
    }
    const visibleIds = computeVisibleQuestionIds(allQuestions, answersByQuestion);
    const missingRequired = allQuestions.filter(q =>
      q.required === 'Y' && q.question_type !== 'appointment' && q.question_type !== 'notice' &&
      visibleIds.has(q.id) && !answeredIds.has(q.id));
    if (missingRequired.length) {
      return res.status(400).json({
        error: 'MISSING_REQUIRED_ANSWERS',
        question_ids: missingRequired.map(q => q.id)
      });
    }

    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();

    let userId;
    let createdAccount = false;
    let createdWithDefaultPassword = false;
    let createdUsername = null;
    if (authUser && authUser.role === 'beneficiary') {
      userId = authUser.id;
    } else {
      const username = String(account.username || '').trim();
      // Password is optional since 2026-08: when the form does not send one the
      // account gets the shared default and a first-login change prompt. Old
      // bundles / native apps that still send a user-chosen password keep it.
      const providedPassword = String(account.password || '');
      const password = providedPassword || DEFAULT_HEALTH_PASSWORD;
      if (!username || (providedPassword && providedPassword.length < 4) || !account.firstName || !account.phone) {
        await connection.rollback();
        logger.error('POST /health-events/:slug/register 400 INVALID_ACCOUNT_DATA (missing username/name/phone or short password)');
        return res.status(400).json({ error: 'INVALID_ACCOUNT_DATA' });
      }
      const email = account.email ? String(account.email).trim() : null;
      const phone = String(account.phone).trim();

      // Same phone rules as the general signup form: exactly 10 digits and not
      // already in use by a visible beneficiary (mirrors /phone/exists/search,
      // which is what the frontend checks against while typing).
      if (!/^[0-9]{10}$/.test(phone)) {
        await connection.rollback();
        logger.error('POST /health-events/:slug/register 400 INVALID_ACCOUNT_DATA (phone not 10 digits)');
        return res.status(400).json({ error: 'INVALID_ACCOUNT_DATA' });
      }
      const [phoneTaken] = await connection.query(
        'SELECT id FROM user WHERE phone = ? AND enabled = "Y" AND role_id = 5 LIMIT 1', [phone]);
      if (phoneTaken.length) {
        await connection.rollback();
        return res.status(409).json({ error: 'PHONE_TAKEN' });
      }

      const [userTaken] = await connection.query('SELECT id FROM user WHERE username = ? LIMIT 1', [username]);
      if (userTaken.length) {
        await connection.rollback();
        return res.status(409).json({ error: 'USERNAME_TAKEN' });
      }
      if (email) {
        const [emailTaken] = await connection.query('SELECT id FROM user WHERE email = ? LIMIT 1', [email]);
        if (emailTaken.length) {
          await connection.rollback();
          return res.status(409).json({ error: 'EMAIL_TAKEN' });
        }
      }

      const passwordHash = await bcryptjs.hash(password, 8);
      userId = await createHealthEventUser(connection, {
        username,
        passwordHash,
        email,
        roleId: 5,
        firstName: account.firstName,
        lastName: account.lastName,
        dateOfBirth: account.dateOfBirth || null,
        phone,
        zipcode: account.zipcode || null,
        locationId: event.location_id,
        uiLanguage: account.uiLanguage,
        resetPassword: providedPassword ? 'N' : 'Y'
      });
      createdAccount = true;
      createdWithDefaultPassword = !providedPassword;
      createdUsername = username;
    }

    let registrationId;
    if (existingRegistrationId) {
      registrationId = existingRegistrationId;
    } else {
      const [existingReg] = await connection.query(
        'SELECT id FROM health_event_registration WHERE health_event_id = ? AND user_id = ? AND registration_role = "beneficiary" LIMIT 1',
        [event.id, userId]);
      if (existingReg.length) {
        await connection.rollback();
        return res.status(409).json({ error: 'ALREADY_REGISTERED', registration_id: existingReg[0].id });
      }

      const [regInsert] = await connection.query(
        'INSERT INTO health_event_registration(health_event_id, user_id, registration_role, contact_email, source, submitted_at) \
         VALUES (?,?,?,?,?,NOW())',
        [event.id, userId, 'beneficiary', account.email || (authUser ? authUser.email : null), 'web']);
      registrationId = regInsert.insertId;
    }

    await upsertAnswers(connection, registrationId, answers, questionsById, 'web-register');
    await syncRegistrationDates(connection, registrationId, answers, questionsById);
    await bookAppointments(connection, registrationId, appointments);

    await connection.commit();

    // Post-commit emails (best effort): confirmation to the beneficiary +
    // notification to the admin-configured beneficiary recipient list.
    // Token fallbacks apply only when the registrant IS the token holder
    // (logged-in beneficiary); a logged-in volunteer/admin registering a
    // walk-in must never receive the walk-in's confirmation.
    // Completar respuestas sobre una registración existente NO re-envía la
    // confirmación (ya la recibieron al registrarse/importarse).
    if (!existingRegistrationId) {
      const tokenIsRegistrant = !createdAccount && !!authUser;
      dispatchRegistrationEmails(event, registrationId, 'beneficiary', {
        to: (account.email && String(account.email).trim()) || (tokenIsRegistrant ? authUser.email : null) || null,
        language: account.uiLanguage || (tokenIsRegistrant ? authUser.language : null) || null,
        firstname: account.firstName || (tokenIsRegistrant ? authUser.firstname : null) || null,
        // Accounts created with the shared default password get their credentials
        // in the confirmation email so people can sign in later at the event.
        credentials: createdWithDefaultPassword
          ? { username: createdUsername, password: DEFAULT_HEALTH_PASSWORD }
          : null
      });
    }

    let token = null;
    if (createdAccount) {
      try {
        token = await buildSigninToken(userId);
      } catch (tokenError) {
        logger.error('healthEvents auto-login token error: ' + tokenError.message);
      }
    }
    res.status(200).json({
      registration_id: registrationId,
      already_registered: !!existingRegistrationId,
      token,
      reset_password: createdWithDefaultPassword ? 'Y' : 'N',
      // Recordatorio en pantalla: hay gente que no deja email, así que el
      // formulario muestra estas credenciales al terminar el registro.
      credentials: createdWithDefaultPassword
        ? { username: createdUsername, password: DEFAULT_HEALTH_PASSWORD }
        : null
    });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    if (error.code === 'SLOT_FULL' || error.code === 'SLOT_NOT_FOUND') {
      return res.status(409).json({ error: error.code, slot_id: error.slot_id });
    }
    if (error && error.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'DUPLICATE', message: error.message });
    }
    logger.error('POST /health-events/:slug/register error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.post('/health-events/:slug/register/volunteer', async (req, res) => {
  let connection;
  try {
    const event = await getEventBySlug(String(req.params.slug).toLowerCase());
    if (!event || event.enabled !== 'Y') {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    if (hasEventEnded(event)) {
      return res.status(410).json({ error: 'EVENT_ENDED' });
    }
    if (!isRegistrationOpen(event)) {
      return res.status(410).json({
        error: 'REGISTRATION_CLOSED',
        registration_closes_at: toSqlDateTimeString(event.registration_closes_at)
      });
    }

    const account = req.body.account || {};
    const answers = Array.isArray(req.body.answers) ? req.body.answers : [];
    const email = account.email ? String(account.email).trim() : null;
    if (!account.firstName || !account.phone || !email) {
      logger.error('POST /health-events/:slug/register/volunteer 400 INVALID_ACCOUNT_DATA (missing firstName/phone/email)');
      return res.status(400).json({ error: 'INVALID_ACCOUNT_DATA' });
    }
    if (!isLegalConsentAccepted(account.legalConsentAccepted)) {
      logger.error('POST /health-events/:slug/register/volunteer 400 LEGAL_CONSENT_REQUIRED (payload did not send legalConsentAccepted=true)');
      return res.status(400).json({
        error: 'LEGAL_CONSENT_REQUIRED',
        message: 'Legal consent must be accepted to register as a volunteer'
      });
    }
    if (!/^[0-9]{10}$/.test(String(account.phone).trim())) {
      logger.error('POST /health-events/:slug/register/volunteer 400 INVALID_ACCOUNT_DATA (phone not 10 digits)');
      return res.status(400).json({ error: 'INVALID_ACCOUNT_DATA' });
    }

    const forms = await fetchForms(event.id, 'volunteer');
    const allQuestions = forms.flatMap(f => f.questions);
    const questionsById = new Map(allQuestions.map(q => [q.id, q]));

    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();

    const [emailTaken] = await connection.query('SELECT id FROM user WHERE email = ? LIMIT 1', [email]);
    if (emailTaken.length) {
      await connection.rollback();
      return res.status(409).json({ error: 'EMAIL_TAKEN' });
    }

    const username = await generateUniqueUsername(connection, account.firstName, account.lastName);
    const password = DEFAULT_HEALTH_PASSWORD;
    const passwordHash = await bcryptjs.hash(password, 8);

    // Security: self-registered volunteer accounts start DISABLED and cannot
    // sign in until an admin approves them from the event's Volunteers tab.
    const userId = await createHealthEventUser(connection, {
      username,
      passwordHash,
      email,
      roleId: 11,
      firstName: account.firstName,
      lastName: account.lastName,
      dateOfBirth: account.dateOfBirth || null,
      phone: String(account.phone).trim(),
      zipcode: account.zipcode || null,
      locationId: event.location_id,
      uiLanguage: account.uiLanguage,
      enabled: 'N',
      resetPassword: 'Y'
    });

    const [regInsert] = await connection.query(
      'INSERT INTO health_event_registration(health_event_id, user_id, registration_role, contact_email, source, submitted_at) \
       VALUES (?,?,?,?,?,NOW())', [event.id, userId, 'volunteer', email, 'web']);
    const registrationId = regInsert.insertId;

    await upsertAnswers(connection, registrationId, answers, questionsById, 'web-register');
    await connection.commit();

    // Registrant emails (best effort, after commit): credentials plus the same
    // signed Terms & Conditions copy sent by the general volunteer form.
    try {
      const emailModule = require('../email/email');
      const language = account.uiLanguage === 'es' ? 'es' : 'en';
      if (typeof emailModule.sendHealthEventVolunteerCredentials === 'function') {
        emailModule.sendHealthEventVolunteerCredentials({
          to: email,
          language,
          eventNameEn: event.name_en,
          eventNameEs: event.name_es,
          username,
          password,
          pendingApproval: true
        }).catch(() => { /* logged inside */ });
      }
      if (typeof emailModule.sendVolunteerConfirmation === 'function') {
        const locationName = [event.organization, event.community_city].filter(Boolean).join(' — ')
          || event.name_en
          || event.name_es
          || '';
        emailModule.sendVolunteerConfirmation(email, locationName, language)
          .catch(() => { /* logged inside */ });
      }
    } catch (emailError) {
      logger.error('healthEvents volunteer registrant email error: ' + emailError.message);
    }

    // Notification to the admin-configured volunteer recipient list (best effort).
    dispatchRegistrationEmails(event, registrationId, 'volunteer');

    res.status(200).json({ registration_id: registrationId, credentials: { username, password }, pending_approval: true });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    if (error && error.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'DUPLICATE', message: error.message });
    }
    logger.error('POST /health-events/:slug/register/volunteer error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

// =====================================================================
// BENEFICIARY ENDPOINTS
// =====================================================================

router.get('/health-events/mine', verifyToken, requireBeneficiary, async (req, res) => {
  try {
    const userId = req.currentUser.id;
    const [events] = await mysqlConnection.promise().query(
      `SELECT he.*, l.organization, l.community_city, l.address,
              r.id AS registration_id
       FROM health_event he
       INNER JOIN location l ON l.id = he.location_id
       LEFT JOIN health_event_registration r
         ON r.health_event_id = he.id AND r.user_id = ? AND r.registration_role = 'beneficiary' AND r.status = 'registered'
       WHERE he.enabled = 'Y' AND (r.id IS NOT NULL OR he.end_date >= (CURDATE() - INTERVAL 1 DAY))
       ORDER BY he.start_date ASC`, [userId]);

    const result = [];
    for (const event of events) {
      // Evento terminado: desaparece de la home del beneficiario (aunque haya
      // estado registrado) — la sección se oculta sola si era el único.
      if (hasEventEnded(event)) continue;
      const open = isRegistrationOpen(event);
      const registered = !!event.registration_id;
      if (!registered && !open) continue;

      let pending = 0;
      if (registered) {
        pending = await countPendingRequiredQuestions(event.id, event.registration_id);
      }
      const today = nowInTimezone(event.timezone).slice(0, 10);
      result.push({
        ...publicEventShape(event),
        registered,
        registration_id: event.registration_id || null,
        pending_required_questions: pending,
        event_active_today: today >= toSqlDateString(event.start_date) && today <= toSqlDateString(event.end_date)
      });
    }
    res.status(200).json({ events: result });
  } catch (error) {
    logger.error('GET /health-events/mine error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

async function countPendingRequiredQuestions(eventId, registrationId) {
  const forms = await fetchForms(eventId, 'beneficiary');
  const gatingForms = forms.filter(f => f.required_before_qr === 'Y');
  if (!gatingForms.length) return 0;
  // Visibility is computed over the FULL list (a child depending on a filtered
  // parent must resolve the same way everywhere); appointments and notices are
  // excluded from the count itself — neither ever produces an answer row.
  const allQuestions = gatingForms.flatMap(f => f.questions);
  if (!allQuestions.length) return 0;

  const answersByQuestion = await fetchExistingAnswerOptions(registrationId);
  const answeredIds = await fetchAnsweredQuestionIds(registrationId);
  const visible = computeVisibleQuestionIds(allQuestions, answersByQuestion);
  return allQuestions.filter(q =>
    q.question_type !== 'appointment' && q.question_type !== 'notice' &&
    visible.has(q.id) && q.required === 'Y' && !answeredIds.has(q.id)).length;
}

async function fetchExistingAnswerOptions(registrationId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT a.question_id, ao.option_id
     FROM health_event_answer a
     LEFT JOIN health_event_answer_option ao ON ao.answer_id = a.id
     WHERE a.registration_id = ?`, [registrationId]);
  const map = new Map();
  for (const row of rows) {
    if (!map.has(row.question_id)) map.set(row.question_id, new Set());
    if (row.option_id != null) map.get(row.question_id).add(row.option_id);
  }
  return map;
}

async function fetchAnsweredQuestionIds(registrationId) {
  const [rows] = await mysqlConnection.promise().query(
    'SELECT question_id FROM health_event_answer WHERE registration_id = ?', [registrationId]);
  return new Set(rows.map(r => r.question_id));
}

async function getRegistrationForUser(eventId, userId, role = 'beneficiary') {
  const [rows] = await mysqlConnection.promise().query(
    'SELECT * FROM health_event_registration WHERE health_event_id = ? AND user_id = ? AND registration_role = ? AND status = "registered" LIMIT 1',
    [eventId, userId, role]);
  return rows.length ? rows[0] : null;
}

router.get('/health-events/:eventId(\\d+)/pending-questions', verifyToken, requireBeneficiary, async (req, res) => {
  try {
    const eventId = Number(req.params.eventId);
    const registration = await getRegistrationForUser(eventId, req.currentUser.id);
    if (!registration) {
      return res.status(404).json({ error: 'NOT_REGISTERED' });
    }
    const forms = await fetchForms(eventId, 'beneficiary');
    const gatingForms = forms.filter(f => f.required_before_qr === 'Y');
    const answersByQuestion = await fetchExistingAnswerOptions(registration.id);
    const answeredIds = await fetchAnsweredQuestionIds(registration.id);

    const resultForms = gatingForms.map(form => {
      // Visibility over the FULL list (same dependency resolution as the count);
      // notices carry no answer, so they can never be "pending".
      const visible = computeVisibleQuestionIds(form.questions, answersByQuestion);
      const questions = form.questions.filter(q => q.question_type !== 'appointment' && q.question_type !== 'notice');
      // Return unanswered questions plus their (possibly answered) parents so the
      // client can evaluate dependencies; mark answered ones.
      const unanswered = questions.filter(q => !answeredIds.has(q.id));
      if (!unanswered.length) return null;
      return {
        ...form,
        questions: questions.map(q => ({ ...q, previously_answered: answeredIds.has(q.id), visible_now: visible.has(q.id) }))
      };
    }).filter(Boolean);

    res.status(200).json({ registration_id: registration.id, forms: resultForms });
  } catch (error) {
    logger.error('GET /health-events/:id/pending-questions error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.post('/health-events/:eventId(\\d+)/answers', verifyToken, requireBeneficiary, async (req, res) => {
  let connection;
  try {
    const eventId = Number(req.params.eventId);
    const registration = await getRegistrationForUser(eventId, req.currentUser.id);
    if (!registration) {
      return res.status(404).json({ error: 'NOT_REGISTERED' });
    }
    const answers = Array.isArray(req.body.answers) ? req.body.answers : [];
    const forms = await fetchForms(eventId, 'beneficiary');
    const questionsById = new Map(forms.flatMap(f => f.questions).map(q => [q.id, q]));

    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();
    const saved = await upsertAnswers(connection, registration.id, answers, questionsById, 'beneficiary-home');
    await syncRegistrationDates(connection, registration.id, answers, questionsById);
    await connection.commit();
    res.status(200).json({ saved });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('POST /health-events/:id/answers error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.post('/health-events/:eventId(\\d+)/self-register', verifyToken, requireBeneficiary, async (req, res) => {
  try {
    const eventId = Number(req.params.eventId);
    const event = await getEventById(eventId);
    if (!event || event.enabled !== 'Y') {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    if (!isRegistrationOpen(event)) {
      return res.status(410).json({ error: 'REGISTRATION_CLOSED' });
    }
    const existing = await getRegistrationForUser(eventId, req.currentUser.id);
    if (existing) {
      return res.status(200).json({ registration_id: existing.id, already_registered: true });
    }
    const [inserted] = await mysqlConnection.promise().query(
      'INSERT INTO health_event_registration(health_event_id, user_id, registration_role, contact_email, source, submitted_at) \
       VALUES (?,?,?,?,?,NOW())',
      [eventId, req.currentUser.id, 'beneficiary', req.currentUser.email || null, 'web']);

    // Post-commit emails (best effort): confirmation + beneficiary list notification.
    // language falls back to the user row inside dispatchRegistrationEmails when
    // the token was minted without it.
    dispatchRegistrationEmails(event, inserted.insertId, 'beneficiary', {
      to: req.currentUser.email || null,
      language: req.currentUser.language || null,
      firstname: req.currentUser.firstname || null
    });

    res.status(200).json({ registration_id: inserted.insertId, already_registered: false });
  } catch (error) {
    logger.error('POST /health-events/:id/self-register error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

/**
 * Short-lived backup PIN for event day: shown next to the event QR so a
 * volunteer can type it when the QR cannot be scanned. Shares the PIN pool
 * with food distribution (beneficiary_delivery_pin).
 */
router.post('/health-events/:eventId(\\d+)/pin', verifyToken, requireBeneficiary, async (req, res) => {
  try {
    const eventId = Number(req.params.eventId);
    const event = await getEventById(eventId);
    if (!event || event.enabled !== 'Y') {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const registration = await getRegistrationForUser(eventId, req.currentUser.id);
    if (!registration) {
      return res.status(403).json({ error: 'NOT_REGISTERED' });
    }
    const pin = await createBeneficiaryPin(req.currentUser.id, event.location_id);
    res.status(200).json(pin);
  } catch (error) {
    logger.error('POST /health-events/:id/pin error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

// =====================================================================
// VOLUNTEER CONSOLE ENDPOINTS
// =====================================================================

router.get('/health-events/volunteer/context', verifyToken, requireVolunteer, async (req, res) => {
  try {
    const [events] = await mysqlConnection.promise().query(
      `SELECT he.*, l.organization, l.community_city, l.address
       FROM health_event he INNER JOIN location l ON l.id = he.location_id
       WHERE he.enabled = 'Y' AND he.end_date >= (CURDATE() - INTERVAL 7 DAY)
       ORDER BY he.start_date ASC`);

    const eventIds = events.map(e => e.id);
    let stands = [];
    let services = [];
    if (eventIds.length) {
      const [standRows] = await mysqlConnection.promise().query(
        `SELECT * FROM health_event_stand
         WHERE health_event_id IN (${eventIds.map(() => '?').join(',')}) AND enabled = 'Y'
         ORDER BY sort_order ASC, id ASC`, eventIds);
      stands = standRows;
      if (stands.length) {
        const [serviceRows] = await mysqlConnection.promise().query(
          `SELECT * FROM health_event_stand_service
           WHERE stand_id IN (${stands.map(() => '?').join(',')}) AND enabled = 'Y'
           ORDER BY sort_order ASC, id ASC`, stands.map(s => s.id));
        services = serviceRows;
      }
    }

    const [assignments] = await mysqlConnection.promise().query(
      'SELECT * FROM health_event_volunteer_assignment WHERE user_id = ? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1',
      [req.currentUser.id]);

    const servicesByStand = new Map();
    for (const service of services) {
      if (!servicesByStand.has(service.stand_id)) servicesByStand.set(service.stand_id, []);
      servicesByStand.get(service.stand_id).push({
        id: service.id, name_en: service.name_en, name_es: service.name_es
      });
    }

    res.status(200).json({
      events: events.map(event => {
        const today = nowInTimezone(event.timezone).slice(0, 10);
        return {
          ...publicEventShape(event),
          active: today >= toSqlDateString(event.start_date) && today <= toSqlDateString(event.end_date),
          stands: stands.filter(s => s.health_event_id === event.id).map(s => ({
            id: s.id,
            name_en: s.name_en,
            name_es: s.name_es,
            icon: s.icon,
            is_entry: s.is_entry,
            has_checkout: s.has_checkout,
            services: servicesByStand.get(s.id) || []
          }))
        };
      }),
      active_assignment: assignments.length ? {
        id: assignments[0].id,
        event_id: assignments[0].health_event_id,
        stand_id: assignments[0].stand_id,
        service_id: assignments[0].service_id
      } : null
    });
  } catch (error) {
    logger.error('GET /health-events/volunteer/context error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.post('/health-events/volunteer/assignment', verifyToken, requireVolunteer, async (req, res) => {
  try {
    const eventId = Number.parseInt(req.body.event_id, 10);
    const standId = Number.parseInt(req.body.stand_id, 10);
    const serviceId = req.body.service_id != null ? Number.parseInt(req.body.service_id, 10) : null;
    if (!Number.isInteger(eventId) || !Number.isInteger(standId)) {
      return res.status(400).json({ error: 'INVALID_DATA' });
    }
    const [standRows] = await mysqlConnection.promise().query(
      `SELECT s.id, he.timezone AS event_timezone, he.end_date AS event_end_date
       FROM health_event_stand s
       INNER JOIN health_event he ON he.id = s.health_event_id
       WHERE s.id = ? AND s.health_event_id = ? AND s.enabled = "Y" LIMIT 1`,
      [standId, eventId]);
    if (!standRows.length) {
      return res.status(404).json({ error: 'STAND_NOT_FOUND' });
    }
    if (hasEventEnded({ timezone: standRows[0].event_timezone, end_date: standRows[0].event_end_date })) {
      return res.status(410).json({ error: 'EVENT_ENDED' });
    }
    await mysqlConnection.promise().query(
      'UPDATE health_event_volunteer_assignment SET ended_at = NOW() WHERE user_id = ? AND ended_at IS NULL',
      [req.currentUser.id]);
    const [inserted] = await mysqlConnection.promise().query(
      'INSERT INTO health_event_volunteer_assignment(health_event_id, user_id, stand_id, service_id) VALUES (?,?,?,?)',
      [eventId, req.currentUser.id, standId, Number.isInteger(serviceId) ? serviceId : null]);
    res.status(200).json({ assignment_id: inserted.insertId });
  } catch (error) {
    logger.error('POST /health-events/volunteer/assignment error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.delete('/health-events/volunteer/assignment', verifyToken, requireVolunteer, async (req, res) => {
  try {
    await mysqlConnection.promise().query(
      'UPDATE health_event_volunteer_assignment SET ended_at = NOW() WHERE user_id = ? AND ended_at IS NULL',
      [req.currentUser.id]);
    res.status(200).json({});
  } catch (error) {
    logger.error('DELETE /health-events/volunteer/assignment error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

/** Digits-only phone normalizer; SQL-side twin is stripPhoneSql(). */
function normalizePhoneDigits(value) {
  return String(value == null ? '' : value).replace(/\D+/g, '');
}

/** MySQL 5.7-safe expression stripping common phone punctuation from a column. */
function stripPhoneSql(column) {
  return `REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(${column}, '-', ''), ' ', ''), '(', ''), ')', ''), '+', ''), '.', '')`;
}

router.post('/health-events/scan', verifyToken, requireVolunteer, async (req, res) => {
  let connection;
  try {
    const eventId = Number.parseInt(req.body.event_id, 10);
    const standId = Number.parseInt(req.body.stand_id, 10);
    const serviceId = req.body.service_id != null ? Number.parseInt(req.body.service_id, 10) : null;
    if (!Number.isInteger(eventId) || !Number.isInteger(standId)) {
      return res.status(400).json({ error: 'INVALID_DATA' });
    }

    const [standRows] = await mysqlConnection.promise().query(
      `SELECT s.*, he.timezone AS event_timezone, he.end_date AS event_end_date
       FROM health_event_stand s
       INNER JOIN health_event he ON he.id = s.health_event_id
       WHERE s.id = ? AND s.health_event_id = ? AND s.enabled = "Y" LIMIT 1`,
      [standId, eventId]);
    if (!standRows.length) {
      return res.status(404).json({ error: 'STAND_NOT_FOUND' });
    }
    const stand = standRows[0];

    // Evento terminado (zona horaria del evento): la consola queda de solo
    // lectura — no se registran más check-ins/check-outs.
    if (hasEventEnded({ timezone: stand.event_timezone, end_date: stand.event_end_date })) {
      return res.status(410).json({ error: 'EVENT_ENDED' });
    }

    // ---- Identity resolution: QR (primary) | PIN | phone (manual fallbacks) ----
    let scannedUserId = NaN;
    let identityMethod = null;
    if (req.body.qr != null) {
      identityMethod = 'qr';
      const qr = parseDailyBeneficiaryQr(req.body.qr);
      scannedUserId = qr ? Number.parseInt(qr.id, 10) : NaN;
      if (!Number.isInteger(scannedUserId) || scannedUserId <= 0) {
        return res.status(400).json({ error: 'INVALID_QR' });
      }
      if (!isCurrentDailyBeneficiaryQr(qr, stand.event_timezone)) {
        return res.status(400).json({ error: 'INVALID_QR', reason: 'expired' });
      }
    } else if (req.body.pin != null) {
      identityMethod = 'pin';
      const resolved = await resolveBeneficiaryPin(req.body.pin);
      if (!resolved) {
        return res.status(404).json({ error: 'PIN_INVALID' });
      }
      scannedUserId = resolved.user_id;
    } else if (req.body.phone != null) {
      identityMethod = 'phone';
      const digits = normalizePhoneDigits(req.body.phone);
      if (digits.length < 7) {
        return res.status(400).json({ error: 'INVALID_PHONE' });
      }
      // Entry stands may admit walk-ins with an existing account, so they search
      // all beneficiaries; other stands only match people registered in the event.
      const registrationJoin = stand.is_entry === 'Y'
        ? ''
        : `INNER JOIN health_event_registration r ON r.user_id = u.id AND r.health_event_id = ${eventId}
             AND r.registration_role = 'beneficiary' AND r.status = 'registered'`;
      const [matches] = await mysqlConnection.promise().query(
        `SELECT DISTINCT u.id, u.firstname, u.lastname
         FROM user u
         ${registrationJoin}
         WHERE u.deleted = 'N' AND u.enabled = 'Y' AND u.role_id = 5
           AND RIGHT(${stripPhoneSql('u.phone')}, 10) = RIGHT(?, 10)
         LIMIT 12`, [digits]);
      if (!matches.length) {
        return res.status(404).json({ error: 'PHONE_NOT_FOUND' });
      }
      const pickedUserId = req.body.user_id != null ? Number.parseInt(req.body.user_id, 10) : null;
      if (matches.length > 1 && !pickedUserId) {
        // Volunteer must confirm WHO the person is (families share phones).
        return res.status(200).json({
          candidates: matches.map(m => ({ user_id: m.id, firstname: m.firstname, lastname: m.lastname }))
        });
      }
      const chosen = pickedUserId ? matches.find(m => m.id === pickedUserId) : matches[0];
      if (!chosen) {
        return res.status(404).json({ error: 'PHONE_NOT_FOUND' });
      }
      scannedUserId = chosen.id;
    } else {
      return res.status(400).json({ error: 'INVALID_QR' });
    }

    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();

    // Serialize every scan for this beneficiary. This closes the race where
    // two cameras/requests could both observe the same open state and insert.
    const [userRows] = await connection.query(
      'SELECT id, firstname, lastname, enabled FROM user WHERE id = ? AND deleted = "N" LIMIT 1 FOR UPDATE',
      [scannedUserId]);
    if (!userRows.length || userRows[0].enabled !== 'Y') {
      await connection.rollback();
      return res.status(404).json({ error: 'USER_NOT_FOUND' });
    }
    const person = { firstname: userRows[0].firstname, lastname: userRows[0].lastname };

    // Registration lookup / walk-in auto-registration at the entry stand.
    let [regRows] = await connection.query(
      'SELECT * FROM health_event_registration WHERE health_event_id = ? AND user_id = ? AND registration_role = "beneficiary" AND status = "registered" LIMIT 1 FOR UPDATE',
      [eventId, scannedUserId]);
    let registration = regRows.length ? regRows[0] : null;
    // Walk-ins (registration created right here) skip the pending-questions
    // confirmation below: they obviously haven't answered anything yet and the
    // entry line must keep moving.
    const preExistingRegistration = !!registration;
    if (!registration) {
      if (stand.is_entry === 'Y') {
        const [regInsert] = await connection.query(
          'INSERT INTO health_event_registration(health_event_id, user_id, registration_role, source, submitted_at) \
           VALUES (?,?,?,?,NOW())', [eventId, scannedUserId, 'beneficiary', 'walkin']);
        [regRows] = await connection.query('SELECT * FROM health_event_registration WHERE id = ?', [regInsert.insertId]);
        registration = regRows[0];
      } else {
        await connection.rollback();
        // Security: no identity details for people without a registration in this
        // event — prevents user enumeration by scanning arbitrary ids.
        return res.status(404).json({ error: 'NOT_REGISTERED' });
      }
    }

    // Idempotency MUST run before selecting check-in/checkout. QR decoding is
    // continuous, so a process-local sliding window also remains alive while
    // the same paper stays in frame. The DB window is the durable fallback.
    const debounceKey = buildHealthScanDebounceKey(eventId, standId, scannedUserId);
    let recentScan = identityMethod === 'qr' ? healthQrSlidingDebounce.take(debounceKey) : null;
    if (!recentScan) {
      recentScan = await findRecentHealthScan(connection, {
        eventId,
        standId,
        userId: scannedUserId,
        windowSeconds: DUPLICATE_SCAN_WINDOW_SECONDS
      });
    }

    let scanId = null;
    let scanType = null;
    let duplicate = false;
    if (recentScan) {
      duplicate = true;
      scanId = recentScan.scanId;
      scanType = recentScan.scanType;
    } else {
      // Determine scan type only after both duplicate guards have passed.
      scanType = 'checkin';
      let pairedScanId = null;
      if (stand.has_checkout === 'Y') {
        // "Mismo día" en la zona horaria DEL EVENTO, no del servidor: con el
        // server en UTC, un check-in antes de las 5pm PT y su check-out después
        // caían en días UTC distintos y el segundo escaneo quedaba como otro
        // check-in. COALESCE mantiene el comportamiento viejo si la base no
        // tiene cargadas las tablas de timezones (CONVERT_TZ => NULL en dev).
        const [openCheckins] = await connection.query(
          `SELECT s.id FROM health_event_scan s
           WHERE s.stand_id = ? AND s.scanned_user_id = ? AND s.scan_type = 'checkin'
             AND DATE(COALESCE(CONVERT_TZ(s.scanned_at, @@session.time_zone, ?), s.scanned_at))
               = DATE(COALESCE(CONVERT_TZ(NOW(), @@session.time_zone, ?), NOW()))
             AND NOT EXISTS (SELECT 1 FROM health_event_scan c WHERE c.paired_scan_id = s.id)
           ORDER BY s.scanned_at DESC LIMIT 1 FOR UPDATE`,
          [standId, scannedUserId, stand.event_timezone, stand.event_timezone]);
        if (openCheckins.length) {
          scanType = 'checkout';
          pairedScanId = openCheckins[0].id;
        }
      }

      // Confirmación en dos fases: si la persona (ya registrada) todavía debe
      // preguntas requeridas del evento, avisar al voluntario ANTES de registrar
      // el check-in y dejar que decida (clientes nuevos mandan confirmed=false en
      // el primer intento; los viejos no mandan el campo y conservan el flujo directo).
      if (scanType === 'checkin' && preExistingRegistration && req.body.confirmed === false) {
        const pendingRequired = await countPendingRequiredQuestions(eventId, registration.id);
        if (pendingRequired > 0) {
          await connection.rollback();
          return res.status(200).json({
            requires_confirmation: true,
            pending_required_questions: pendingRequired,
            person
          });
        }
      }

      const [scanInsert] = await connection.query(
        'INSERT INTO health_event_scan(health_event_id, stand_id, service_id, registration_id, scanned_user_id, volunteer_user_id, scan_type, paired_scan_id) \
         VALUES (?,?,?,?,?,?,?,?)',
        [eventId, standId, Number.isInteger(serviceId) ? serviceId : null, registration.id,
          scannedUserId, req.currentUser.id, scanType, pairedScanId]);
      scanId = scanInsert.insertId;
    }
    await connection.commit();

    // Remember every successful state transition (including manual overrides),
    // but consult this process-local layer only for QR requests.
    healthQrSlidingDebounce.remember(debounceKey, scanId, scanType);

    // Context payload (outside the transaction).
    const [dates] = await mysqlConnection.promise().query(
      'SELECT event_date, priority_service FROM health_event_registration_date WHERE registration_id = ? ORDER BY event_date ASC',
      [registration.id]);
    const [appointments] = await mysqlConnection.promise().query(
      `SELECT sl.service_key, sl.slot_date, TIME_FORMAT(sl.start_time, '%H:%i') AS start_time,
              TIME_FORMAT(sl.end_time, '%H:%i') AS end_time
       FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
       WHERE a.registration_id = ? AND a.status = 'booked'
       ORDER BY sl.slot_date ASC, sl.start_time ASC`, [registration.id]);

    let checkoutForm = null;
    if (scanType === 'checkout' && !duplicate) {
      const forms = await fetchForms(eventId, 'checkout', standId);
      checkoutForm = forms.length ? forms[0] : null;
    }

    res.status(200).json({
      scan_id: scanId,
      scan_type: scanType,
      duplicate,
      person,
      registration: {
        id: registration.id,
        status: registration.status,
        source: registration.source,
        dates: dates.map(d => ({ event_date: toSqlDateString(d.event_date), priority_service: d.priority_service })),
        appointments: appointments.map(a => ({ ...a, slot_date: toSqlDateString(a.slot_date) }))
      },
      checkout_form: checkoutForm
    });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('POST /health-events/scan error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.post('/health-events/scan/:scanId(\\d+)/answers', verifyToken, requireVolunteer, async (req, res) => {
  let connection;
  try {
    const scanId = Number(req.params.scanId);
    const [scanRows] = await mysqlConnection.promise().query(
      'SELECT * FROM health_event_scan WHERE id = ? LIMIT 1', [scanId]);
    if (!scanRows.length) {
      return res.status(404).json({ error: 'SCAN_NOT_FOUND' });
    }
    const scan = scanRows[0];
    if (scan.volunteer_user_id !== req.currentUser.id && !ADMIN_ROLES.includes(req.currentUser.role)) {
      return res.status(403).json({ error: 'FORBIDDEN' });
    }

    // Evento terminado: los voluntarios ya no cargan respuestas de checkout;
    // los admins conservan la corrección posterior de datos.
    if (!ADMIN_ROLES.includes(req.currentUser.role)) {
      const scanEvent = await getEventById(scan.health_event_id);
      if (scanEvent && hasEventEnded(scanEvent)) {
        return res.status(410).json({ error: 'EVENT_ENDED' });
      }
    }

    const forms = await fetchForms(scan.health_event_id, 'checkout', scan.stand_id);
    const questionsById = new Map(forms.flatMap(f => f.questions).map(q => [q.id, q]));
    const answers = Array.isArray(req.body.answers) ? req.body.answers : [];

    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();
    let saved = 0;
    for (const item of answers) {
      const question = questionsById.get(item.question_id);
      if (!question) continue;

      let answerText = null;
      let answerNumber = null;
      let optionIds = [];
      if (question.question_type === 'text') {
        if (item.answer == null || String(item.answer).trim() === '') continue;
        answerText = String(item.answer).trim().slice(0, 5000);
      } else if (question.question_type === 'number') {
        if (item.answer == null || Number.isNaN(Number(item.answer))) continue;
        answerNumber = Number(item.answer);
      } else if (question.question_type === 'single') {
        const optionId = Number.parseInt(item.answer, 10);
        if (!Number.isInteger(optionId)) continue;
        optionIds = [optionId];
      } else if (question.question_type === 'multiple') {
        optionIds = (Array.isArray(item.answer) ? item.answer : [])
          .map(v => Number.parseInt(v, 10)).filter(Number.isInteger);
        if (!optionIds.length) continue;
      } else {
        continue;
      }

      const [existing] = await connection.query(
        'SELECT id FROM health_event_scan_answer WHERE scan_id = ? AND question_id = ? LIMIT 1',
        [scanId, item.question_id]);
      let answerId;
      if (existing.length) {
        answerId = existing[0].id;
        await connection.query('UPDATE health_event_scan_answer SET answer_text = ?, answer_number = ? WHERE id = ?',
          [answerText, answerNumber, answerId]);
        await connection.query('DELETE FROM health_event_scan_answer_option WHERE scan_answer_id = ?', [answerId]);
      } else {
        const [inserted] = await connection.query(
          'INSERT INTO health_event_scan_answer(scan_id, question_id, answer_text, answer_number) VALUES (?,?,?,?)',
          [scanId, item.question_id, answerText, answerNumber]);
        answerId = inserted.insertId;
      }
      for (const optionId of optionIds) {
        await connection.query(
          'INSERT IGNORE INTO health_event_scan_answer_option(scan_answer_id, option_id) VALUES (?,?)',
          [answerId, optionId]);
      }
      saved++;
    }
    await connection.commit();
    res.status(200).json({ saved });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('POST /health-events/scan/:id/answers error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.get('/health-events/stand/:standId(\\d+)/attending', verifyToken, requireVolunteer, async (req, res) => {
  try {
    const standId = Number(req.params.standId);
    const [rows] = await mysqlConnection.promise().query(
      `SELECT s.id AS scan_id, s.scanned_user_id AS user_id, u.firstname, u.lastname,
              s.scanned_at AS checked_in_at, s.service_id, ss.name_en AS service_name_en,
              ss.name_es AS service_name_es,
              TRIM(CONCAT(vu.firstname, ' ', COALESCE(vu.lastname, ''))) AS volunteer_name
       FROM health_event_scan s
       INNER JOIN user u ON u.id = s.scanned_user_id
       INNER JOIN user vu ON vu.id = s.volunteer_user_id
       LEFT JOIN health_event_stand_service ss ON ss.id = s.service_id
       WHERE s.stand_id = ? AND s.scan_type = 'checkin' AND DATE(s.scanned_at) = CURDATE()
         AND NOT EXISTS (SELECT 1 FROM health_event_scan c WHERE c.paired_scan_id = s.id)
       ORDER BY s.scanned_at DESC
       LIMIT 100`, [standId]);
    res.status(200).json({ attending: rows });
  } catch (error) {
    logger.error('GET /health-events/stand/:id/attending error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.get('/health-events/volunteer/recent-scans', verifyToken, requireVolunteer, async (req, res) => {
  try {
    const standId = Number.parseInt(req.query.stand_id, 10);
    const limit = Math.min(Number.parseInt(req.query.limit, 10) || 20, 50);
    const params = [req.currentUser.id];
    let standFilter = '';
    if (Number.isInteger(standId)) {
      standFilter = ' AND s.stand_id = ?';
      params.push(standId);
    }
    params.push(limit);
    const [rows] = await mysqlConnection.promise().query(
      `SELECT s.id AS scan_id, s.scan_type, s.scanned_at, s.service_id, u.firstname, u.lastname
       FROM health_event_scan s INNER JOIN user u ON u.id = s.scanned_user_id
       WHERE s.volunteer_user_id = ?${standFilter}
       ORDER BY s.scanned_at DESC LIMIT ?`, params);
    res.status(200).json({ scans: rows });
  } catch (error) {
    logger.error('GET /health-events/volunteer/recent-scans error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

/**
 * Entry-desk permission: the requester must hold an ACTIVE assignment on an
 * entry stand of the event (admins always pass). The entry desk helps people
 * find their registration and hands out sign-in credentials, so it may browse
 * the event's beneficiary list — other stands may not.
 */
async function hasActiveEntryAssignment(userId, eventId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT a.id
     FROM health_event_volunteer_assignment a
     INNER JOIN health_event_stand st ON st.id = a.stand_id
     WHERE a.user_id = ? AND a.health_event_id = ? AND a.ended_at IS NULL AND st.is_entry = 'Y'
     LIMIT 1`, [userId, eventId]);
  return rows.length > 0;
}

/**
 * Assignments are self-service (volunteers pick their stand), so an assignment
 * alone is not a permission boundary. The entry desk additionally requires an
 * APPROVED volunteer registration for THIS event — a volunteer from another
 * event cannot self-assign here and browse/reset this event's registrants.
 */
async function canOperateEntryDesk(currentUser, eventId) {
  if (ADMIN_ROLES.includes(currentUser.role)) return true;
  const [registered] = await mysqlConnection.promise().query(
    `SELECT r.id
     FROM health_event_registration r
     INNER JOIN user u ON u.id = r.user_id
     WHERE r.health_event_id = ? AND r.user_id = ? AND r.registration_role = 'volunteer'
       AND r.status = 'registered' AND u.enabled = 'Y'
     LIMIT 1`, [eventId, currentUser.id]);
  if (!registered.length) return false;
  return hasActiveEntryAssignment(currentUser.id, eventId);
}

/** Paged beneficiary registrant list for entry-stand volunteers (search included). */
router.get('/health-events/:id(\\d+)/registrants', verifyToken, requireVolunteer, async (req, res) => {
  try {
    const eventId = Number(req.params.id);
    if (!(await canOperateEntryDesk(req.currentUser, eventId))) {
      return res.status(403).json({ error: 'FORBIDDEN', message: 'Entry stand assignment required' });
    }

    const page = Math.max(Number.parseInt(req.query.page, 10) || 1, 1);
    const pageSize = Math.min(Math.max(Number.parseInt(req.query.pageSize, 10) || 25, 1), 100);
    const search = String(req.query.search || '').trim();

    const params = [eventId];
    let searchFilter = '';
    if (search) {
      searchFilter = ' AND (u.firstname LIKE ? OR u.lastname LIKE ? OR u.email LIKE ? OR r.contact_email LIKE ? OR u.phone LIKE ? OR u.username LIKE ?)';
      const like = `%${search}%`;
      params.push(like, like, like, like, like, like);
    }

    const [countRows] = await mysqlConnection.promise().query(
      `SELECT COUNT(*) AS total
       FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
       WHERE r.health_event_id = ? AND r.registration_role = 'beneficiary'${searchFilter}`, params);

    const [rows] = await mysqlConnection.promise().query(
      `SELECT r.id AS registration_id, r.user_id, u.firstname, u.lastname, u.email, u.username, u.phone,
              u.reset_password, u.password AS password_hash, r.source, r.submitted_at,
              (SELECT GROUP_CONCAT(DATE_FORMAT(d.event_date, '%Y-%m-%d') ORDER BY d.event_date SEPARATOR ', ')
                 FROM health_event_registration_date d WHERE d.registration_id = r.id) AS dates,
              (SELECT COUNT(*) FROM health_event_scan s
                 WHERE s.registration_id = r.id AND s.scan_type = 'checkin') AS checkins,
              (SELECT GROUP_CONCAT(CONCAT(sl.service_key, ' ', DATE_FORMAT(sl.slot_date, '%m/%d'), ' ',
                      TIME_FORMAT(sl.start_time, '%H:%i')) ORDER BY sl.slot_date, sl.start_time SEPARATOR ' | ')
                 FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
                 WHERE a.registration_id = r.id AND a.status = 'booked') AS appointment_summary
       FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
       WHERE r.health_event_id = ? AND r.registration_role = 'beneficiary'${searchFilter}
       ORDER BY u.firstname ASC, u.lastname ASC, r.id DESC
       LIMIT ? OFFSET ?`, [...params, pageSize, (page - 1) * pageSize]);

    // Page-sized bcrypt checks only (≤100): tells the desk whether the person
    // can sign in with the shared default password or has set their own.
    const mapped = [];
    for (const row of rows) {
      const hasDefault = row.reset_password === 'Y' && await hasDefaultHealthPassword(row.password_hash);
      mapped.push({
        registration_id: row.registration_id,
        user_id: row.user_id,
        firstname: row.firstname,
        lastname: row.lastname,
        email: row.email,
        username: row.username,
        phone: row.phone,
        source: row.source,
        submitted_at: row.submitted_at,
        dates: row.dates,
        checkins: row.checkins,
        appointment_summary: row.appointment_summary,
        has_default_password: hasDefault
      });
    }

    res.status(200).json({ total: countRows[0].total, page, pageSize, rows: mapped });
  } catch (error) {
    logger.error('GET /health-events/:id/registrants error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

/** Entry-stand volunteers can hand a registrant back the default password. */
router.put('/health-events/:id(\\d+)/registrants/:userId(\\d+)/reset-password', verifyToken, requireVolunteer, async (req, res) => {
  try {
    const eventId = Number(req.params.id);
    const userId = Number(req.params.userId);
    if (!(await canOperateEntryDesk(req.currentUser, eventId))) {
      return res.status(403).json({ error: 'FORBIDDEN', message: 'Entry stand assignment required' });
    }
    // Evento terminado: el entry desk queda de solo lectura para voluntarios
    // (los admins conservan sus resets desde el panel del evento).
    if (!ADMIN_ROLES.includes(req.currentUser.role)) {
      const endedEvent = await getEventById(eventId);
      if (endedEvent && hasEventEnded(endedEvent)) {
        return res.status(410).json({ error: 'EVENT_ENDED' });
      }
    }
    const [registered] = await mysqlConnection.promise().query(
      `SELECT id FROM health_event_registration
       WHERE health_event_id = ? AND user_id = ? AND registration_role = 'beneficiary' LIMIT 1`,
      [eventId, userId]);
    if (!registered.length) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const credentials = await resetUserToDefaultPassword(userId, 5);
    if (!credentials) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.status(200).json({ credentials });
  } catch (error) {
    logger.error('PUT /health-events/:id/registrants/:userId/reset-password error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

// =====================================================================
// ADMIN ENDPOINTS
// =====================================================================

router.get('/health-events', verifyToken, requireAdmin, async (req, res) => {
  try {
    const [events] = await mysqlConnection.promise().query(
      `SELECT he.id, he.slug, he.name_en, he.name_es, he.location_id, he.client_id,
              he.start_date, he.end_date, he.start_time, he.end_time, he.enabled,
              he.registration_opens_at, he.registration_closes_at, he.timezone,
              l.organization, l.community_city,
              (SELECT COUNT(*) FROM health_event_registration r
                WHERE r.health_event_id = he.id AND r.registration_role = 'beneficiary' AND r.status = 'registered') AS beneficiary_count,
              (SELECT COUNT(*) FROM health_event_registration r
                WHERE r.health_event_id = he.id AND r.registration_role = 'volunteer' AND r.status = 'registered') AS volunteer_count
       FROM health_event he INNER JOIN location l ON l.id = he.location_id
       ORDER BY he.start_date DESC, he.id DESC`);
    res.status(200).json({
      events: events.map(e => ({
        ...e,
        start_date: toSqlDateString(e.start_date),
        end_date: toSqlDateString(e.end_date),
        registration_opens_at: toSqlDateTimeString(e.registration_opens_at),
        registration_closes_at: toSqlDateTimeString(e.registration_closes_at),
        registration_open: isRegistrationOpen(e),
        registrations: { beneficiary: e.beneficiary_count, volunteer: e.volunteer_count }
      }))
    });
  } catch (error) {
    logger.error('GET /health-events error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

function validateEventBody(body, isUpdate = false) {
  const errors = [];
  const slug = String(body.slug || '').toLowerCase().trim();
  if (!SLUG_REGEX.test(slug)) errors.push('INVALID_SLUG');
  if (RESERVED_SLUGS.has(slug)) errors.push('RESERVED_SLUG');
  if (!body.name_en || !body.name_es) errors.push('NAME_REQUIRED');
  if (!body.location_id) errors.push('LOCATION_REQUIRED');
  if (!body.start_date || !body.end_date) errors.push('DATES_REQUIRED');
  return { errors, slug };
}

router.post('/health-events', verifyToken, requireAdmin, async (req, res) => {
  try {
    const { errors, slug } = validateEventBody(req.body);
    if (errors.length) {
      return res.status(400).json({ error: 'VALIDATION', details: errors });
    }
    const [slugTaken] = await mysqlConnection.promise().query(
      'SELECT id FROM health_event WHERE slug = ? LIMIT 1', [slug]);
    if (slugTaken.length) {
      return res.status(409).json({ error: 'SLUG_TAKEN' });
    }
    const [inserted] = await mysqlConnection.promise().query(
      'INSERT INTO health_event(slug, name_en, name_es, location_id, client_id, start_date, end_date, start_time, end_time, \
        timezone, registration_opens_at, registration_closes_at, enabled, created_by_user_id) \
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [slug, req.body.name_en, req.body.name_es, req.body.location_id, req.body.client_id || null,
        toSqlDateString(req.body.start_date), toSqlDateString(req.body.end_date),
        req.body.start_time || null, req.body.end_time || null,
        req.body.timezone || 'America/Los_Angeles',
        toSqlDateTimeString(req.body.registration_opens_at), toSqlDateTimeString(req.body.registration_closes_at),
        req.body.enabled === 'N' ? 'N' : 'Y', req.currentUser.id]);
    res.status(200).json({ id: inserted.insertId });
  } catch (error) {
    logger.error('POST /health-events error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.put('/health-events/:id(\\d+)', verifyToken, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { errors, slug } = validateEventBody(req.body, true);
    if (errors.length) {
      return res.status(400).json({ error: 'VALIDATION', details: errors });
    }
    const [slugTaken] = await mysqlConnection.promise().query(
      'SELECT id FROM health_event WHERE slug = ? AND id <> ? LIMIT 1', [slug, id]);
    if (slugTaken.length) {
      return res.status(409).json({ error: 'SLUG_TAKEN' });
    }

    // Public-home visibility + promo dialog (absent fields preserve stored values).
    const [currentRows] = await mysqlConnection.promise().query(
      'SELECT public_home_visible, promo_dialog_enabled, promo_dialog_version, promo_json FROM health_event WHERE id = ? LIMIT 1', [id]);
    const current = currentRows[0] || {};
    const publicHomeVisible = req.body.public_home_visible === undefined
      ? (current.public_home_visible === 'Y' ? 'Y' : 'N')
      : (req.body.public_home_visible === 'Y' ? 'Y' : 'N');
    let promoEnabled = req.body.promo_dialog_enabled === undefined
      ? (current.promo_dialog_enabled === 'Y' ? 'Y' : 'N')
      : (req.body.promo_dialog_enabled === 'Y' ? 'Y' : 'N');
    // The dialog only makes sense if the event is listed on the public home.
    if (publicHomeVisible !== 'Y') promoEnabled = 'N';
    let promoVersion = Number(current.promo_dialog_version) || 1;
    if (promoEnabled === 'Y' && current.promo_dialog_enabled !== 'Y') {
      // (Re)activation bumps the version so users who dismissed it see it again.
      promoVersion += 1;
    }
    let promoJson = current.promo_json != null ? current.promo_json : null;
    if (req.body.promo_json !== undefined) {
      if (req.body.promo_json && typeof req.body.promo_json === 'object') {
        const rawLink = String(req.body.promo_json.link_url || '').trim().slice(0, 500);
        promoJson = JSON.stringify({
          text_en: String(req.body.promo_json.text_en || '').trim().slice(0, 600) || null,
          text_es: String(req.body.promo_json.text_es || '').trim().slice(0, 600) || null,
          link_url: /^https?:\/\//i.test(rawLink) ? rawLink : null
        });
      } else {
        promoJson = null;
      }
    }

    await mysqlConnection.promise().query(
      'UPDATE health_event SET slug = ?, name_en = ?, name_es = ?, location_id = ?, client_id = ?, start_date = ?, \
        end_date = ?, start_time = ?, end_time = ?, timezone = ?, registration_opens_at = ?, registration_closes_at = ?, \
        enabled = ?, landing_enabled = ?, public_home_visible = ?, promo_dialog_enabled = ?, promo_dialog_version = ?, \
        promo_json = ? WHERE id = ?',
      [slug, req.body.name_en, req.body.name_es, req.body.location_id, req.body.client_id || null,
        toSqlDateString(req.body.start_date), toSqlDateString(req.body.end_date),
        req.body.start_time || null, req.body.end_time || null,
        req.body.timezone || 'America/Los_Angeles',
        toSqlDateTimeString(req.body.registration_opens_at), toSqlDateTimeString(req.body.registration_closes_at),
        req.body.enabled === 'N' ? 'N' : 'Y', req.body.landing_enabled === 'N' ? 'N' : 'Y',
        publicHomeVisible, promoEnabled, promoVersion, promoJson, id]);
    publicHomeCache.at = 0; // public-home payload changed
    res.status(200).json({});
  } catch (error) {
    logger.error('PUT /health-events/:id error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.get('/health-events/:id(\\d+)/full', verifyToken, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const event = await getEventById(id);
    if (!event) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const [images] = await mysqlConnection.promise().query(
      'SELECT * FROM health_event_image WHERE health_event_id = ? ORDER BY section_key ASC, display_order ASC, id ASC', [id]);
    const [stands] = await mysqlConnection.promise().query(
      'SELECT * FROM health_event_stand WHERE health_event_id = ? ORDER BY sort_order ASC, id ASC', [id]);
    let services = [];
    if (stands.length) {
      const [serviceRows] = await mysqlConnection.promise().query(
        `SELECT * FROM health_event_stand_service WHERE stand_id IN (${stands.map(() => '?').join(',')})
         ORDER BY sort_order ASC, id ASC`, stands.map(s => s.id));
      services = serviceRows;
    }
    const beneficiaryForms = await fetchForms(id, 'beneficiary');
    const volunteerForms = await fetchForms(id, 'volunteer');
    const checkoutForms = await fetchForms(id, 'checkout');
    const slots = await fetchSlotsWithBooked(id, true);

    res.status(200).json({
      event: {
        ...publicEventShape(event),
        enabled: event.enabled,
        landing_enabled: event.landing_enabled,
        public_home_visible: event.public_home_visible === 'Y' ? 'Y' : 'N',
        promo_dialog_enabled: event.promo_dialog_enabled === 'Y' ? 'Y' : 'N',
        promo_dialog_version: Number(event.promo_dialog_version) || 1,
        promo_json: safeParseJson(event.promo_json, null),
        landing_json: safeParseJson(event.landing_json, null)
      },
      images: await attachImageUrls(images),
      stands: stands.map(s => ({
        ...s,
        services: services.filter(sv => sv.stand_id === s.id)
      })),
      forms: { beneficiary: beneficiaryForms, volunteer: volunteerForms, checkout: checkoutForms },
      slots
    });
  } catch (error) {
    logger.error('GET /health-events/:id/full error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.put('/health-events/:id(\\d+)/landing', verifyToken, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const landing = req.body.landing_json;
    if (landing == null || typeof landing !== 'object') {
      return res.status(400).json({ error: 'INVALID_LANDING_JSON' });
    }
    await mysqlConnection.promise().query(
      'UPDATE health_event SET landing_json = ? WHERE id = ?', [JSON.stringify(landing), id]);
    res.status(200).json({});
  } catch (error) {
    logger.error('PUT /health-events/:id/landing error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.post('/health-events/:id(\\d+)/images', verifyToken, requireAdmin, (req, res) => {
  imageUpload(req, res, async (uploadError) => {
    try {
      if (uploadError) {
        return res.status(400).json({ error: 'INVALID_IMAGE', message: uploadError.message });
      }
      if (!req.file) {
        return res.status(400).json({ error: 'IMAGE_REQUIRED' });
      }
      const id = Number(req.params.id);
      const event = await getEventById(id);
      if (!event) {
        return res.status(404).json({ error: 'NOT_FOUND' });
      }
      const sectionKey = String(req.body.section_key || 'gallery').slice(0, 40);
      const originalKey = `health-events/${event.slug}/${crypto.randomBytes(16).toString('hex')}`;
      const uploadResult = await uploadImageWithVariants({
        originalKey,
        buffer: req.file.buffer,
        contentType: req.file.mimetype,
        presetName: 'article'
      });
      const [inserted] = await mysqlConnection.promise().query(
        'INSERT INTO health_event_image(health_event_id, section_key, s3_key, s3_key_small, s3_key_medium, mime_type, \
          original_filename, alt_en, alt_es, display_order) VALUES (?,?,?,?,?,?,?,?,?,?)',
        [id, sectionKey, uploadResult.originalKey, uploadResult.smallKey, uploadResult.mediumKey,
          req.file.mimetype, req.file.originalname || null, req.body.alt_en || null, req.body.alt_es || null,
          Number.parseInt(req.body.display_order, 10) || 0]);
      res.status(200).json({
        id: inserted.insertId,
        section_key: sectionKey,
        url: await signImageUrl(uploadResult.originalKey),
        url_small: await signImageUrl(uploadResult.smallKey),
        url_medium: await signImageUrl(uploadResult.mediumKey)
      });
    } catch (error) {
      logger.error('POST /health-events/:id/images error: ' + error.message);
      res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
    }
  });
});

router.put('/health-events/images/:imageId(\\d+)', verifyToken, requireAdmin, async (req, res) => {
  try {
    const imageId = Number(req.params.imageId);
    const [rows] = await mysqlConnection.promise().query(
      'SELECT * FROM health_event_image WHERE id = ? LIMIT 1', [imageId]);
    if (!rows.length) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const image = rows[0];
    let linkUrl = image.link_url;
    if (req.body.link_url !== undefined) {
      const rawLink = String(req.body.link_url || '').trim().slice(0, 500);
      linkUrl = /^https?:\/\//i.test(rawLink) ? rawLink : null;
    }
    await mysqlConnection.promise().query(
      'UPDATE health_event_image SET section_key = ?, alt_en = ?, alt_es = ?, link_url = ?, display_order = ?, enabled = ? WHERE id = ?',
      [req.body.section_key != null ? String(req.body.section_key).slice(0, 40) : image.section_key,
        req.body.alt_en !== undefined ? req.body.alt_en : image.alt_en,
        req.body.alt_es !== undefined ? req.body.alt_es : image.alt_es,
        linkUrl,
        req.body.display_order !== undefined ? Number.parseInt(req.body.display_order, 10) || 0 : image.display_order,
        req.body.enabled === 'N' ? 'N' : 'Y', imageId]);
    res.status(200).json({});
  } catch (error) {
    logger.error('PUT /health-events/images/:id error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.delete('/health-events/images/:imageId(\\d+)', verifyToken, requireAdmin, async (req, res) => {
  try {
    const imageId = Number(req.params.imageId);
    const [rows] = await mysqlConnection.promise().query(
      'SELECT * FROM health_event_image WHERE id = ? LIMIT 1', [imageId]);
    if (!rows.length) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const image = rows[0];
    try {
      await deleteS3Objects([image.s3_key, image.s3_key_small, image.s3_key_medium].filter(Boolean));
    } catch (s3Error) {
      logger.error('healthEvents delete image s3 error: ' + s3Error.message);
    }
    await mysqlConnection.promise().query('DELETE FROM health_event_image WHERE id = ?', [imageId]);
    res.status(200).json({});
  } catch (error) {
    logger.error('DELETE /health-events/images/:id error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.put('/health-events/:id(\\d+)/stands', verifyToken, requireAdmin, async (req, res) => {
  let connection;
  try {
    const id = Number(req.params.id);
    const stands = Array.isArray(req.body.stands) ? req.body.stands : [];
    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();

    const [existingStands] = await connection.query(
      'SELECT id FROM health_event_stand WHERE health_event_id = ?', [id]);
    const keptStandIds = new Set();

    for (const stand of stands) {
      let standId = Number.parseInt(stand.id, 10);
      const values = [
        String(stand.name_en || '').slice(0, 150), String(stand.name_es || '').slice(0, 150),
        stand.icon ? String(stand.icon).slice(0, 40) : null,
        stand.is_entry === 'Y' ? 'Y' : 'N', stand.has_checkout === 'Y' ? 'Y' : 'N',
        Number.parseInt(stand.sort_order, 10) || 1, stand.enabled === 'N' ? 'N' : 'Y'
      ];
      if (Number.isInteger(standId) && standId > 0 && existingStands.some(s => s.id === standId)) {
        await connection.query(
          'UPDATE health_event_stand SET name_en=?, name_es=?, icon=?, is_entry=?, has_checkout=?, sort_order=?, enabled=? WHERE id=? AND health_event_id=?',
          [...values, standId, id]);
      } else {
        const [inserted] = await connection.query(
          'INSERT INTO health_event_stand(health_event_id, name_en, name_es, icon, is_entry, has_checkout, sort_order, enabled) \
           VALUES (?,?,?,?,?,?,?,?)', [id, ...values]);
        standId = inserted.insertId;
      }
      keptStandIds.add(standId);

      const services = Array.isArray(stand.services) ? stand.services : [];
      const [existingServices] = await connection.query(
        'SELECT id FROM health_event_stand_service WHERE stand_id = ?', [standId]);
      const keptServiceIds = new Set();
      for (const service of services) {
        let serviceId = Number.parseInt(service.id, 10);
        const serviceValues = [
          String(service.name_en || '').slice(0, 150), String(service.name_es || '').slice(0, 150),
          Number.parseInt(service.sort_order, 10) || 1, service.enabled === 'N' ? 'N' : 'Y'
        ];
        if (Number.isInteger(serviceId) && serviceId > 0 && existingServices.some(s => s.id === serviceId)) {
          await connection.query(
            'UPDATE health_event_stand_service SET name_en=?, name_es=?, sort_order=?, enabled=? WHERE id=? AND stand_id=?',
            [...serviceValues, serviceId, standId]);
        } else {
          const [insertedService] = await connection.query(
            'INSERT INTO health_event_stand_service(stand_id, name_en, name_es, sort_order, enabled) VALUES (?,?,?,?,?)',
            [standId, ...serviceValues]);
          serviceId = insertedService.insertId;
        }
        keptServiceIds.add(serviceId);
      }
      for (const existing of existingServices) {
        if (!keptServiceIds.has(existing.id)) {
          await connection.query('UPDATE health_event_stand_service SET enabled = "N" WHERE id = ?', [existing.id]);
        }
      }
    }
    for (const existing of existingStands) {
      if (!keptStandIds.has(existing.id)) {
        await connection.query('UPDATE health_event_stand SET enabled = "N" WHERE id = ?', [existing.id]);
      }
    }
    await connection.commit();
    res.status(200).json({ stand_ids: Array.from(keptStandIds) });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('PUT /health-events/:id/stands error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.put('/health-events/:id(\\d+)/forms/:audience', verifyToken, requireAdmin, async (req, res) => {
  let connection;
  try {
    const id = Number(req.params.id);
    const audience = String(req.params.audience);
    if (!['beneficiary', 'volunteer', 'checkout'].includes(audience)) {
      return res.status(400).json({ error: 'INVALID_AUDIENCE' });
    }
    const standId = audience === 'checkout' ? Number.parseInt(req.query.stand_id, 10) : null;
    if (audience === 'checkout' && !Number.isInteger(standId)) {
      return res.status(400).json({ error: 'STAND_ID_REQUIRED' });
    }
    const forms = Array.isArray(req.body.forms) ? req.body.forms : [];

    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();

    const scopeParams = [id, audience];
    let scopeFilter = 'health_event_id = ? AND audience = ?';
    if (audience === 'checkout') {
      scopeFilter += ' AND stand_id = ?';
      scopeParams.push(standId);
    }
    const [existingForms] = await connection.query(
      `SELECT id FROM health_event_form WHERE ${scopeFilter}`, scopeParams);
    const keptFormIds = new Set();
    const questionIdMap = new Map(); // temp/new question key -> real id
    const optionIdMap = new Map();
    const responseForms = [];

    for (const form of forms) {
      let formId = Number.parseInt(form.id, 10);
      const formValues = [
        String(form.title_en || '').slice(0, 255), String(form.title_es || '').slice(0, 255),
        form.intro_en || null, form.intro_es || null,
        Number.parseInt(form.section_order, 10) || 1,
        form.required_before_qr === 'Y' ? 'Y' : 'N',
        form.enabled === 'N' ? 'N' : 'Y'
      ];
      if (Number.isInteger(formId) && formId > 0 && existingForms.some(f => f.id === formId)) {
        await connection.query(
          'UPDATE health_event_form SET title_en=?, title_es=?, intro_en=?, intro_es=?, section_order=?, required_before_qr=?, enabled=? WHERE id=?',
          [...formValues, formId]);
      } else {
        const [inserted] = await connection.query(
          'INSERT INTO health_event_form(health_event_id, audience, stand_id, title_en, title_es, intro_en, intro_es, section_order, required_before_qr, enabled) \
           VALUES (?,?,?,?,?,?,?,?,?,?)', [id, audience, standId, ...formValues]);
        formId = inserted.insertId;
      }
      keptFormIds.add(formId);
      const responseForm = { id: formId, questions: [] };

      const questions = Array.isArray(form.questions) ? form.questions : [];
      const [existingQuestions] = await connection.query(
        'SELECT id FROM health_event_question WHERE form_id = ?', [formId]);
      const keptQuestionIds = new Set();

      for (const question of questions) {
        const tempQuestionId = question.id; // may be negative temp id
        let questionId = Number.parseInt(question.id, 10);
        const questionType = QUESTION_TYPES.has(question.question_type) ? question.question_type : 'text';
        const questionValues = [
          questionType,
          String(question.name_en || '').slice(0, 1000), String(question.name_es || '').slice(0, 1000),
          question.help_en || null, question.help_es || null,
          // A notice is never answerable — required='Y' would block registration.
          (questionType === 'notice' || question.required === 'N') ? 'N' : 'Y',
          question.allow_other === 'Y' ? 'Y' : 'N',
          question.maps_to ? String(question.maps_to).slice(0, 40) : null,
          question.config_json != null ? JSON.stringify(question.config_json) : null,
          Number.parseInt(question.sort_order, 10) || 1,
          question.enabled === 'N' ? 'N' : 'Y'
        ];
        if (Number.isInteger(questionId) && questionId > 0 && existingQuestions.some(q => q.id === questionId)) {
          await connection.query(
            'UPDATE health_event_question SET question_type=?, name_en=?, name_es=?, help_en=?, help_es=?, required=?, \
             allow_other=?, maps_to=?, config_json=?, sort_order=?, enabled=? WHERE id=?',
            [...questionValues, questionId]);
        } else {
          const [inserted] = await connection.query(
            'INSERT INTO health_event_question(form_id, question_type, name_en, name_es, help_en, help_es, required, \
              allow_other, maps_to, config_json, sort_order, enabled) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            [formId, ...questionValues]);
          questionId = inserted.insertId;
        }
        questionIdMap.set(tempQuestionId, questionId);
        keptQuestionIds.add(questionId);
        const responseQuestion = { id: questionId, temp_id: tempQuestionId, options: [] };

        const options = Array.isArray(question.options) ? question.options : [];
        const [existingOptions] = await connection.query(
          'SELECT id FROM health_event_question_option WHERE question_id = ?', [questionId]);
        const keptOptionIds = new Set();
        for (const option of options) {
          const tempOptionId = option.id;
          let optionId = Number.parseInt(option.id, 10);
          const optionValues = [
            String(option.name_en || '').slice(0, 500), String(option.name_es || '').slice(0, 500),
            option.is_other === 'Y' ? 'Y' : 'N',
            option.event_date ? toSqlDateString(option.event_date) : null,
            option.service_key ? String(option.service_key).slice(0, 40) : null,
            Number.parseInt(option.sort_order, 10) || 1,
            option.enabled === 'N' ? 'N' : 'Y'
          ];
          if (Number.isInteger(optionId) && optionId > 0 && existingOptions.some(o => o.id === optionId)) {
            await connection.query(
              'UPDATE health_event_question_option SET name_en=?, name_es=?, is_other=?, event_date=?, service_key=?, sort_order=?, enabled=? WHERE id=?',
              [...optionValues, optionId]);
          } else {
            const [inserted] = await connection.query(
              'INSERT INTO health_event_question_option(question_id, name_en, name_es, is_other, event_date, service_key, sort_order, enabled) \
               VALUES (?,?,?,?,?,?,?,?)', [questionId, ...optionValues]);
            optionId = inserted.insertId;
          }
          optionIdMap.set(tempOptionId, optionId);
          keptOptionIds.add(optionId);
          responseQuestion.options.push({ id: optionId, temp_id: tempOptionId });
        }
        for (const existing of existingOptions) {
          if (!keptOptionIds.has(existing.id)) {
            await connection.query('UPDATE health_event_question_option SET enabled = "N" WHERE id = ?', [existing.id]);
          }
        }
        responseForm.questions.push(responseQuestion);
      }
      for (const existing of existingQuestions) {
        if (!keptQuestionIds.has(existing.id)) {
          await connection.query('UPDATE health_event_question SET enabled = "N" WHERE id = ?', [existing.id]);
        }
      }
      responseForms.push(responseForm);
    }

    // Second pass: dependencies (may reference temp negative ids resolved above).
    for (const form of forms) {
      for (const question of (form.questions || [])) {
        const realQuestionId = questionIdMap.get(question.id);
        if (!realQuestionId) continue;
        let dependsQuestion = null;
        let dependsOption = null;
        if (question.depends_on_question_id != null) {
          dependsQuestion = questionIdMap.get(question.depends_on_question_id) ||
            (Number.parseInt(question.depends_on_question_id, 10) > 0 ? Number.parseInt(question.depends_on_question_id, 10) : null);
        }
        if (question.depends_on_option_id != null) {
          dependsOption = optionIdMap.get(question.depends_on_option_id) ||
            (Number.parseInt(question.depends_on_option_id, 10) > 0 ? Number.parseInt(question.depends_on_option_id, 10) : null);
        }
        await connection.query(
          'UPDATE health_event_question SET depends_on_question_id = ?, depends_on_option_id = ? WHERE id = ?',
          [dependsQuestion, dependsOption, realQuestionId]);

        // Resolve temp option ids embedded in config_json.option_dates if present.
        if (question.config_json && question.config_json.option_dates) {
          const resolved = {};
          for (const [key, value] of Object.entries(question.config_json.option_dates)) {
            const realOption = optionIdMap.get(Number(key)) || optionIdMap.get(key) || key;
            resolved[realOption] = value;
          }
          await connection.query('UPDATE health_event_question SET config_json = ? WHERE id = ?',
            [JSON.stringify({ ...question.config_json, option_dates: resolved }), realQuestionId]);
        }
      }
    }

    for (const existing of existingForms) {
      if (!keptFormIds.has(existing.id)) {
        await connection.query('UPDATE health_event_form SET enabled = "N" WHERE id = ?', [existing.id]);
      }
    }

    await connection.commit();
    res.status(200).json({ forms: responseForms });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('PUT /health-events/:id/forms error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.get('/health-events/:id(\\d+)/registrations', verifyToken, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const role = req.query.role === 'volunteer' ? 'volunteer' : 'beneficiary';
    const page = Math.max(Number.parseInt(req.query.page, 10) || 1, 1);
    const pageSize = Math.min(Math.max(Number.parseInt(req.query.pageSize, 10) || 25, 1), 200);
    const search = String(req.query.search || '').trim();

    const params = [id, role];
    let searchFilter = '';
    if (search) {
      searchFilter = ' AND (u.firstname LIKE ? OR u.lastname LIKE ? OR u.email LIKE ? OR r.contact_email LIKE ? OR u.phone LIKE ?)';
      const like = `%${search}%`;
      params.push(like, like, like, like, like);
    }

    const [countRows] = await mysqlConnection.promise().query(
      `SELECT COUNT(*) AS total
       FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
       WHERE r.health_event_id = ? AND r.registration_role = ?${searchFilter}`, params);

    const [rows] = await mysqlConnection.promise().query(
      `SELECT r.id AS registration_id, r.user_id, u.firstname, u.lastname, u.email, u.username, u.phone,
              u.enabled AS user_enabled,
              r.contact_email, r.source, r.status, r.submitted_at,
              (SELECT GROUP_CONCAT(DATE_FORMAT(d.event_date, '%Y-%m-%d') ORDER BY d.event_date SEPARATOR ', ')
                 FROM health_event_registration_date d WHERE d.registration_id = r.id) AS dates,
              (SELECT COUNT(*) FROM health_event_scan s
                 WHERE s.registration_id = r.id AND s.scan_type = 'checkin') AS checkins,
              (SELECT GROUP_CONCAT(CONCAT(sl.service_key, ' ', DATE_FORMAT(sl.slot_date, '%m/%d'), ' ',
                      TIME_FORMAT(sl.start_time, '%H:%i')) ORDER BY sl.slot_date, sl.start_time SEPARATOR ' | ')
                 FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
                 WHERE a.registration_id = r.id AND a.status = 'booked') AS appointment_summary
       FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
       WHERE r.health_event_id = ? AND r.registration_role = ?${searchFilter}
       ORDER BY r.id DESC LIMIT ? OFFSET ?`, [...params, pageSize, (page - 1) * pageSize]);

    res.status(200).json({ total: countRows[0].total, page, pageSize, rows });
  } catch (error) {
    logger.error('GET /health-events/:id/registrations error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

/** answer row → display text, mirrors the admin UI's answerDisplay() precedence. */
function answerToCsvText(answer, lang) {
  if (answer.answer_text != null && answer.answer_text !== '') return String(answer.answer_text);
  if (answer.answer_number != null) return String(answer.answer_number);
  if (answer.answer_date != null) return toSqlDateString(answer.answer_date) || '';
  const options = lang === 'es' ? (answer.options_es || answer.options_en) : (answer.options_en || answer.options_es);
  if (options) return answer.other_text ? `${options} (${answer.other_text})` : options;
  return answer.other_text || '';
}

/**
 * True when the stored hash is the shared default password. Only called for
 * rows flagged reset_password='Y' (cheap gate): the flag alone is not enough
 * because older admin tools set it with a different default ('communitydata').
 */
async function hasDefaultHealthPassword(passwordHash) {
  if (!passwordHash) return false;
  try {
    return await bcryptjs.compare(DEFAULT_HEALTH_PASSWORD, passwordHash);
  } catch (e) {
    return false;
  }
}

/**
 * Full CSV export of one audience's registrations: profile + credentials
 * status + dates/appointments/check-ins + one column per form question.
 */
router.get('/health-events/:id(\\d+)/registrations/csv', verifyToken, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const role = req.query.role === 'volunteer' ? 'volunteer' : 'beneficiary';
    const lang = req.query.lang === 'es' ? 'es' : 'en';
    const event = await getEventById(id);
    if (!event) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }

    const forms = await fetchForms(id, role);
    const questions = forms.flatMap(f => f.questions).filter(q => q.question_type !== 'notice');

    const [rows] = await mysqlConnection.promise().query(
      `SELECT r.id AS registration_id, r.user_id, u.firstname, u.lastname, u.email, u.username, u.phone,
              u.date_of_birth, u.zipcode, u.enabled AS user_enabled, u.reset_password, u.password AS password_hash,
              r.contact_email, r.source, r.status, r.submitted_at,
              (SELECT GROUP_CONCAT(CONCAT(DATE_FORMAT(d.event_date, '%Y-%m-%d'),
                      COALESCE(CONCAT(' (', d.priority_service, ')'), '')) ORDER BY d.event_date SEPARATOR ', ')
                 FROM health_event_registration_date d WHERE d.registration_id = r.id) AS dates,
              (SELECT COUNT(*) FROM health_event_scan s
                 WHERE s.registration_id = r.id AND s.scan_type = 'checkin') AS checkins,
              (SELECT GROUP_CONCAT(CONCAT(sl.service_key, ' ', DATE_FORMAT(sl.slot_date, '%m/%d'), ' ',
                      TIME_FORMAT(sl.start_time, '%H:%i')) ORDER BY sl.slot_date, sl.start_time SEPARATOR ' | ')
                 FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
                 WHERE a.registration_id = r.id AND a.status = 'booked') AS appointment_summary
       FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
       WHERE r.health_event_id = ? AND r.registration_role = ?
       ORDER BY r.id DESC`, [id, role]);

    const [answerRows] = await mysqlConnection.promise().query(
      `SELECT a.registration_id, a.question_id, a.answer_text, a.answer_number, a.answer_date, a.other_text,
              (SELECT GROUP_CONCAT(o.name_en ORDER BY o.sort_order SEPARATOR ' | ')
                 FROM health_event_answer_option ao INNER JOIN health_event_question_option o ON o.id = ao.option_id
                 WHERE ao.answer_id = a.id) AS options_en,
              (SELECT GROUP_CONCAT(o.name_es ORDER BY o.sort_order SEPARATOR ' | ')
                 FROM health_event_answer_option ao INNER JOIN health_event_question_option o ON o.id = ao.option_id
                 WHERE ao.answer_id = a.id) AS options_es
       FROM health_event_answer a
       INNER JOIN health_event_registration r ON r.id = a.registration_id
       WHERE r.health_event_id = ? AND r.registration_role = ?`, [id, role]);

    const answersByRegistration = new Map();
    for (const answer of answerRows) {
      if (!answersByRegistration.has(answer.registration_id)) {
        answersByRegistration.set(answer.registration_id, new Map());
      }
      answersByRegistration.get(answer.registration_id).set(answer.question_id, answer);
    }

    const ownPasswordLabel = lang === 'es' ? 'Tiene contraseña propia' : 'Has their own password';
    const header = [
      { id: 'registration_id', title: 'Registration ID' },
      { id: 'firstname', title: 'First name' },
      { id: 'lastname', title: 'Last name' },
      { id: 'email', title: 'Email' },
      { id: 'phone', title: 'Phone' },
      { id: 'date_of_birth', title: 'Date of birth' },
      { id: 'zipcode', title: 'Zipcode' },
      { id: 'username', title: 'Username' },
      { id: 'password', title: 'Password' },
      ...(role === 'volunteer' ? [{ id: 'approved', title: 'Approved' }] : []),
      { id: 'source', title: 'Source' },
      { id: 'status', title: 'Status' },
      { id: 'submitted_at', title: 'Submitted at' },
      { id: 'dates', title: 'Attendance dates' },
      { id: 'appointment_summary', title: 'Appointments' },
      { id: 'checkins', title: 'Check-ins' },
      ...questions.map(q => ({
        id: `q_${q.id}`,
        title: (lang === 'es' ? (q.name_es || q.name_en) : (q.name_en || q.name_es)) || `Question ${q.id}`
      }))
    ];

    const records = [];
    for (const row of rows) {
      const record = {
        registration_id: row.registration_id,
        firstname: row.firstname || '',
        lastname: row.lastname || '',
        email: row.email || row.contact_email || '',
        phone: row.phone || '',
        date_of_birth: row.date_of_birth ? toSqlDateString(row.date_of_birth) : '',
        zipcode: row.zipcode || '',
        username: row.username || '',
        password: (row.reset_password === 'Y' && await hasDefaultHealthPassword(row.password_hash))
          ? DEFAULT_HEALTH_PASSWORD
          : ownPasswordLabel,
        source: row.source || '',
        status: row.status || '',
        submitted_at: toSqlDateTimeString(row.submitted_at) || '',
        dates: row.dates || '',
        appointment_summary: row.appointment_summary || '',
        checkins: row.checkins || 0
      };
      if (role === 'volunteer') {
        record.approved = row.user_enabled === 'Y' ? 'Yes' : 'No';
      }
      const registrationAnswers = answersByRegistration.get(row.registration_id);
      for (const question of questions) {
        const answer = registrationAnswers ? registrationAnswers.get(question.id) : null;
        record[`q_${question.id}`] = answer ? answerToCsvText(answer, lang) : '';
      }
      records.push(record);
    }

    const csvStringifier = createCsvStringifier({ header, fieldDelimiter: ';' });
    // BOM so Excel opens the UTF-8 file with accents intact.
    const csvData = '\ufeff' + csvStringifier.getHeaderString() + csvStringifier.stringifyRecords(records);
    const fileSlug = role === 'volunteer' ? 'volunteers' : 'beneficiaries';
    res.setHeader('Content-disposition', `attachment; filename=health-event-${id}-${fileSlug}.csv`);
    res.setHeader('Content-type', 'text/csv; charset=utf-8');
    res.send(csvData);
  } catch (error) {
    logger.error('GET /health-events/:id/registrations/csv error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.get('/health-events/registrations/:registrationId(\\d+)', verifyToken, requireAdmin, async (req, res) => {
  try {
    const registrationId = Number(req.params.registrationId);
    const [regRows] = await mysqlConnection.promise().query(
      `SELECT r.*, u.firstname, u.lastname, u.email, u.username, u.phone, u.date_of_birth, u.zipcode
       FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
       WHERE r.id = ? LIMIT 1`, [registrationId]);
    if (!regRows.length) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const registration = regRows[0];
    const [dates] = await mysqlConnection.promise().query(
      'SELECT event_date, priority_service FROM health_event_registration_date WHERE registration_id = ? ORDER BY event_date',
      [registrationId]);
    const [appointments] = await mysqlConnection.promise().query(
      `SELECT sl.service_key, sl.slot_date, TIME_FORMAT(sl.start_time, '%H:%i') AS start_time,
              TIME_FORMAT(sl.end_time, '%H:%i') AS end_time, a.status
       FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
       WHERE a.registration_id = ? ORDER BY sl.slot_date, sl.start_time`, [registrationId]);
    const [answers] = await mysqlConnection.promise().query(
      `SELECT a.question_id, q.name_en AS question_en, q.name_es AS question_es, q.question_type,
              a.answer_text, a.answer_number, a.answer_date, a.other_text,
              (SELECT GROUP_CONCAT(o.name_en ORDER BY o.sort_order SEPARATOR ' | ')
                 FROM health_event_answer_option ao INNER JOIN health_event_question_option o ON o.id = ao.option_id
                 WHERE ao.answer_id = a.id) AS options_en,
              (SELECT GROUP_CONCAT(o.name_es ORDER BY o.sort_order SEPARATOR ' | ')
                 FROM health_event_answer_option ao INNER JOIN health_event_question_option o ON o.id = ao.option_id
                 WHERE ao.answer_id = a.id) AS options_es
       FROM health_event_answer a INNER JOIN health_event_question q ON q.id = a.question_id
       WHERE a.registration_id = ?
       ORDER BY q.form_id, q.sort_order`, [registrationId]);
    const [scans] = await mysqlConnection.promise().query(
      `SELECT s.id, s.scan_type, s.scanned_at, st.name_en AS stand_en, st.name_es AS stand_es,
              ss.name_en AS service_en, ss.name_es AS service_es,
              TRIM(CONCAT(vu.firstname, ' ', COALESCE(vu.lastname, ''))) AS volunteer_name
       FROM health_event_scan s
       INNER JOIN health_event_stand st ON st.id = s.stand_id
       LEFT JOIN health_event_stand_service ss ON ss.id = s.service_id
       LEFT JOIN user vu ON vu.id = s.volunteer_user_id
       WHERE s.registration_id = ? ORDER BY s.scanned_at`, [registrationId]);

    // Volunteer movement history: every stand/service session ever started
    // (rows are soft-ended with ended_at, never deleted).
    let assignments = [];
    if (registration.registration_role === 'volunteer') {
      const [assignmentRows] = await mysqlConnection.promise().query(
        `SELECT a.started_at, a.ended_at, st.name_en AS stand_en, st.name_es AS stand_es,
                ss.name_en AS service_en, ss.name_es AS service_es
         FROM health_event_volunteer_assignment a
         INNER JOIN health_event_stand st ON st.id = a.stand_id
         LEFT JOIN health_event_stand_service ss ON ss.id = a.service_id
         WHERE a.health_event_id = ? AND a.user_id = ?
         ORDER BY a.started_at`, [registration.health_event_id, registration.user_id]);
      assignments = assignmentRows;
    }

    res.status(200).json({
      registration: {
        id: registration.id,
        health_event_id: registration.health_event_id,
        registration_role: registration.registration_role,
        status: registration.status,
        source: registration.source,
        contact_email: registration.contact_email,
        submitted_at: registration.submitted_at
      },
      user: {
        id: registration.user_id,
        firstname: registration.firstname,
        lastname: registration.lastname,
        email: registration.email,
        username: registration.username,
        phone: registration.phone,
        date_of_birth: toSqlDateString(registration.date_of_birth),
        zipcode: registration.zipcode
      },
      dates: dates.map(d => ({ event_date: toSqlDateString(d.event_date), priority_service: d.priority_service })),
      appointments: appointments.map(a => ({ ...a, slot_date: toSqlDateString(a.slot_date) })),
      answers,
      scans,
      assignments
    });
  } catch (error) {
    logger.error('GET /health-events/registrations/:id error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.post('/health-events/:id(\\d+)/volunteers', verifyToken, requireAdmin, async (req, res) => {
  let connection;
  try {
    const id = Number(req.params.id);
    const event = await getEventById(id);
    if (!event) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const { firstName, lastName, email, phone, dateOfBirth } = req.body;
    if (!firstName || !email) {
      return res.status(400).json({ error: 'INVALID_ACCOUNT_DATA' });
    }
    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();

    const [emailTaken] = await connection.query('SELECT id, role_id, enabled FROM user WHERE email = ? LIMIT 1', [String(email).trim()]);
    let userId;
    let credentials = null;
    // Self-registered volunteers stay enabled='N' until approved: the
    // credentials email must keep saying "pending approval" in that case.
    let pendingApproval = false;
    if (emailTaken.length) {
      userId = emailTaken[0].id;
      pendingApproval = emailTaken[0].role_id === 11 && emailTaken[0].enabled !== 'Y';
    } else {
      const username = await generateUniqueUsername(connection, firstName, lastName);
      const password = DEFAULT_HEALTH_PASSWORD;
      userId = await createHealthEventUser(connection, {
        username,
        passwordHash: await bcryptjs.hash(password, 8),
        email: String(email).trim(),
        roleId: 11,
        firstName,
        lastName,
        dateOfBirth: dateOfBirth || null,
        phone: phone || null,
        zipcode: null,
        locationId: event.location_id,
        uiLanguage: 'en',
        resetPassword: 'Y'
      });
      credentials = { username, password };
    }

    const [regResult] = await connection.query(
      'INSERT IGNORE INTO health_event_registration(health_event_id, user_id, registration_role, contact_email, source, submitted_at) \
       VALUES (?,?,?,?,?,NOW())', [id, userId, 'volunteer', String(email).trim(), 'admin']);

    // Existing role-11 user: rotate credentials only when this call actually
    // attached them to a new event — a duplicate add must not silently break
    // a password that may already be in use.
    if (emailTaken.length && emailTaken[0].role_id === 11 && regResult.affectedRows > 0) {
      const password = DEFAULT_HEALTH_PASSWORD;
      await connection.query('UPDATE user SET password = ?, reset_password = "Y" WHERE id = ?',
        [await bcryptjs.hash(password, 8), userId]);
      const [userRow] = await connection.query('SELECT username FROM user WHERE id = ?', [userId]);
      credentials = { username: userRow[0].username, password };
    }
    await connection.commit();

    // Best-effort emails after commit: credentials to the volunteer (when fresh
    // ones were generated) + the per-event volunteer notification list.
    if (credentials) {
      try {
        const emailModule = require('../email/email');
        if (typeof emailModule.sendHealthEventVolunteerCredentials === 'function') {
          emailModule.sendHealthEventVolunteerCredentials({
            to: String(email).trim(),
            language: 'en',
            eventNameEn: event.name_en,
            eventNameEs: event.name_es,
            username: credentials.username,
            password: credentials.password,
            pendingApproval
          }).catch(() => { /* logged inside */ });
        }
      } catch (emailError) {
        logger.error('healthEvents admin volunteer credentials email error: ' + emailError.message);
      }
    }
    if (regResult.affectedRows > 0) {
      dispatchRegistrationEmails(event, regResult.insertId, 'volunteer');
    }

    res.status(200).json({ user_id: userId, credentials });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('POST /health-events/:id/volunteers error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.put('/health-events/volunteers/:userId(\\d+)/approve', verifyToken, requireAdmin, async (req, res) => {
  try {
    const userId = Number(req.params.userId);
    const [result] = await mysqlConnection.promise().query(
      'UPDATE user SET enabled = "Y" WHERE id = ? AND role_id = 11', [userId]);
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.status(200).json({ approved: true });
  } catch (error) {
    logger.error('PUT /health-events/volunteers/:id/approve error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

/**
 * Shared reset: sets the account back to the default health-event password and
 * flags reset_password='Y' so the first-login change prompt appears. roleId
 * scopes which accounts each endpoint may touch (11 volunteers, 5 beneficiaries).
 */
async function resetUserToDefaultPassword(userId, roleId) {
  const [rows] = await mysqlConnection.promise().query(
    'SELECT id, username FROM user WHERE id = ? AND role_id = ? LIMIT 1', [userId, roleId]);
  if (!rows.length) return null;
  await mysqlConnection.promise().query(
    'UPDATE user SET password = ?, reset_password = "Y" WHERE id = ?',
    [await bcryptjs.hash(DEFAULT_HEALTH_PASSWORD, 8), userId]);
  return { username: rows[0].username, password: DEFAULT_HEALTH_PASSWORD };
}

router.put('/health-events/volunteers/:userId(\\d+)/reset-password', verifyToken, requireAdmin, async (req, res) => {
  try {
    const credentials = await resetUserToDefaultPassword(Number(req.params.userId), 11);
    if (!credentials) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.status(200).json({ credentials });
  } catch (error) {
    logger.error('PUT /health-events/volunteers/:id/reset-password error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

router.put('/health-events/beneficiaries/:userId(\\d+)/reset-password', verifyToken, requireAdmin, async (req, res) => {
  try {
    const credentials = await resetUserToDefaultPassword(Number(req.params.userId), 5);
    if (!credentials) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    res.status(200).json({ credentials });
  } catch (error) {
    logger.error('PUT /health-events/beneficiaries/:id/reset-password error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

// =====================================================================
// ADMIN — PER-EVENT NOTIFICATION RECIPIENT LISTS (beneficiary / volunteer)
// Independent from the global food-distribution volunteer list
// (/volunteer/notification-recipients): these emails receive each new
// registration form of THIS health event.
// =====================================================================

const NOTIFICATION_EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const NOTIFICATION_AUDIENCES = ['beneficiary', 'volunteer'];
const MAX_NOTIFICATION_RECIPIENTS = 100;

/** Trims, validates and case-insensitively de-duplicates one recipient list. */
function normalizeNotificationList(rawList) {
  if (rawList == null) return { list: [] };
  if (!Array.isArray(rawList)) return { error: 'INVALID_LIST' };
  const seen = new Set();
  const list = [];
  for (const item of rawList) {
    const email = item && typeof item.email === 'string' ? item.email.trim() : '';
    if (!email) continue;
    if (email.length > 255 || !NOTIFICATION_EMAIL_REGEX.test(email)) {
      return { error: 'INVALID_EMAIL', email };
    }
    const key = email.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    list.push({ email, language: item.language === 'es' ? 'es' : 'en' });
    if (list.length > MAX_NOTIFICATION_RECIPIENTS) return { error: 'TOO_MANY' };
  }
  return { list };
}

router.get('/health-events/:id(\\d+)/notification-recipients', verifyToken, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const [rows] = await mysqlConnection.promise().query(
      'SELECT id, audience, email, language FROM health_event_notification_recipient \
       WHERE health_event_id = ? AND enabled = "Y" ORDER BY audience ASC, id ASC', [id]);
    const shape = (audience) => rows
      .filter(r => r.audience === audience)
      .map(r => ({ id: r.id, email: r.email, language: r.language === 'es' ? 'es' : 'en' }));
    res.status(200).json({ beneficiary: shape('beneficiary'), volunteer: shape('volunteer') });
  } catch (error) {
    logger.error('GET /health-events/:id/notification-recipients error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

/** Full replace of both lists: body { beneficiary: [{email, language}], volunteer: [...] }. */
router.put('/health-events/:id(\\d+)/notification-recipients', verifyToken, requireAdmin, async (req, res) => {
  let connection;
  try {
    const id = Number(req.params.id);
    const event = await getEventById(id);
    if (!event) {
      return res.status(404).json({ error: 'NOT_FOUND' });
    }
    const normalized = {};
    for (const audience of NOTIFICATION_AUDIENCES) {
      const result = normalizeNotificationList(req.body ? req.body[audience] : null);
      if (result.error) {
        return res.status(400).json({ error: 'VALIDATION', audience, detail: result.error, email: result.email || null });
      }
      normalized[audience] = result.list;
    }

    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();
    await connection.query('DELETE FROM health_event_notification_recipient WHERE health_event_id = ?', [id]);
    for (const audience of NOTIFICATION_AUDIENCES) {
      for (const recipient of normalized[audience]) {
        await connection.query(
          'INSERT INTO health_event_notification_recipient(health_event_id, audience, email, language) VALUES (?,?,?,?)',
          [id, audience, recipient.email, recipient.language]);
      }
    }
    await connection.commit();
    res.status(200).json(normalized);
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('PUT /health-events/:id/notification-recipients error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.put('/health-events/:id(\\d+)/slots', verifyToken, requireAdmin, async (req, res) => {
  let connection;
  try {
    const id = Number(req.params.id);
    connection = await mysqlConnection.promise().getConnection();
    await connection.beginTransaction();

    const generate = req.body.generate;
    if (generate && generate.service_key && Array.isArray(generate.dates)) {
      const interval = Number.parseInt(generate.interval_minutes, 10) || 60;
      const from = String(generate.from || '08:00');
      const to = String(generate.to || '16:00');
      const capacity = generate.capacity != null ? Number.parseInt(generate.capacity, 10) : null;
      for (const date of generate.dates) {
        const slotDate = toSqlDateString(date);
        let [h, m] = from.split(':').map(Number);
        const [endH, endM] = to.split(':').map(Number);
        while (h * 60 + m + interval <= endH * 60 + endM) {
          const start = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00`;
          const endTotal = h * 60 + m + interval;
          const end = `${String(Math.floor(endTotal / 60)).padStart(2, '0')}:${String(endTotal % 60).padStart(2, '0')}:00`;
          await connection.query(
            'INSERT INTO health_event_slot(health_event_id, service_key, slot_date, start_time, end_time, capacity) \
             VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE enabled = "Y"',
            [id, String(generate.service_key).slice(0, 40), slotDate, start, end, capacity]);
          m += interval;
          h += Math.floor(m / 60);
          m = m % 60;
        }
      }
    }

    const updates = Array.isArray(req.body.update) ? req.body.update : [];
    for (const update of updates) {
      const slotId = Number.parseInt(update.id, 10);
      if (!Number.isInteger(slotId)) continue;
      await connection.query(
        'UPDATE health_event_slot SET capacity = ?, enabled = ? WHERE id = ? AND health_event_id = ?',
        [update.capacity != null ? Number.parseInt(update.capacity, 10) : null,
          update.enabled === 'N' ? 'N' : 'Y', slotId, id]);
    }

    await connection.commit();
    res.status(200).json({ slots: await fetchSlotsWithBooked(id, true) });
  } catch (error) {
    if (connection) {
      try { await connection.rollback(); } catch (e) { /* noop */ }
    }
    logger.error('PUT /health-events/:id/slots error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  } finally {
    if (connection) connection.release();
  }
});

router.get('/health-events/:id(\\d+)/metrics-summary', verifyToken, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const [bySource] = await mysqlConnection.promise().query(
      `SELECT registration_role, source, COUNT(*) AS total
       FROM health_event_registration WHERE health_event_id = ? AND status = 'registered'
       GROUP BY registration_role, source`, [id]);
    const [scansPerStand] = await mysqlConnection.promise().query(
      `SELECT st.id AS stand_id, st.name_en, st.name_es, DATE(s.scanned_at) AS day, s.scan_type, COUNT(*) AS total,
              COUNT(DISTINCT s.scanned_user_id) AS unique_people
       FROM health_event_scan s INNER JOIN health_event_stand st ON st.id = s.stand_id
       WHERE s.health_event_id = ?
       GROUP BY st.id, st.name_en, st.name_es, DATE(s.scanned_at), s.scan_type
       ORDER BY st.sort_order, day`, [id]);
    const [uniquePerDay] = await mysqlConnection.promise().query(
      `SELECT DATE(s.scanned_at) AS day, COUNT(DISTINCT s.scanned_user_id) AS unique_attendees
       FROM health_event_scan s WHERE s.health_event_id = ? GROUP BY DATE(s.scanned_at) ORDER BY day`, [id]);
    const [appointmentsPerService] = await mysqlConnection.promise().query(
      `SELECT sl.service_key, sl.slot_date, COUNT(*) AS booked
       FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
       WHERE sl.health_event_id = ? AND a.status = 'booked'
       GROUP BY sl.service_key, sl.slot_date ORDER BY sl.slot_date`, [id]);
    const [checkoutStatus] = await mysqlConnection.promise().query(
      `SELECT st.name_en AS stand_en, st.name_es AS stand_es, o.name_en, o.name_es, COUNT(*) AS total
       FROM health_event_scan_answer sa
       INNER JOIN health_event_scan s ON s.id = sa.scan_id
       INNER JOIN health_event_stand st ON st.id = s.stand_id
       INNER JOIN health_event_scan_answer_option sao ON sao.scan_answer_id = sa.id
       INNER JOIN health_event_question_option o ON o.id = sao.option_id
       WHERE s.health_event_id = ?
       GROUP BY st.name_en, st.name_es, o.name_en, o.name_es`, [id]);

    res.status(200).json({
      registrations_by_source: bySource,
      scans_per_stand: scansPerStand.map(r => ({ ...r, day: toSqlDateString(r.day) })),
      unique_attendees_per_day: uniquePerDay.map(r => ({ ...r, day: toSqlDateString(r.day) })),
      appointments_per_service: appointmentsPerService.map(r => ({ ...r, slot_date: toSqlDateString(r.slot_date) })),
      checkout_status: checkoutStatus
    });
  } catch (error) {
    logger.error('GET /health-events/:id/metrics-summary error: ' + error.message);
    res.status(500).json({ error: 'INTERNAL', message: 'Internal server error' });
  }
});

module.exports = router;
