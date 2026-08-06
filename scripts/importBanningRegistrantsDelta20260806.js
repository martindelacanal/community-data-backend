/**
 * Delta import — Jotform export 2026-08-06 (317 filas) para el evento Banning.
 *
 * Diferencia clave con importBanningRegistrants.js (corrida 2026-07-28, 235
 * filas): el export nuevo está ordenado con lo más reciente primero, por lo
 * que los números de fila CAMBIARON y el external_ref por fila ya no sirve
 * como idempotencia. Acá el matcheo es por PERSONA contra la base:
 *   1. email + (DOB o nombre)  — misma regla que la corrida original;
 *   2. nombre exacto + DOB     — encuentra a los creados por la corrida
 *      original (mismo parser => mismos firstname/lastname/dob) y a los
 *      registrados vía web.
 *
 * Según el resultado:
 *   - Usuario con registración al evento  -> solo se AGREGA lo faltante
 *     (días, turnos, respuestas no respondidas). No pisa respuestas.
 *   - Usuario sin registración            -> registración + respuestas
 *     completas (conserva su contraseña).
 *   - Sin usuario                         -> usuario nuevo con contraseña
 *     'bienestarcommunity' + registración completa.
 *
 * El CSV de credenciales lista SOLO a las personas procesadas en esta corrida
 * (registración nueva), no a las ya importadas.
 *
 * Usage:
 *   PW='***' node importBanningRegistrantsDelta20260806.js <host> <user> <database> <port> [--dry-run]
 */
const fs = require('fs');
const XLSX = require('xlsx');
const mysql = require('mysql2/promise');
const bcryptjs = require('bcryptjs');

const [, , host, user, database, port] = process.argv;
const password = process.env.PW;
const DRY_RUN = process.argv.includes('--dry-run');

if (!host || !user || !database || !port || password == null) {
  console.error('Usage: PW=*** node importBanningRegistrantsDelta20260806.js <host> <user> <database> <port> [--dry-run]');
  process.exit(1);
}

const BASE = 'c:/Users/marti/Desktop/TRABAJO/PROYECTOS/COMMUNITY_DATA/BASE DE DATOS/BANNING';
const SOURCE_XLSX = `${BASE}/D5_Community_Health_Clinic_Regi2026-08-06_01_38_20.xlsx`;
const PARSED_JSON = `${BASE}/registrants_parsed_2026-08-06.json`;
const OUTPUT_CSV = `${BASE}/credenciales_migrados_${database}_${host === 'localhost' ? 'dev' : 'prod'}_2026-08-06${DRY_RUN ? '.dryrun' : ''}.csv`;
const DEFAULT_PASSWORD = 'bienestarcommunity';
const EXTERNAL_REF_PREFIX = 'jotform20260806';

const norm = (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, ' ');

// Misma persona con variante de nombre, verificada a mano contra prod
// (mismo teléfono + misma DOB, o mismo email personal + apellido):
//   Fabiola Villaseñor Velazquez  = #41590 Fabiola Villaseñor (dob 1977-07-17)
//   Elizabeth Gallegos            = #36089 Elizabeth Maravilla (dob 1962-09-20)
//   Edelmira Márquez De Sibrian   = #13133 Eslmita Márquez (dob 1968-05-08)
//   Jeanette Catano Nunez         = #42051 Jeanette Nunez (mismo email; dob typo 01 vs 11)
const MANUAL_USER_OVERRIDES = new Map([
  ['fabiola villaseñor velazquez|1977-07-17', 41590],
  ['elizabeth gallegos|1962-09-20', 36089],
  ['edelmira márquez de sibrian|1968-05-08', 13133],
  ['jeanette catano nunez|1959-11-02', 42051]
]);

function splitFullName(fullName) {
  const parts = String(fullName || '').trim().split(/\s+/);
  if (parts.length <= 1) return { firstName: parts[0] || null, lastName: null };
  return { firstName: parts.slice(0, -1).join(' '), lastName: parts[parts.length - 1] };
}

function normalizeForUsername(text) {
  return String(text || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase().replace(/[^a-z0-9]+/g, '.')
    .replace(/^\.+|\.+$/g, '').replace(/\.{2,}/g, '.');
}

const MONTHS = { jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06', jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12' };

/** 'Feb 10, 1966' -> '1966-02-10' */
function parseDob(raw) {
  const match = String(raw || '').match(/([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})/);
  if (!match) return null;
  const month = MONTHS[match[1].slice(0, 3).toLowerCase()];
  if (!month) return null;
  return `${match[3]}-${month}-${String(match[2]).padStart(2, '0')}`;
}

/** 'Saturday, Aug 08, 2026 9:00 AM-10:00 AM' -> {date:'2026-08-08', start:'09:00'} */
function parseAppointment(raw) {
  if (!raw) return null;
  const match = String(raw).match(/([A-Za-z]+) (\d{2}), (\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)/i);
  if (!match) return null;
  const month = MONTHS[match[1].slice(0, 3).toLowerCase()];
  if (!month) return null;
  let hour = Number(match[4]);
  const isPm = match[6].toUpperCase() === 'PM';
  if (isPm && hour !== 12) hour += 12;
  if (!isPm && hour === 12) hour = 0;
  return { date: `${match[3]}-${month}-${match[2]}`, start: `${String(hour).padStart(2, '0')}:${match[5]}` };
}

function dateFromAttend(raw) {
  if (!raw) return null;
  if (/august 8/i.test(raw)) return '2026-08-08';
  if (/august 9/i.test(raw)) return '2026-08-09';
  return null;
}

function priorityKey(raw) {
  if (!raw) return null;
  if (/dental/i.test(raw)) return 'dental';
  if (/oftalmol|vision/i.test(raw)) return 'vision';
  return null;
}

function normalizePhone(raw) {
  if (!raw) return null;
  let digits = String(raw).replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) digits = digits.slice(1);
  return digits.slice(0, 20) || null;
}

// --- parse XLSX (mismas posiciones de columna que el export de julio) --------
function parseWorkbook() {
  const wb = XLSX.readFile(SOURCE_XLSX);
  const ws = wb.Sheets[wb.SheetNames[0]];
  const raw = XLSX.utils.sheet_to_json(ws, { header: 1, raw: false, defval: null });
  const rows = [];
  for (let i = 1; i < raw.length; i++) {
    const r = raw[i];
    if (!r || !r[5]) continue; // sin nombre -> fila vacía
    rows.push({
      row: i + 1, // número de fila real de la planilla (header = 1)
      submission_date: r[0],
      biw_participant: r[1],
      terms: r[2],
      previous_event: r[3],
      registering: r[4],
      full_name: String(r[5]).trim(),
      parent_guardian: r[6],
      dob_raw: r[7],
      dob: parseDob(r[7]),
      age: r[9],
      phone: normalizePhone(r[10]),
      email: r[11] ? String(r[11]).trim().toLowerCase() : null,
      sex_assigned: r[12],
      gender_identity: r[13],
      ethnicity: r[14],
      d5_resident: r[15],
      city: r[16],
      zip: r[17] ? String(r[17]).trim() : null,
      participant_id: r[18],
      services_interested: r[19],
      date_attend: r[20],
      priority_sat: r[21],
      dental_appt_sat: r[22],
      vision_appt_sat: r[23],
      priority_sun: r[24],
      dental_appt_sun: r[25],
      vision_appt_sun: r[26],
      heard_about: r[27],
      consent: r[28]
    });
  }
  return rows;
}

(async () => {
  const rows = parseWorkbook();
  fs.writeFileSync(PARSED_JSON, JSON.stringify(rows, null, 1), 'utf8');
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });
  const log = (...a) => console.log('[delta]', ...a);
  log(`parsed rows=${rows.length} -> ${PARSED_JSON}`);

  // --- event + questions (idéntico a la corrida original) -------------------
  const [[event]] = await c.query('SELECT * FROM health_event WHERE slug = "banning" LIMIT 1').then(([r]) => [r]);
  if (!event) { console.error('Event "banning" not found.'); process.exit(1); }

  const [questions] = await c.query(
    `SELECT q.*, f.audience FROM health_event_question q
     INNER JOIN health_event_form f ON f.id = q.form_id
     WHERE f.health_event_id = ? AND f.audience = 'beneficiary'`, [event.id]);
  const [options] = await c.query(
    `SELECT o.* FROM health_event_question_option o
     INNER JOIN health_event_question q ON q.id = o.question_id
     INNER JOIN health_event_form f ON f.id = q.form_id
     WHERE f.health_event_id = ?`, [event.id]);
  const optionsByQuestion = new Map();
  for (const o of options) {
    if (!optionsByQuestion.has(o.question_id)) optionsByQuestion.set(o.question_id, []);
    optionsByQuestion.get(o.question_id).push(o);
  }
  const qByName = new Map(questions.map(q => [q.name_en, q]));
  const findQ = (name) => {
    const q = qByName.get(name);
    if (!q) { console.error('MISSING QUESTION:', name); process.exit(1); }
    return q;
  };
  const findOpt = (q, value, aliases = {}) => {
    if (value == null) return null;
    const target = norm(aliases[norm(value)] || value);
    return (optionsByQuestion.get(q.id) || []).find(o => norm(o.name_en) === target) || null;
  };
  const otherOpt = (q) => (optionsByQuestion.get(q.id) || []).find(o => o.is_other === 'Y') || null;

  const Q = {
    biw: findQ('Are you currently registered as a Bienestar Program participant?'),
    consentReg: qByName.get('Do you consent for your registration information to be entered into the Bienestar participant system for record keeping and follow-up?') || null,
    previous: findQ('Have you attended a previous Bienestar or D5 Community Health Fair event?'),
    who: findQ('Are you registering yourself or someone else?'),
    relationship: findQ('If registering someone else, relationship to participant'),
    guardian: findQ('Parent/guardian name, if participant is under 18'),
    sex: findQ('What was your sex assigned at birth on your original birth certificate?'),
    gender: findQ('What is your current gender identity?'),
    ethnicity: findQ('Ethnicity'),
    city: findQ('Which city are you from?'),
    zip: findQ('Zip Code'),
    d5: findQ('Are you a Riverside County District 5 resident?'),
    date: findQ('Which date would you like to attend?'),
    services: findQ('Which services are you interested in receiving?'),
    prioSat: findQ('Which service would you like to prioritize? (Saturday 8/8/26)'),
    prioSun: findQ('Which service would you like to prioritize? (Sunday 8/9/26)'),
    heard: findQ('How did you hear about this event?'),
    consent: findQ('Consent'),
    photo: findQ('Photo/video consent: I authorize Bienestar is Wellbeing and its partners to photograph and/or record me during this event.')
  };

  const [slots] = await c.query(
    'SELECT id, service_key, DATE_FORMAT(slot_date, "%Y-%m-%d") AS slot_date, TIME_FORMAT(start_time, "%H:%i") AS start FROM health_event_slot WHERE health_event_id = ?',
    [event.id]);
  const slotIndex = new Map(slots.map(s => [`${s.service_key}|${s.slot_date}|${s.start}`, s.id]));

  const [[{ client_id: eventClientId } = {}]] = await c.query(
    'SELECT client_id FROM client_location WHERE location_id = ? LIMIT 1', [event.location_id]).then(([r]) => [r]);

  // --- merge personas (mismo criterio: nombre normalizado + DOB) ------------
  const persons = new Map();
  for (const row of rows) {
    const key = `${norm(row.full_name)}|${row.dob}`;
    if (!persons.has(key)) persons.set(key, { rows: [row] });
    else persons.get(key).rows.push(row);
  }
  log(`unique persons=${persons.size}`);

  const usedEmails = new Set();
  const credentials = [];
  const warnings = [];
  let createdUsers = 0, newRegsExistingUsers = 0, mergedExisting = 0, appointmentsBooked = 0, datesAdded = 0, answersAdded = 0;

  for (const [personKey, person] of persons) {
    person.rows.sort((a, b) => a.row - b.row);
    // La planilla viene ordenada de más nuevo a más viejo: la fila MENOR es la
    // submission más reciente -> esa manda para los datos demográficos.
    const primary = person.rows[0];
    const externalRef = `${EXTERNAL_REF_PREFIX}:${person.rows.map(r => r.row).join(',')}`;

    const { firstName, lastName } = splitFullName(primary.full_name);
    const email = primary.email;
    const phone = primary.phone;
    const zip = primary.zip;

    // --- 1. matchear usuario existente ------------------------------------
    let userId = null;
    let username = null;
    let assignedEmail = null;
    let matchedExisting = false;
    let matchedBy = null;

    const override = MANUAL_USER_OVERRIDES.get(personKey);
    if (override) {
      const [[overrideUser]] = await c.query(
        'SELECT id, username, email FROM user WHERE id = ? AND deleted = "N" LIMIT 1', [override]).then(([r]) => [r]);
      if (overrideUser) {
        userId = overrideUser.id;
        username = overrideUser.username;
        assignedEmail = overrideUser.email;
        matchedExisting = true;
        matchedBy = 'manual-override';
        if (email) usedEmails.add(email);
      }
    }

    if (!userId && email) {
      const [candidates] = await c.query(
        'SELECT id, username, firstname, lastname, date_of_birth, role_id FROM user WHERE email = ? AND deleted = "N" LIMIT 1', [email]);
      if (candidates.length) {
        const cand = candidates[0];
        const dobMatches = cand.date_of_birth && primary.dob &&
          new Date(cand.date_of_birth).toISOString().slice(0, 10) === primary.dob;
        const nameMatches = norm(`${cand.firstname} ${cand.lastname}`) === norm(primary.full_name) ||
          norm(cand.firstname) === norm(firstName);
        if (cand.role_id === 5 && (dobMatches || nameMatches)) {
          userId = cand.id;
          username = cand.username || email;
          assignedEmail = email;
          matchedExisting = true;
          matchedBy = 'email';
        }
        usedEmails.add(email);
      }
    }

    // Nombre exacto + DOB: encuentra a los creados por la corrida de julio y
    // a los que se registraron por la web con los mismos datos.
    if (!userId && primary.dob && firstName && lastName) {
      const [byName] = await c.query(
        `SELECT u.id, u.username, u.email,
                (SELECT r.id FROM health_event_registration r
                 WHERE r.user_id = u.id AND r.health_event_id = ? AND r.registration_role = 'beneficiary' LIMIT 1) AS reg_id
         FROM user u
         WHERE u.deleted = 'N' AND u.role_id = 5 AND u.date_of_birth = ?
           AND LOWER(TRIM(u.firstname)) = ? AND LOWER(TRIM(u.lastname)) = ?
         ORDER BY reg_id IS NULL, u.id LIMIT 1`,
        [event.id, primary.dob, norm(firstName), norm(lastName)]);
      if (byName.length) {
        userId = byName[0].id;
        username = byName[0].username;
        assignedEmail = byName[0].email;
        matchedExisting = true;
        matchedBy = 'name+dob';
      }
    }

    // --- 2. ¿ya tiene registración? ---------------------------------------
    let registrationId = null;
    if (userId) {
      const [existingReg] = await c.query(
        'SELECT id FROM health_event_registration WHERE health_event_id = ? AND user_id = ? AND registration_role = "beneficiary" LIMIT 1',
        [event.id, userId]);
      if (existingReg.length) registrationId = existingReg[0].id;
    }
    const isNewRegistration = registrationId == null;

    // Aviso de posible duplicado: sin match pero hay usuario con mismo teléfono
    if (!userId && phone) {
      const [samePhone] = await c.query(
        'SELECT id, firstname, lastname FROM user WHERE phone = ? AND deleted = "N" AND role_id = 5 LIMIT 1', [phone]);
      if (samePhone.length) {
        warnings.push(`POSIBLE DUP (tel ${phone}): "${primary.full_name}" vs user #${samePhone[0].id} ${samePhone[0].firstname} ${samePhone[0].lastname} — se crea usuario nuevo igual (familias comparten teléfono)`);
      }
    }

    // --- 3. crear usuario si hace falta -----------------------------------
    if (!userId) {
      let base = normalizeForUsername(`${firstName}.${lastName || ''}`).slice(0, 40) || `participant.${primary.row}`;
      username = base;
      let n = 1;
      while (true) {
        const [taken] = await c.query('SELECT id FROM user WHERE username = ? LIMIT 1', [username]);
        if (!taken.length) break;
        n++;
        username = `${base.slice(0, 40 - String(n).length)}${n}`;
      }
      assignedEmail = (email && !usedEmails.has(email)) ? email : null;
      if (email) usedEmails.add(email);

      if (!DRY_RUN) {
        const passwordHash = await bcryptjs.hash(DEFAULT_PASSWORD, 8);
        const [ins] = await c.query(
          'INSERT INTO user(username, password, email, role_id, client_id, firstname, lastname, date_of_birth, phone, \
            zipcode, first_location_id, location_id, household_size, language, legal_consent_accepted, \
            legal_consent_accepted_at, legal_consent_version, reset_password) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
          [username, passwordHash, assignedEmail, 5, eventClientId || null, firstName, lastName, primary.dob, phone,
            zip, event.location_id, event.location_id, 1, 'en', 1, new Date(), '2026-03-02', 'Y']);
        userId = ins.insertId;
        if (eventClientId) {
          await c.query('INSERT IGNORE INTO client_user(client_id, user_id) VALUES (?,?)', [eventClientId, userId]);
        }
      }
      createdUsers++;
    }

    if (DRY_RUN) {
      if (isNewRegistration) {
        credentials.push({ name: primary.full_name, username, email: assignedEmail || '', contact: email || '', matched: matchedExisting, dates: person.rows.map(r => dateFromAttend(r.date_attend)).filter(Boolean).join(' + ') });
      } else {
        mergedExisting++;
      }
      continue;
    }

    // --- 4. registración ---------------------------------------------------
    if (isNewRegistration) {
      const submittedAt = primary.submission_date ? new Date(primary.submission_date) : new Date();
      const [regIns] = await c.query(
        'INSERT INTO health_event_registration(health_event_id, user_id, registration_role, status, contact_email, source, external_ref, submitted_at) \
         VALUES (?,?,?,?,?,?,?,?)',
        [event.id, userId, 'beneficiary', 'registered', email, 'import_jotform', externalRef, submittedAt]);
      registrationId = regIns.insertId;
      if (matchedExisting) newRegsExistingUsers++;
    } else {
      mergedExisting++;
    }

    // --- 5. días + turnos (siempre, agrega lo faltante) --------------------
    for (const row of person.rows) {
      const date = dateFromAttend(row.date_attend);
      if (date) {
        const priority = date === '2026-08-08' ? priorityKey(row.priority_sat) : priorityKey(row.priority_sun);
        const [dr] = await c.query(
          'INSERT INTO health_event_registration_date(registration_id, event_date, priority_service) VALUES (?,?,?) \
           ON DUPLICATE KEY UPDATE priority_service = COALESCE(VALUES(priority_service), priority_service)',
          [registrationId, date, priority]);
        if (dr.affectedRows === 1) datesAdded++;
      }
      for (const [rawAppt, service] of [
        [row.dental_appt_sat, 'dental'], [row.vision_appt_sat, 'vision'],
        [row.dental_appt_sun, 'dental'], [row.vision_appt_sun, 'vision']
      ]) {
        const parsed = parseAppointment(rawAppt);
        if (!parsed) continue;
        const slotId = slotIndex.get(`${service}|${parsed.date}|${parsed.start}`);
        if (!slotId) { log(`WARN no slot for ${service} ${parsed.date} ${parsed.start} (row ${row.row})`); continue; }
        const [ar] = await c.query(
          'INSERT IGNORE INTO health_event_appointment(registration_id, slot_id) VALUES (?,?)', [registrationId, slotId]);
        if (ar.affectedRows > 0) appointmentsBooked++;
      }
    }

    // --- 6. respuestas -----------------------------------------------------
    // Registración nueva: respuestas completas. Registración existente: SOLO
    // preguntas sin respuesta previa (no pisar lo que ya contestaron).
    const insertAnswer = async (q, { text = null, number = null, optionIds = [], otherText = null }) => {
      const [existing] = await c.query(
        'SELECT id FROM health_event_answer WHERE registration_id = ? AND question_id = ? LIMIT 1', [registrationId, q.id]);
      if (existing.length) {
        if (!isNewRegistration) return; // no pisar
        const answerId = existing[0].id;
        await c.query('UPDATE health_event_answer SET answer_text=?, answer_number=?, other_text=?, source="import_jotform" WHERE id=?',
          [text, number, otherText, answerId]);
        await c.query('DELETE FROM health_event_answer_option WHERE answer_id = ?', [answerId]);
        for (const optionId of optionIds) {
          await c.query('INSERT IGNORE INTO health_event_answer_option(answer_id, option_id) VALUES (?,?)', [answerId, optionId]);
        }
        return;
      }
      const [ins] = await c.query(
        'INSERT INTO health_event_answer(registration_id, question_id, answer_text, answer_number, other_text, source) VALUES (?,?,?,?,?,"import_jotform")',
        [registrationId, q.id, text, number, otherText]);
      for (const optionId of optionIds) {
        await c.query('INSERT IGNORE INTO health_event_answer_option(answer_id, option_id) VALUES (?,?)', [ins.insertId, optionId]);
      }
      answersAdded++;
    };
    const answerSingle = async (q, rawValue, aliases = {}, otherTextWhenUnknown = true) => {
      if (rawValue == null || rawValue === '') return;
      let option = findOpt(q, rawValue, aliases);
      let otherText = null;
      if (!option && otherTextWhenUnknown) {
        option = otherOpt(q);
        otherText = String(rawValue).slice(0, 500);
      }
      if (!option) return;
      await insertAnswer(q, { optionIds: [option.id], otherText });
    };

    await answerSingle(Q.biw, primary.biw_participant, {}, false);
    if (primary.terms && Q.consentReg && Q.consentReg.question_type !== 'notice') {
      await insertAnswer(Q.consentReg, { number: 1 });
    }
    await answerSingle(Q.previous, primary.previous_event, {}, false);
    const whoOptions = optionsByQuestion.get(Q.who.id);
    const isMyself = ['myself', 'ami mismo', 'a mí mismo/a'].includes(norm(primary.registering));
    await insertAnswer(Q.who, { optionIds: [whoOptions[isMyself ? 0 : 1].id] });
    if (!isMyself && primary.registering) {
      await insertAnswer(Q.relationship, { text: String(primary.registering).slice(0, 500) });
    }
    if (primary.parent_guardian) await insertAnswer(Q.guardian, { text: String(primary.parent_guardian).slice(0, 500) });
    await answerSingle(Q.sex, primary.sex_assigned, { 'decline to state / prefer not to respond': 'Decline to state / Prefer not to respond' }, false);
    await answerSingle(Q.gender, primary.gender_identity, {}, true);
    await answerSingle(Q.ethnicity, primary.ethnicity, { 'white/caucasian': 'White' }, true);
    await answerSingle(Q.city, primary.city, {}, false);
    await answerSingle(Q.zip, primary.zip, {}, true);
    await answerSingle(Q.d5, primary.d5_resident, {}, false);

    const attendDate = dateFromAttend(primary.date_attend);
    if (attendDate) {
      const dateOption = (optionsByQuestion.get(Q.date.id) || []).find(o =>
        o.event_date && new Date(o.event_date).toISOString().slice(0, 10) === attendDate);
      if (dateOption) await insertAnswer(Q.date, { optionIds: [dateOption.id] });
    }

    if (primary.services_interested) {
      const parts = String(primary.services_interested).split('\n').map(s => s.trim()).filter(Boolean);
      const optionIds = [];
      const unknown = [];
      for (const part of parts) {
        const option = findOpt(Q.services, part, {
          'general health screening / clinical service': 'General health screening / clinical service'
        });
        if (option) optionIds.push(option.id);
        else unknown.push(part);
      }
      if (unknown.length) {
        const other = otherOpt(Q.services);
        if (other) optionIds.push(other.id);
      }
      if (optionIds.length) {
        await insertAnswer(Q.services, { optionIds: [...new Set(optionIds)], otherText: unknown.length ? unknown.join('; ').slice(0, 500) : null });
      }
    }

    const prioSatKey = priorityKey(primary.priority_sat);
    if (prioSatKey) {
      const option = (optionsByQuestion.get(Q.prioSat.id) || []).find(o => o.service_key === prioSatKey);
      if (option) await insertAnswer(Q.prioSat, { optionIds: [option.id] });
    }
    const prioSunKey = priorityKey(primary.priority_sun);
    if (prioSunKey) {
      const option = (optionsByQuestion.get(Q.prioSun.id) || []).find(o => o.service_key === prioSunKey);
      if (option) await insertAnswer(Q.prioSun, { optionIds: [option.id] });
    }

    await answerSingle(Q.heard, primary.heard_about, {
      'email': 'Other', 'school district email': 'Other', 'school district': 'Other',
      'i’m on the email list': 'Other', 'facebook': 'Social Media (Facebook/Instagram/Tiktok, etc)'
    }, true);
    if (primary.consent) {
      await insertAnswer(Q.consent, { number: 1 });
      const yesOption = (optionsByQuestion.get(Q.photo.id) || [])[0];
      if (yesOption) await insertAnswer(Q.photo, { optionIds: [yesOption.id] });
    }

    if (isNewRegistration) {
      credentials.push({
        name: primary.full_name,
        username,
        email: assignedEmail || '',
        contact: email || '',
        matched: matchedExisting,
        matchedBy,
        dates: person.rows.map(r => dateFromAttend(r.date_attend)).filter(Boolean).join(' + ')
      });
    }
  }

  // --- CSV (solo registraciones nuevas de esta corrida) ----------------------
  const csvLines = ['full_name,username,login_email,contact_email,password,existing_account,dates'];
  for (const cred of credentials) {
    csvLines.push([
      `"${cred.name.replace(/"/g, '""')}"`, cred.username, cred.email, cred.contact,
      cred.matched ? '(kept their current password)' : DEFAULT_PASSWORD,
      cred.matched ? 'yes' : 'no', cred.dates || ''
    ].join(','));
  }
  fs.writeFileSync(OUTPUT_CSV, '\ufeff' + csvLines.join('\n'), 'utf8');

  log(`${DRY_RUN ? 'DRY-RUN' : 'DONE'} — nuevos usuarios=${createdUsers}, registraciones nuevas de usuarios existentes=${newRegsExistingUsers}, personas ya registradas (merge, no CSV)=${mergedExisting}`);
  if (!DRY_RUN) log(`agregado: turnos=${appointmentsBooked}, días=${datesAdded}, respuestas=${answersAdded}`);
  log(`credenciales nuevas en CSV: ${credentials.length} -> ${OUTPUT_CSV}`);
  for (const warning of warnings) log(warning);

  const [[regCount]] = await c.query(
    'SELECT COUNT(*) n FROM health_event_registration WHERE health_event_id = ? AND registration_role = "beneficiary"', [event.id]);
  log(`verify — total registraciones beneficiary del evento: ${regCount.n}`);
  await c.end();
})().catch(e => { console.error('IMPORT ERROR:', e.message, e.stack); process.exit(1); });
