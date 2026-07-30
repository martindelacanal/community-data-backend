/**
 * Import of the 235 Jotform pre-registrations for the Banning health clinic.
 *
 * Source: BASE DE DATOS/BANNING/registrants_parsed.json (parsed from
 * D5_Community_Health_Clinic_Regi2026-07-27_11_47_06.xlsx).
 *
 * For every unique person (same normalized full name + DOB rows are merged —
 * those are people who registered for BOTH days):
 *   - Reuses an existing user when the email already belongs to a user whose
 *     DOB or name matches (BIW participants); their password is NOT touched.
 *   - Otherwise creates a role-5 user with username firstname.lastname[N] and
 *     password 'bienestarcommunity' (bcrypt). The email is stored on the user
 *     only for its FIRST occurrence (user.email is UNIQUE; families share
 *     emails) — every registration keeps contact_email regardless.
 *   - Creates the health_event_registration (source 'import_jotform',
 *     external_ref 'jotform:<row>'), attendance dates + priority, appointment
 *     slots, and section 1 + 2 answers mapped onto the seeded questions.
 *
 * Idempotent: people whose external_ref already has a registration are skipped.
 * Outputs a credentials CSV next to the source JSON.
 *
 * Usage:
 *   PW='***' node importBanningRegistrants.js <host> <user> <database> <port> [--dry-run]
 */
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const bcryptjs = require('bcryptjs');

const [, , host, user, database, port] = process.argv;
const password = process.env.PW;
const DRY_RUN = process.argv.includes('--dry-run');

if (!host || !user || !database || !port || password == null) {
  console.error('Usage: PW=*** node importBanningRegistrants.js <host> <user> <database> <port> [--dry-run]');
  process.exit(1);
}

const SOURCE = 'c:/Users/marti/Desktop/TRABAJO/PROYECTOS/COMMUNITY_DATA/BASE DE DATOS/BANNING/registrants_parsed.json';
const OUTPUT_CSV = `c:/Users/marti/Desktop/TRABAJO/PROYECTOS/COMMUNITY_DATA/BASE DE DATOS/BANNING/credenciales_migrados_${database}_${host === 'localhost' ? 'dev' : 'prod'}.csv`;
const DEFAULT_PASSWORD = 'bienestarcommunity';

const norm = (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, ' ');

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

/** 'Saturday, Aug 08, 2026 9:00 AM-10:00 AM' -> {date:'2026-08-08', start:'09:00'} */
function parseAppointment(raw) {
  if (!raw) return null;
  const match = String(raw).match(/([A-Za-z]+) (\d{2}), (\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)/i);
  if (!match) return null;
  const months = { jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06', jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12' };
  const month = months[match[1].slice(0, 3).toLowerCase()];
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

(async () => {
  const rows = JSON.parse(fs.readFileSync(SOURCE, 'utf8'));
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });
  const log = (...a) => console.log('[import]', ...a);

  // --- event + questions ---------------------------------------------------
  const [[event]] = await c.query('SELECT * FROM health_event WHERE slug = "banning" LIMIT 1').then(([r]) => [r]);
  if (!event) { console.error('Event "banning" not seeded in this database. Run seedBanningHealthEvent.js first.'); process.exit(1); }

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
    // 2026-07-29: this consent question became a non-answerable 'notice' (new
    // text, type 'notice') — optional lookup so the import still runs on
    // adjusted databases; when absent/notice the terms answer is simply skipped.
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

  // --- merge duplicate persons ----------------------------------------------
  const persons = new Map(); // key name|dob -> merged person
  for (const row of rows) {
    if (!row.full_name) continue;
    const key = `${norm(row.full_name)}|${row.dob}`;
    if (!persons.has(key)) {
      persons.set(key, { rows: [row] });
    } else {
      persons.get(key).rows.push(row);
    }
  }
  log(`rows=${rows.length} unique persons=${persons.size}`);

  // Track emails already consumed (existing DB users + earlier persons in this run).
  const usedEmails = new Set();
  const credentials = [];
  let created = 0, reusedUsers = 0, skipped = 0, appointmentsBooked = 0;

  for (const [, person] of persons) {
    person.rows.sort((a, b) => a.row - b.row);
    const primary = person.rows[person.rows.length - 1]; // latest submission wins for demographics
    const externalRef = `jotform:${person.rows.map(r => r.row).join(',')}`;

    // Idempotency: skip if any of this person's row refs are already imported.
    const [existingRef] = await c.query(
      'SELECT id FROM health_event_registration WHERE health_event_id = ? AND external_ref = ? LIMIT 1',
      [event.id, externalRef]);
    if (existingRef.length) { skipped++; continue; }

    const { firstName, lastName } = splitFullName(primary.full_name);
    const email = primary.email ? String(primary.email).trim().toLowerCase() : null;
    const phone = primary.phone ? String(primary.phone).replace(/\D/g, '').slice(0, 20) : null;
    const zip = primary.zip ? String(primary.zip).trim() : null;

    // 1. try to match an existing user by email + (dob or name)
    let userId = null;
    let username = null;
    let assignedEmail = null;
    let matchedExisting = false;
    if (email) {
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
        }
        usedEmails.add(email); // taken either way
      }
    }

    // 2. create user
    if (!userId) {
      let base = normalizeForUsername(`${firstName}.${lastName || ''}`).slice(0, 40) || `participant.${primary.row}`;
      username = base;
      let n = 1;
      // unique username
      // eslint-disable-next-line no-constant-condition
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
            legal_consent_accepted_at, legal_consent_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
          [username, passwordHash, assignedEmail, 5, eventClientId || null, firstName, lastName, primary.dob, phone,
            zip, event.location_id, event.location_id, 1, 'en', 1, new Date(), '2026-03-02']);
        userId = ins.insertId;
        if (eventClientId) {
          await c.query('INSERT IGNORE INTO client_user(client_id, user_id) VALUES (?,?)', [eventClientId, userId]);
        }
      }
      created++;
    } else {
      reusedUsers++;
    }

    if (DRY_RUN) {
      credentials.push({ name: primary.full_name, username, email: assignedEmail || '', contact: email || '', matched: matchedExisting });
      continue;
    }

    // 3. registration (guard against a user already registered via another path)
    const [existingReg] = await c.query(
      'SELECT id FROM health_event_registration WHERE health_event_id = ? AND user_id = ? AND registration_role = "beneficiary" LIMIT 1',
      [event.id, userId]);
    let registrationId;
    if (existingReg.length) {
      registrationId = existingReg[0].id;
    } else {
      const submittedAt = primary.submission_date ? new Date(primary.submission_date) : new Date();
      const [regIns] = await c.query(
        'INSERT INTO health_event_registration(health_event_id, user_id, registration_role, status, contact_email, source, external_ref, submitted_at) \
         VALUES (?,?,?,?,?,?,?,?)',
        [event.id, userId, 'beneficiary', 'registered', email, 'import_jotform', externalRef, submittedAt]);
      registrationId = regIns.insertId;
    }

    // 4. dates + priority + appointments from ALL merged rows
    for (const row of person.rows) {
      const date = dateFromAttend(row.date_attend);
      if (date) {
        const priority = date === '2026-08-08' ? priorityKey(row.priority_sat) : priorityKey(row.priority_sun);
        await c.query(
          'INSERT INTO health_event_registration_date(registration_id, event_date, priority_service) VALUES (?,?,?) \
           ON DUPLICATE KEY UPDATE priority_service = COALESCE(VALUES(priority_service), priority_service)',
          [registrationId, date, priority]);
      }
      for (const [rawAppt, service] of [
        [row.dental_appt_sat, 'dental'], [row.vision_appt_sat, 'vision'],
        [row.dental_appt_sun, 'dental'], [row.vision_appt_sun, 'vision']
      ]) {
        const parsed = parseAppointment(rawAppt);
        if (!parsed) continue;
        const slotId = slotIndex.get(`${service}|${parsed.date}|${parsed.start}`);
        if (!slotId) { log(`WARN no slot for ${service} ${parsed.date} ${parsed.start} (row ${row.row})`); continue; }
        await c.query(
          'INSERT IGNORE INTO health_event_appointment(registration_id, slot_id) VALUES (?,?)', [registrationId, slotId]);
        appointmentsBooked++;
      }
    }

    // 5. answers (sections 1 & 2) — from the primary row
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
    const insertAnswer = async (q, { text = null, number = null, optionIds = [], otherText = null }) => {
      const [existing] = await c.query(
        'SELECT id FROM health_event_answer WHERE registration_id = ? AND question_id = ? LIMIT 1', [registrationId, q.id]);
      let answerId;
      if (existing.length) {
        answerId = existing[0].id;
        await c.query('UPDATE health_event_answer SET answer_text=?, answer_number=?, other_text=?, source="import_jotform" WHERE id=?',
          [text, number, otherText, answerId]);
        await c.query('DELETE FROM health_event_answer_option WHERE answer_id = ?', [answerId]);
      } else {
        const [ins] = await c.query(
          'INSERT INTO health_event_answer(registration_id, question_id, answer_text, answer_number, other_text, source) VALUES (?,?,?,?,?,"import_jotform")',
          [registrationId, q.id, text, number, otherText]);
        answerId = ins.insertId;
      }
      for (const optionId of optionIds) {
        await c.query('INSERT IGNORE INTO health_event_answer_option(answer_id, option_id) VALUES (?,?)', [answerId, optionId]);
      }
    };

    await answerSingle(Q.biw, primary.biw_participant, {}, false);
    if (primary.terms && Q.consentReg && Q.consentReg.question_type !== 'notice') {
      await insertAnswer(Q.consentReg, { number: 1 });
    }
    await answerSingle(Q.previous, primary.previous_event, {}, false);
    // who: 'Myself' vs anything else
    const whoOptions = optionsByQuestion.get(Q.who.id);
    const isMyself = norm(primary.registering) === 'myself' || norm(primary.registering) === 'ami mismo' || norm(primary.registering) === 'a mí mismo/a';
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

    // attend date: option(s) for ALL merged rows -> use latest row's for the single-answer question,
    // but registration_date rows above already capture both days.
    const attendDate = dateFromAttend(primary.date_attend);
    if (attendDate) {
      const dateOption = (optionsByQuestion.get(Q.date.id) || []).find(o =>
        o.event_date && new Date(o.event_date).toISOString().slice(0, 10) === attendDate);
      if (dateOption) await insertAnswer(Q.date, { optionIds: [dateOption.id] });
    }

    // services multi
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

    // priorities
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

    credentials.push({
      name: primary.full_name,
      username,
      email: assignedEmail || '',
      contact: email || '',
      matched: matchedExisting,
      dates: person.rows.map(r => dateFromAttend(r.date_attend)).filter(Boolean).join(' + ')
    });
  }

  // credentials CSV
  const csvLines = ['full_name,username,login_email,contact_email,password,existing_biw_account,dates'];
  for (const cred of credentials) {
    csvLines.push([
      `"${cred.name.replace(/"/g, '""')}"`, cred.username, cred.email, cred.contact,
      cred.matched ? '(kept their current password)' : DEFAULT_PASSWORD,
      cred.matched ? 'yes' : 'no', cred.dates || ''
    ].join(','));
  }
  fs.writeFileSync(OUTPUT_CSV, '\ufeff' + csvLines.join('\n'), 'utf8');

  log(`DONE — created users=${created}, matched existing=${reusedUsers}, skipped(already imported)=${skipped}, appointments=${appointmentsBooked}`);
  log(`credentials CSV: ${OUTPUT_CSV}`);

  const [[regCount]] = await c.query(
    'SELECT COUNT(*) n FROM health_event_registration WHERE health_event_id = ? AND source = "import_jotform"', [event.id]);
  const [[dateCount]] = await c.query(
    'SELECT COUNT(*) n FROM health_event_registration_date d INNER JOIN health_event_registration r ON r.id = d.registration_id WHERE r.health_event_id = ? AND r.source = "import_jotform"', [event.id]);
  const [[apptCount]] = await c.query(
    'SELECT COUNT(*) n FROM health_event_appointment a INNER JOIN health_event_registration r ON r.id = a.registration_id WHERE r.health_event_id = ? AND r.source = "import_jotform"', [event.id]);
  const [[answerCount]] = await c.query(
    'SELECT COUNT(*) n FROM health_event_answer a INNER JOIN health_event_registration r ON r.id = a.registration_id WHERE r.health_event_id = ? AND r.source = "import_jotform"', [event.id]);
  log(`verify — registrations=${regCount.n} dates=${dateCount.n} appointments=${apptCount.n} answers=${answerCount.n}`);
  await c.end();
})().catch(e => { console.error('IMPORT ERROR:', e.message, e.stack); process.exit(1); });
