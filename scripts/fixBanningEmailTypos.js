/**
 * Corrige typos de email de TODOS los inscriptos al evento Banning (beneficiarios
 * y voluntarios, cualquier source: import_jotform, web, walkin, admin), reenvía
 * el correo de bienvenida oficial que no pudieron recibir y los da de alta en
 * Mailchimp. Al final genera el CSV unificado de ambas importaciones con los
 * emails ya corregidos.
 *
 * Detección: mismas reglas que FRONTEND/src/app/utils/email-typo.util.ts
 * (TLDs inexistentes corregibles para cualquier dominio + distancia de edición
 * contra proveedores conocidos, con lista de dominios protegidos).
 *
 * - user.email es UNIQUE: si el email corregido ya pertenece a otro usuario
 *   (familias), NO se toca user.email (se reporta) pero sí se corrige el
 *   contact_email de la registración y el correo se envía igual.
 * - Email de bienvenida: beneficiary -> confirmación de registro (con
 *   credenciales si conserva la contraseña por defecto); volunteer -> email de
 *   credenciales solo si conserva la contraseña por defecto.
 *
 * Usage:
 *   PW='***' node fixBanningEmailTypos.js <host> <user> <database> <port> [--dry-run] [--skip-csv]
 */
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const bcryptjs = require('bcryptjs');
const { parse } = require('csv-parse/sync');

const [, , host, user, database, port] = process.argv;
const password = process.env.PW;
const DRY_RUN = process.argv.includes('--dry-run');
const SKIP_CSV = process.argv.includes('--skip-csv');

if (!host || !user || !database || !port || password == null) {
  console.error('Usage: PW=*** node fixBanningEmailTypos.js <host> <user> <database> <port> [--dry-run]');
  process.exit(1);
}

const BASE = 'c:/Users/marti/Desktop/TRABAJO/PROYECTOS/COMMUNITY_DATA/BASE DE DATOS/BANNING';
const CSV_JULY = `${BASE}/credenciales_migrados_db_community_data_prod.csv`;
const CSV_AUG = `${BASE}/credenciales_migrados_db_community_data_prod_2026-08-06.csv`;
const CSV_OUT = `${BASE}/credenciales_migrados_db_community_data_prod_COMPLETO.csv`;
const DEFAULT_PASSWORD = 'bienestarcommunity';

// ---------------------------------------------------------------------------
// Detector de typos — MISMAS reglas que email-typo.util.ts del frontend
// ---------------------------------------------------------------------------
const SUGGESTABLE_DOMAINS = [
  'gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com', 'icloud.com',
  'aol.com', 'live.com', 'msn.com', 'ymail.com', 'comcast.net', 'att.net',
  'sbcglobal.net', 'verizon.net'
];
const PROTECTED_DOMAINS = new Set([
  ...SUGGESTABLE_DOMAINS,
  'googlemail.com', 'mail.com', 'me.com', 'mac.com', 'pm.me', 'proton.me',
  'protonmail.com', 'hotmail.es', 'outlook.es', 'yahoo.es', 'yahoo.com.mx',
  'hotmail.com.mx', 'live.com.mx', 'prodigy.net.mx', 'rocketmail.com',
  'roadrunner.com', 'charter.net', 'cox.net', 'earthlink.net', 'yandex.com',
  'gmx.com', 'zoho.com'
]);
const INVALID_TLD_FIXES = {
  con: 'com', vom: 'com', xom: 'com', cok: 'com', cmo: 'com', ocm: 'com',
  cim: 'com', cpm: 'com', comm: 'com', coom: 'com', comn: 'com', ccom: 'com',
  nrt: 'net', nte: 'net', nett: 'net'
};
const DOMAIN_SHAPE = /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/;

function editDistance(a, b, maxDistance) {
  if (Math.abs(a.length - b.length) > maxDistance) return maxDistance + 1;
  const rows = [];
  for (let i = 0; i <= a.length; i++) {
    rows[i] = [i];
    for (let j = 1; j <= b.length; j++) {
      if (i === 0) { rows[0][j] = j; continue; }
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      let value = Math.min(rows[i - 1][j] + 1, rows[i][j - 1] + 1, rows[i - 1][j - 1] + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) {
        value = Math.min(value, rows[i - 2][j - 2] + 1);
      }
      rows[i][j] = value;
    }
  }
  return rows[a.length][b.length];
}

function suggestEmailFix(rawEmail) {
  const email = String(rawEmail || '').trim().toLowerCase();
  const atIndex = email.lastIndexOf('@');
  if (atIndex <= 0 || atIndex === email.length - 1) return null;
  const localPart = email.slice(0, atIndex);
  const domain = email.slice(atIndex + 1);
  if (localPart.includes('@') || !DOMAIN_SHAPE.test(domain) || domain.includes('..')) return null;
  if (PROTECTED_DOMAINS.has(domain)) return null;

  const labels = domain.split('.');
  const tld = labels[labels.length - 1];
  if (labels.length >= 2 && INVALID_TLD_FIXES[tld]) {
    labels[labels.length - 1] = INVALID_TLD_FIXES[tld];
    return `${localPart}@${labels.join('.')}`;
  }

  let bestDomain = null;
  let bestDistance = Infinity;
  for (const known of SUGGESTABLE_DOMAINS) {
    const distance = editDistance(domain, known, 2);
    if (distance < bestDistance) { bestDistance = distance; bestDomain = known; }
  }
  const threshold = domain.length <= 9 ? 1 : 2;
  if (bestDomain && bestDistance > 0 && bestDistance <= threshold) {
    return `${localPart}@${bestDomain}`;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Mailchimp (config desde BACKEND/.env, mismo payload que el signup de food)
// ---------------------------------------------------------------------------
function readEnvFile() {
  const envPath = path.join(__dirname, '..', '.env');
  const vars = {};
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
    if (match) vars[match[1]] = match[2].trim();
  }
  return vars;
}

function formatDateForMailchimp(value) {
  if (!value) return '';
  const s = value instanceof Date ? value.toISOString().slice(0, 10) : String(value).slice(0, 10);
  const [, month, day] = s.split('-');
  if (!month || !day) return '';
  return `${month.padStart(2, '0')}/${day.padStart(2, '0')}`;
}

function isMemberExistsError(err) {
  try {
    const body = err?.response?.body
      ? (typeof err.response.body === 'string' ? JSON.parse(err.response.body) : err.response.body)
      : null;
    if (!body) return false;
    return body.title === 'Member Exists' || (body.detail && /already a list member/i.test(body.detail));
  } catch (_) { return false; }
}

const toSqlDate = (v) => {
  if (!v) return null;
  if (v instanceof Date) {
    return `${v.getFullYear()}-${String(v.getMonth() + 1).padStart(2, '0')}-${String(v.getDate()).padStart(2, '0')}`;
  }
  return String(v).slice(0, 10);
};

(async () => {
  const log = (...a) => console.log('[typos]', ...a);
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });

  const [[event]] = await c.query(
    `SELECT he.*, l.organization, l.community_city, l.address
     FROM health_event he INNER JOIN location l ON l.id = he.location_id
     WHERE he.slug = 'banning' LIMIT 1`).then(([r]) => [r]);
  if (!event) { console.error('Event not found'); process.exit(1); }
  const locationName = [event.organization, event.community_city].filter(Boolean).join(' — ');

  // --- 1. escaneo -----------------------------------------------------------
  const [regs] = await c.query(
    `SELECT r.id AS reg_id, r.registration_role, r.contact_email, r.status,
            u.id AS user_id, u.username, u.email, u.firstname, u.lastname, u.phone,
            u.date_of_birth, u.zipcode, u.language, u.reset_password, u.password AS password_hash,
            u.enabled AS user_enabled, u.gender_id, u.ethnicity_id
     FROM health_event_registration r
     INNER JOIN user u ON u.id = r.user_id
     WHERE r.health_event_id = ?`, [event.id]);
  log(`registraciones analizadas: ${regs.length}`);

  const fixes = [];
  for (const reg of regs) {
    const userFix = suggestEmailFix(reg.email);
    const contactFix = suggestEmailFix(reg.contact_email);
    if (userFix || contactFix) {
      fixes.push({ reg, userFix, contactFix });
    }
  }

  log(`emails con typo detectados: ${fixes.length}`);
  for (const f of fixes) {
    log(`  [${f.reg.registration_role}] ${f.reg.firstname} ${f.reg.lastname} (user #${f.reg.user_id}, ${f.reg.username})`);
    if (f.userFix) log(`     user.email:      ${f.reg.email}  ->  ${f.userFix}`);
    if (f.contactFix) log(`     contact_email:  ${f.reg.contact_email}  ->  ${f.contactFix}`);
  }

  if (DRY_RUN) {
    log('DRY-RUN: sin cambios, sin emails, sin Mailchimp, sin CSV.');
    await c.end();
    return;
  }

  // --- 2. aplicar + bienvenida + mailchimp ---------------------------------
  const emailModule = require('../api/email/email');
  const env = readEnvFile();
  const mailchimp = require('@mailchimp/mailchimp_marketing');
  mailchimp.setConfig({ apiKey: env.MAILCHIMP_KEY, server: env.MAILCHIMP_SERVER_PREFIX });
  const audienceId = env.MAILCHIMP_AUDIENCE_ID;

  let emailsSent = 0, mailchimpOk = 0, mailchimpFail = 0;

  for (const f of fixes) {
    const reg = f.reg;

    // 2a. user.email (UNIQUE: si colisiona con otro usuario, no se toca)
    if (f.userFix) {
      const [clash] = await c.query('SELECT id FROM user WHERE email = ? AND id <> ? LIMIT 1', [f.userFix, reg.user_id]);
      if (clash.length) {
        log(`  COLISION user.email: ${f.userFix} ya es de user #${clash[0].id} — user #${reg.user_id} conserva su email tal cual`);
        f.userFix = null;
      } else {
        await c.query('UPDATE user SET email = ? WHERE id = ?', [f.userFix, reg.user_id]);
      }
    }
    // 2b. contact_email de la registración (sin UNIQUE)
    if (f.contactFix) {
      await c.query('UPDATE health_event_registration SET contact_email = ? WHERE id = ?', [f.contactFix, reg.reg_id]);
    }

    const sendTo = f.contactFix || f.userFix;
    if (!sendTo) continue;

    const language = reg.language === 'es' ? 'es' : 'en';
    const hasDefaultPassword = reg.reset_password === 'Y' && bcryptjs.compareSync(DEFAULT_PASSWORD, reg.password_hash || '');
    const credentials = hasDefaultPassword ? { username: reg.username, password: DEFAULT_PASSWORD } : null;

    // 2c. bienvenida
    if (reg.registration_role === 'beneficiary') {
      const [dates] = await c.query(
        'SELECT event_date, priority_service FROM health_event_registration_date WHERE registration_id = ? ORDER BY event_date', [reg.reg_id]);
      const [appointments] = await c.query(
        `SELECT sl.service_key, sl.slot_date, TIME_FORMAT(sl.start_time, '%H:%i') AS start_time,
                TIME_FORMAT(sl.end_time, '%H:%i') AS end_time
         FROM health_event_appointment a INNER JOIN health_event_slot sl ON sl.id = a.slot_id
         WHERE a.registration_id = ? AND a.status = 'booked' ORDER BY sl.slot_date, sl.start_time`, [reg.reg_id]);
      await emailModule.sendHealthEventBeneficiaryConfirmation({
        to: sendTo,
        language,
        firstname: reg.firstname,
        eventNameEn: event.name_en,
        eventNameEs: event.name_es,
        locationName,
        address: event.address || null,
        startTime: event.start_time || null,
        endTime: event.end_time || null,
        eventStartDate: toSqlDate(event.start_date),
        eventEndDate: toSqlDate(event.end_date),
        dates: dates.map(d => ({ event_date: toSqlDate(d.event_date), priority_service: d.priority_service })),
        appointments: appointments.map(a => ({ ...a, slot_date: toSqlDate(a.slot_date) })),
        credentials
      });
      emailsSent++;
      log(`  EMAIL beneficiary -> ${sendTo}${credentials ? ' (con credenciales)' : ''}`);
    } else if (reg.registration_role === 'volunteer') {
      if (credentials) {
        await emailModule.sendHealthEventVolunteerCredentials({
          to: sendTo,
          language,
          eventNameEn: event.name_en,
          eventNameEs: event.name_es,
          username: reg.username,
          password: DEFAULT_PASSWORD,
          pendingApproval: reg.user_enabled === 'N'
        });
        emailsSent++;
        log(`  EMAIL volunteer -> ${sendTo}`);
      } else {
        log(`  SKIP email volunteer ${reg.username}: contraseña propia (no hay credenciales para reenviar)`);
      }
    }

    // 2d. mailchimp (sin campos SMS para evitar rechazos por formato de teléfono)
    try {
      const [[gender]] = await c.query('SELECT name FROM gender WHERE id = ?', [reg.gender_id]).then(([r]) => [r]);
      const [[ethnicity]] = await c.query('SELECT name FROM ethnicity WHERE id = ?', [reg.ethnicity_id]).then(([r]) => [r]);
      await mailchimp.lists.addListMember(audienceId, {
        email_address: sendTo,
        status: 'subscribed',
        merge_fields: {
          FNAME: reg.firstname || '',
          LNAME: reg.lastname || '',
          PHONE: reg.phone || '',
          BIRTHDAY: formatDateForMailchimp(reg.date_of_birth),
          MMERGE8: (gender && gender.name) || '',
          MMERGE9: (ethnicity && ethnicity.name) || '',
          EMAIL_CONSENT: 'yes'
        }
      });
      await c.query('UPDATE user SET mailchimp_error = "N" WHERE id = ?', [reg.user_id]);
      mailchimpOk++;
      log(`  MAILCHIMP ok -> ${sendTo}`);
    } catch (err) {
      if (isMemberExistsError(err)) {
        await c.query('UPDATE user SET mailchimp_error = "N" WHERE id = ?', [reg.user_id]);
        mailchimpOk++;
        log(`  MAILCHIMP ya suscripto -> ${sendTo}`);
      } else {
        await c.query('UPDATE user SET mailchimp_error = "Y" WHERE id = ?', [reg.user_id]);
        mailchimpFail++;
        log(`  MAILCHIMP ERROR -> ${sendTo}: ${err.message} ${err.response?.body ? JSON.stringify(err.response.body).slice(0, 200) : ''}`);
      }
    }
  }

  log(`correcciones aplicadas=${fixes.length}, emails enviados=${emailsSent}, mailchimp ok=${mailchimpOk}, mailchimp fail=${mailchimpFail}`);

  // --- 3. CSV unificado (emails actuales post-corrección desde la DB) -------
  if (!SKIP_CSV) {
    const readCsv = (file) => parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''), { columns: true, skip_empty_lines: true });
    const julyRows = readCsv(CSV_JULY).map(r => ({ ...r, existing_account: r.existing_biw_account ?? r.existing_account, batch: '2026-07-28' }));
    const augRows = readCsv(CSV_AUG).map(r => ({ ...r, batch: '2026-08-06' }));
    const all = [...julyRows, ...augRows];
    const outLines = ['full_name,username,login_email,contact_email,password,existing_account,dates,import_batch'];
    let refreshed = 0, notFound = 0;
    for (const row of all) {
      let username = row.username;
      let loginEmail = row.login_email || '';
      let contactEmail = row.contact_email || '';
      const [[dbUser] = []] = await c.query(
        `SELECT u.id, u.username, u.email,
                (SELECT r.contact_email FROM health_event_registration r
                 WHERE r.user_id = u.id AND r.health_event_id = ? LIMIT 1) AS reg_contact
         FROM user u WHERE u.username = ? AND u.deleted = 'N' LIMIT 1`, [event.id, username]).then(([r]) => [r]);
      if (dbUser) {
        loginEmail = dbUser.email || '';
        contactEmail = dbUser.reg_contact || contactEmail;
        refreshed++;
      } else {
        notFound++;
        log(`  CSV: username no encontrado en DB: ${username}`);
      }
      outLines.push([
        `"${String(row.full_name || '').replace(/"/g, '""')}"`,
        username, loginEmail, contactEmail, row.password || '',
        row.existing_account || 'no', row.dates || '', row.batch
      ].join(','));
    }
    fs.writeFileSync(CSV_OUT, '\ufeff' + outLines.join('\n'), 'utf8');
    log(`CSV unificado: ${all.length} personas (refrescadas de DB=${refreshed}, no encontradas=${notFound}) -> ${CSV_OUT}`);
  }

  await c.end();
})().catch(e => { console.error('ERROR:', e.message, e.stack); process.exit(1); });
