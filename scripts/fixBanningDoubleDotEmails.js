/**
 * Corrige emails con doble punto en el dominio (ej. amytriste@live..com) de los
 * inscriptos al evento Banning — caso que fixBanningEmailTypos.js no detecta
 * porque su detector descarta dominios con '..'. Aplica la corrección en
 * user.email (respetando el UNIQUE: si colisiona con otro usuario, no se toca)
 * y en health_event_registration.contact_email, y da de alta el email corregido
 * en Mailchimp con el mismo payload que el signup de food / fixBanningEmailTypos.
 *
 * NO reenvía correos de bienvenida (el correo de la encuesta ya se envió a mano
 * a la dirección corregida el 2026-08-12).
 *
 * Usage (desde BACKEND/):
 *   PW='***' node scripts/fixBanningDoubleDotEmails.js <host> <user> <database> <port> [--dry-run]
 */
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

const [, , host, user, database, port] = process.argv;
const password = process.env.PW;
const DRY_RUN = process.argv.includes('--dry-run');

if (!host || !user || !database || !port || password == null) {
  console.error("Usage: PW='***' node scripts/fixBanningDoubleDotEmails.js <host> <user> <database> <port> [--dry-run]");
  process.exit(1);
}

// Colapsa puntos consecutivos SOLO en el dominio (el local part puede llevar
// puntos legítimos y no se toca).
function fixDoubleDots(rawEmail) {
  const email = String(rawEmail || '').trim();
  const atIndex = email.lastIndexOf('@');
  if (atIndex <= 0 || atIndex === email.length - 1) return null;
  const localPart = email.slice(0, atIndex);
  const domain = email.slice(atIndex + 1);
  if (!domain.includes('..')) return null;
  return `${localPart}@${domain.replace(/\.{2,}/g, '.')}`;
}

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

(async () => {
  const log = (...a) => console.log('[doubledot]', ...a);
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });

  const [[event]] = await c.query("SELECT id FROM health_event WHERE slug = 'banning' LIMIT 1").then(([r]) => [r]);
  if (!event) { console.error('Event not found'); process.exit(1); }

  const [regs] = await c.query(
    `SELECT r.id AS reg_id, r.registration_role, r.contact_email,
            u.id AS user_id, u.username, u.email, u.firstname, u.lastname, u.phone,
            u.date_of_birth, u.gender_id, u.ethnicity_id
     FROM health_event_registration r
     INNER JOIN user u ON u.id = r.user_id
     WHERE r.health_event_id = ?
       AND (u.email LIKE '%..%' OR r.contact_email LIKE '%..%')`, [event.id]);

  log(`registraciones con '..' en el email: ${regs.length}`);
  const fixes = [];
  for (const reg of regs) {
    const userFix = fixDoubleDots(reg.email);
    const contactFix = fixDoubleDots(reg.contact_email);
    if (!userFix && !contactFix) continue;
    fixes.push({ reg, userFix, contactFix });
    log(`  [${reg.registration_role}] ${reg.firstname} ${reg.lastname} (user #${reg.user_id}, ${reg.username})`);
    if (userFix) log(`     user.email:     ${reg.email}  ->  ${userFix}`);
    if (contactFix) log(`     contact_email:  ${reg.contact_email}  ->  ${contactFix}`);
  }

  if (DRY_RUN) {
    log('DRY-RUN: sin cambios en DB, sin Mailchimp.');
    await c.end();
    return;
  }

  const env = readEnvFile();
  const mailchimp = require('@mailchimp/mailchimp_marketing');
  mailchimp.setConfig({ apiKey: env.MAILCHIMP_KEY, server: env.MAILCHIMP_SERVER_PREFIX });
  const audienceId = env.MAILCHIMP_AUDIENCE_ID;

  let mailchimpOk = 0, mailchimpFail = 0;
  for (const f of fixes) {
    const reg = f.reg;

    if (f.userFix) {
      const [clash] = await c.query('SELECT id FROM user WHERE email = ? AND id <> ? LIMIT 1', [f.userFix, reg.user_id]);
      if (clash.length) {
        log(`  COLISION user.email: ${f.userFix} ya es de user #${clash[0].id} — user #${reg.user_id} conserva su email tal cual`);
        f.userFix = null;
      } else {
        await c.query('UPDATE user SET email = ? WHERE id = ?', [f.userFix, reg.user_id]);
        log(`  DB user.email actualizado (user #${reg.user_id})`);
      }
    }
    if (f.contactFix) {
      await c.query('UPDATE health_event_registration SET contact_email = ? WHERE id = ?', [f.contactFix, reg.reg_id]);
      log(`  DB contact_email actualizado (reg #${reg.reg_id})`);
    }

    const email = f.contactFix || f.userFix;
    if (!email) continue;

    try {
      const [[gender]] = await c.query('SELECT name FROM gender WHERE id = ?', [reg.gender_id]).then(([r]) => [r]);
      const [[ethnicity]] = await c.query('SELECT name FROM ethnicity WHERE id = ?', [reg.ethnicity_id]).then(([r]) => [r]);
      await mailchimp.lists.addListMember(audienceId, {
        email_address: email,
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
      log(`  MAILCHIMP ok -> ${email}`);
    } catch (err) {
      if (isMemberExistsError(err)) {
        await c.query('UPDATE user SET mailchimp_error = "N" WHERE id = ?', [reg.user_id]);
        mailchimpOk++;
        log(`  MAILCHIMP ya suscripto -> ${email}`);
      } else {
        await c.query('UPDATE user SET mailchimp_error = "Y" WHERE id = ?', [reg.user_id]);
        mailchimpFail++;
        log(`  MAILCHIMP ERROR -> ${email}: ${err.message} ${err.response?.body ? JSON.stringify(err.response.body).slice(0, 200) : ''}`);
      }
    }
  }

  log(`fin: correcciones=${fixes.length}, mailchimp ok=${mailchimpOk}, fail=${mailchimpFail}`);
  await c.end();
})().catch(e => { console.error(e); process.exit(1); });
