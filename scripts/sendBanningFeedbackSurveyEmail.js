/**
 * Envío del correo de encuesta de feedback a los BENEFICIARIOS registrados del
 * evento de salud Banning (8-9 ago 2026). Bilingüe EN+ES en un solo correo,
 * asunto "We Value Your Feedback | Valoramos su opinión", con la estética de
 * marca de los correos del sistema (rose/sky, Quicksand) y el logo inline.
 *
 * Un correo INDIVIDUAL por dirección (los destinatarios no se ven entre sí).
 * Dedupe por email en minúsculas (familias comparten dirección). Log JSON
 * reanudable: si se corta, re-ejecutar salta los ya enviados.
 *
 * Usage (desde BACKEND/; GMAIL_PW = app password de bienestarcommunity@gmail.com):
 *   Preview (no toca la DB, envía solo a esa dirección):
 *     GMAIL_PW='***' node scripts/sendBanningFeedbackSurveyEmail.js --preview martin.delacanalerbetta@gmail.com
 *   Dry-run (lista cuántos enviaría, no envía nada):
 *     GMAIL_PW='***' PW='***' node scripts/sendBanningFeedbackSurveyEmail.js <host> <user> <database> <port> --dry-run
 *   Envío real:
 *     GMAIL_PW='***' PW='***' node scripts/sendBanningFeedbackSurveyEmail.js <host> <user> <database> <port> --send
 */
const fs = require('fs');
const path = require('path');
const nodemailer = require('nodemailer');

const SURVEY_URL = 'https://forms.gle/uYD5NKzG7b6J4tPi7';
const SUBJECT = 'We Value Your Feedback | Valoramos su opinión';
const LOGO_PATH = path.join(__dirname, '..', '..', 'FRONTEND', 'src', 'assets', 'imgs', 'bienestar_logo_color_nuevo.png');
const LOG_PATH = path.join(__dirname, '..', '..', 'BASE DE DATOS', 'BANNING', 'feedback_survey_send_log.json');
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const THROTTLE_MS = 700;

// Misma paleta que VOLUNTEER_NOTIFICATION_BRAND en api/email/email.js
const B = {
  rose: '#df3d7a', roseDark: '#c72f69', sky: '#11b3d1', textDark: '#434543',
  border: '#c5e1e1', lightCyan: '#d1f8f8', pageBg: '#f4fbfb'
};

// App password de Gmail por env var (misma cuenta que api/email/email.js).
const GMAIL_PW = process.env.GMAIL_PW;
if (!GMAIL_PW) {
  console.error('Missing GMAIL_PW env var (Gmail app password for bienestarcommunity@gmail.com).');
  process.exit(1);
}

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: { user: 'bienestarcommunity@gmail.com', pass: GMAIL_PW }
});

// ---------------------------------------------------------------------------
// Contenido
// ---------------------------------------------------------------------------

const EN = {
  greeting: 'Dear Community Health Clinic Participant,',
  paragraphs: [
    'Thank you for joining us at our recent Community Health Clinic in Banning. We hope you had a positive experience and that the services you received were helpful to you and your family.',
    'As we prepare for future Community Health Clinics, including our next clinic in December, we would greatly appreciate your feedback. Your experience will help us understand what went well and how we can make future clinics even better for our community.',
    'Please take a few minutes to complete our participant feedback survey:'
  ],
  surveyLabel: 'D5 Community Health Clinic Banning Participant Feedback Survey',
  closingParagraph: 'Thank you for trusting us to serve you. We sincerely appreciate your time and feedback and look forward to continuing to support the health and well-being of our community.',
  signOff: 'Warm regards,',
  team: 'The Bienestar Community Team'
};

const ES = {
  greeting: 'Estimado(a) participante de la Clínica Comunitaria de Salud:',
  paragraphs: [
    'Gracias por acompañarnos en nuestra reciente Clínica Comunitaria de Salud en Banning. Esperamos que haya tenido una experiencia positiva y que los servicios que recibió hayan sido de beneficio para usted y su familia.',
    'Mientras nos preparamos para futuras Clínicas Comunitarias de Salud, incluyendo nuestra próxima clínica en diciembre, agradeceríamos mucho sus comentarios. Su experiencia nos ayudará a comprender qué funcionó bien y cómo podemos mejorar las próximas clínicas para nuestra comunidad.',
    'Por favor, le pedimos se tome unos minutos para completar nuestra encuesta de opinión para participantes:'
  ],
  surveyLabel: 'Encuesta de Opinión para Participantes de la Clínica Comunitaria de Salud D5 en Banning',
  closingParagraph: 'Gracias por confiar en nosotros para servirle. Agradecemos sinceramente su tiempo y sus comentarios, y esperamos continuar apoyando la salud y el bienestar de nuestra comunidad.',
  signOff: 'Atentamente,',
  team: 'El equipo de Bienestar Community'
};

const PREHEADER = 'Tell us about your experience at the Banning Community Health Clinic · Cuéntenos su experiencia en la Clínica de Salud de Banning';

function esc(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

const P = (text) =>
  `<p style="margin:0 0 16px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${B.textDark};">${esc(text)}</p>`;

// Tarjeta CTA: fondo cian claro + botón rose con el nombre completo de la encuesta.
function surveyCard(label) {
  return `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:4px 0 20px 0;">
    <tr>
      <td style="background:${B.lightCyan};background:linear-gradient(160deg,${B.lightCyan} 0%,#e9fbfb 100%);border:1px solid ${B.border};border-radius:14px;padding:22px 20px;text-align:center;">
        <div style="font-size:30px;line-height:1;margin-bottom:12px;">&#128221;</div>
        <a href="${SURVEY_URL}" target="_blank" rel="noopener"
           style="display:inline-block;box-sizing:border-box;max-width:420px;background:${B.rose};background:linear-gradient(135deg,${B.rose} 0%,${B.roseDark} 100%);border-radius:12px;padding:14px 26px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;font-weight:700;line-height:1.5;color:#ffffff;text-decoration:none;">${esc(label)}</a>
        <p style="margin:12px 0 0 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:12px;color:#7c8a8a;word-break:break-all;"><a href="${SURVEY_URL}" target="_blank" rel="noopener" style="color:${B.sky};text-decoration:underline;">${SURVEY_URL}</a></p>
      </td>
    </tr>
  </table>`;
}

function languageBlock(t) {
  return `
    ${P(t.greeting)}
    ${t.paragraphs.map(P).join('')}
    ${surveyCard(t.surveyLabel)}
    ${P(t.closingParagraph)}
    <p style="margin:0 0 2px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${B.textDark};">${esc(t.signOff)}</p>
    <p style="margin:0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;font-weight:700;color:${B.rose};">${esc(t.team)}</p>`;
}

function buildHtml() {
  const divider = `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:26px 0;">
    <tr>
      <td style="border-top:2px solid ${B.lightCyan};font-size:0;line-height:0;">&nbsp;</td>
    </tr>
  </table>`;

  const logoSignature = `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:26px 0 4px 0;">
    <tr>
      <td align="center">
        <img src="cid:bienestar-logo" alt="Bienestar Community" width="120" style="display:block;width:120px;max-width:120px;height:auto;">
      </td>
    </tr>
  </table>`;

  const bodyHtml = `${languageBlock(EN)}${divider}${languageBlock(ES)}${logoSignature}`;

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>We Value Your Feedback</title>
</head>
<body style="margin:0;padding:0;background:${B.pageBg};">
  <span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;">${esc(PREHEADER)}</span>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${B.pageBg};padding:24px 12px;">
    <tr>
      <td align="center">
        <!--[if mso]><table role="presentation" width="600" cellpadding="0" cellspacing="0"><tr><td><![endif]-->
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 6px 24px rgba(67,69,67,0.08);">
          <tr>
            <td style="background:${B.rose};background:linear-gradient(135deg,${B.rose} 0%,${B.roseDark} 100%);padding:32px 28px;">
              <p style="margin:0 0 6px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:rgba(255,255,255,0.85);">Bienestar Community</p>
              <h1 style="margin:0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:24px;font-weight:700;color:#ffffff;">We Value Your Feedback</h1>
              <p style="margin:10px 0 0 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;color:#ffffff;font-weight:600;">Valoramos su opinión · Community Health Clinic Banning</p>
            </td>
          </tr>
          <tr>
            <td style="padding:26px 28px 8px 28px;">${bodyHtml}</td>
          </tr>
          <tr>
            <td style="padding:8px 28px 28px 28px;">
              <div style="border-top:2px solid ${B.lightCyan};padding-top:16px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:12px;line-height:1.7;color:#9aa6a6;text-align:center;">You are receiving this email because you registered for the Community Health Clinic in Banning.<br>Recibe este correo porque se registró en la Clínica Comunitaria de Salud en Banning.</div>
            </td>
          </tr>
        </table>
        <!--[if mso]></td></tr></table><![endif]-->
      </td>
    </tr>
  </table>
</body>
</html>`;
}

function buildText() {
  const block = (t) => [
    t.greeting, '',
    ...t.paragraphs.flatMap(p => [p, '']),
    `${t.surveyLabel}:`, SURVEY_URL, '',
    t.closingParagraph, '',
    t.signOff, t.team
  ].join('\n');
  return `${block(EN)}\n\n----------------------------------------\n\n${block(ES)}\n`;
}

// ---------------------------------------------------------------------------
// Envío
// ---------------------------------------------------------------------------

function loadLog() {
  try { return JSON.parse(fs.readFileSync(LOG_PATH, 'utf8')); } catch { return { sent: {}, failed: {} }; }
}
function saveLog(log) {
  fs.writeFileSync(LOG_PATH, JSON.stringify(log, null, 2));
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function sendOne(to) {
  return transporter.sendMail({
    from: 'bienestarcommunity@gmail.com',
    to,
    subject: SUBJECT,
    text: buildText(),
    html: buildHtml(),
    attachments: [{
      filename: 'bienestar-community.png',
      path: LOGO_PATH,
      contentType: 'image/png',
      cid: 'bienestar-logo'
    }]
  });
}

async function fetchRecipients(host, user, password, database, port) {
  const mysql = require('mysql2/promise');
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port) });
  const [rows] = await c.query(
    `SELECT COALESCE(NULLIF(TRIM(r.contact_email), ''), NULLIF(TRIM(u.email), '')) AS email
     FROM health_event_registration r
     INNER JOIN health_event e ON e.id = r.health_event_id
     LEFT JOIN user u ON u.id = r.user_id
     WHERE e.slug = 'banning' AND r.registration_role = 'beneficiary' AND r.status = 'registered'`);
  await c.end();

  const unique = new Set();
  for (const r of rows) {
    const email = String(r.email || '').trim().toLowerCase();
    if (email && EMAIL_RE.test(email)) unique.add(email);
  }
  return [...unique];
}

(async () => {
  const argv = process.argv.slice(2);
  const previewIdx = argv.indexOf('--preview');

  if (previewIdx !== -1) {
    const to = argv[previewIdx + 1];
    if (!to || !EMAIL_RE.test(to)) {
      console.error('Usage: node scripts/sendBanningFeedbackSurveyEmail.js --preview <email>');
      process.exit(1);
    }
    const info = await sendOne(to);
    console.log(`PREVIEW sent to ${to}: ${info.response}`);
    return;
  }

  const [host, user, database, port] = argv.filter(a => !a.startsWith('--'));
  const password = process.env.PW;
  const DRY_RUN = argv.includes('--dry-run');
  const SEND = argv.includes('--send');

  if (!host || !user || !database || !port || password == null || (!DRY_RUN && !SEND)) {
    console.error("Usage: PW='***' node scripts/sendBanningFeedbackSurveyEmail.js <host> <user> <database> <port> [--dry-run | --send]");
    process.exit(1);
  }

  const recipients = await fetchRecipients(host, user, password, database, port);
  const log = loadLog();
  const pending = recipients.filter(e => !log.sent[e]);
  console.log(`Unique beneficiary emails: ${recipients.length} · already sent: ${recipients.length - pending.length} · pending: ${pending.length}`);

  if (DRY_RUN) {
    console.log('Dry-run: no emails sent.');
    return;
  }

  let ok = 0, fail = 0;
  for (let i = 0; i < pending.length; i++) {
    const email = pending[i];
    try {
      await sendOne(email);
      log.sent[email] = new Date().toISOString();
      delete log.failed[email];
      ok++;
      console.log(`[${i + 1}/${pending.length}] OK ${email}`);
    } catch (err) {
      log.failed[email] = { at: new Date().toISOString(), error: err && err.message ? err.message : String(err) };
      fail++;
      console.log(`[${i + 1}/${pending.length}] FAIL ${email}: ${err && err.message}`);
    }
    saveLog(log);
    if (i < pending.length - 1) await sleep(THROTTLE_MS);
  }
  console.log(`Done. Sent OK: ${ok} · Failed: ${fail} · Log: ${LOG_PATH}`);
})().catch(e => { console.error(e); process.exit(1); });
