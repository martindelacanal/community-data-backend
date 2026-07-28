/**
 * Health Events adjustments — 2026-07-28 (idempotent, run on dev AND prod).
 *
 *  1. ALTER health_event_image: add link_url (organizer cards can link to an
 *     external website from the landing).
 *  2. Banning "Resource Table" stand: seed the promoter/agency services so each
 *     scan records what information was requested (health plans, social
 *     services, therapy, support groups).
 *  3. Every checkout form of the Banning event: add the "Referred to the
 *     Resource Table?" question (internal referral tracking from the meeting
 *     notes), placed between "Service status" and "Notes".
 *
 * Run:  PW='<password>' node scripts/2026-07-28_healthEventsAdjustments.js <host> <user> <database> [port]
 */
const mysql = require('mysql2/promise');

const [host, user, database, port = '3306'] = process.argv.slice(2);
const password = process.env.PW;
if (!host || !user || !database || !password) {
  console.error('Usage: PW=<password> node scripts/2026-07-28_healthEventsAdjustments.js <host> <user> <database> [port]');
  process.exit(1);
}

const RESOURCE_SERVICES = [
  ['Health insurance plans', 'Planes de salud'],
  ['Social services', 'Servicios sociales'],
  ['Therapy & counseling', 'Terapia y consejería'],
  ['Support groups', 'Grupos de apoyo']
];

const REFERRAL_QUESTION = {
  name_en: 'Referred to the Resource Table?',
  name_es: '¿Derivado/a a la Mesa de Recursos?',
  options: [['Yes', 'Sí'], ['No', 'No']]
};

(async () => {
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });
  const log = (...args) => console.log('[adjust]', ...args);

  // --- 1. link_url column ----------------------------------------------------
  const [columns] = await c.query(
    `SELECT COLUMN_NAME FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'health_event_image' AND COLUMN_NAME = 'link_url'`, [database]);
  if (columns.length) {
    log('link_url column already exists');
  } else {
    await c.query('ALTER TABLE health_event_image ADD COLUMN link_url varchar(500) NULL AFTER alt_es');
    log('link_url column added to health_event_image');
  }

  // --- 2. Resource Table services ---------------------------------------------
  const [events] = await c.query('SELECT id FROM health_event WHERE slug = ? LIMIT 1', ['banning']);
  if (!events.length) {
    log('WARN: banning event not found — skipping data seeds');
    await c.end();
    return;
  }
  const eventId = events[0].id;

  const [standRows] = await c.query(
    'SELECT id FROM health_event_stand WHERE health_event_id = ? AND name_en = ? LIMIT 1',
    [eventId, 'Resource Table']);
  if (!standRows.length) {
    log('WARN: Resource Table stand not found');
  } else {
    const standId = standRows[0].id;
    for (let i = 0; i < RESOURCE_SERVICES.length; i++) {
      const [en, es] = RESOURCE_SERVICES[i];
      const [existing] = await c.query(
        'SELECT id FROM health_event_stand_service WHERE stand_id = ? AND name_en = ? LIMIT 1', [standId, en]);
      if (existing.length) {
        log('service exists:', en);
      } else {
        await c.query(
          'INSERT INTO health_event_stand_service(stand_id, name_en, name_es, sort_order) VALUES (?,?,?,?)',
          [standId, en, es, i + 1]);
        log('service created:', en);
      }
    }
  }

  // --- 3. Referral question on every checkout form -----------------------------
  const [checkoutForms] = await c.query(
    'SELECT id, title_en FROM health_event_form WHERE health_event_id = ? AND audience = "checkout" AND enabled = "Y"',
    [eventId]);
  for (const form of checkoutForms) {
    const [existing] = await c.query(
      'SELECT id FROM health_event_question WHERE form_id = ? AND name_en = ? LIMIT 1',
      [form.id, REFERRAL_QUESTION.name_en]);
    if (existing.length) {
      log('referral question exists on form:', form.title_en);
      continue;
    }
    // Keep order: status (1) -> referral (2) -> notes (3+).
    await c.query(
      'UPDATE health_event_question SET sort_order = sort_order + 1 WHERE form_id = ? AND sort_order >= 2', [form.id]);
    const [inserted] = await c.query(
      `INSERT INTO health_event_question(form_id, question_type, name_en, name_es, required, allow_other, sort_order, enabled)
       VALUES (?, 'single', ?, ?, 'Y', 'N', 2, 'Y')`,
      [form.id, REFERRAL_QUESTION.name_en, REFERRAL_QUESTION.name_es]);
    for (let i = 0; i < REFERRAL_QUESTION.options.length; i++) {
      const [en, es] = REFERRAL_QUESTION.options[i];
      await c.query(
        'INSERT INTO health_event_question_option(question_id, name_en, name_es, sort_order) VALUES (?,?,?,?)',
        [inserted.insertId, en, es, i + 1]);
    }
    log('referral question added to form:', form.title_en);
  }

  log('done.');
  await c.end();
})().catch(err => {
  console.error('[adjust] FAILED:', err.message);
  process.exit(1);
});
