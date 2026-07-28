/**
 * Health Events — public-home visibility + promo dialog (idempotent, dev AND prod).
 *
 *  1. ALTER health_event:
 *       public_home_visible  char(1) 'N'  -> event card listed on /home (public)
 *       promo_dialog_enabled char(1) 'N'  -> invite dialog on the public home
 *       promo_dialog_version int 1        -> bumping re-shows the dialog to users
 *                                            who already dismissed it
 *       promo_json           json         -> {text_en, text_es, link_url} (image via
 *                                            health_event_image section 'promo_dialog')
 *  2. Banning: turn both flags ON with a default bilingual promo text.
 *
 * Run:  PW='<password>' node scripts/2026-07-28_healthEventsPublicHome.js <host> <user> <database> [port]
 */
const mysql = require('mysql2/promise');

const [host, user, database, port = '3306'] = process.argv.slice(2);
const password = process.env.PW;
if (!host || !user || !database || !password) {
  console.error('Usage: PW=<password> node scripts/2026-07-28_healthEventsPublicHome.js <host> <user> <database> [port]');
  process.exit(1);
}

const COLUMNS = [
  ['public_home_visible', "char(1) COLLATE utf8mb4_spanish_ci NOT NULL DEFAULT 'N' AFTER landing_enabled"],
  ['promo_dialog_enabled', "char(1) COLLATE utf8mb4_spanish_ci NOT NULL DEFAULT 'N' AFTER public_home_visible"],
  ['promo_dialog_version', 'int NOT NULL DEFAULT 1 AFTER promo_dialog_enabled'],
  ['promo_json', 'json NULL AFTER promo_dialog_version']
];

const DEFAULT_PROMO = {
  text_en: 'Free Health Clinic in Banning — Aug 8 & 9! Dental, vision and medical checkups at no cost. Spots are limited: tap to learn more and book your appointment.',
  text_es: '¡Clínica de Salud Gratuita en Banning — 8 y 9 de agosto! Servicios dentales, de visión y chequeos médicos sin costo. Los cupos son limitados: toca para conocer más y reservar tu turno.',
  link_url: null
};

(async () => {
  const c = await mysql.createConnection({ host, user, password, database, port: Number(port), connectTimeout: 30000 });
  const log = (...args) => console.log('[public-home]', ...args);

  for (const [name, definition] of COLUMNS) {
    const [exists] = await c.query(
      `SELECT COLUMN_NAME FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'health_event' AND COLUMN_NAME = ?`, [database, name]);
    if (exists.length) {
      log('column exists:', name);
    } else {
      await c.query(`ALTER TABLE health_event ADD COLUMN ${name} ${definition}`);
      log('column added:', name);
    }
  }

  const [events] = await c.query(
    'SELECT id, public_home_visible, promo_dialog_enabled, promo_json FROM health_event WHERE slug = ? LIMIT 1', ['banning']);
  if (!events.length) {
    log('WARN: banning event not found — flags not set');
  } else {
    const event = events[0];
    const promoJson = event.promo_json ? event.promo_json : JSON.stringify(DEFAULT_PROMO);
    if (event.public_home_visible === 'Y' && event.promo_dialog_enabled === 'Y') {
      log('banning already visible + promoted');
    } else {
      await c.query(
        `UPDATE health_event SET public_home_visible = 'Y', promo_dialog_enabled = 'Y', promo_json = ? WHERE id = ?`,
        [promoJson, event.id]);
      log('banning set: public_home_visible=Y, promo_dialog_enabled=Y, promo text', event.promo_json ? '(kept)' : '(default)');
    }
  }

  log('done.');
  await c.end();
})().catch(err => {
  console.error('[public-home] FAILED:', err.message);
  process.exit(1);
});
