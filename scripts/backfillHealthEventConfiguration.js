'use strict';

const assert = require('node:assert/strict');
const { copyHealthEventConfiguration, readHealthEventConfiguration } = require('../api/services/healthEventConfigurationCopy');
const counts = data => Object.fromEntries(Object.entries(data).map(([key, rows]) => [key, rows.length]));

/** Only fills an entirely empty setup; edited event details/landing/images are never written. */
async function backfillHealthEventConfiguration({ pool, sourceId, targetId, sourceSlug, targetSlug, apply = false, backup }) {
  assert.ok(Number.isSafeInteger(sourceId) && sourceId > 0 && Number.isSafeInteger(targetId) && targetId > 0 && sourceId !== targetId, 'Distinct positive source/target IDs are required');
  assert.ok(sourceSlug && targetSlug, 'Expected source and target slugs are required');
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    const [events] = await connection.query('SELECT * FROM health_event WHERE id IN (?,?) ORDER BY id FOR UPDATE', [sourceId, targetId]);
    const source = events.find(e => e.id === sourceId), target = events.find(e => e.id === targetId);
    assert.equal(source?.slug, sourceSlug, 'Source identity changed');
    assert.equal(target?.slug, targetSlug, 'Target identity changed');
    const existing = await readHealthEventConfiguration(connection, targetId, { lock: true });
    assert.ok(Object.values(existing).every(rows => rows.length === 0), 'Target already has configuration; no existing work will be overwritten. Inspect it before retrying.');
    const [registrations] = await connection.query('SELECT id FROM health_event_registration WHERE health_event_id = ? LIMIT 1 FOR UPDATE', [targetId]);
    assert.equal(registrations.length, 0, 'Target already has registrations; manual review is required');
    const original = await readHealthEventConfiguration(connection, sourceId, { lock: true });
    const [images] = await connection.query('SELECT * FROM health_event_image WHERE health_event_id IN (?,?) ORDER BY id FOR UPDATE', [sourceId, targetId]);
    const summary = { sourceId, targetId, sourceSlug, targetSlug, counts: counts(original), dates: { start: target.start_date, end: target.end_date }, preservedFlags: {
      enabled: target.enabled, landing_enabled: target.landing_enabled, public_home_visible: target.public_home_visible, promo_dialog_enabled: target.promo_dialog_enabled
    } };
    if (!apply) { await connection.rollback(); return { ...summary, status: 'dry-run' }; }
    assert.equal(typeof backup, 'function', 'A backup must be saved before applying the copy');
    await backup({ events, images, sourceConfiguration: original, targetConfiguration: existing });
    const copied = await copyHealthEventConfiguration({ connection, source, target });
    const actual = await readHealthEventConfiguration(connection, targetId);
    assert.deepEqual(counts(actual), copied, 'Copied configuration counts must match');
    const questionIds = new Set(actual.questions.map(q => q.id)), optionIds = new Set(actual.options.map(o => o.id));
    const formIds = new Set(actual.forms.map(f => f.id)), standIds = new Set(actual.stands.map(s => s.id));
    for (const q of actual.questions) {
      assert.ok(formIds.has(q.form_id));
      assert.ok(q.depends_on_question_id == null || questionIds.has(q.depends_on_question_id));
      assert.ok(q.depends_on_option_id == null || optionIds.has(q.depends_on_option_id));
    }
    for (const o of actual.options) assert.ok(questionIds.has(o.question_id));
    for (const f of actual.forms) assert.ok(f.stand_id == null || standIds.has(f.stand_id));
    for (const s of actual.services) assert.ok(standIds.has(s.stand_id));
    const [eventsAfter] = await connection.query('SELECT * FROM health_event WHERE id IN (?,?) ORDER BY id FOR UPDATE', [sourceId, targetId]);
    const [imagesAfter] = await connection.query('SELECT * FROM health_event_image WHERE health_event_id IN (?,?) ORDER BY id FOR UPDATE', [sourceId, targetId]);
    assert.deepEqual(eventsAfter, events, 'Event details, landing and publication settings must remain unchanged');
    assert.deepEqual(imagesAfter, images, 'Images must remain unchanged');
    assert.deepEqual(await readHealthEventConfiguration(connection, sourceId), original, 'Source configuration must remain unchanged');
    await connection.commit();
    return { ...summary, status: 'applied', sourceUnchanged: true, targetPresentationUnchanged: true };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally { connection.release(); }
}

async function main() {
  const fs = require('node:fs'), path = require('node:path'), os = require('node:os'), { randomUUID } = require('node:crypto');
  require('dotenv').config({ path: path.join(__dirname, '..', '.env'), quiet: true });
  const args = process.argv.slice(2), apply = args.includes('--apply');
  const positional = args.filter(a => a !== '--apply');
  assert.equal(positional.length, 4, 'Usage: node scripts/backfillHealthEventConfiguration.js SOURCE_ID SOURCE_SLUG TARGET_ID TARGET_SLUG [--apply]');
  const [sourceId, sourceSlug, targetId, targetSlug] = positional;
  const pool = require('mysql2/promise').createPool({ host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_DATABASE, port: Number(process.env.DB_PORT || 3306), connectionLimit: 1, dateStrings: true });
  const backupPath = path.join(os.tmpdir(), `health-event-configuration-${sourceId}-${targetId}-${randomUUID()}.json`);
  try {
    const result = await backfillHealthEventConfiguration({ pool, sourceId: Number(sourceId), targetId: Number(targetId), sourceSlug, targetSlug, apply,
      backup: async data => fs.writeFileSync(backupPath, JSON.stringify(data, null, 2), { flag: 'wx', mode: 0o600 }) });
    if (apply) fs.writeFileSync(`${backupPath}.receipt`, JSON.stringify(result, null, 2), { flag: 'wx', mode: 0o600 });
    console.log(JSON.stringify({ ...result, ...(apply ? { backupPath } : {}) }));
  } finally { await pool.end(); }
}

if (require.main === module) main().catch(error => { console.error(error.code === 'ERR_ASSERTION' ? error.message : error.code || 'Configuration copy failed'); process.exitCode = 1; });
module.exports = { backfillHealthEventConfiguration };
