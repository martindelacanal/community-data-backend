'use strict';

const { randomUUID } = require('node:crypto');
const { copyHealthEventConfiguration } = require('./healthEventConfigurationCopy');

class HealthEventDuplicateError extends Error {
  constructor(code, status) {
    super(code);
    this.code = code;
    this.status = status;
  }
}

function validDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

/** Copy presentation and registration setup, never participant or clinical records. */
async function duplicateHealthEvent({ pool, sourceId, details, userId, copyObject, deleteObjects, logger }) {
  if (!validDate(details.start_date) || !validDate(details.end_date) || details.end_date < details.start_date) {
    throw new HealthEventDuplicateError('INVALID_DATES', 400);
  }
  const connection = await pool.getConnection();
  const copiedKeys = [];
  let committed = false;
  try {
    await connection.beginTransaction();
    const [sources] = await connection.query('SELECT * FROM health_event WHERE id = ? LIMIT 1 FOR UPDATE', [sourceId]);
    if (!sources.length) throw new HealthEventDuplicateError('NOT_FOUND', 404);
    const source = sources[0];
    const [existing] = await connection.query('SELECT id FROM health_event WHERE slug = ? LIMIT 1', [details.slug]);
    if (existing.length) throw new HealthEventDuplicateError('SLUG_TAKEN', 409);
    const [inserted] = await connection.query(
      `INSERT INTO health_event(slug, name_en, name_es, location_id, client_id, start_date, end_date,
        start_time, end_time, timezone, registration_opens_at, registration_closes_at,
        landing_json, promo_json, enabled, landing_enabled, public_home_visible, promo_dialog_enabled,
        promo_dialog_version, created_by_user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [details.slug, details.name_en, details.name_es, details.location_id, details.client_id || null,
        details.start_date, details.end_date, details.start_time || null, details.end_time || null,
        source.timezone || 'America/Los_Angeles', null, details.registration_closes_at || null,
        jsonValue(source.landing_json), jsonValue(source.promo_json), 'N', 'N', 'N', 'N', 1, userId]);
    const id = inserted.insertId;
    await copyHealthEventConfiguration({ connection, source, target: { ...details, id } });
    const [images] = await connection.query(
      'SELECT * FROM health_event_image WHERE health_event_id = ? ORDER BY id ASC', [sourceId]);
    for (const image of images) {
      // Separate object keys keep image removal/replacement independent in either event.
      const baseKey = `health-events/${id}/copy-${randomUUID()}`;
      const keys = {};
      for (const [column, suffix] of [['s3_key', ''], ['s3_key_small', '__small'], ['s3_key_medium', '__medium']]) {
        keys[column] = null;
        if (!image[column]) continue;
        const destination = `${baseKey}${suffix}`;
        // Track before the request, since a lost response may follow a successful S3 copy.
        copiedKeys.push(destination);
        await copyObject(image[column], destination);
        keys[column] = destination;
      }
      await connection.query(
        `INSERT INTO health_event_image(health_event_id, section_key, s3_key, s3_key_small, s3_key_medium,
          mime_type, original_filename, alt_en, alt_es, link_url, display_order, enabled)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
        [id, image.section_key, keys.s3_key, keys.s3_key_small, keys.s3_key_medium,
          image.mime_type, image.original_filename, image.alt_en, image.alt_es, image.link_url,
          image.display_order, image.enabled]);
    }
    await connection.commit();
    committed = true;
    return { id };
  } catch (error) {
    try { await connection.rollback(); } catch (rollbackError) { logger.error('Health event duplicate rollback: ' + rollbackError.message); }
    if (!committed && copiedKeys.length) {
      try { await deleteObjects(copiedKeys); }
      catch (cleanupError) { logger.error('Health event duplicate image cleanup: ' + cleanupError.message); }
    }
    if (error.code === 'ER_DUP_ENTRY') throw new HealthEventDuplicateError('SLUG_TAKEN', 409);
    throw error;
  } finally {
    connection.release();
  }
}

function jsonValue(value) {
  return value == null || typeof value === 'string' ? value : JSON.stringify(value);
}

module.exports = { duplicateHealthEvent, HealthEventDuplicateError };
