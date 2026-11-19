'use strict';

const { randomUUID } = require('node:crypto');
const { buildReservationUrl, includesReservationLinks, readReservationSchedule } = require('./healthEventReservation');
const { normalizeParticipantEmail } = require('../utils/participantEmailAddress');

const DEFAULT_HEALTH_PASSWORD = 'bienestar';

async function deliverConfirmation(db, send, job) {
  const [[row]] = await db.query(
    `SELECT r.id, r.reservation_version, r.contact_email, u.firstname, u.username, u.reset_password,
            e.name_en, e.name_es, e.start_time, e.end_time, e.enabled, e.timezone, l.organization, l.community_city, l.address,
            DATE_FORMAT(e.start_date, '%Y-%m-%d') AS start_date, DATE_FORMAT(e.end_date, '%Y-%m-%d') AS end_date
       FROM health_event_registration r JOIN user u ON u.id = r.user_id JOIN health_event e ON e.id = r.health_event_id
       JOIN location l ON l.id = e.location_id
      WHERE r.id = ? AND r.reservation_version = ? AND r.status = 'registered' AND r.registration_role = 'beneficiary'
        AND u.deleted = 'N' AND e.enabled = 'Y'`, [job.registration_id, job.reservation_version]);
  const to = normalizeParticipantEmail(row?.contact_email);
  if (!to) return 'skipped';
  const schedule = await readReservationSchedule(db, row.id);
  await send({
    to, language: job.language, firstname: row.firstname, eventNameEn: row.name_en, eventNameEs: row.name_es,
    locationName: [row.organization, row.community_city].filter(Boolean).join(' — '), address: row.address,
    startTime: row.start_time, endTime: row.end_time, eventStartDate: row.start_date, eventEndDate: row.end_date,
    ...schedule, manageUrl: includesReservationLinks(row) ? buildReservationUrl(row, job.language) : null,
    messageId: '<health-event-confirmation-' + row.id + '-' + row.reservation_version + '@bienestarcommunity.org>',
    credentials: job.include_default_login && row.reset_password === 'Y' ? { username: row.username, password: DEFAULT_HEALTH_PASSWORD } : null
  });
  return 'sent';
}

async function processDueConfirmations(db, send, logger) {
  const [jobs] = await db.query(
    `SELECT registration_id,reservation_version,language,include_default_login,attempts FROM health_event_confirmation
      WHERE state = 'queued' AND available_at <= NOW() ORDER BY available_at,registration_id LIMIT 20`);
  for (const job of jobs) {
    const key = [job.registration_id, job.reservation_version];
    const claim = randomUUID();
    const [claimed] = await db.query(
      `UPDATE health_event_confirmation SET claim_id = ?, attempts = attempts + 1, available_at = DATE_ADD(NOW(),INTERVAL 15 MINUTE)
        WHERE registration_id = ? AND reservation_version = ? AND state = 'queued' AND available_at <= NOW() AND attempts = ?`,
      [claim, ...key, job.attempts]);
    if (!claimed.affectedRows) continue;
    let state;
    try {
      state = await deliverConfirmation(db, send, job);
    } catch (error) {
      // A permanent rejection of the recipient cannot be repaired by retrying.
      const rejected = error.code === 'EENVELOPE' && error.responseCode >= 500;
      state = rejected || job.attempts >= 8 ? 'failed' : 'queued';
      logger.error('Clinic confirmation ' + state + ' for registration ' + job.registration_id + ' (' + (error.code || error.name) + ')');
    }
    const delayMinutes = Math.min(2 ** job.attempts, 60);
    await db.query(
      `UPDATE health_event_confirmation SET state = ?, claim_id = NULL, available_at = DATE_ADD(NOW(),INTERVAL ? MINUTE)
        WHERE registration_id = ? AND reservation_version = ? AND claim_id = ?`, [state, delayMinutes, ...key, claim]);
  }
}

module.exports = { DEFAULT_HEALTH_PASSWORD, processDueConfirmations };
