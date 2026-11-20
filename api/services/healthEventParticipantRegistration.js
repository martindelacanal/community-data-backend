'use strict';

const { normalizeParticipantEmail } = require('../utils/participantEmailAddress');
const { previousParticipantContactEmailSql } = require('./healthEventParticipantContact');

async function previousContact(db, existing) {
  if (!existing?.contact_email) return null;
  const [[row]] = await db.query(
    `SELECT ${previousParticipantContactEmailSql} AS contact_email FROM health_event_registration r
       JOIN user u ON u.id = r.user_id WHERE r.id = ?`, [existing.id]);
  return normalizeParticipantEmail(row.contact_email);
}

// Called inside the registration transaction. The user lock also serializes
// parallel self-registration and walk-in check-in for the same participant.
async function saveParticipantRegistration(db, eventId, userId, options = {}) {
  const [[participant]] = await db.query('SELECT email, language FROM user WHERE id = ? FOR UPDATE', [userId]);
  const [[existing]] = await db.query(
    `SELECT id, status, reservation_version, contact_email FROM health_event_registration
      WHERE health_event_id = ? AND user_id = ? AND registration_role = 'beneficiary' FOR UPDATE`, [eventId, userId]);
  if (options.previousVersion && (existing?.status !== 'registered' || existing.reservation_version !== options.previousVersion)) {
    const error = new Error('REGISTRATION_CHANGED');
    error.code = 'REGISTRATION_CHANGED';
    throw error;
  }
  if (existing && existing.status !== 'cancelled') {
    return { registration_id: existing.id, already_registered: true, confirmation_email: 'not_requested' };
  }
  // Only the participant's own account can supply the fallback. For new
  // family accounts, the submitted contact may legitimately be shared.
  const contact = normalizeParticipantEmail(options.email) || normalizeParticipantEmail(participant.email)
    || await previousContact(db, existing);
  const language = (options.language || participant.language) === 'es' ? 'es' : 'en';
  let registrationId;
  let version;
  if (existing) {
    registrationId = existing.id;
    version = existing.reservation_version + 1;
    await db.query(
      `UPDATE health_event_registration SET status = 'registered', reservation_version = ?,
              contact_email = ?, source = 'web', submitted_at = NOW() WHERE id = ?`, [version, contact, registrationId]);
    // A fresh booking gets new schedule choices. Preserve other answers,
    // including the registration's consent and any survey responses.
    await db.query('DELETE FROM health_event_registration_date WHERE registration_id = ?', [registrationId]);
    await db.query(
      `DELETE a FROM health_event_answer a JOIN health_event_question q ON q.id = a.question_id
        WHERE a.registration_id = ? AND (q.maps_to IN ('attend_date','priority_service') OR q.question_type = 'appointment')`,
      [registrationId]);
  } else {
    const [inserted] = await db.query(
      `INSERT INTO health_event_registration(health_event_id,user_id,registration_role,contact_email,source,submitted_at)
       VALUES (?,?,'beneficiary',?,'web',NOW())`, [eventId, userId, contact]);
    registrationId = inserted.insertId;
    version = 1;
  }
  if (contact) {
    await db.query(
      `INSERT INTO health_event_confirmation(registration_id,reservation_version,language,include_default_login)
       VALUES (?,?,?,?)`, [registrationId, version, language, options.includeDefaultLogin ? 1 : 0]);
  }
  return { registration_id: registrationId, already_registered: false, confirmation_email: contact ? 'queued' : 'no_email' };
}

module.exports = { saveParticipantRegistration };
