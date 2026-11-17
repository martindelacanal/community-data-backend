'use strict';

class VolunteerRegistrationError extends Error {
  constructor(code, status) { super(code); this.code = code; this.status = status; }
}

function volunteerApprovalStatus(status) {
  if (status === 'registered') return 'approved';
  if (status === 'pending' || status === 'pending_activation') return 'pending';
  return 'rejected';
}

// Both operations run inside the caller's transaction and lock the user first.
// This serializes a re-submission with a decision for the same volunteer.
async function requestVolunteerRegistration(db, eventId, userId) {
  const [[user]] = await db.query(
    `SELECT id, email FROM user WHERE id = ? AND role_id = 11
       AND enabled = 'Y' AND deleted = 'N' FOR UPDATE`, [userId]);
  if (!user) throw new VolunteerRegistrationError('FORBIDDEN', 403);
  const [[existing]] = await db.query(
    `SELECT id, status FROM health_event_registration WHERE health_event_id = ?
       AND user_id = ? AND registration_role = 'volunteer' FOR UPDATE`, [eventId, userId]);
  if (existing) {
    return { registration_id: existing.id, approval_status: volunteerApprovalStatus(existing.status),
      already_registered: true, email: user.email };
  }
  if (!user.email?.trim()) throw new VolunteerRegistrationError('ACCOUNT_EMAIL_REQUIRED', 409);
  const [insert] = await db.query(
    `INSERT INTO health_event_registration(health_event_id, user_id, registration_role, status, contact_email, source, submitted_at)
     VALUES (?,?,'volunteer','pending',?,'web',NOW())`, [eventId, userId, user.email]);
  return { registration_id: insert.insertId, approval_status: 'pending', already_registered: false, email: user.email };
}

async function reviewVolunteerRegistration(db, eventId, userId, approve) {
  const [[user]] = await db.query(
    "SELECT id, enabled FROM user WHERE id = ? AND role_id = 11 AND deleted = 'N' FOR UPDATE", [userId]);
  if (!user) throw new VolunteerRegistrationError('NOT_FOUND', 404);
  // Old clients only send a user ID. Only a single registration identifies
  // the clinic unambiguously, even if just one of several is still pending.
  const [rows] = await db.query(
    `SELECT id, status FROM health_event_registration
     WHERE user_id = ? AND registration_role = 'volunteer'${eventId == null ? '' : ' AND health_event_id = ?'}
     LIMIT 2 FOR UPDATE`, eventId == null ? [userId] : [userId, eventId]);
  if (!rows.length) throw new VolunteerRegistrationError('NOT_FOUND', 404);
  if (rows.length > 1) throw new VolunteerRegistrationError('EVENT_REQUIRED', 409);
  const registration = rows[0];
  const previous = volunteerApprovalStatus(registration.status);
  const target = approve ? 'approved' : 'rejected';
  if (approve && user.enabled !== 'Y' && registration.status !== 'pending_activation') {
    throw new VolunteerRegistrationError('ACCOUNT_DISABLED', 409);
  }
  if (previous === target) return target;
  if (previous !== 'pending') throw new VolunteerRegistrationError('ALREADY_REVIEWED', 409);
  if (approve && user.enabled !== 'Y') {
    // Only first-time signups were created disabled by the registration itself.
    await db.query("UPDATE user SET enabled = 'Y' WHERE id = ?", [userId]);
  }
  await db.query('UPDATE health_event_registration SET status = ? WHERE id = ?',
    [approve ? 'registered' : 'rejected', registration.id]);
  return target;
}

module.exports = { VolunteerRegistrationError, volunteerApprovalStatus, requestVolunteerRegistration, reviewVolunteerRegistration };
