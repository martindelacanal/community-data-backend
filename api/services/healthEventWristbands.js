'use strict';

const {
  normalizeWristbandUid, normalizeWristbandSource, assertAssignableRegistration, HealthWristbandError
} = require('../utils/healthWristband');

function credentialShape(row) {
  if (!row) return null;
  return { id: Number(row.id), uid: row.uid, registration_id: Number(row.registration_id),
    assigned_at: row.assigned_at, source: row.source, released_at: row.released_at || null };
}

/** SELECT active UID globally; never reveal an unrelated event's participant. */
async function resolveWristband(executor, eventId, rawUid) {
  const uid = normalizeWristbandUid(rawUid);
  if (!uid) throw new HealthWristbandError('INVALID_WRISTBAND_UID');
  const [rows] = await executor.query(
    `SELECT w.*, r.user_id, r.registration_role, r.status, u.firstname, u.lastname,
            u.enabled AS user_enabled, u.deleted AS user_deleted
     FROM health_event_wristband w
     INNER JOIN health_event_registration r ON r.id = w.registration_id AND r.health_event_id = w.health_event_id
     INNER JOIN user u ON u.id = r.user_id
     WHERE w.active_uid = ? AND w.health_event_id = ? AND w.released_at IS NULL LIMIT 1`, [uid, eventId]);
  if (!rows.length) throw new HealthWristbandError('WRISTBAND_NOT_ASSIGNED', 404);
  assertAssignableRegistration(rows[0], eventId);
  return rows[0];
}

/** Retried only for database deadlocks; all assignment mutations are atomic. */
async function withTransaction(pool, operation) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();
      const result = await operation(connection);
      await connection.commit();
      return result;
    } catch (error) {
      try { await connection.rollback(); } catch (_) { /* retain original failure */ }
      if (error.code === 'ER_LOCK_DEADLOCK' && attempt < 2) continue;
      if (error.code === 'ER_DUP_ENTRY') throw new HealthWristbandError('WRISTBAND_IN_USE', 409);
      throw error;
    } finally {
      connection.release();
    }
  }
}

async function assignWristband(pool, { eventId, registrationId, uid: rawUid, source: rawSource, operatorId, replace = false }) {
  const uid = normalizeWristbandUid(rawUid);
  const source = normalizeWristbandSource(rawSource);
  if (!uid) throw new HealthWristbandError('INVALID_WRISTBAND_UID');
  if (!source || !Number.isSafeInteger(registrationId) || registrationId <= 0) throw new HealthWristbandError('INVALID_DATA');
  return withTransaction(pool, async connection => {
    const [lookup] = await connection.query(
      'SELECT user_id FROM health_event_registration WHERE id = ? AND health_event_id = ? LIMIT 1', [registrationId, eventId]);
    if (!lookup.length) throw new HealthWristbandError('NOT_REGISTERED', 404);
    // Scan locks its participant first. Keep the same order when assigning.
    await connection.query('SELECT id FROM user WHERE id = ? FOR UPDATE', [lookup[0].user_id]);
    const [registrations] = await connection.query(
      `SELECT r.*, u.enabled AS user_enabled, u.deleted AS user_deleted
       FROM health_event_registration r INNER JOIN user u ON u.id = r.user_id
       WHERE r.id = ? AND r.health_event_id = ? LIMIT 1 FOR UPDATE`, [registrationId, eventId]);
    assertAssignableRegistration(registrations[0], eventId);
    const [existingRows] = await connection.query(
      'SELECT * FROM health_event_wristband WHERE active_registration_id = ? LIMIT 1 FOR UPDATE', [registrationId]);
    const existing = existingRows[0];
    if (existing && existing.uid === uid) return { credential: credentialShape(existing), already_assigned: true };
    if (existing && replace !== true) throw new HealthWristbandError('PARTICIPANT_HAS_WRISTBAND', 409);
    const [conflicts] = await connection.query(
      'SELECT id FROM health_event_wristband WHERE active_uid = ? LIMIT 1 FOR UPDATE', [uid]);
    if (conflicts.length) throw new HealthWristbandError('WRISTBAND_IN_USE', 409);
    if (existing) {
      await connection.query(
        `UPDATE health_event_wristband SET active_uid = NULL, active_registration_id = NULL,
         released_at = NOW(3), released_by_user_id = ?, release_reason = 'replaced' WHERE id = ?`, [operatorId, existing.id]);
    }
    const [result] = await connection.query(
      `INSERT INTO health_event_wristband
       (health_event_id, registration_id, uid, active_uid, active_registration_id, source, assigned_by_user_id)
       VALUES (?,?,?,?,?,?,?)`, [eventId, registrationId, uid, uid, registrationId, source, operatorId]);
    const [rows] = await connection.query('SELECT * FROM health_event_wristband WHERE id = ?', [result.insertId]);
    return { credential: credentialShape(rows[0]), already_assigned: false };
  });
}

async function returnWristband(pool, { eventId, credentialId, operatorId }) {
  return withTransaction(pool, async connection => {
    // Match scan/assignment lock order: registration first, then its binding.
    const [lookup] = await connection.query(
      'SELECT registration_id FROM health_event_wristband WHERE id = ? AND health_event_id = ? LIMIT 1', [credentialId, eventId]);
    if (!lookup.length) throw new HealthWristbandError('WRISTBAND_NOT_FOUND', 404);
    await connection.query('SELECT id FROM health_event_registration WHERE id = ? FOR UPDATE', [lookup[0].registration_id]);
    const [rows] = await connection.query(
      'SELECT * FROM health_event_wristband WHERE id = ? AND health_event_id = ? LIMIT 1 FOR UPDATE', [credentialId, eventId]);
    const alreadyReturned = rows[0].released_at != null;
    if (!alreadyReturned) {
      await connection.query(
        `UPDATE health_event_wristband SET active_uid = NULL, active_registration_id = NULL,
         released_at = NOW(3), released_by_user_id = ?, release_reason = 'returned' WHERE id = ?`, [operatorId, credentialId]);
    }
    return { returned: true, already_returned: alreadyReturned };
  });
}

/** Must run after the registration lock; a return racing a scan invalidates it. */
async function lockActiveWristband(connection, eventId, credentialId, registrationId) {
  const [rows] = await connection.query(
    `SELECT * FROM health_event_wristband WHERE id = ? AND health_event_id = ?
     AND active_registration_id = ? AND active_uid IS NOT NULL AND released_at IS NULL LIMIT 1 FOR UPDATE`,
    [credentialId, eventId, registrationId]);
  if (!rows.length) throw new HealthWristbandError('WRISTBAND_NOT_ASSIGNED', 404);
  return rows[0];
}

async function recordStaffAttendance(connection, { eventId, registration, credential, standId, operatorId, source, timezone }) {
  const [recent] = await connection.query(
    `SELECT id, scan_type FROM health_event_staff_attendance
     WHERE registration_id = ? AND health_event_id = ? AND scanned_at >= NOW(3) - INTERVAL 10 SECOND
     ORDER BY scanned_at DESC, id DESC LIMIT 1`, [registration.id, eventId]);
  if (recent.length) return { scan_id: Number(recent[0].id), scan_type: recent[0].scan_type, duplicate: true,
    duplicate_reason: 'recent' };
  const [last] = await connection.query(
    `SELECT id, scan_type FROM health_event_staff_attendance
     WHERE registration_id = ? AND health_event_id = ?
       AND DATE(COALESCE(CONVERT_TZ(scanned_at, @@session.time_zone, ?), scanned_at))
           = DATE(COALESCE(CONVERT_TZ(NOW(), @@session.time_zone, ?), NOW()))
     ORDER BY scanned_at DESC, id DESC LIMIT 1 FOR UPDATE`, [registration.id, eventId, timezone, timezone]);
  const scanType = last.length && last[0].scan_type === 'checkin' ? 'checkout' : 'checkin';
  const [insert] = await connection.query(
    `INSERT INTO health_event_staff_attendance
     (health_event_id,registration_id,wristband_id,stand_id,volunteer_user_id,scan_type,paired_scan_id,source)
     VALUES (?,?,?,?,?,?,?,?)`, [eventId, registration.id, credential.id, standId, operatorId, scanType,
      scanType === 'checkout' ? last[0].id : null, source]);
  return { scan_id: Number(insert.insertId), scan_type: scanType, duplicate: false, duplicate_reason: null };
}

module.exports = { credentialShape, resolveWristband, assignWristband, returnWristband, lockActiveWristband,
  recordStaffAttendance, withTransaction };
