'use strict';

const { createHmac, timingSafeEqual } = require('node:crypto');

function signature(claims, secret) {
  return createHmac('sha256', secret).update('health-event-reservation/v1/' + claims).digest('base64url');
}

function buildReservationUrl(registration, language = 'en', secret = process.env.JWT_SECRET) {
  const claims = [registration.id, registration.reservation_version].join('.');
  if (!/^[1-9]\d{0,9}\.[1-9]\d{0,9}$/.test(claims)) throw new Error('Invalid reservation');
  const url = new URL('https://api.bienestarcommunity.org/api/health-events/reservation/open');
  url.searchParams.set('lang', language === 'es' ? 'es' : 'en');
  url.hash = claims + '.' + signature(claims, secret);
  return url.toString();
}

function readReservationToken(token, secret = process.env.JWT_SECRET) {
  if (typeof token !== 'string' || token.length > 65) return null;
  const match = /^([1-9]\d{0,9})\.([1-9]\d{0,9})\.([A-Za-z0-9_-]{43})$/.exec(token);
  if (!match) return null;
  const [, id, version, signed] = match;
  const expected = signature(id + '.' + version, secret);
  return timingSafeEqual(Buffer.from(expected), Buffer.from(signed)) ? { id: Number(id), version: Number(version) } : null;
}

async function readReservationSchedule(db, registrationId) {
  const [dates] = await db.query(
    "SELECT DATE_FORMAT(event_date, '%Y-%m-%d') AS event_date, priority_service FROM health_event_registration_date WHERE registration_id = ? ORDER BY event_date",
    [registrationId]);
  const [appointments] = await db.query(
    `SELECT s.service_key, DATE_FORMAT(s.slot_date, '%Y-%m-%d') AS slot_date,
            TIME_FORMAT(s.start_time, '%H:%i') AS start_time, TIME_FORMAT(s.end_time, '%H:%i') AS end_time
       FROM health_event_appointment a JOIN health_event_slot s ON s.id = a.slot_id
      WHERE a.registration_id = ? AND a.status = 'booked' ORDER BY s.slot_date, s.start_time, a.id`,
    [registrationId]);
  return { dates, appointments };
}

class ReservationError extends Error {
  constructor(code, status) {
    super(code);
    this.code = code;
    this.status = status;
  }
}

async function manageReservation(pool, claims, cancel, hasEventEnded) {
  const db = await pool.getConnection();
  try {
    await db.beginTransaction();
    const [[registration]] = await db.query(
      `SELECT id, health_event_id, user_id, status FROM health_event_registration
        WHERE id = ? AND reservation_version = ? AND registration_role = 'beneficiary'
          AND status IN ('registered','cancelled')` + (cancel ? ' FOR UPDATE' : ''), [claims.id, claims.version]);
    if (!registration) throw new ReservationError('INVALID_RESERVATION', 404);
    const [[event]] = await db.query(
      `SELECT e.name_en, e.name_es, e.enabled, DATE_FORMAT(e.start_date, '%Y-%m-%d') AS start_date,
              DATE_FORMAT(e.end_date, '%Y-%m-%d') AS end_date, e.timezone, l.address, u.firstname
         FROM health_event e JOIN location l ON l.id = e.location_id
         JOIN user u ON u.id = ? AND u.deleted = 'N'
        WHERE e.id = ?`, [registration.user_id, registration.health_event_id]);
    if (!event) throw new ReservationError('INVALID_RESERVATION', 404);
    const [[checkin]] = await db.query(
      "SELECT id FROM health_event_scan WHERE registration_id = ? AND scan_type = 'checkin' LIMIT 1",
      [registration.id]);
    let canCancel = registration.status === 'registered' && event.enabled === 'Y' && !hasEventEnded(event) && !checkin;
    if (cancel && registration.status !== 'cancelled') {
      if (!canCancel) throw new ReservationError('CANCELLATION_CLOSED', 409);
      await db.query("UPDATE health_event_appointment SET status = 'cancelled' WHERE registration_id = ? AND status = 'booked'", [registration.id]);
      await db.query("UPDATE health_event_registration SET status = 'cancelled' WHERE id = ?", [registration.id]);
      registration.status = 'cancelled';
      canCancel = false;
    }
    const schedule = registration.status === 'cancelled' ? { dates: [], appointments: [] } : await readReservationSchedule(db, registration.id);
    await db.commit();
    const { firstname, enabled, ...publicEvent } = event;
    return { status: registration.status, can_cancel: canCancel, participant_firstname: firstname,
      event: publicEvent, dates: schedule.dates.map(({ event_date }) => ({ event_date })), appointments: schedule.appointments };
  } catch (error) {
    await db.rollback();
    throw error;
  } finally {
    db.release();
  }
}

module.exports = { buildReservationUrl, readReservationToken, readReservationSchedule, manageReservation, ReservationError };
