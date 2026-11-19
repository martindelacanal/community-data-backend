'use strict';

const { normalizeParticipantEmail } = require('../utils/participantEmailAddress');
const { participantContactEmailSql } = require('./healthEventParticipantContact');
const { buildReservationUrl } = require('./healthEventReservation');

class ParticipantEmailError extends Error {
  constructor(code, status) {
    super(code);
    this.code = code;
    this.status = status;
  }
}

function parseSlot(input) {
  if (input == null) return null;
  if (!['number', 'string'].includes(typeof input) || !/^\d+$/.test(String(input))) {
    throw new ParticipantEmailError('INVALID_SLOT', 400);
  }
  const id = Number(input);
  if (!Number.isSafeInteger(id) || id < 1) throw new ParticipantEmailError('INVALID_SLOT', 400);
  return id;
}

function parseDraft(body) {
  const subject = typeof body.subject === 'string' ? body.subject.trim() : '';
  const message = typeof body.message === 'string' ? body.message.trim() : '';
  if (!subject || subject.length > 160 || /[\x00-\x1f\x7f\u2028\u2029]/.test(body.subject) ||
      !message || message.length > 5000 || /\0/.test(message)) {
    throw new ParticipantEmailError('INVALID_MESSAGE', 400);
  }
  return { subject, message, slotId: parseSlot(body.slot_id) };
}

function parseMessage(body = {}) {
  const draft = parseDraft(body);
  if (typeof body.request_id !== 'string' || !/^[a-f\d]{8}(?:-[a-f\d]{4}){3}-[a-f\d]{12}$/i.test(body.request_id)) {
    throw new ParticipantEmailError('INVALID_REQUEST_ID', 400);
  }
  return { ...draft, requestId: body.request_id.toLowerCase() };
}

async function sampleDraft(db, eventId, body) {
  const { subject, message, slotId } = parseDraft(body);
  const language = body.language ?? 'en';
  if (!['en', 'es'].includes(language)) throw new ParticipantEmailError('INVALID_LANGUAGE', 400);
  const [[event]] = await db.query('SELECT name_en, name_es FROM health_event WHERE id = ?', [eventId]);
  if (!event) throw new ParticipantEmailError('NOT_FOUND', 404);
  const params = slotId === null ? [eventId] : [eventId, slotId];
  const filter = slotId === null ? '' : 'AND id = ?';
  // Use a configured slot to illustrate the footer, even for clinics with no participants.
  const [appointments] = await db.query(`SELECT service_key, DATE_FORMAT(slot_date,'%Y-%m-%d') AS slot_date,
    TIME_FORMAT(start_time,'%H:%i') AS start_time FROM health_event_slot WHERE health_event_id = ? ${filter}
    ORDER BY slot_date, start_time, id LIMIT 1`, params);
  if (slotId !== null && !appointments.length) throw new ParticipantEmailError('INVALID_SLOT', 400);
  return { subject, message, reservations: [{
    firstname: language === 'es' ? 'Participante de ejemplo' : 'Sample participant', language,
    eventName: language === 'es' ? event.name_es : event.name_en, appointments,
    manageUrl: 'https://example.invalid/reservation'
  }] };
}

async function sendTest(db, eventId, body, send) {
  const to = normalizeParticipantEmail(body.to);
  if (!to) throw new ParticipantEmailError('INVALID_TEST_EMAIL', 400);
  const draft = await sampleDraft(db, eventId, body);
  const prefix = draft.reservations[0].language === 'es' ? '[Prueba]' : '[Test]';
  let receipt;
  try {
    receipt = await send({ ...draft, subject: prefix + ' ' + draft.subject, to });
  } catch (error) {
    const code = deliveryFailure(error) === 'failed' ? 'TEST_EMAIL_FAILED' : 'TEST_EMAIL_UNCERTAIN';
    throw new ParticipantEmailError(code, 502);
  }
  if (receipt?.accepted?.length !== 1 || receipt.rejected?.length) {
    throw new ParticipantEmailError('TEST_EMAIL_FAILED', 502);
  }
  return { accepted: true };
}

function summarizeAddresses(registrations) {
  const addresses = new Map();
  let withoutEmail = 0;
  for (const row of registrations) {
    const address = normalizeParticipantEmail(row.contact_email) || normalizeParticipantEmail(row.email);
    if (!address) { withoutEmail++; continue; }
    const key = address.toLowerCase();
    if (!addresses.has(key)) addresses.set(key, { address, registrations: [] });
    addresses.get(key).registrations.push(row);
  }
  const counts = { registrations: registrations.length, recipients: addresses.size,
    without_email: withoutEmail, duplicates: registrations.length - addresses.size - withoutEmail };
  return { addresses, counts };
}

async function checkEvent(db, eventId, lock = false) {
  const [events] = await db.query('SELECT id FROM health_event WHERE id = ?' + (lock ? ' FOR UPDATE' : ''), [eventId]);
  if (!events.length) throw new ParticipantEmailError('NOT_FOUND', 404);
}

async function checkSlot(db, eventId, slotId) {
  if (slotId === null) return;
  const [slots] = await db.query('SELECT id FROM health_event_slot WHERE id = ? AND health_event_id = ?', [slotId, eventId]);
  if (!slots.length) throw new ParticipantEmailError('INVALID_SLOT', 400);
}

async function loadAudience(db, eventId, slotId) {
  const params = [eventId];
  let scope = '';
  if (slotId !== null) {
    scope = `AND EXISTS (SELECT 1 FROM health_event_appointment a JOIN health_event_slot sl ON sl.id = a.slot_id
      WHERE a.registration_id = r.id AND a.status = 'booked' AND sl.health_event_id = r.health_event_id AND sl.id = ?)`;
    params.push(slotId);
  }
  const [rows] = await db.query(
    `SELECT ${participantContactEmailSql} AS contact_email, u.email, u.firstname, u.language, r.id, r.reservation_version
     FROM health_event_registration r JOIN user u ON u.id = r.user_id
     WHERE r.health_event_id = ? AND r.registration_role = 'beneficiary' AND r.status = 'registered'
       AND COALESCE(u.deleted, 'N') = 'N' ${scope} ORDER BY r.id`, params);
  return summarizeAddresses(rows);
}

async function previewAudience(db, eventId, rawSlot) {
  const slotId = parseSlot(rawSlot);
  await checkEvent(db, eventId);
  await checkSlot(db, eventId, slotId);
  return (await loadAudience(db, eventId, slotId)).counts;
}

async function getJob(db, eventId, jobId = null) {
  const filter = jobId === null ? 'ORDER BY id DESC LIMIT 1' : 'AND id = ?';
  const [jobs] = await db.query(`SELECT id, subject, slot_id, registrations, recipients, without_email, duplicates
    FROM health_event_participant_email WHERE health_event_id = ? ${filter}`, jobId === null ? [eventId] : [eventId, jobId]);
  if (!jobs.length) {
    if (jobId === null) return null;
    throw new ParticipantEmailError('NOT_FOUND', 404);
  }
  const job = jobs[0];
  const [states] = await db.query(`SELECT status, COUNT(*) AS total FROM health_event_participant_delivery
    WHERE job_id = ? GROUP BY status`, [job.id]);
  const totals = { pending: 0, sending: 0, sent: 0, failed: 0, skipped: 0, uncertain: 0 };
  for (const row of states) totals[row.status] = Number(row.total);
  const pending = totals.pending + totals.sending;
  const status = !pending ? 'completed' : totals.pending === job.recipients ? 'pending' : 'sending';
  return { ...job, status, sent: totals.sent, failed: totals.failed, uncertain: totals.uncertain, skipped: totals.skipped, pending };
}

async function enqueue(db, eventId, userId, rawMessage) {
  const { requestId, subject, message, slotId } = parseMessage(rawMessage);
  const connection = await db.getConnection();
  try {
    await connection.beginTransaction();
    // Serialize duplicate browser submissions, including concurrent retries.
    await checkEvent(connection, eventId, true);
    const [existing] = await connection.query(`SELECT id, subject, message, slot_id, created_by_user_id
      FROM health_event_participant_email WHERE health_event_id = ? AND request_id = ?`, [eventId, requestId]);
    let jobId;
    if (existing.length) {
      const original = existing[0];
      if (original.subject !== subject || original.message !== message || original.slot_id !== slotId || original.created_by_user_id !== userId) {
        throw new ParticipantEmailError('REQUEST_ID_CONFLICT', 409);
      }
      jobId = original.id;
    } else {
      await checkSlot(connection, eventId, slotId);
      const { addresses, counts } = await loadAudience(connection, eventId, slotId);
      if (!counts.recipients) throw new ParticipantEmailError('NO_RECIPIENTS', 409);
      const [created] = await connection.query(`INSERT INTO health_event_participant_email
        (health_event_id, request_id, created_by_user_id, slot_id, subject, message, registrations, recipients, without_email, duplicates)
        VALUES (?,?,?,?,?,?,?,?,?,?)`,
      [eventId, requestId, userId, slotId, subject, message, counts.registrations, counts.recipients, counts.without_email, counts.duplicates]);
      jobId = created.insertId;
      const entries = Array.from(addresses.values(), recipient => [jobId, recipient.address]);
      for (let offset = 0; offset < entries.length; offset += 500) {
        await connection.query('INSERT INTO health_event_participant_delivery (job_id,email) VALUES ?', [entries.slice(offset, offset + 500)]);
      }
    }
    const job = await getJob(connection, eventId, jobId);
    await connection.commit();
    return job;
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

function deliveryFailure(error) {
  // If the connection broke while waiting for the final SMTP acknowledgement,
  // the provider may already have accepted the message.
  if ((error?.responseCode >= 400 && error.responseCode < 600) ||
      ['EAUTH', 'EDNS', 'EENVELOPE'].includes(error?.code)) return 'failed';
  return 'uncertain';
}

async function loadBatchAppointments(db, job, registrations) {
  const appointments = new Map();
  if (!registrations.length) return appointments;
  const ids = registrations.map(r => r.id);
  const params = [job.health_event_id, ...ids];
  const slotFilter = job.slot_id == null ? '' : 'AND s.id = ?';
  if (job.slot_id != null) params.push(job.slot_id);
  const [rows] = await db.query(`SELECT a.registration_id, s.service_key, DATE_FORMAT(s.slot_date,'%Y-%m-%d') AS slot_date,
    TIME_FORMAT(s.start_time,'%H:%i') AS start_time FROM health_event_appointment a JOIN health_event_slot s ON s.id = a.slot_id
    WHERE s.health_event_id = ? AND a.status = 'booked' AND a.registration_id IN (${ids.map(() => '?').join(',')})
      ${slotFilter} ORDER BY s.slot_date, s.start_time, s.id`, params);
  for (const row of rows) {
    if (!appointments.has(row.registration_id)) appointments.set(row.registration_id, []);
    appointments.get(row.registration_id).push(row);
  }
  return appointments;
}

async function sendBatch(db, send) {
  const [jobs] = await db.query(`SELECT j.id, j.health_event_id, j.slot_id, j.subject, j.message, e.name_en, e.name_es
    FROM health_event_participant_email j JOIN health_event e ON e.id = j.health_event_id JOIN health_event_participant_delivery d ON d.job_id = j.id
    WHERE d.status = 'pending' ORDER BY d.job_id, d.id LIMIT 1`);
  if (!jobs.length) return;
  const job = jobs[0];
  const [deliveries] = await db.query(`SELECT id, email FROM health_event_participant_delivery WHERE job_id = ?
    AND status = 'pending' ORDER BY id LIMIT 6`, [job.id]);
  const { addresses } = await loadAudience(db, job.health_event_id, job.slot_id);
  const registrations = deliveries.flatMap(d => addresses.get(d.email.toLowerCase())?.registrations || []);
  const appointments = await loadBatchAppointments(db, job, registrations);

  const results = await Promise.allSettled(deliveries.map(async delivery => {
    const recipient = addresses.get(delivery.email.toLowerCase());
    if (!recipient) {
      await db.query("UPDATE health_event_participant_delivery SET status='skipped', email=NULL WHERE id=? AND status='pending'", [delivery.id]);
      return;
    }
    const reservations = recipient.registrations.map(r => {
      const language = r.language === 'es' ? 'es' : 'en';
      return { firstname: r.firstname, language, eventName: language === 'es' ? job.name_es : job.name_en,
        appointments: appointments.get(r.id) || [], manageUrl: buildReservationUrl(r, language) };
    });
    const [claim] = await db.query("UPDATE health_event_participant_delivery SET status='sending' WHERE id=? AND status='pending'", [delivery.id]);
    if (!claim.affectedRows) return;
    let status;
    try {
      const receipt = await send({ to: delivery.email, subject: job.subject, message: job.message, reservations });
      status = receipt?.accepted?.length === 1 && !receipt.rejected?.length ? 'sent' : 'failed';
    } catch (error) {
      status = deliveryFailure(error);
    }
    await db.query('UPDATE health_event_participant_delivery SET status=?, email=NULL WHERE id=?', [status, delivery.id]);
  }));
  const failure = results.find(result => result.status === 'rejected');
  if (failure) throw failure.reason;
}

// Only the holder of this connection-level database lock may send. When a
// process dies mid-SMTP, preserve the unknown outcome instead of duplicating
// the message. Pending deliveries can resume in any process after restart.
async function processQueue(pool, send) {
  const connection = await pool.getConnection();
  let locked = false;
  try {
    const [[row]] = await connection.query("SELECT GET_LOCK('health_event_participant_mail_worker',0) AS acquired");
    locked = row.acquired === 1;
    if (!locked) return;
    await connection.query("UPDATE health_event_participant_delivery SET status='uncertain', email=NULL WHERE status='sending'");
    await sendBatch(connection, send);
  } finally {
    try {
      if (locked) await connection.query("SELECT RELEASE_LOCK('health_event_participant_mail_worker')");
    } finally {
      connection.release();
    }
  }
}

module.exports = { ParticipantEmailError, parseMessage, sampleDraft, sendTest, previewAudience, getJob, enqueue, processQueue };
