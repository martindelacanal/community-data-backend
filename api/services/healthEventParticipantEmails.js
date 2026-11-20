'use strict';

const { createHmac } = require('node:crypto');
const { normalizeParticipantEmail } = require('../utils/participantEmailAddress');
const { participantContactEmailSql } = require('./healthEventParticipantContact');
const { buildReservationUrl, includesReservationLinks } = require('./healthEventReservation');
const { classifyMailFailure } = require('./healthEventMailFailure');

const retryDelaysMinutes = [15, 60, 360];

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
  const event = await getEmailEvent(db, eventId);
  if (!includesReservationLinks(event)) {
    await checkSlot(db, eventId, slotId);
    return { subject, message, reservations: [] };
  }
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
  const prefix = body.language === 'es' ? '[Prueba]' : '[Test]';
  let receipt;
  try {
    receipt = await send({ ...draft, subject: prefix + ' ' + draft.subject, to });
  } catch (error) {
    const code = classifyMailFailure(error) === 'uncertain' ? 'TEST_EMAIL_UNCERTAIN' : 'TEST_EMAIL_FAILED';
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

async function getEmailEvent(db, eventId, lock = false) {
  const [[event]] = await db.query("SELECT name_en, name_es, enabled, timezone, DATE_FORMAT(end_date,'%Y-%m-%d') AS end_date FROM health_event WHERE id = ?"
    + (lock ? ' FOR UPDATE' : ''), [eventId]);
  if (!event) throw new ParticipantEmailError('NOT_FOUND', 404);
  return event;
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
  const event = await getEmailEvent(db, eventId);
  await checkSlot(db, eventId, slotId);
  const { counts } = await loadAudience(db, eventId, slotId);
  return { ...counts, includes_reservation_links: includesReservationLinks(event) };
}

function recipientKey(eventId, requestId, address = '') {
  if (!process.env.JWT_SECRET) return null;
  return createHmac('sha256', process.env.JWT_SECRET)
    .update(JSON.stringify(['clinic-participant-email/v1', String(eventId), requestId, address.toLowerCase()])).digest('hex');
}

function registrationSnapshot(registrations) {
  return registrations.map(row => [row.id, row.reservation_version]);
}

function readSnapshot(value) {
  return typeof value === 'string' ? JSON.parse(value) : value;
}

async function readJob(db, eventId, jobId, lock = false) {
  const filter = jobId === null ? 'ORDER BY j.id DESC LIMIT 1' : 'AND j.id = ?';
  const [jobs] = await db.query(`SELECT j.id, j.subject, j.slot_id, j.registrations, j.recipients, j.without_email, j.duplicates,
    j.created_at, j.request_id, j.retry_generation, j.retry_key_check,
    (SELECT MAX(p.id) FROM health_event_participant_email p WHERE p.health_event_id=j.health_event_id AND p.id<j.id) AS previous_job_id,
    (SELECT MIN(n.id) FROM health_event_participant_email n WHERE n.health_event_id=j.health_event_id AND n.id>j.id) AS next_job_id,
    EXISTS(SELECT 1 FROM health_event_participant_mail_sender WHERE id=1 AND paused_until > UTC_TIMESTAMP()) AS sender_paused
    FROM health_event_participant_email j WHERE j.health_event_id = ? ${filter}${lock ? ' FOR UPDATE' : ''}`,
  jobId === null ? [eventId] : [eventId, jobId]);
  if (!jobs.length) {
    if (jobId === null) return null;
    throw new ParticipantEmailError('NOT_FOUND', 404);
  }
  return jobs[0];
}

async function manualRetryPlan(db, eventId, job, requireComplete = false) {
  const [deliveries] = await db.query(`SELECT id, status, recipient_key, retry_recipient_key, registration_snapshot
    FROM health_event_participant_delivery WHERE job_id=?`, [job.id]);
  if (deliveries.some(row => row.status === 'pending' || row.status === 'sending')) {
    if (requireComplete) throw new ParticipantEmailError('RETRY_IN_PROGRESS', 409);
    return [];
  }
  // Older campaigns and rotated keys cannot reliably exclude a mailbox already contacted.
  if (!job.retry_key_check || recipientKey(eventId, job.request_id) !== job.retry_key_check ||
      deliveries.some(row => !row.recipient_key)) return [];

  const { addresses } = await loadAudience(db, eventId, job.slot_id);
  const byRegistration = new Map();
  for (const recipient of addresses.values()) {
    for (const r of recipient.registrations) byRegistration.set(r.id, { row: r, recipient });
  }

  const reserved = new Map();
  for (const row of deliveries) {
    for (const key of [row.recipient_key, row.retry_recipient_key].filter(Boolean)) {
      reserved.set(key, reserved.has(key) && reserved.get(key) !== row.id ? null : row.id);
    }
  }

  const candidates = new Map();
  for (const row of deliveries) {
    if (row.status !== 'failed' || !row.registration_snapshot) continue;
    const matches = [];
    for (const [id, version] of readSnapshot(row.registration_snapshot)) {
      const current = byRegistration.get(id);
      if (current && current.row.reservation_version === version) matches.push(current);
    }
    if (!matches.length) continue;
    const recipient = matches[0].recipient;
    // Never split a formerly shared household delivery across unverified new targets.
    if (matches.some(match => match.recipient !== recipient)) continue;
    const key = recipientKey(eventId, job.request_id, recipient.address);
    if (reserved.has(key) && reserved.get(key) !== row.id) continue;
    // Two independent failures converging on the same new address need individual review.
    candidates.set(key, candidates.has(key) ? null : {
      id: row.id, address: recipient.address, key, snapshot: registrationSnapshot(matches.map(match => match.row))
    });
  }
  return Array.from(candidates.values()).filter(Boolean);
}

async function getJob(db, eventId, jobId = null) {
  const job = await readJob(db, eventId, jobId);
  if (!job) return null;
  const [states] = await db.query(`SELECT status, COUNT(*) AS total,
    SUM(CASE WHEN retry_at > UTC_TIMESTAMP() THEN 1 ELSE 0 END) AS deferred,
    SUM(recipient_rejected) AS recipient_rejections FROM health_event_participant_delivery
    WHERE job_id = ? GROUP BY status`, [job.id]);
  const totals = { pending: 0, sending: 0, sent: 0, failed: 0, skipped: 0, uncertain: 0 };
  let deferred = 0;
  let recipientRejections = 0;
  for (const row of states) {
    totals[row.status] = Number(row.total);
    if (row.status === 'pending') deferred = Number(row.deferred);
    if (row.status === 'failed') recipientRejections = Number(row.recipient_rejections);
  }
  const pending = totals.pending + totals.sending;
  const status = !pending ? 'completed' : totals.pending === job.recipients ? 'pending' : 'sending';
  const retryable = !pending && totals.failed && job.retry_key_check ? (await manualRetryPlan(db, eventId, job)).length : 0;
  const { retry_key_check, request_id, ...publicJob } = job;
  return { ...publicJob, status, retryable_failed: retryable, sent: totals.sent, failed: totals.failed, uncertain: totals.uncertain, skipped: totals.skipped,
    pending, deferred, sender_paused: totals.pending > 0 && Boolean(job.sender_paused), recipient_rejections: recipientRejections };
}

async function retryFailed(db, eventId, jobId, generation) {
  if (!Number.isSafeInteger(generation) || generation < 0) throw new ParticipantEmailError('INVALID_RETRY_GENERATION', 400);
  const connection = await db.getConnection();
  try {
    await connection.beginTransaction();
    const job = await readJob(connection, eventId, jobId, true);
    if (generation > job.retry_generation) throw new ParticipantEmailError('RETRY_GENERATION_CONFLICT', 409);
    if (generation === job.retry_generation) {
      const plan = await manualRetryPlan(connection, eventId, job, true);
      if (!plan.length) throw new ParticipantEmailError('NO_RETRYABLE_FAILURES', 409);
      for (const item of plan) {
        await connection.query(`UPDATE health_event_participant_delivery SET status='pending', email=?, attempts=0,
          recipient_rejected=0, retry_at=NULL, retry_recipient_key=?, registration_snapshot=? WHERE id=? AND status='failed'`,
        [item.address, item.key, JSON.stringify(item.snapshot), item.id]);
      }
      await connection.query('UPDATE health_event_participant_email SET retry_generation=retry_generation+1 WHERE id=?', [job.id]);
    }
    const result = await getJob(connection, eventId, job.id);
    await connection.commit();
    return result;
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

async function enqueue(db, eventId, userId, rawMessage) {
  const { requestId, subject, message, slotId } = parseMessage(rawMessage);
  const connection = await db.getConnection();
  try {
    await connection.beginTransaction();
    // Serialize duplicate browser submissions, including concurrent retries.
    await getEmailEvent(connection, eventId, true);
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
        (health_event_id, request_id, created_by_user_id, slot_id, subject, message, registrations, recipients, without_email, duplicates, retry_key_check)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
      [eventId, requestId, userId, slotId, subject, message, counts.registrations, counts.recipients,
        counts.without_email, counts.duplicates, recipientKey(eventId, requestId)]);
      jobId = created.insertId;
      const entries = Array.from(addresses.values(), recipient => [jobId, recipient.address,
        recipientKey(eventId, requestId, recipient.address), JSON.stringify(registrationSnapshot(recipient.registrations))]);
      for (let offset = 0; offset < entries.length; offset += 500) {
        await connection.query('INSERT INTO health_event_participant_delivery (job_id,email,recipient_key,registration_snapshot) VALUES ?',
          [entries.slice(offset, offset + 500)]);
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

async function recordDelivery(db, delivery, outcome) {
  const pauseSender = outcome === 'daily_sender_limit' || outcome === 'temporary_sender';
  const temporary = pauseSender || outcome === 'temporary_recipient';
  const retry = temporary && delivery.attempts < retryDelaysMinutes.length;
  const delay = outcome === 'daily_sender_limit' ? 24 * 60 :
    retryDelaysMinutes[Math.min(delivery.attempts, retryDelaysMinutes.length - 1)];
  // The worker holds the advisory lock. Persist the backoff and the known
  // refusal together before another worker can take over after a restart.
  if (pauseSender) await db.beginTransaction();
  try {
    if (pauseSender) {
      await db.query('UPDATE health_event_participant_mail_sender SET paused_until=TIMESTAMPADD(MINUTE, ?, UTC_TIMESTAMP()) WHERE id=1', [delay]);
    }
    if (retry) {
      await db.query(`UPDATE health_event_participant_delivery SET status='pending',
        retry_at=TIMESTAMPADD(MINUTE, ?, UTC_TIMESTAMP()) WHERE id=?`, [delay, delivery.id]);
    } else {
      const status = outcome === 'sent' || outcome === 'uncertain' ? outcome : 'failed';
      const clearSnapshot = status === 'failed' ? '' : ', registration_snapshot=NULL';
      await db.query(`UPDATE health_event_participant_delivery SET status=?, email=NULL, retry_at=NULL, recipient_rejected=?${clearSnapshot} WHERE id=?`,
        [status, outcome === 'invalid_recipient' ? 1 : 0, delivery.id]);
    }
    if (pauseSender) await db.commit();
  } catch (error) {
    if (pauseSender) await db.rollback();
    throw error;
  }
  return pauseSender;
}

async function sendBatch(db, send) {
  const [jobs] = await db.query(`SELECT j.id, j.health_event_id, j.slot_id, j.subject, j.message, e.name_en, e.name_es,
      e.enabled, e.timezone, DATE_FORMAT(e.end_date,'%Y-%m-%d') AS end_date
    FROM health_event_participant_email j JOIN health_event e ON e.id = j.health_event_id JOIN health_event_participant_delivery d ON d.job_id = j.id
    WHERE d.status = 'pending' AND (d.retry_at IS NULL OR d.retry_at <= UTC_TIMESTAMP())
      AND NOT EXISTS (SELECT 1 FROM health_event_participant_mail_sender WHERE id=1 AND paused_until > UTC_TIMESTAMP())
    ORDER BY d.job_id, d.id LIMIT 1`);
  if (!jobs.length) return;
  const job = jobs[0];
  const [deliveries] = await db.query(`SELECT id, email, attempts, registration_snapshot FROM health_event_participant_delivery WHERE job_id = ?
    AND status = 'pending' AND (retry_at IS NULL OR retry_at <= UTC_TIMESTAMP()) ORDER BY id LIMIT 6`, [job.id]);
  const { addresses } = await loadAudience(db, job.health_event_id, job.slot_id);
  for (const delivery of deliveries) {
    const members = addresses.get(delivery.email.toLowerCase())?.registrations || [];
    const versions = delivery.registration_snapshot ? new Map(readSnapshot(delivery.registration_snapshot)) : null;
    delivery.registrations = versions ? members.filter(r => versions.get(r.id) === r.reservation_version) : members;
  }
  const includeReservations = includesReservationLinks(job);
  const appointments = includeReservations ? await loadBatchAppointments(db, job, deliveries.flatMap(d => d.registrations)) : null;

  for (const delivery of deliveries) {
    if (!delivery.registrations.length) {
      await db.query("UPDATE health_event_participant_delivery SET status='skipped', email=NULL, retry_at=NULL, registration_snapshot=NULL WHERE id=? AND status='pending'", [delivery.id]);
      continue;
    }
    const reservations = includeReservations ? delivery.registrations.map(r => {
      const language = r.language === 'es' ? 'es' : 'en';
      return { firstname: r.firstname, language, eventName: language === 'es' ? job.name_es : job.name_en,
        appointments: appointments.get(r.id) || [], manageUrl: buildReservationUrl(r, language) };
    }) : [];
    const [claim] = await db.query("UPDATE health_event_participant_delivery SET status='sending', attempts=attempts+1, retry_at=NULL WHERE id=? AND status='pending'", [delivery.id]);
    if (!claim.affectedRows) continue;
    let outcome;
    try {
      const receipt = await send({ to: delivery.email, subject: job.subject, message: job.message, reservations });
      if (receipt?.accepted?.length === 1 && !receipt.rejected?.length) outcome = 'sent';
      else if (!receipt?.accepted?.length && receipt?.rejected?.length) outcome = classifyMailFailure({ ...receipt, code: 'EENVELOPE' });
      else outcome = 'uncertain';
    } catch (error) {
      outcome = classifyMailFailure(error);
    }
    if (await recordDelivery(db, delivery, outcome)) break;
  }
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
    await connection.query("UPDATE health_event_participant_delivery SET status='uncertain', email=NULL, retry_at=NULL, registration_snapshot=NULL WHERE status='sending'");
    await sendBatch(connection, send);
  } finally {
    try {
      if (locked) await connection.query("SELECT RELEASE_LOCK('health_event_participant_mail_worker')");
    } finally {
      connection.release();
    }
  }
}

module.exports = { ParticipantEmailError, parseMessage, sampleDraft, sendTest, previewAudience, getJob, enqueue, retryFailed, processQueue };
