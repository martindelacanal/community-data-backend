/**
 * Shared short-lived beneficiary PIN ("token") system.
 *
 * Used as the manual fallback when a QR scan is not possible:
 *  - food distribution pickups (user.js: POST /beneficiary/pin + /upload/beneficiaryPIN)
 *  - health events check-in/check-out (healthEvents.js: POST /health-events/:id/pin + /scan)
 *
 * 2026-07-28 revamp (PINs "sometimes failed" in the field):
 *  - Alphanumeric Crockford-style base32 alphabet (no I, L, O, U): 32^4 =
 *    1,048,576 combinations vs 10,000 with the old 4-digit numeric PINs, so
 *    the global UNIQUE(pin) constraint stops colliding under event-scale load.
 *  - Input normalization forgives the classic misreadings: lowercase,
 *    spaces/dashes, O->0, I->1, L->1, U->V.
 *  - TTL raised 60s -> 300s: PINs kept expiring while the person walked to the
 *    stand or read the code out loud. Clients render the countdown from the
 *    ttl_seconds/expires_at_ms fields, so they adapt automatically.
 */
const crypto = require('crypto');
const mysqlConnection = require('../connection/connection');

const BENEFICIARY_PIN_TTL_SECONDS = 300;
const BENEFICIARY_PIN_LENGTH = 4;
const BENEFICIARY_PIN_MAX_ATTEMPTS = 200;

// Crockford base32: digits + consonant-heavy letters, minus I, L, O, U.
const PIN_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
const PIN_CHAR_FIXUPS = { O: '0', I: '1', L: '1', U: 'V' };

function encodePin(number) {
  let value = number;
  let pin = '';
  for (let i = 0; i < BENEFICIARY_PIN_LENGTH; i++) {
    pin = PIN_ALPHABET[value % PIN_ALPHABET.length] + pin;
    value = Math.floor(value / PIN_ALPHABET.length);
  }
  return pin;
}

/**
 * Normalizes user-typed input to the canonical PIN form, or null when it can
 * never match. Accepts legacy all-digit PINs too (digits are in the alphabet).
 */
function normalizeBeneficiaryPin(pin) {
  if (pin == null) return null;
  const cleaned = String(pin).toUpperCase().replace(/[\s-]+/g, '');
  if (cleaned.length !== BENEFICIARY_PIN_LENGTH) return null;
  let normalized = '';
  for (const char of cleaned) {
    const fixed = PIN_CHAR_FIXUPS[char] || char;
    if (!PIN_ALPHABET.includes(fixed)) return null;
    normalized += fixed;
  }
  return normalized;
}

function mapBeneficiaryPinResponse(row) {
  return {
    pin: row.pin,
    expires_at_ms: Number(row.expires_at_ms),
    server_time_ms: Number(row.server_time_ms),
    ttl_seconds: BENEFICIARY_PIN_TTL_SECONDS,
    pin_length: BENEFICIARY_PIN_LENGTH
  };
}

async function cleanupExpiredBeneficiaryPins() {
  await mysqlConnection.promise().query(
    'delete from beneficiary_delivery_pin where expires_at <= UTC_TIMESTAMP()'
  );
}

async function getActiveBeneficiaryPin(userId) {
  const [rows] = await mysqlConnection.promise().query(
    `select
        pin,
        TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', expires_at) / 1000 as expires_at_ms,
        TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(3)) / 1000 as server_time_ms
      from beneficiary_delivery_pin
      where user_id = ?
        and expires_at > UTC_TIMESTAMP()
      order by expires_at desc
      limit 1`,
    [userId]
  );

  return rows.length > 0 ? mapBeneficiaryPinResponse(rows[0]) : null;
}

async function getBeneficiaryPinById(pinId) {
  const [rows] = await mysqlConnection.promise().query(
    `select
        pin,
        TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', expires_at) / 1000 as expires_at_ms,
        TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(3)) / 1000 as server_time_ms
      from beneficiary_delivery_pin
      where id = ?`,
    [pinId]
  );

  return rows.length > 0 ? mapBeneficiaryPinResponse(rows[0]) : null;
}

/**
 * Returns the user's active PIN or allocates a new unique one.
 * location_id records WHERE the PIN was requested (event location / pickup
 * location); resolution is by pin alone, so one PIN works across stands.
 */
async function createBeneficiaryPin(userId, locationId) {
  await cleanupExpiredBeneficiaryPins();

  const activePin = await getActiveBeneficiaryPin(userId);
  if (activePin) {
    return activePin;
  }

  await mysqlConnection.promise().query(
    'delete from beneficiary_delivery_pin where user_id = ?',
    [userId]
  );

  for (let attempt = 0; attempt < BENEFICIARY_PIN_MAX_ATTEMPTS; attempt++) {
    const candidatePin = encodePin(crypto.randomInt(0, PIN_ALPHABET.length ** BENEFICIARY_PIN_LENGTH));

    try {
      const [insertResult] = await mysqlConnection.promise().query(
        `insert into beneficiary_delivery_pin(user_id, location_id, pin, expires_at)
         values(?, ?, ?, DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND))`,
        [userId, locationId, candidatePin, BENEFICIARY_PIN_TTL_SECONDS]
      );

      return await getBeneficiaryPinById(insertResult.insertId);
    } catch (error) {
      if (error && error.code === 'ER_DUP_ENTRY') {
        continue;
      }
      throw error;
    }
  }

  throw new Error('Could not allocate a unique beneficiary PIN');
}

/**
 * Resolves an active PIN to its user. Returns {user_id, location_id} or null.
 * Callers add their own eligibility conditions on the user afterwards.
 */
async function resolveBeneficiaryPin(pin) {
  const normalized = normalizeBeneficiaryPin(pin);
  if (!normalized) return null;

  await cleanupExpiredBeneficiaryPins();

  const [rows] = await mysqlConnection.promise().query(
    `select bp.user_id, bp.location_id
       from beneficiary_delivery_pin bp
      where bp.pin = ?
        and bp.expires_at > UTC_TIMESTAMP()
      order by bp.expires_at desc
      limit 1`,
    [normalized]
  );

  return rows.length > 0 ? { user_id: rows[0].user_id, location_id: rows[0].location_id } : null;
}

module.exports = {
  BENEFICIARY_PIN_TTL_SECONDS,
  BENEFICIARY_PIN_LENGTH,
  normalizeBeneficiaryPin,
  cleanupExpiredBeneficiaryPins,
  getActiveBeneficiaryPin,
  createBeneficiaryPin,
  resolveBeneficiaryPin
};
