'use strict';

const WRISTBAND_SOURCES = new Set(['usb', 'bluetooth', 'nfc_native', 'nfc_web']);

/** Preserve all seven UID bytes, including leading zeroes and byte order. */
function normalizeWristbandUid(value) {
  if (typeof value !== 'string') return null;
  const raw = value.trim();
  if (!raw || raw.length > 40 || !/^[0-9a-fA-F\s:-]+$/.test(raw)) return null;
  const uid = raw.replace(/[\s:-]/g, '').toUpperCase();
  return /^[0-9A-F]{14}$/.test(uid) ? uid : null;
}

function normalizeWristbandSource(value) {
  return WRISTBAND_SOURCES.has(value) ? value : null;
}

function buildHealthStaffDebounceKey(eventId, standId, userId) {
  return `staff:${eventId}:${standId}:${userId}`;
}

class HealthWristbandError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.code = code;
    this.status = status;
  }
}

function assertAssignableRegistration(registration, eventId) {
  if (!registration || Number(registration.health_event_id) !== Number(eventId)) {
    throw new HealthWristbandError('NOT_REGISTERED', 404);
  }
  if (registration.status !== 'registered' || registration.user_enabled !== 'Y' || registration.user_deleted !== 'N'
      || !['beneficiary', 'volunteer'].includes(registration.registration_role)) {
    throw new HealthWristbandError('PARTICIPANT_NOT_ACTIVE', 409);
  }
}

module.exports = {
  normalizeWristbandUid,
  normalizeWristbandSource,
  buildHealthStaffDebounceKey,
  assertAssignableRegistration,
  HealthWristbandError
};
