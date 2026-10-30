'use strict';
const { validPersonName } = require('../utils/participantIdentity');
const VERSION = '2026-10-30';
const DOCUMENT = '/assets/documents/volunteer-staff-data-privacy-byod-2026-10-30.pdf';
function validPrivacyConsent(consent) {
  return consent && consent.accepted === true && consent.version === VERSION
    && validPersonName(consent.fullName) && ['volunteer', 'staff'].includes(consent.role)
    && typeof consent.deviceModel === 'string' && consent.deviceModel.trim().length > 0 && consent.deviceModel.trim().length <= 200;
}
async function savePrivacyConsent(connection, consent, { userId = null, volunteerId = null, registrationId = null, email }) {
  if (!validPrivacyConsent(consent)) throw new Error('PRIVACY_CONSENT_REQUIRED');
  await connection.query(`INSERT INTO volunteer_privacy_consent
    (user_id, volunteer_id, registration_id, document_version, document_path, full_name, participant_role, email, device_model, accepted_at)
    VALUES (?,?,?,?,?,?,?,?,?,NOW(3))`, [userId, volunteerId, registrationId, VERSION, DOCUMENT,
    consent.fullName.trim(), consent.role, email, consent.deviceModel.trim()]);
}
module.exports = { VERSION, DOCUMENT, validPrivacyConsent, savePrivacyConsent };
