'use strict';

// Queries use r for the registration, u for its participant. Old web forms could
// copy the operator's login email when enrolling someone else with no contact.
// A shared address explicitly supplied to the new form has a confirmation row.
// Historical web rows with an address belonging to a different account alone
// do not establish which of those two cases produced the address.
const participantContactEmailSql = `CASE
  WHEN r.source = 'web' AND r.contact_email IS NOT NULL
    AND (u.email IS NULL OR r.contact_email <> u.email)
    AND EXISTS (SELECT 1 FROM user other_user WHERE other_user.email = r.contact_email AND other_user.id <> r.user_id)
    AND NOT EXISTS (SELECT 1 FROM health_event_confirmation c WHERE c.registration_id = r.id AND c.reservation_version = r.reservation_version)
  THEN NULL ELSE r.contact_email END`;

module.exports = { participantContactEmailSql };
