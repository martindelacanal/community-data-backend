'use strict';

// Location creators and recipient replacements must use the same connection
// and open transaction. Acquire the lock before inserting a location, then add
// its recipient links before committing so a partial location cannot survive.
async function lockVolunteerNotificationRecipientSettings(connection) {
  await connection.query('UPDATE volunteer_notification_recipient_settings SET id = id WHERE id = 1');
}

async function addVolunteerNotificationRecipientsForLocation(connection, locationId) {
  if (!Number.isSafeInteger(locationId) || locationId <= 0) {
    throw new TypeError('A positive integer location ID is required');
  }
  const [result] = await connection.query(
    `INSERT INTO volunteer_notification_recipient_location (recipient_id, location_id)
     SELECT recipient.id, ? FROM volunteer_notification_recipient AS recipient
     LEFT JOIN volunteer_notification_recipient_location AS existing_location
       ON existing_location.recipient_id = recipient.id AND existing_location.location_id = ?
     WHERE recipient.enabled = 'Y' AND recipient.auto_include_new_locations = 1
       AND existing_location.recipient_id IS NULL`,
    [locationId, locationId]
  );
  return result.affectedRows;
}

module.exports = { lockVolunteerNotificationRecipientSettings, addVolunteerNotificationRecipientsForLocation };
