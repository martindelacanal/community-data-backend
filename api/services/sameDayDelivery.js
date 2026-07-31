'use strict';

const FOOD_DELIVERY_TIME_ZONE = 'America/Los_Angeles';

const SAME_DAY_RANGE_CONDITION = `
    db.creation_date >= CONVERT_TZ(
      CONCAT(DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', ?)), ' 00:00:00'),
      ?,
      @@session.time_zone
    )
    AND db.creation_date < CONVERT_TZ(
      CONCAT(
        DATE_ADD(DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', ?)), INTERVAL 1 DAY),
        ' 00:00:00'
      ),
      ?,
      @@session.time_zone
    )
`;

/**
 * Food deliveries are operated on California calendar days, while
 * delivery_beneficiary.creation_date is a DATETIME populated in the active
 * database session time zone. Convert the LA day boundaries to that time zone
 * so this works both in UTC production and SYSTEM-time development, while the
 * indexed creation_date column remains unwrapped.
 */
const SAME_DAY_APPROVED_DELIVERIES_QUERY = `
  SELECT
    db.location_id,
    l.organization,
    l.community_city,
    l.address,
    COUNT(*) AS delivery_count
  FROM delivery_beneficiary AS db
  LEFT JOIN location AS l ON l.id = db.location_id
  WHERE db.receiving_user_id = ?
    AND db.approved = 'Y'
    AND ${SAME_DAY_RANGE_CONDITION}
  GROUP BY db.location_id, l.organization, l.community_city, l.address
  ORDER BY MIN(db.creation_date), db.location_id
`;

const LATEST_SAME_DAY_DELIVERY_QUERY = `
  SELECT db.id, db.approved, db.delivering_user_id, db.location_id, db.client_id
  FROM delivery_beneficiary AS db
  WHERE db.location_id = ?
    AND db.receiving_user_id = ?
    AND ${SAME_DAY_RANGE_CONDITION}
  ORDER BY db.creation_date DESC, db.id DESC
  LIMIT 1
`;

function getSameDayTimeZoneParams() {
  return [
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE
  ];
}

async function getSameDayApprovedDeliveries(mysqlConnection, receivingUserId) {
  if (!Number.isInteger(Number(receivingUserId)) || Number(receivingUserId) <= 0) {
    return [];
  }

  const [rows] = await mysqlConnection.promise().query(
    SAME_DAY_APPROVED_DELIVERIES_QUERY,
    [Number(receivingUserId), ...getSameDayTimeZoneParams()]
  );

  return rows.map((row) => ({
    location_id: row.location_id == null ? null : Number(row.location_id),
    organization: row.organization || null,
    community_city: row.community_city || null,
    address: row.address || null,
    delivery_count: Number(row.delivery_count) || 0
  }));
}

async function getLatestSameDayDelivery(mysqlConnection, { receivingUserId, locationId }) {
  const normalizedReceivingUserId = Number(receivingUserId);
  const normalizedLocationId = Number(locationId);
  if (
    !Number.isInteger(normalizedReceivingUserId) || normalizedReceivingUserId <= 0 ||
    !Number.isInteger(normalizedLocationId) || normalizedLocationId <= 0
  ) {
    return [];
  }

  const [rows] = await mysqlConnection.promise().query(
    LATEST_SAME_DAY_DELIVERY_QUERY,
    [normalizedLocationId, normalizedReceivingUserId, ...getSameDayTimeZoneParams()]
  );

  return rows;
}

module.exports = {
  FOOD_DELIVERY_TIME_ZONE,
  SAME_DAY_RANGE_CONDITION,
  SAME_DAY_APPROVED_DELIVERIES_QUERY,
  LATEST_SAME_DAY_DELIVERY_QUERY,
  getSameDayApprovedDeliveries,
  getLatestSameDayDelivery
};
