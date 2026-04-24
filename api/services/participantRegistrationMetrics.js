const mysqlConnection = require('../connection/connection');
const { EXCLUDED_REPORT_USER_IDS } = require('./rawDataReport');

const PARTICIPANT_LA_TIME_ZONE_SQL = "'America/Los_Angeles'";
const PARTICIPANT_UTC_TIME_ZONE_SQL = "'+00:00'";
const PARTICIPANT_REGISTER_CACHE_TTL_MS = 5000;
const PARTICIPANT_REGISTER_CACHE_MAX_ENTRIES = 100;

const participantRegisterCache = new Map();

function parseDateOnly(value) {
  if (!value) {
    return null;
  }

  const match = String(value).trim().match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

function normalizeNumberArray(values) {
  if (!Array.isArray(values)) {
    return [];
  }

  return [...new Set(
    values
      .map(value => Number(value))
      .filter(value => Number.isFinite(value))
  )].sort((a, b) => a - b);
}

function normalizeParticipantRegisterFilters(filters = {}) {
  return {
    from_date: parseDateOnly(filters.from_date) || '1970-01-01',
    to_date: parseDateOnly(filters.to_date) || '2100-01-01',
    locations: normalizeNumberArray(filters.locations)
  };
}

function normalizeClientId(value) {
  const clientId = Number(value);
  return Number.isFinite(clientId) && clientId > 0 ? clientId : null;
}

function resolveClientId(cabecera = {}, rawFilters = {}, options = {}) {
  const explicitClientId = normalizeClientId(options.clientId ?? options.client_id);
  if (explicitClientId !== null) {
    return explicitClientId;
  }

  if (cabecera.role === 'client') {
    return normalizeClientId(cabecera.client_id);
  }

  if (['admin', 'director', 'auditor'].includes(cabecera.role)) {
    return normalizeClientId(rawFilters.client_id);
  }

  return null;
}

function getExcludedUserIds(options = {}) {
  if (Array.isArray(options.excludedUserIds)) {
    return normalizeNumberArray(options.excludedUserIds);
  }

  return normalizeNumberArray(EXCLUDED_REPORT_USER_IDS);
}

function placeholders(values) {
  return values.map(() => '?').join(',');
}

function appendDateRangeParams(params, fromDate, toDate) {
  params.push(fromDate, toDate);
}

function appendLocationParams(params, filters) {
  if (filters.locations.length > 0) {
    params.push(...filters.locations);
  }
}

function buildScopedUsersCte(clientId, excludedUserIds) {
  const joins = [];
  const conditions = [
    'u.role_id = 5',
    "u.enabled = 'Y'"
  ];
  const params = [];

  if (clientId !== null) {
    joins.push('INNER JOIN client_user cu ON cu.user_id = u.id');
    conditions.push('cu.client_id = ?');
    params.push(clientId);
  }

  if (excludedUserIds.length > 0) {
    conditions.push(`u.id NOT IN (${placeholders(excludedUserIds)})`);
    params.push(...excludedUserIds);
  }

  return {
    sql: `scoped_users AS (
         SELECT DISTINCT u.id, u.creation_date, u.first_location_id
         FROM user u
         ${joins.join('\n')}
         WHERE ${conditions.join('\n           AND ')}
       )`,
    params
  };
}

function buildRegisterCountQuery(clientId, filters, excludedUserIds) {
  const scopedUsersCte = buildScopedUsersCte(clientId, excludedUserIds);
  const locationPlaceholders = placeholders(filters.locations);
  const hasLocationFilter = filters.locations.length > 0;
  const params = [...scopedUsersCte.params];

  appendDateRangeParams(params, filters.from_date, filters.to_date);
  appendLocationParams(params, filters);

  appendDateRangeParams(params, filters.from_date, filters.to_date);
  appendLocationParams(params, filters);
  appendLocationParams(params, filters);
  params.push(filters.from_date);
  appendLocationParams(params, filters);
  params.push(filters.from_date);

  appendDateRangeParams(params, filters.from_date, filters.to_date);
  appendLocationParams(params, filters);

  appendDateRangeParams(params, filters.from_date, filters.to_date);
  appendLocationParams(params, filters);

  const sql = `WITH ${scopedUsersCte.sql},
       new_users AS (
         SELECT su.id
         FROM scoped_users su
         WHERE su.creation_date >= CONVERT_TZ(?, ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           AND su.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           ${hasLocationFilter ? `AND su.first_location_id IN (${locationPlaceholders})` : ''}
       ),
       recurring_users AS (
         SELECT DISTINCT db_range.receiving_user_id AS user_id
         FROM delivery_beneficiary db_range
         INNER JOIN scoped_users su ON su.id = db_range.receiving_user_id
         WHERE db_range.creation_date >= CONVERT_TZ(?, ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           AND db_range.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           ${hasLocationFilter ? `AND db_range.location_id IN (${locationPlaceholders})` : ''}
           AND (
             EXISTS (
               SELECT 1
               FROM delivery_beneficiary db_prev
               WHERE db_prev.receiving_user_id = su.id
                 AND db_prev.creation_date < db_range.creation_date
                 ${hasLocationFilter ? `AND db_prev.location_id IN (${locationPlaceholders})` : ''}
             )
             OR (
               su.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
               ${hasLocationFilter ? `AND su.first_location_id IN (${locationPlaceholders})` : ''}
               AND NOT EXISTS (
                 SELECT 1
                 FROM delivery_beneficiary db_no_past
                 WHERE db_no_past.receiving_user_id = su.id
                   AND db_no_past.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
               )
             )
           )
       ),
       recurring_without_new_users AS (
         SELECT ru.user_id
         FROM recurring_users ru
         LEFT JOIN new_users nu ON nu.id = ru.user_id
         WHERE nu.id IS NULL
       ),
       delivery_participations AS (
         SELECT COUNT(db_part.id) AS total
         FROM delivery_beneficiary db_part
         INNER JOIN scoped_users su ON su.id = db_part.receiving_user_id
         WHERE db_part.creation_date >= CONVERT_TZ(?, ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           AND db_part.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           AND db_part.delivering_user_id IS NOT NULL
           ${hasLocationFilter ? `AND db_part.location_id IN (${locationPlaceholders})` : ''}
       ),
       registration_participations AS (
         SELECT COUNT(DISTINCT u_reg.id) AS total
         FROM scoped_users u_reg
         WHERE u_reg.creation_date >= CONVERT_TZ(?, ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           AND u_reg.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           ${hasLocationFilter ? `AND u_reg.first_location_id IN (${locationPlaceholders})` : ''}
           AND NOT EXISTS (
             SELECT 1
             FROM delivery_beneficiary db_same_day
             WHERE db_same_day.receiving_user_id = u_reg.id
               AND db_same_day.delivering_user_id IS NOT NULL
               AND db_same_day.creation_date >= CONVERT_TZ(
                 DATE(CONVERT_TZ(u_reg.creation_date, ${PARTICIPANT_UTC_TIME_ZONE_SQL}, ${PARTICIPANT_LA_TIME_ZONE_SQL})),
                 ${PARTICIPANT_LA_TIME_ZONE_SQL},
                 ${PARTICIPANT_UTC_TIME_ZONE_SQL}
               )
               AND db_same_day.creation_date < DATE_ADD(
                 CONVERT_TZ(
                   DATE(CONVERT_TZ(u_reg.creation_date, ${PARTICIPANT_UTC_TIME_ZONE_SQL}, ${PARTICIPANT_LA_TIME_ZONE_SQL})),
                   ${PARTICIPANT_LA_TIME_ZONE_SQL},
                   ${PARTICIPANT_UTC_TIME_ZONE_SQL}
                 ),
                 INTERVAL 1 DAY
               )
           )
       )
       SELECT
         (SELECT COUNT(*) FROM scoped_users) AS total,
         (SELECT COUNT(*) FROM new_users) AS new,
         (SELECT COUNT(*) FROM recurring_users) AS recurring,
         (SELECT COUNT(*) FROM recurring_without_new_users) AS recurring_without_new,
         COALESCE((SELECT total FROM delivery_participations), 0) +
         COALESCE((SELECT total FROM registration_participations), 0) AS participations`;

  return { sql, params };
}

function buildRegisterDetailsQuery(clientId, filters, excludedUserIds) {
  const scopedUsersCte = buildScopedUsersCte(clientId, excludedUserIds);
  const locationPlaceholders = placeholders(filters.locations);
  const hasLocationFilter = filters.locations.length > 0;
  const params = [...scopedUsersCte.params];

  appendDateRangeParams(params, filters.from_date, filters.to_date);
  appendLocationParams(params, filters);

  appendDateRangeParams(params, filters.from_date, filters.to_date);
  appendLocationParams(params, filters);
  appendLocationParams(params, filters);
  params.push(filters.from_date);
  appendLocationParams(params, filters);
  params.push(filters.from_date);

  const sql = `WITH ${scopedUsersCte.sql},
       new_users AS (
         SELECT su.id AS user_id, su.first_location_id AS location_id
         FROM scoped_users su
         WHERE su.creation_date >= CONVERT_TZ(?, ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           AND su.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           ${hasLocationFilter ? `AND su.first_location_id IN (${locationPlaceholders})` : ''}
       ),
       recurring_delivery_rows AS (
         SELECT
           db_range.id AS delivery_id,
           db_range.receiving_user_id AS user_id,
           db_range.location_id,
           db_range.creation_date
         FROM delivery_beneficiary db_range
         INNER JOIN scoped_users su ON su.id = db_range.receiving_user_id
         WHERE db_range.creation_date >= CONVERT_TZ(?, ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           AND db_range.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
           ${hasLocationFilter ? `AND db_range.location_id IN (${locationPlaceholders})` : ''}
           AND (
             EXISTS (
               SELECT 1
               FROM delivery_beneficiary db_prev
               WHERE db_prev.receiving_user_id = su.id
                 AND db_prev.creation_date < db_range.creation_date
                 ${hasLocationFilter ? `AND db_prev.location_id IN (${locationPlaceholders})` : ''}
             )
             OR (
               su.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
               ${hasLocationFilter ? `AND su.first_location_id IN (${locationPlaceholders})` : ''}
               AND NOT EXISTS (
                 SELECT 1
                 FROM delivery_beneficiary db_no_past
                 WHERE db_no_past.receiving_user_id = su.id
                   AND db_no_past.creation_date < CONVERT_TZ(DATE_ADD(?, INTERVAL 1 DAY), ${PARTICIPANT_LA_TIME_ZONE_SQL}, ${PARTICIPANT_UTC_TIME_ZONE_SQL})
               )
             )
           )
       ),
       recurring_assigned AS (
         SELECT
           rdr.user_id,
           CAST(
             SUBSTRING_INDEX(
               MIN(CONCAT(DATE_FORMAT(rdr.creation_date, '%Y%m%d%H%i%s'), LPAD(rdr.delivery_id, 20, '0'), '#', rdr.location_id)),
               '#',
               -1
             ) AS UNSIGNED
           ) AS location_id
         FROM recurring_delivery_rows rdr
         GROUP BY rdr.user_id
       )
       SELECT 'new' AS metric_type, user_id, location_id
       FROM new_users
       UNION ALL
       SELECT 'recurring' AS metric_type, user_id, location_id
       FROM recurring_assigned
       ORDER BY metric_type, user_id`;

  return { sql, params };
}

function cleanupParticipantRegisterCache() {
  if (participantRegisterCache.size <= PARTICIPANT_REGISTER_CACHE_MAX_ENTRIES) {
    return;
  }

  const now = Date.now();
  for (const [cacheKey, cacheEntry] of participantRegisterCache.entries()) {
    if (cacheEntry.expiresAt <= now && !cacheEntry.promise) {
      participantRegisterCache.delete(cacheKey);
    }
  }
}

async function getCachedParticipantRegisterMetrics(cacheKey, computeFn) {
  cleanupParticipantRegisterCache();

  const now = Date.now();
  const cachedEntry = participantRegisterCache.get(cacheKey);

  if (cachedEntry) {
    if (cachedEntry.value !== undefined && cachedEntry.expiresAt > now) {
      return cachedEntry.value;
    }

    if (cachedEntry.promise) {
      return cachedEntry.promise;
    }

    participantRegisterCache.delete(cacheKey);
  }

  const pendingPromise = (async () => {
    try {
      const value = await computeFn();
      participantRegisterCache.set(cacheKey, {
        value,
        expiresAt: Date.now() + PARTICIPANT_REGISTER_CACHE_TTL_MS
      });
      return value;
    } catch (error) {
      participantRegisterCache.delete(cacheKey);
      throw error;
    }
  })();

  participantRegisterCache.set(cacheKey, {
    promise: pendingPromise,
    expiresAt: now + PARTICIPANT_REGISTER_CACHE_TTL_MS
  });

  return pendingPromise;
}

async function fetchParticipantRegisterCounts(clientId, filters, excludedUserIds) {
  const { sql, params } = buildRegisterCountQuery(clientId, filters, excludedUserIds);
  const [rows] = await mysqlConnection.promise().query(sql, params);
  const row = rows[0] || {};

  return {
    total: Number(row.total || 0),
    new: Number(row.new || 0),
    recurring: Number(row.recurring || 0),
    recurring_without_new: Number(row.recurring_without_new || 0),
    participations: Number(row.participations || 0)
  };
}

async function fetchParticipantRegisterDetails(clientId, filters, excludedUserIds) {
  const { sql, params } = buildRegisterDetailsQuery(clientId, filters, excludedUserIds);
  const [rows] = await mysqlConnection.promise().query(sql, params);

  const newUserIds = [];
  const recurringUserIds = [];
  const newPerLocationMap = {};
  const recurringPerLocationMap = {};

  rows.forEach(row => {
    const userId = Number(row.user_id);
    const locationId = Number(row.location_id);

    if (!Number.isFinite(userId)) {
      return;
    }

    if (row.metric_type === 'new') {
      newUserIds.push(userId);
      if (Number.isFinite(locationId)) {
        newPerLocationMap[locationId] = (newPerLocationMap[locationId] || 0) + 1;
      }
      return;
    }

    if (row.metric_type === 'recurring') {
      recurringUserIds.push(userId);
      if (Number.isFinite(locationId)) {
        recurringPerLocationMap[locationId] = (recurringPerLocationMap[locationId] || 0) + 1;
      }
    }
  });

  return {
    newUserIds,
    recurringUserIds,
    newPerLocationMap,
    recurringPerLocationMap
  };
}

async function getParticipantRegisterSummary(cabecera = {}, rawFilters = {}, options = {}) {
  const filters = normalizeParticipantRegisterFilters(rawFilters);
  const clientId = resolveClientId(cabecera, rawFilters, options);
  const excludedUserIds = getExcludedUserIds(options);
  const includeDetails = options.includeDetails === true;
  const cacheKey = JSON.stringify({
    scope: 'participant-register-summary',
    clientId,
    filters,
    excludedUserIds
  });

  const counts = await getCachedParticipantRegisterMetrics(cacheKey, () => (
    fetchParticipantRegisterCounts(clientId, filters, excludedUserIds)
  ));

  if (!includeDetails) {
    return counts;
  }

  const details = await fetchParticipantRegisterDetails(clientId, filters, excludedUserIds);

  return {
    ...counts,
    new: details.newUserIds.length,
    recurring: details.recurringUserIds.length,
    newUserIds: details.newUserIds,
    recurringUserIds: details.recurringUserIds,
    newPerLocationMap: details.newPerLocationMap,
    recurringPerLocationMap: details.recurringPerLocationMap,
    filters,
    clientId,
    excludedUserIds
  };
}

module.exports = {
  getParticipantRegisterSummary,
  normalizeParticipantRegisterFilters
};
