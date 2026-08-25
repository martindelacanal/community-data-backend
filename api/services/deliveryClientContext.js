'use strict';

function normalizePositiveInteger(value) {
  const textValue = typeof value === 'string' ? value.trim().toLowerCase() : value;
  if (
    textValue === null
    || textValue === undefined
    || textValue === ''
    || textValue === 'null'
    || textValue === 'undefined'
  ) {
    return null;
  }

  const normalized = Number(textValue);
  return Number.isInteger(normalized) && normalized > 0 ? normalized : null;
}

function getQueryExecutor(database) {
  if (database && typeof database.promise === 'function') {
    return database.promise();
  }
  return database;
}

/**
 * Resolves the client used at a food-distribution location.
 *
 * The location/client relation is authoritative. Older clients may send
 * `null` when a location has only one client, so in that case the server
 * safely infers it. At multi-client locations an explicit selection remains
 * mandatory.
 */
async function resolveDeliveryClientContext(database, {
  locationId,
  requestedClientId,
  authenticatedClientId
}) {
  const normalizedLocationId = normalizePositiveInteger(locationId);
  if (!normalizedLocationId) {
    return { error: 'location_required', locationId: null, clientId: null };
  }

  const executor = getQueryExecutor(database);
  const [rows] = await executor.query(
    `SELECT client_id
       FROM client_location
      WHERE location_id = ?
      ORDER BY client_id`,
    [normalizedLocationId]
  );
  const allowedClientIds = [...new Set(rows
    .map((row) => normalizePositiveInteger(row.client_id))
    .filter((clientId) => clientId !== null))];

  if (allowedClientIds.length === 0) {
    return {
      error: 'client_not_configured',
      locationId: normalizedLocationId,
      clientId: null
    };
  }

  const normalizedRequestedClientId = normalizePositiveInteger(requestedClientId);
  const requestedClientText = typeof requestedClientId === 'string'
    ? requestedClientId.trim().toLowerCase()
    : requestedClientId;
  const requestIncludedClient = requestedClientText !== null
    && requestedClientText !== undefined
    && requestedClientText !== ''
    && requestedClientText !== 'null'
    && requestedClientText !== 'undefined';

  if (requestIncludedClient) {
    if (
      !normalizedRequestedClientId
      || !allowedClientIds.includes(normalizedRequestedClientId)
    ) {
      return {
        error: 'client_invalid',
        locationId: normalizedLocationId,
        clientId: null
      };
    }

    return {
      error: null,
      locationId: normalizedLocationId,
      clientId: normalizedRequestedClientId
    };
  }

  const normalizedAuthenticatedClientId = normalizePositiveInteger(authenticatedClientId);
  if (
    normalizedAuthenticatedClientId
    && allowedClientIds.includes(normalizedAuthenticatedClientId)
  ) {
    return {
      error: null,
      locationId: normalizedLocationId,
      clientId: normalizedAuthenticatedClientId
    };
  }

  if (allowedClientIds.length === 1) {
    return {
      error: null,
      locationId: normalizedLocationId,
      clientId: allowedClientIds[0]
    };
  }

  return {
    error: 'client_required',
    locationId: normalizedLocationId,
    clientId: null
  };
}

module.exports = {
  normalizePositiveInteger,
  resolveDeliveryClientContext
};
