'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizePositiveInteger,
  resolveDeliveryClientContext
} = require('./deliveryClientContext');

function databaseWithClients(clientIds) {
  return {
    promise: () => ({
      query: async (sql, params) => {
        assert.match(sql, /FROM client_location/);
        assert.deepEqual(params, [83]);
        return [clientIds.map((clientId) => ({ client_id: clientId }))];
      }
    })
  };
}

test('normalizes only positive integer identifiers', () => {
  assert.equal(normalizePositiveInteger('2'), 2);
  assert.equal(normalizePositiveInteger(83), 83);
  assert.equal(normalizePositiveInteger('null'), null);
  assert.equal(normalizePositiveInteger(' undefined '), null);
  assert.equal(normalizePositiveInteger(null), null);
  assert.equal(normalizePositiveInteger('2.5'), null);
  assert.equal(normalizePositiveInteger(0), null);
});

test('infers the sole configured client when old apps send null', async () => {
  const context = await resolveDeliveryClientContext(databaseWithClients([2]), {
    locationId: '83',
    requestedClientId: 'null',
    authenticatedClientId: null
  });

  assert.deepEqual(context, { error: null, locationId: 83, clientId: 2 });
});

test('uses an authenticated client only when it belongs to the location', async () => {
  const context = await resolveDeliveryClientContext(databaseWithClients([2, 7]), {
    locationId: 83,
    requestedClientId: null,
    authenticatedClientId: '7'
  });

  assert.deepEqual(context, { error: null, locationId: 83, clientId: 7 });
});

test('requires a choice at multi-client locations without a valid fallback', async () => {
  const context = await resolveDeliveryClientContext(databaseWithClients([2, 7]), {
    locationId: 83,
    requestedClientId: null,
    authenticatedClientId: 99
  });

  assert.deepEqual(context, { error: 'client_required', locationId: 83, clientId: null });
});

test('rejects an explicitly requested client outside the location', async () => {
  const context = await resolveDeliveryClientContext(databaseWithClients([2]), {
    locationId: 83,
    requestedClientId: 7,
    authenticatedClientId: 2
  });

  assert.deepEqual(context, { error: 'client_invalid', locationId: 83, clientId: null });
});

test('reports a location without client configuration instead of returning null', async () => {
  const context = await resolveDeliveryClientContext(databaseWithClients([]), {
    locationId: 83,
    requestedClientId: null,
    authenticatedClientId: null
  });

  assert.deepEqual(context, {
    error: 'client_not_configured',
    locationId: 83,
    clientId: null
  });
});
