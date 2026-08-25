'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  FOOD_DELIVERY_TIME_ZONE,
  LATEST_SAME_DAY_DELIVERY_QUERY,
  SAME_DAY_APPROVED_DELIVERIES_QUERY,
  getSameDayApprovedDeliveries,
  getLatestSameDayDelivery
} = require('./sameDayDelivery');

test('loads only approved deliveries from the current California day', async () => {
  let capturedSql;
  let capturedParams;
  const connection = {
    promise: () => ({
      query: async (sql, params) => {
        capturedSql = sql;
        capturedParams = params;
        return [[{
          location_id: '17',
          organization: 'Community Center',
          community_city: 'Banning',
          address: '100 Main St',
          delivery_count: '2'
        }]];
      }
    })
  };

  const deliveries = await getSameDayApprovedDeliveries(connection, 42);

  assert.match(capturedSql, /db\.approved = 'Y'/);
  assert.match(capturedSql, /db\.creation_date >= CONVERT_TZ/);
  assert.match(capturedSql, /db\.creation_date < CONVERT_TZ/);
  assert.match(capturedSql, /@@session\.time_zone/);
  assert.deepEqual(capturedParams, [
    42,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE
  ]);
  assert.deepEqual(deliveries, [{
    location_id: 17,
    organization: 'Community Center',
    community_city: 'Banning',
    address: '100 Main St',
    delivery_count: 2
  }]);
});

test('does not query for an invalid participant id', async () => {
  let queried = false;
  const connection = {
    promise: () => ({
      query: async () => {
        queried = true;
        return [[]];
      }
    })
  };

  assert.deepEqual(await getSameDayApprovedDeliveries(connection, null), []);
  assert.equal(queried, false);
  assert.ok(SAME_DAY_APPROVED_DELIVERIES_QUERY.includes('LEFT JOIN location'));
});

test('loads the latest delivery from the current California day using an indexable range', async () => {
  let capturedSql;
  let capturedParams;
  const expectedRow = {
    id: 91,
    approved: 'N',
    delivering_user_id: 7,
    location_id: 17,
    client_id: 3
  };
  const connection = {
    promise: () => ({
      query: async (sql, params) => {
        capturedSql = sql;
        capturedParams = params;
        return [[expectedRow]];
      }
    })
  };

  const rows = await getLatestSameDayDelivery(connection, {
    receivingUserId: 42,
    locationId: 17
  });

  assert.equal(capturedSql, LATEST_SAME_DAY_DELIVERY_QUERY);
  assert.match(capturedSql, /db\.creation_date >= CONVERT_TZ/);
  assert.match(capturedSql, /db\.creation_date < CONVERT_TZ/);
  assert.doesNotMatch(capturedSql, /DATE\(db\.creation_date\)/i);
  assert.deepEqual(capturedParams, [
    17,
    42,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE,
    FOOD_DELIVERY_TIME_ZONE
  ]);
  assert.deepEqual(rows, [expectedRow]);
});

test('accepts a promise connection so delivery mutations can share a transaction', async () => {
  let queried = false;
  const transactionConnection = {
    query: async () => {
      queried = true;
      return [[]];
    }
  };

  await getLatestSameDayDelivery(transactionConnection, {
    receivingUserId: 42,
    locationId: 17
  });

  assert.equal(queried, true);
});
