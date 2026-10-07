'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {
  lockVolunteerNotificationRecipientSettings,
  addVolunteerNotificationRecipientsForLocation,
} = require('./volunteerNotificationRecipients');

test('auto-association uses a parameterized idempotent insert for enabled opted-in recipients', async () => {
  const statements = [];
  const connection = { query: async (sql, params = []) => {
    statements.push({ sql, params });
    return [{ affectedRows: 2 }];
  } };
  await lockVolunteerNotificationRecipientSettings(connection);
  const added = await addVolunteerNotificationRecipientsForLocation(connection, 42);
  assert.equal(added, 2);
  assert.match(statements[0].sql, /^UPDATE volunteer_notification_recipient_settings/);
  assert.match(statements[1].sql, /recipient.enabled = 'Y' AND recipient.auto_include_new_locations = 1/);
  assert.match(statements[1].sql, /existing_location.recipient_id IS NULL/);
  assert.deepEqual(statements[1].params, [42, 42]);
});

test('auto-association rejects invalid location identifiers before a query', async () => {
  let queries = 0;
  const connection = { query: async () => { queries += 1; return [{ affectedRows: 0 }]; } };
  for (const locationId of [null, undefined, 0, -1, '2', 2.5, NaN, Number.MAX_SAFE_INTEGER + 1]) {
    await assert.rejects(addVolunteerNotificationRecipientsForLocation(connection, locationId), /positive integer/);
  }
  assert.equal(queries, 0);
});

const seedSource = fs.readFileSync(path.resolve(__dirname, '../../scripts/seedBanningHealthEvent.js'), 'utf8');
const locationStart = seedSource.indexOf('  // --- 1. location + client');
const locationEnd = seedSource.indexOf('  let [clientRows]', locationStart);
assert.ok(locationStart >= 0 && locationEnd > locationStart);
const seedLocationBlock = seedSource.slice(locationStart, locationEnd);

async function seedFixture({ existing = false, failAssociations = false } = {}) {
  const events = [];
  const c = {
    beginTransaction: async () => events.push('begin'),
    commit: async () => events.push('commit'),
    rollback: async () => events.push('rollback'),
    query: async sql => {
      if (/SELECT id FROM location/.test(sql)) { events.push('lookup'); return [existing ? [{ id: 42 }] : []]; }
      if (/UPDATE volunteer_notification_recipient_settings/.test(sql)) { events.push('lock'); return [{ affectedRows: 1 }]; }
      if (/INSERT INTO location/.test(sql)) { events.push('insert'); return [{ insertId: 42 }]; }
      if (/INSERT INTO volunteer_notification_recipient_location/.test(sql)) {
        events.push('associate');
        if (failAssociations) throw new Error('Simulated association failure');
        return [{ affectedRows: 2 }];
      }
      throw new Error('Unexpected seed query');
    },
  };
  const context = vm.createContext({
    c, LOCATION: { organization: 'School', community_city: 'City', address: 'Address' }, log: () => {},
    lockVolunteerNotificationRecipientSettings, addVolunteerNotificationRecipientsForLocation,
  });
  return { events, execute: () => vm.runInContext(`(async () => { ${seedLocationBlock}\n return locationId; })()`, context) };
}

test('the seed location path commits automatic associations with the new location', async () => {
  const f = await seedFixture();
  assert.equal(await f.execute(), 42);
  assert.deepEqual(f.events, ['lookup', 'begin', 'lock', 'insert', 'associate', 'commit']);
});

test('the seed location path rolls back when automatic association fails', async () => {
  const f = await seedFixture({ failAssociations: true });
  await assert.rejects(f.execute(), /Simulated association failure/);
  assert.deepEqual(f.events, ['lookup', 'begin', 'lock', 'insert', 'associate', 'rollback']);
});

test('the seed does not retroactively associate an existing location', async () => {
  const f = await seedFixture({ existing: true });
  assert.equal(await f.execute(), 42);
  assert.deepEqual(f.events, ['lookup']);
});
