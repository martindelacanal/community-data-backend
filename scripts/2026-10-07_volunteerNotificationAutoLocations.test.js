'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { migrate, TRIGGER_BODY, DEFAULT_EMAILS } = require('./2026-10-07_volunteerNotificationAutoLocations');

function fixture({ existing = false, initialized = false, trigger = false, triggerBody = TRIGGER_BODY, failInitialization = false } = {}) {
  const statements = [];
  const events = [];
  const connection = {
    beginTransaction: async () => events.push('begin'),
    commit: async () => events.push('commit'),
    rollback: async () => events.push('rollback'),
    end: async () => events.push('end'),
    query: async (sql, params = []) => {
      statements.push({ sql, params });
      if (/information_schema.TABLES/.test(sql) && /TABLE_NAME IN/.test(sql)) {
        return [[{ TABLE_NAME: 'location', ENGINE: 'InnoDB' },
          { TABLE_NAME: 'volunteer_notification_recipient', ENGINE: 'InnoDB' },
          { TABLE_NAME: 'volunteer_notification_recipient_location', ENGINE: 'InnoDB' }]];
      }
      if (/information_schema.COLUMNS/.test(sql)) return [existing ? [{ COLUMN_NAME: 'auto_include_new_locations' }] : []];
      if (/information_schema.TABLES/.test(sql)) return [existing ? [{ TABLE_NAME: 'volunteer_notification_recipient_settings' }] : []];
      if (/information_schema.TRIGGERS/.test(sql)) return [trigger ? [{
        ACTION_STATEMENT: triggerBody, ACTION_TIMING: 'AFTER', EVENT_MANIPULATION: 'INSERT', EVENT_OBJECT_TABLE: 'location',
      }] : []];
      if (/SELECT defaults_initialized/.test(sql)) return [[{ defaults_initialized: initialized ? 1 : 0 }]];
      if (/^UPDATE volunteer_notification_recipient\s+SET auto_include_new_locations/.test(sql) && failInitialization) {
        throw new Error('Simulated seed failure');
      }
      if (/SELECT COUNT\(\*\)/.test(sql)) return [[{ total: 4, opted_in: 2 }]];
      return [{ affectedRows: 1 }];
    },
  };
  const options = { connect: async () => connection, config: () => ({}), log: () => {} };
  return { statements, events, options };
}

test('a fresh migration seeds exactly the specified addresses once without changing selected locations', async () => {
  const f = fixture();
  await migrate('development', { ...f.options, apply: true });
  const seed = f.statements.find(({ sql }) => /^UPDATE volunteer_notification_recipient\s+SET auto_include_new_locations/.test(sql));
  assert.deepEqual(seed.params, ['alex@bienestariswellbeing.org', 'karenantillon22@yahoo.com']);
  assert.match(seed.sql, /LOWER\(TRIM\(email\)\) IN \(\?, \?\)/);
  assert.match(seed.sql, /modification_date = modification_date/);
  assert.ok(f.statements.some(({ sql }) => /ALTER TABLE.*TINYINT\(1\) NOT NULL DEFAULT 0/.test(sql)));
  assert.equal(f.statements.some(({ sql }) => /^CREATE TRIGGER|^DROP TRIGGER/.test(sql)), false);
  assert.equal(f.statements.some(({ sql }) => /^(INSERT|UPDATE|DELETE).*volunteer_notification_recipient_location/s.test(sql)), false);
  assert.deepEqual(f.events, ['begin', 'commit', 'end']);
  assert.deepEqual(DEFAULT_EMAILS, seed.params);
});

test('rerunning preserves later user preferences and removes only the exact legacy trigger', async () => {
  const f = fixture({ existing: true, initialized: true, trigger: true });
  await migrate('production', { ...f.options, apply: true });
  assert.equal(f.statements.some(({ sql }) => /^ALTER TABLE|^CREATE TRIGGER/.test(sql)), false);
  assert.equal(f.statements.some(({ sql }) => /^UPDATE volunteer_notification_recipient\s+SET auto_include_new_locations/.test(sql)), false);
  assert.deepEqual(f.statements.filter(({ sql }) => /^DROP TRIGGER/.test(sql)).map(({ sql }) => sql), [
    'DROP TRIGGER `location_volunteer_notification_recipients_ai`',
  ]);
  assert.deepEqual(f.events, ['begin', 'commit', 'end']);
});

test('an unexpected legacy trigger is never dropped or replaced', async () => {
  const f = fixture({ existing: true, initialized: true, trigger: true, triggerBody: 'BEGIN SELECT 1; END' });
  await assert.rejects(migrate('development', { ...f.options, apply: true }), /unexpected trigger/);
  assert.ok(f.statements.every(({ sql }) => /^SELECT/.test(sql)));
  assert.deepEqual(f.events, ['end']);
});

test('a retry after initial column creation finishes default initialization transactionally', async () => {
  const f = fixture({ existing: true, initialized: false });
  await migrate('development', { ...f.options, apply: true });
  assert.equal(f.statements.some(({ sql }) => /^ALTER TABLE/.test(sql)), false);
  const seedIndex = f.statements.findIndex(({ sql }) => /^UPDATE volunteer_notification_recipient\s+SET auto_include_new_locations/.test(sql));
  const markerIndex = f.statements.findIndex(({ sql }) => /SET defaults_initialized = 1/.test(sql));
  assert.ok(seedIndex >= 0 && markerIndex > seedIndex);
  assert.deepEqual(f.events, ['begin', 'commit', 'end']);
});

test('failed initialization rolls back and closes the connection', async () => {
  const f = fixture({ failInitialization: true });
  await assert.rejects(migrate('development', { ...f.options, apply: true }), /Simulated seed failure/);
  assert.deepEqual(f.events, ['begin', 'rollback', 'end']);
  assert.equal(f.statements.some(({ sql }) => /SET defaults_initialized = 1|^CREATE TRIGGER/.test(sql)), false);
});

test('dry run reads only metadata and writes no migration state', async () => {
  const f = fixture();
  await migrate('production', f.options);
  assert.ok(f.statements.length > 0);
  assert.ok(f.statements.every(({ sql }) => /^SELECT/.test(sql)));
  assert.deepEqual(f.events, ['end']);
});
