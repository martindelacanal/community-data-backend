'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { assignBeneficiaryUsername } = require('./beneficiaryUsername');

test('simple usernames normalize names and remain distinct for simultaneous registrations', async () => {
  const assigned = new Map();
  const connection = { query: async (sql, params) => {
    if (sql.startsWith('SELECT')) return [[]];
    assigned.set(params[1], params[0]);
    return [{ affectedRows: 1 }];
  } };
  const names = await Promise.all([assignBeneficiaryUsername(connection, 101, 'María'), assignBeneficiaryUsername(connection, 102, 'María')]);
  assert.deepEqual(names, ['maria101', 'maria102']);
  assert.equal(await assignBeneficiaryUsername(connection, 103, ''), 'bienestar103');
});

test('an existing username collision gets a short unique suffix', async () => {
  const connection = { query: async (sql, params) => sql.startsWith('SELECT')
    ? [params[0] === 'ana101' ? [{ id: 9 }] : []] : [{ affectedRows: 1 }] };
  assert.equal(await assignBeneficiaryUsername(connection, 101, 'Ana'), 'ana101.1');
});
