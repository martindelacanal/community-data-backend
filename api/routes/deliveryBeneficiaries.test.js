'use strict';
const assert = require('node:assert/strict');
const { test } = require('node:test');
const express = require('express');
const { createDeliveryBeneficiariesRouter } = require('./deliveryBeneficiaries');

async function withServer(database, operation, actorEnabled = true) {
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => { req.data = { data: JSON.stringify({ id: 4, role: req.headers['x-test-role'] || 'delivery' }) }; next(); });
  app.use(createDeliveryBeneficiariesRouter({ query: (sql, params) => {
    if (sql.includes('AS delivery_support_actor')) return Promise.resolve([actorEnabled ? [{ id: 4 }] : []]);
    return database.query(sql, params);
  } }, { hash: async password => `hashed:${password}` }));
  const server = await new Promise(resolve => { const value = app.listen(0, '127.0.0.1', () => resolve(value)); });
  try { await operation(`http://127.0.0.1:${server.address().port}`); }
  finally { await new Promise(resolve => server.close(resolve)); }
}

test('beneficiaries and unrelated staff roles cannot search or reset through delivery support', async () => {
  let calls = 0;
  await withServer({ query: async () => { calls++; return [[]]; } }, async base => {
    for (const role of ['beneficiary', 'eventvolunteer', 'client', 'admin']) {
      assert.equal((await fetch(base, { headers: { 'x-test-role': role } })).status, 403);
      assert.equal((await fetch(`${base}/3/reset-password`, { method: 'POST', headers: { 'x-test-role': role } })).status, 403);
    }
  });
  assert.equal(calls, 0);
});

test('a stale delivery token cannot reset passwords after its account was disabled or demoted', async () => {
  let writes = 0;
  await withServer({ query: async () => { writes++; return [{ affectedRows: 1 }]; } }, async base => {
    assert.equal((await fetch(base)).status, 403);
    assert.equal((await fetch(`${base}/9/reset-password`, { method: 'POST' })).status, 403);
  }, false);
  assert.equal(writes, 0);
});

test('search only exposes beneficiaries basic fields, binds search and caps pagination', async () => {
  const calls = [];
  const users = Array.from({ length: 26 }, (_, id) => ({ id: id + 1, firstname: 'Ana' }));
  await withServer({ query: async (...args) => { calls.push(args); return [users]; } }, async base => {
    const result = await fetch(`${base}/?search=${encodeURIComponent("Ana O'Brien")}&page=2`).then(r => r.json());
    assert.equal(result.users.length, 25);
    assert.equal(result.has_more, true);
    assert.equal(result.page, 2);
  });
  assert.match(calls[0][0], /r\.name = 'beneficiary'/);
  assert.match(calls[0][0], /u\.deleted = 'N'/);
  assert.match(calls[0][0], /u\.enabled = 'Y'/);
  assert.doesNotMatch(calls[0][0], /O'Brien|password|SELECT \*/);
  assert.equal(calls[0][1][7], "%O'Brien%");
  assert.equal(calls[0][1].at(-1), 50);
});

test('a formatted phone is matched as one normalized number; dates keep their separators', async () => {
  const calls = [];
  await withServer({ query: async (...args) => { calls.push(args); return [[]]; } }, async base => {
    await fetch(`${base}/?search=${encodeURIComponent('+1 (213) 555-0199')}`);
    await fetch(`${base}/?search=${encodeURIComponent('31/12/1950')}`);
    await fetch(`${base}/?search=1950-12-31`);
  });
  assert.deepEqual(calls[0][1], ['%2135550199%', 0]);
  assert.match(calls[0][0], /CAST\(u.phone AS CHAR\)/);
  assert.match(calls[1][0], /%d\/%m\/%Y/);
  assert.equal(calls[1][1][0], '%31/12/1950%');
  assert.equal(calls[2][1][0], '%1950-12-31%');
  assert.match(calls[2][0], /%Y-%m-%d/);
});

test('reset refuses invalid IDs and atomically excludes staff, deleted and disabled users', async () => {
  const calls = [];
  await withServer({ query: async (...args) => { calls.push(args); return [{ affectedRows: 0 }]; } }, async base => {
    assert.equal((await fetch(`${base}/-1/reset-password`, { method: 'POST' })).status, 400);
    assert.equal((await fetch(`${base}/3/reset-password`, { method: 'POST' })).status, 404);
  });
  assert.equal(calls.length, 1);
  assert.match(calls[0][0], /r\.name = 'beneficiary'.*u\.deleted = 'N'.*u\.enabled = 'Y'/);
  assert.deepEqual(calls[0][1], ['hashed:bienestar', 3]);
});

test('delivery reset returns lowercase bienestar only after the beneficiary was updated', async () => {
  await withServer({ query: async () => [{ affectedRows: 1 }] }, async base => {
    const result = await fetch(`${base}/9/reset-password`, { method: 'POST' });
    assert.equal(result.status, 200);
    assert.deepEqual(await result.json(), { password: 'bienestar' });
  });
});
