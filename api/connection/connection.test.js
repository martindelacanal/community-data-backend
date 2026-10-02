'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, 'connection.js'), 'utf8');

function poolConfig(env = {}) {
  let options;
  vm.runInNewContext(source, {
    process: { env }, module: { exports: {} }, console,
    require(name) {
      assert.equal(name, 'mysql2');
      return { createPool(config) { options = config; return { on() {} }; } };
    }
  });
  return options;
}

test('production pool reserves database capacity and bounds waiting requests', () => {
  const config = poolConfig();
  assert.equal(config.connectionLimit, 15);
  assert.equal(config.queueLimit, 100);
  assert.equal(config.maxIdle, 5);
  assert.equal(config.waitForConnections, true);
});

test('pool configuration accepts bounded overrides and rejects unsafe values', () => {
  const config = poolConfig({ DB_POOL_CONNECTION_LIMIT: '3', DB_POOL_QUEUE_LIMIT: '50' });
  assert.equal(config.connectionLimit, 3);
  assert.equal(config.maxIdle, 3);
  assert.equal(config.queueLimit, 50);
  for (const value of ['0', '-1', '1.5', '1000', 'NaN', '']) {
    assert.equal(poolConfig({ DB_POOL_CONNECTION_LIMIT: value }).connectionLimit, 15);
  }
  assert.equal(poolConfig({ DB_POOL_QUEUE_LIMIT: '0' }).queueLimit, 100);
});
