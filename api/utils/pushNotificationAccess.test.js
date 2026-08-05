'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  PUSH_NOTIFICATION_MANAGER_ROLES,
  canManagePushNotifications
} = require('./pushNotificationAccess');

test('push notification management is available to admin and opsmanager', () => {
  assert.deepEqual(PUSH_NOTIFICATION_MANAGER_ROLES, ['admin', 'opsmanager']);
  assert.equal(canManagePushNotifications({ role: 'admin' }), true);
  assert.equal(canManagePushNotifications({ role: 'opsmanager' }), true);
});

test('push notification management rejects every other or missing role', () => {
  for (const user of [
    { role: 'client' },
    { role: 'stocker' },
    { role: 'director' },
    { role: 'contentmanager' },
    { role: 'OpsManager' },
    {},
    null,
    undefined
  ]) {
    assert.equal(canManagePushNotifications(user), false);
  }
});
