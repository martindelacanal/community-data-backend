'use strict';

const PUSH_NOTIFICATION_MANAGER_ROLES = Object.freeze([
  'admin',
  'opsmanager'
]);

function canManagePushNotifications(user) {
  return Boolean(user && PUSH_NOTIFICATION_MANAGER_ROLES.includes(user.role));
}

module.exports = {
  PUSH_NOTIFICATION_MANAGER_ROLES,
  canManagePushNotifications
};
