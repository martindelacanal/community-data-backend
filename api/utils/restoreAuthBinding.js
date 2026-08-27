const crypto = require('node:crypto');

// An opaque claim in newly authenticated JWTs. A token issued before a
// password reset must not be able to enroll a new, long-lived restore key.
function buildRestoreAuthBinding(user, secret) {
  return crypto.createHmac('sha256', secret)
    .update('android-restore-auth-v1\0' + JSON.stringify([
      Number(user.id), user.password, user.enabled, user.deleted,
      user.reset_password, user.creation_date,
    ])).digest('hex');
}

module.exports = { buildRestoreAuthBinding };
