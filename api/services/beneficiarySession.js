const crypto = require('node:crypto');
const jwt = require('jsonwebtoken');
const { buildRestoreAuthBinding } = require('../utils/restoreAuthBinding');

const ACCOUNT_SQL = `
  SELECT u.id, u.firstname, u.username, u.email, u.password, u.client_id,
         u.language, u.enabled, u.deleted, u.reset_password, u.creation_date,
         r.name AS role
  FROM user AS u INNER JOIN role AS r ON r.id = u.role_id
  WHERE u.id = ? LIMIT 1`;
const SESSION_ROLES = new Set([
  'admin', 'client', 'stocker', 'delivery', 'beneficiary', 'opsmanager',
  'director', 'auditor', 'contentmanager', 'eventvolunteer',
]);

class SessionRevokedError extends Error {
  constructor() {
    super('Session is no longer valid');
    this.status = 401;
  }
}

function sameBinding(actual, expected) {
  return typeof actual === 'string' && /^[a-f0-9]{64}$/.test(actual)
    && crypto.timingSafeEqual(Buffer.from(actual, 'hex'), Buffer.from(expected, 'hex'));
}

function sessionForAccount(user, secret) {
  const data = JSON.stringify({
    id: Number(user.id), firstname: user.firstname, username: user.username,
    email: user.email, client_id: user.client_id, role: user.role,
    language: user.language, enabled: user.enabled,
  });
  return {
    token: jwt.sign({ data, restore_auth_binding: buildRestoreAuthBinding(user, secret) },
      secret, { algorithm: 'HS256', expiresIn: '6h' }),
    reset_password: user.reset_password,
  };
}

// Use only immediately after verifying a password or creating an account.
async function issueSessionForUser(userId, pool, secret) {
  const [rows] = await pool.query(ACCOUNT_SQL, [userId]);
  const user = rows[0];
  if (!user || user.enabled !== 'Y' || user.deleted !== 'N' || !SESSION_ROLES.has(user.role)) {
    throw new SessionRevokedError();
  }
  return sessionForAccount(user, secret);
}

/**
 * Beneficiaries can renew their finite access token after an absence. The signed
 * credential remains bound to the current password and account state, so password
 * resets, account deletion/disabling and role changes end that persistent session.
 * Legacy tokens may upgrade only while their original access is still valid.
 */
async function renewSession(token, pool, secret) {
  let claims;
  let previousUser;
  try {
    claims = jwt.verify(token, secret, { algorithms: ['HS256'], ignoreExpiration: true });
    previousUser = JSON.parse(claims.data);
    if (!previousUser?.id || !SESSION_ROLES.has(previousUser.role)
      || !Number.isFinite(claims.exp)) throw new Error('Invalid session');
  } catch {
    throw new SessionRevokedError();
  }

  const expired = claims.exp * 1000 <= Date.now();
  if (previousUser.role !== 'beneficiary') {
    if (expired) throw new SessionRevokedError();
    return {
      token: jwt.sign({ data: claims.data, restore_auth_binding: claims.restore_auth_binding },
        secret, { algorithm: 'HS256', expiresIn: '6h' }),
    };
  }

  const [rows] = await pool.query(ACCOUNT_SQL, [previousUser.id]);
  const user = rows[0];
  if (!user || user.enabled !== 'Y' || user.deleted !== 'N' || user.role !== 'beneficiary') {
    throw new SessionRevokedError();
  }
  const binding = buildRestoreAuthBinding(user, secret);
  if (claims.restore_auth_binding
    ? !sameBinding(claims.restore_auth_binding, binding)
    : expired) {
    throw new SessionRevokedError();
  }
  return sessionForAccount(user, secret);
}

module.exports = { renewSession, issueSessionForUser, SessionRevokedError };
