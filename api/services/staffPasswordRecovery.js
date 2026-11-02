'use strict';
const crypto = require('node:crypto');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');

const STAFF_ROLES = new Set(['admin', 'client', 'stocker', 'delivery', 'opsmanager', 'director', 'auditor', 'contentmanager', 'eventvolunteer']);
const ACCOUNT_COLUMNS = `u.id, u.username, u.email, u.password, u.enabled, u.deleted,
  u.creation_date, u.reset_password, r.name AS role`;
const AUDIENCE = 'bienestar-staff-password-reset';
const ISSUER = 'bienestarcommunity';
const TTL_SECONDS = 20 * 60;
const ACCEPTED = Object.freeze({ status: 'accepted' });

class RecoveryError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}

function createRecoveryLimiter({ clock = Date.now, maxEntries = 5000 } = {}) {
  const entries = new Map();
  return {
    take(key, limit, duration) {
      const now = clock();
      let entry = entries.get(key);
      if (entry?.until <= now) { entries.delete(key); entry = null; }
      if (!entry) {
        if (entries.size >= maxEntries) {
          for (const [candidate, value] of entries) if (value.until <= now) entries.delete(candidate);
          if (entries.size >= maxEntries) return false;
        }
        entry = { count: 0, until: now + duration };
        entries.set(key, entry);
      }
      if (entry.count >= limit) return false;
      entry.count += 1;
      return true;
    }
  };
}

function validEmail(email) {
  return typeof email === 'string' && email.length <= 254
    && /^[^\s@<>,;:"\\]+@[^\s@<>,;:"\\]+\.[^\s@<>,;:"\\]+$/.test(email);
}
function eligible(user) {
  return user && user.enabled === 'Y' && user.deleted === 'N' && STAFF_ROLES.has(user.role);
}
function sameDigest(actual, expected) {
  return typeof actual === 'string' && /^[a-f0-9]{64}$/.test(actual)
    && crypto.timingSafeEqual(Buffer.from(actual, 'hex'), Buffer.from(expected, 'hex'));
}

function createStaffPasswordRecovery({ pool, sendEmail, secret, logger = { error() {} }, clock = Date.now,
  maxPending = 10, schedule = setImmediate } = {}) {
  const key = secret ? crypto.createHmac('sha256', secret).update('staff-password-reset-signing-v1').digest() : null;
  const cooldown = createRecoveryLimiter({ clock });
  const pending = new Set();
  const binding = user => crypto.createHmac('sha256', key).update(JSON.stringify([
    'staff-password-reset-binding-v1', Number(user.id), user.password, user.email,
    user.enabled, user.deleted, user.role, user.creation_date, user.reset_password
  ])).digest('hex');
  const unavailable = () => { throw new RecoveryError('unavailable', 503); };

  return {
    async request({ identifier, language } = {}) {
      if (typeof identifier !== 'string' || !identifier.trim() || identifier.trim().length > 254) {
        throw new RecoveryError('invalid_request');
      }
      if (!key || pending.size >= maxPending) return unavailable();
      const normalized = identifier.trim();
      const [rows] = await pool.query(
        `SELECT ${ACCOUNT_COLUMNS} FROM user u INNER JOIN role r ON r.id = u.role_id
         WHERE (TRIM(u.email) = ? OR TRIM(u.username) = ?) AND u.enabled = 'Y' AND u.deleted = 'N'
           AND r.name IN ('admin','client','stocker','delivery','opsmanager','director','auditor','contentmanager','eventvolunteer')
         LIMIT 2`, [normalized, normalized]);
      if (rows.length !== 1 || !eligible(rows[0])) return ACCEPTED;
      const user = rows[0];
      const email = typeof user.email === 'string' ? user.email.trim() : '';
      if (!validEmail(email)) return ACCEPTED;
      // Reserve before starting any asynchronous email work. Store only a digest
      // of the mailbox, and respond identically when it is unknown or cooling down.
      const mailbox = crypto.createHash('sha256').update(email.toLowerCase()).digest('hex');
      if (!cooldown.take(mailbox, 3, 60 * 60 * 1000)) return ACCEPTED;
      if (pending.size >= maxPending) return ACCEPTED;
      const token = jwt.sign({ purpose: 'staff_password_reset', binding: binding(user),
        iat: Math.floor(clock() / 1000) }, key, {
        algorithm: 'HS256', audience: AUDIENCE, issuer: ISSUER, subject: String(user.id),
        jwtid: crypto.randomBytes(16).toString('hex'), expiresIn: TTL_SECONDS
      });
      let job;
      job = new Promise(resolve => schedule(resolve))
        .then(() => sendEmail({ email, token, language: language === 'es' ? 'es' : 'en' }))
        .catch(error => logger.error(`Staff password recovery email failed: ${error.code || error.name || 'Error'}`))
        .finally(() => pending.delete(job));
      pending.add(job);
      return ACCEPTED;
    },

    async complete({ token, password } = {}) {
      if (!key) return unavailable();
      if (typeof password !== 'string' || Array.from(password).length < 8 || Buffer.byteLength(password, 'utf8') > 72) {
        throw new RecoveryError('invalid_password');
      }
      let claims;
      try {
        if (typeof token !== 'string' || token.length > 4096) throw new Error();
        claims = jwt.verify(token, key, { algorithms: ['HS256'], audience: AUDIENCE, issuer: ISSUER,
          clockTimestamp: Math.floor(clock() / 1000) });
        if (claims.purpose !== 'staff_password_reset' || typeof claims.sub !== 'string' || !/^[1-9]\d*$/.test(claims.sub)
          || !Number.isSafeInteger(Number(claims.sub)) || typeof claims.jti !== 'string'
          || !/^[a-f0-9]{32}$/.test(claims.jti) || !Number.isInteger(claims.iat)
          || !Number.isInteger(claims.exp) || claims.exp <= claims.iat
          || claims.exp - claims.iat > TTL_SECONDS || claims.iat > Math.floor(clock() / 1000)) throw new Error();
      } catch { throw new RecoveryError('invalid_or_expired_token'); }

      const [rows] = await pool.query(
        `SELECT ${ACCOUNT_COLUMNS} FROM user u INNER JOIN role r ON r.id = u.role_id WHERE u.id = ? LIMIT 1`,
        [Number(claims.sub)]);
      const user = rows[0];
      if (!eligible(user) || !sameDigest(claims.binding, binding(user))) throw new RecoveryError('invalid_or_expired_token');
      const passwordHash = await bcrypt.hash(password, 10);
      // The old password is a single-use compare-and-swap guard. BINARY matters:
      // account columns use a case/accent-insensitive collation, bcrypt does not.
      const [result] = await pool.query(
        `UPDATE user u INNER JOIN role r ON r.id = u.role_id SET u.password = ?, u.reset_password = 'N'
         WHERE u.id = ? AND BINARY u.password = BINARY ? AND BINARY u.email = BINARY ?
           AND u.enabled = 'Y' AND u.deleted = 'N' AND BINARY r.name = BINARY ?
           AND u.creation_date <=> ? AND u.reset_password <=> ?`,
        [passwordHash, user.id, user.password, user.email, user.role, user.creation_date, user.reset_password]);
      if (result.affectedRows !== 1) throw new RecoveryError('invalid_or_expired_token');
      return { status: 'password_changed', username: user.username };
    },

    // Test/operational shutdown hook; no email is sent by the health endpoints.
    async waitForIdle() { await Promise.all([...pending]); }
  };
}

module.exports = { createStaffPasswordRecovery, createRecoveryLimiter, RecoveryError };
