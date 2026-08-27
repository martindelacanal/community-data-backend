const express = require('express');
const crypto = require('node:crypto');
const jwt = require('jsonwebtoken');
const { getRestoreCredentialsConfig } = require('../services/restoreCredentialsConfig');
const { createRestoreCredentialsRepository } = require('../services/restoreCredentialsRepository');
const { createRestoreCredentialsService, RestoreCredentialError } = require('../services/restoreCredentials');

const MAX_BODY_BYTES = 32 * 1024;

function createRateLimiter({ clock = Date.now, maxEntries = 10000, perIp = 60, globalLimit = 1200 } = {}) {
  const entries = new Map();
  const windowMs = 60000;
  function take(key, limit, duration = windowMs) {
    const now = clock();
    let entry = entries.get(key);
    if (entry && entry.until <= now) {
      entries.delete(key);
      entry = null;
    }
    if (!entry) {
      if (entries.size >= maxEntries) {
        for (const [candidate, value] of entries) {
          if (value.until <= now) entries.delete(candidate);
        }
        if (entries.size >= maxEntries) return false;
      }
      entry = { count: 0, until: now + duration };
      entries.set(key, entry);
    }
    if (entry.count >= limit) return false;
    entry.count += 1;
    return true;
  }
  return {
    request(ip) {
      const key = crypto.createHash('sha256').update(String(ip)).digest('hex');
      return take('global', globalLimit) && take('ip:' + key, perIp);
    },
    registration(userId) {
      return take('user:' + userId, 10, 10 * windowMs);
    },
  };
}

function createRestoreCredentialsRouter({ pool, env = process.env, service, logger, limiterOptions } = {}) {
  const config = getRestoreCredentialsConfig(env);
  const restoreService = service || createRestoreCredentialsService({
    repository: createRestoreCredentialsRepository(pool), config,
  });
  const limiter = createRateLimiter(limiterOptions);
  const router = express.Router();

  function rateLimit(res) {
    res.set('Retry-After', '60');
    return res.status(429).json({ error: 'restore_rate_limited' });
  }

  router.use((req, res, next) => {
    res.set('Cache-Control', 'no-store');
    res.set('Pragma', 'no-cache');
    if (!limiter.request(req.ip || req.socket.remoteAddress)) return rateLimit(res);
    next();
  });
  router.use(express.json({ limit: MAX_BODY_BYTES, strict: true }));
  router.use((req, res, next) => {
    // app.js already parses JSON globally. Recheck that parsed body too, so its
    // larger default limit cannot bypass this feature's request-size limit.
    if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
      return res.status(400).json({ error: 'invalid_restore_request' });
    }
    if (Buffer.byteLength(JSON.stringify(req.body), 'utf8') > MAX_BODY_BYTES) {
      return res.status(413).json({ error: 'invalid_restore_request' });
    }
    next();
  });

  function authenticatedUser(req, res, next) {
    try {
      const authorization = req.headers.authorization;
      if (typeof authorization !== 'string' || !authorization.startsWith('Bearer ')
          || authorization.length > 8192 || !config.jwtSecret) throw new Error('Unauthorized');
      const auth = jwt.verify(authorization.slice(7), config.jwtSecret, { algorithms: ['HS256'] });
      const user = typeof auth.data === 'string' ? JSON.parse(auth.data) : null;
      const userId = Number(user?.id);
      if (!Number.isSafeInteger(userId) || userId <= 0) throw new Error('Unauthorized');
      req.restoreUserId = userId;
      req.restoreAuthBinding = auth.restore_auth_binding;
      next();
    } catch {
      return res.status(401).json({ error: 'restore_authentication_required' });
    }
  }

  function handle(operation) {
    return async (req, res) => {
      try {
        res.status(200).json(await operation(req, res));
      } catch (error) {
        if (error instanceof RestoreCredentialError) {
          return res.status(error.status).json({ error: error.code });
        }
        // Do not log WebAuthn responses, JWTs, DB bindings or SQL parameters.
        logger?.error('Android Restore request failed', {
          code: /^[A-Z0-9_]{1,64}$/.test(error.code || '') ? error.code : 'RESTORE_BACKEND_ERROR',
        });
        return res.status(503).json({ error: 'restore_unavailable' });
      }
    };
  }

  router.post('/registration/options', authenticatedUser, (req, res, next) => {
    if (!limiter.registration(req.restoreUserId)) return rateLimit(res);
    next();
  }, handle((req) => restoreService.registrationOptions(req.restoreUserId, req.restoreAuthBinding)));
  router.post('/registration/verify', authenticatedUser,
    handle((req) => restoreService.registrationVerify(req.restoreUserId, req.body, req.restoreAuthBinding)));
  router.post('/registration/cancel', handle((req) => restoreService.cancelRegistration(req.body)));
  router.post('/authentication/options', handle(() => restoreService.authenticationOptions()));
  router.post('/authentication/verify', handle((req) => restoreService.authenticationVerify(req.body)));
  router.post('/revoke', handle((req) => restoreService.revoke(req.body)));
  router.use((error, req, res, next) => {
    if (error.type === 'entity.too.large') {
      return res.status(413).json({ error: 'invalid_restore_request' });
    }
    if (error instanceof SyntaxError) {
      return res.status(400).json({ error: 'invalid_restore_request' });
    }
    return res.status(503).json({ error: 'restore_unavailable' });
  });
  return router;
}

module.exports = { createRestoreCredentialsRouter, createRateLimiter };
