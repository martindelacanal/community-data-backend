'use strict';
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { createStaffPasswordRecovery, createRecoveryLimiter, RecoveryError } = require('../services/staffPasswordRecovery');

function createStaffPasswordRecoveryRouter({ pool, sendEmail, secret, logger = { error() {} }, service,
  clock = Date.now, perIp = 10, globalLimit = 100 } = {}) {
  const recovery = service || createStaffPasswordRecovery({ pool, sendEmail, secret, logger, clock });
  const limiter = createRecoveryLimiter({ clock });
  const router = express.Router();
  router.use((req, res, next) => {
    res.set({ 'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff' });
    next();
  });

  router.get('/open', (req, res) => {
    const html = fs.readFileSync(path.join(__dirname, '../public/password-reset.html'), 'utf8').replace(/\r\n?/g, '\n');
    const hashes = tag => [...html.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'g'))]
      .map(match => `'sha256-${crypto.createHash('sha256').update(match[1]).digest('base64')}'`).join(' ');
    res.set({ 'X-Frame-Options': 'DENY', 'Content-Security-Policy': `default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'; connect-src 'self'; script-src ${hashes('script')}; style-src ${hashes('style')}` });
    res.type('html').send(html);
  });

  router.use(express.json({ limit: '8kb' }));
  router.use((req, res, next) => {
    const ip = crypto.createHash('sha256').update(String(req.ip || req.socket.remoteAddress)).digest('hex');
    if (!limiter.take('global', globalLimit, 60000) || !limiter.take(ip, perIp, 15 * 60000)) {
      res.set('Retry-After', '900');
      return res.status(429).json({ error: 'rate_limited' });
    }
    if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)
      || Buffer.byteLength(JSON.stringify(req.body), 'utf8') > 8192) {
      return res.status(400).json({ error: 'invalid_request' });
    }
    next();
  });

  for (const operation of ['request', 'complete']) {
    router.post('/' + operation, async (req, res) => {
      try {
        const result = await recovery[operation](req.body);
        return res.status(operation === 'request' ? 202 : 200).json(result);
      } catch (error) {
        if (error instanceof RecoveryError) return res.status(error.status).json({ error: error.code });
        logger.error(`Staff password recovery failed: ${error.code || error.name || 'Error'}`);
        return res.status(503).json({ error: 'unavailable' });
      }
    });
  }
  return router;
}

module.exports = { createStaffPasswordRecoveryRouter };
