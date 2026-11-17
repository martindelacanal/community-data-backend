'use strict';

const { createHash } = require('node:crypto');
const { readFileSync } = require('node:fs');
const path = require('node:path');

const page = readFileSync(path.join(__dirname, '../public/health-reservation.html'), 'utf8').replace(/\r\n?/g, '\n');
function contentHash(tag) {
  const source = page.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`))[1];
  return `'sha256-${createHash('sha256').update(source).digest('base64')}'`;
}
const policy = `default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'; connect-src 'self'; script-src ${contentHash('script')}; style-src ${contentHash('style')}`;

function showReservationPage(req, res) {
  res.set({
    'Cache-Control': 'private, no-store',
    'Content-Security-Policy': policy,
    'Referrer-Policy': 'no-referrer',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY'
  }).type('html').send(page);
}

module.exports = { showReservationPage };
