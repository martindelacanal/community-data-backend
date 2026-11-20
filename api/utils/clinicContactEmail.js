'use strict';

const { Resolver } = require('node:dns/promises');
const { isIP } = require('node:net');
const { normalizeParticipantEmail } = require('./participantEmailAddress');

const cachedDomains = new Map();
const pendingDomains = new Map();
const cacheLifetime = 5 * 60 * 1000;

// Check only the domain. A temporary DNS failure cannot establish that an
// address is invalid. Never probe the participant's mailbox.
async function lookupMailDomain(domain) {
  const resolver = new Resolver({ timeout: 750, tries: 1 });
  const deadline = setTimeout(() => resolver.cancel(), 1500);
  try {
    let records;
    try {
      records = await resolver.resolveMx(domain);
    } catch (error) {
      if (error.code === 'ENOTFOUND') return false;
      if (error.code !== 'ENODATA') return null;
      records = [];
    }
    if (records.length) {
      // Node may return the root label of a null MX as an empty string.
      return records.some(record => record.exchange && record.exchange !== '.');
    }
    // SMTP uses the domain itself when it has no MX (RFC 5321 section 5.1).
    const addresses = await Promise.allSettled([resolver.resolve4(domain), resolver.resolve6(domain)]);
    if (addresses.some(result => result.status === 'fulfilled' && result.value.length > 0)) return true;
    if (addresses.some(result => result.status === 'rejected' && !['ENOTFOUND', 'ENODATA'].includes(result.reason.code))) return null;
    return false;
  } finally {
    clearTimeout(deadline);
  }
}

async function domainAcceptsMail(domain) {
  const cached = cachedDomains.get(domain);
  if (cached && cached.expires > Date.now()) return cached.result;
  if (cached) cachedDomains.delete(domain);
  if (pendingDomains.has(domain)) return pendingDomains.get(domain);
  if (pendingDomains.size >= 32) return null;
  const lookup = lookupMailDomain(domain).catch(() => null);
  pendingDomains.set(domain, lookup);
  try {
    const result = await lookup;
    if (result !== null) {
      if (cachedDomains.size >= 512) cachedDomains.delete(cachedDomains.keys().next().value);
      cachedDomains.set(domain, { result, expires: Date.now() + cacheLifetime });
    }
    return result;
  } finally {
    pendingDomains.delete(domain);
  }
}

async function validateClinicContactEmail(value) {
  if (value == null || (typeof value === 'string' && !value.trim())) return { email: null };
  const email = normalizeParticipantEmail(value);
  if (!email) return { error: 'INVALID_EMAIL' };
  const domain = email.slice(email.lastIndexOf('@') + 1);
  if (isIP(domain)) return { error: 'INVALID_EMAIL' };
  if (await domainAcceptsMail(domain) === false) return { error: 'EMAIL_DOMAIN_UNDELIVERABLE' };
  return { email };
}

module.exports = { validateClinicContactEmail };
