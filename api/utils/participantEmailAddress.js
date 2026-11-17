'use strict';

const { domainToASCII } = require('node:url');

// The address becomes a single SMTP envelope recipient. Ignore display names,
// groups and address lists, including signed-in and imported legacy values.
function normalizeParticipantEmail(value) {
  if (typeof value !== 'string' || /[\x00-\x1f\x7f]/.test(value)) return null;
  const address = value.trim();
  const parts = address.split('@');
  if (parts.length !== 2 || !parts[1] || parts[0].length > 64) return null;
  const [local, rawDomain] = parts;
  if (!/^[A-Z0-9!#$%&'*+/=?^_\x60{|}~-]+(?:\.[A-Z0-9!#$%&'*+/=?^_\x60{|}~-]+)*$/i.test(local)) return null;
  if (!/^[\p{L}\p{M}\p{N}.-]+$/u.test(rawDomain)) return null;
  const domain = domainToASCII(rawDomain);
  if (!/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(domain)) return null;
  const normalized = local + '@' + domain;
  return normalized.length <= 254 ? normalized : null;
}

module.exports = { normalizeParticipantEmail };
