'use strict';

// A campaign uses one SMTP envelope per address. Nodemailer can wrap the
// recipient's actual SMTP reply in an EENVELOPE error.
function classifyMailFailure(error) {
  const detail = error?.rejectedErrors?.length === 1 ? error.rejectedErrors[0] : error;
  const code = Number(detail?.responseCode);
  const response = String(detail?.response || '');
  const command = String(detail?.command || '').toUpperCase();

  if (code === 550 && (/\b5\.4\.5\b/.test(response) ||
      (/\b5\.7\.1\b/.test(response) && /\bdaily\b.*\b(?:sending|relay)\b.*\b(?:limit|quota)\b/i.test(response)))) {
    return 'daily_sender_limit';
  }
  if (code >= 400 && code < 500) {
    if (/\b(?:smtp )?relay limit\b/i.test(response) || /\b4\.7\.28\b/.test(response)) return 'temporary_sender';
    // A recipient's full inbox or per-mailbox rate limit affects only that address.
    if (/\b4\.2\.[012]\b/.test(response) || (command === 'RCPT TO' && code !== 421)) return 'temporary_recipient';
    return 'temporary_sender';
  }
  if (detail?.code === 'EAUTH' || detail?.code === 'EDNS') return 'temporary_sender';
  if (code >= 500 && code < 600) {
    if (/\b5\.1\.[1236]\b/.test(response) && (command === 'RCPT TO' || command === 'DATA')) return 'invalid_recipient';
    if (command === 'RCPT TO' && /\b(?:no such (?:user|mailbox|recipient)|unknown (?:mailbox|recipient)|(?:user|mailbox|recipient) (?:unknown|not found|does not exist))\b/i.test(response)) return 'invalid_recipient';
    return 'rejected';
  }
  if (detail?.code === 'EENVELOPE') return 'rejected';
  // A connection failure before the envelope was submitted is safe to retry.
  if (['EHLO', 'HELO', 'STARTTLS', 'AUTH', 'MAIL FROM', 'RCPT TO'].includes(command)) return 'temporary_sender';
  if (command === 'CONN' && (detail.code === 'ECONNREFUSED' || detail.originalCode === 'ECONNREFUSED' ||
      ['CONNECT_TIMEOUT', 'GREETING_TIMEOUT', 'UPGRADE_TIMEOUT'].includes(detail.timeoutType))) return 'temporary_sender';
  // A disconnect while waiting for acknowledgement of DATA may have delivered the message.
  return 'uncertain';
}

module.exports = { classifyMailFailure };
