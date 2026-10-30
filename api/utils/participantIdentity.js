'use strict';
const crypto = require('node:crypto');

function validPersonName(value) {
  return typeof value === 'string' && value.trim().length > 0 && value.trim().length <= 100 && !/\p{N}/u.test(value);
}
function normalizeName(value) {
  return String(value || '').normalize('NFKD').replace(/\p{M}/gu, '').trim().replace(/\s+/g, ' ').toLowerCase();
}
function validBirthDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T12:00:00Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value && value <= new Date().toISOString().slice(0, 10) && Number(value.slice(0, 4)) >= new Date().getUTCFullYear() - 120;
}
// Serialize registrations of the same identity, including requests with different contacts.
async function lockParticipantIdentity(connection, account) {
  const key = 'participant:' + crypto.createHash('sha256').update([
    normalizeName(account.firstName), normalizeName(account.lastName), account.dateOfBirth
  ].join('|')).digest('hex').slice(0, 48);
  const [[row]] = await connection.query('SELECT GET_LOCK(?, 10) AS acquired', [key]);
  if (Number(row.acquired) !== 1) throw Object.assign(new Error('Registration busy'), { code: 'REGISTRATION_BUSY' });
  return key;
}
async function findDuplicateParticipant(connection, account) {
  const [rows] = await connection.query(
    `SELECT firstname, lastname FROM user WHERE date_of_birth = ? AND role_id = 5 AND deleted = 'N'`,
    [account.dateOfBirth]);
  return rows.some(row => normalizeName(row.firstname) === normalizeName(account.firstName)
    && normalizeName(row.lastname) === normalizeName(account.lastName));
}
module.exports = { validPersonName, normalizeName, validBirthDate, lockParticipantIdentity, findDuplicateParticipant };
