const test = require('node:test');
const assert = require('node:assert/strict');
const {
  formatDailyQrDate,
  isCurrentDailyBeneficiaryQr,
  parseDailyBeneficiaryQr
} = require('./dailyBeneficiaryQr');

const LA = 'America/Los_Angeles';
const NOW = new Date('2026-08-01T02:30:00.000Z'); // July 31 in Los Angeles.

test('parses and validates the compact daily token', () => {
  const qr = parseDailyBeneficiaryQr('B12345.20260731');

  assert.equal(qr.id, 12345);
  assert.equal(qr.role, 'beneficiary');
  assert.equal(qr.date, '20260731');
  assert.equal(isCurrentDailyBeneficiaryQr(qr, LA, NOW), true);
});

test('rejects a compact QR captured on a previous day', () => {
  const qr = parseDailyBeneficiaryQr('B12345.20260730');

  assert.equal(isCurrentDailyBeneficiaryQr(qr, LA, NOW), false);
});

test('accepts same-day legacy JSON and rejects an old screenshot', () => {
  const current = parseDailyBeneficiaryQr(JSON.stringify({
    id: '12345', role: 'beneficiary', date: '7/31/2026, 10:00:00 AM', approved: 'N'
  }));
  const stale = parseDailyBeneficiaryQr(JSON.stringify({
    id: '12345', role: 'beneficiary', date: '7/30/2026, 10:00:00 AM', approved: 'N'
  }));

  assert.equal(isCurrentDailyBeneficiaryQr(current, LA, NOW), true);
  assert.equal(current.approved, 'N');
  assert.equal(isCurrentDailyBeneficiaryQr(stale, LA, NOW), false);
});

test('accepts the day-first locale format emitted by legacy Spanish devices', () => {
  const qr = parseDailyBeneficiaryQr(JSON.stringify({
    id: 12345, role: 'beneficiary', date: '31/7/2026, 10:00:00', approved: 'N'
  }));

  assert.equal(isCurrentDailyBeneficiaryQr(qr, LA, NOW), true);
});

test('evaluates ISO timestamps in the business timezone', () => {
  const qr = parseDailyBeneficiaryQr({
    id: 12345, role: 'beneficiary', date: '2026-08-01T02:00:00.000Z', approved: 'N'
  });

  assert.equal(formatDailyQrDate(NOW, LA), '20260731');
  assert.equal(isCurrentDailyBeneficiaryQr(qr, LA, NOW), true);
});

test('rejects legacy numeric or undated payloads', () => {
  assert.equal(parseDailyBeneficiaryQr('12345'), null);
  assert.equal(parseDailyBeneficiaryQr({ id: 12345, role: 'beneficiary' }), null);
});
