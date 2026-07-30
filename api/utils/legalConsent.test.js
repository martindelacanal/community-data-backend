'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  LEGAL_CONSENT_VERSION,
  isLegalConsentAccepted
} = require('./legalConsent');

test('volunteer legal consent accepts only the JSON boolean true', () => {
  assert.equal(isLegalConsentAccepted(true), true);

  for (const value of [false, 'true', 1, null, undefined, {}]) {
    assert.equal(isLegalConsentAccepted(value), false);
  }
});

test('legal consent version matches the waiver shown by the registration forms', () => {
  assert.equal(LEGAL_CONSENT_VERSION, '2026-03-02');
});
