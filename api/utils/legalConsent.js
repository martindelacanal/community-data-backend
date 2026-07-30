'use strict';

const LEGAL_CONSENT_VERSION = '2026-03-02';

/**
 * Consent sent by a public form must be the JSON boolean true. Truthy values
 * such as "true" or 1 are intentionally rejected so API callers cannot bypass
 * the same required-true check enforced by the Angular forms.
 */
function isLegalConsentAccepted(value) {
  return value === true;
}

module.exports = {
  LEGAL_CONSENT_VERSION,
  isLegalConsentAccepted
};
