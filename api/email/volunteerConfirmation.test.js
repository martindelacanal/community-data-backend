'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { buildVolunteerConfirmationContent } = require('./email');

test('volunteer confirmation includes the signed waiver copy and event location', () => {
  const content = buildVolunteerConfirmationContent(
    'Community Clinic — Riverside',
    '07-30-2026',
    'en'
  );

  assert.equal(content.subject, 'Terms and conditions signed');
  assert.match(content.text, /Volunteer Liability Waiver, Terms and Conditions/);
  assert.match(content.text, /Community Clinic — Riverside/);
  assert.match(content.text, /07-30-2026/);
  assert.match(content.html, /Knowing and Voluntary Execution/);
});
