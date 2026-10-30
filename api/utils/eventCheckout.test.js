const test = require('node:test');
const assert = require('node:assert/strict');
const { checkoutValidation } = require('./eventCheckout');
const { validPersonName, normalizeName, validBirthDate } = require('./participantIdentity');
const { validPrivacyConsent } = require('../services/volunteerPrivacyConsent');
const questions = [
  { id: 1, question_type: 'single', required: 'Y', options: [{ id: 10 }, { id: 11 }] },
  { id: 2, question_type: 'text', required: 'Y', depends_on_question_id: 1, depends_on_option_id: 10 },
  { id: 3, question_type: 'multiple', required: 'Y', options: [{ id: 30 }, { id: 31, is_other: 'Y' }] },
  { id: 4, question_type: 'notice', required: 'Y' }
];
test('checkout enforces reachable required questions and valid options', () => {
  assert.deepEqual(checkoutValidation(questions, []).missing, [1, 3]);
  assert.deepEqual(checkoutValidation(questions, [{ question_id: 1, answer: 10 }, { question_id: 3, answer: [30] }]).missing, [2]);
  assert.deepEqual(checkoutValidation(questions, [{ question_id: 1, answer: 11 }, { question_id: 3, answer: [30] }]).missing, []);
  assert.deepEqual(checkoutValidation(questions, [{ question_id: 1, answer: 999 }, { question_id: 3, answer: [30] }]).missing, [1]);
  assert.deepEqual(checkoutValidation(questions, [{ question_id: 1, answer: 11 }, { question_id: 3, answer: [31] }]).missing, [3]);
  assert.deepEqual(checkoutValidation(questions, [{ question_id: 1, answer: 11 }, { question_id: 3, answer: [31], other_text: 'Other reason' }]).missing, []);
});
test('names preserve accents, apostrophes and hyphens but reject numbers in any script', () => {
  for (const name of ['María José', "O’Connor", 'Anne-Marie', '李']) assert.equal(validPersonName(name), true);
  for (const name of ['Irma123', 'Irma１２３', 'Irma١٢٣', '', '  ']) assert.equal(validPersonName(name), false);
  assert.equal(normalizeName('  MARÍA   José '), normalizeName('Maria Jose'));
  assert.equal(validBirthDate('2020-02-30'), false);
  assert.equal(validBirthDate('2020-02-29'), true);
});
test('the new agreement needs its own explicit, versioned signature', () => {
  const consent = { accepted: true, version: '2026-10-30', fullName: 'Alex Volunteer', role: 'volunteer', deviceModel: 'Organization device' };
  assert.equal(validPrivacyConsent(consent), true);
  for (const patch of [{ accepted: false }, { accepted: 'true' }, { version: 'old' }, { fullName: 'Alex123' }, { deviceModel: ' ' }, { role: 'admin' }]) {
    assert.equal(!!validPrivacyConsent({ ...consent, ...patch }), false);
  }
});
