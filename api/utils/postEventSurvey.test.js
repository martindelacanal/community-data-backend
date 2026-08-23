'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  answersUseOnlyAllowedQuestions,
  canAccessBeneficiaryAnswers,
  isPostEventSurveyOpen,
  pendingRequiredState,
  shouldIncludeBeneficiaryEvent
} = require('./postEventSurvey');

test('ended events expose answers only for an explicitly reopened registration', () => {
  assert.equal(canAccessBeneficiaryAnswers(true, { post_event_survey_open: 'N' }), false);
  assert.equal(canAccessBeneficiaryAnswers(true, { post_event_survey_open: 'Y' }), true);
  assert.equal(canAccessBeneficiaryAnswers(false, { post_event_survey_open: 'N' }), true);
  assert.equal(isPostEventSurveyOpen(null), false);
});

test('ended event card requires the reopen flag and at least one pending question', () => {
  const base = {
    eventEnded: true,
    registrationOpen: false,
    registered: true,
    postEventSurveyOpen: 'Y'
  };
  assert.equal(shouldIncludeBeneficiaryEvent({ ...base, pendingRequiredQuestions: 1 }), true);
  assert.equal(shouldIncludeBeneficiaryEvent({ ...base, pendingRequiredQuestions: 0 }), false);
  assert.equal(shouldIncludeBeneficiaryEvent({
    ...base,
    postEventSurveyOpen: 'N',
    pendingRequiredQuestions: 4
  }), false);
  assert.equal(shouldIncludeBeneficiaryEvent({
    ...base,
    registered: false,
    pendingRequiredQuestions: 4
  }), false);
});

test('active registered events retain their previous visibility behavior', () => {
  assert.equal(shouldIncludeBeneficiaryEvent({
    eventEnded: false,
    registrationOpen: false,
    registered: true,
    postEventSurveyOpen: 'N',
    pendingRequiredQuestions: 0
  }), true);
  assert.equal(shouldIncludeBeneficiaryEvent({
    eventEnded: false,
    registrationOpen: true,
    registered: false,
    postEventSurveyOpen: 'N',
    pendingRequiredQuestions: 0
  }), true);
});

test('post-event submissions reject questions outside the pending survey forms', () => {
  const allowed = new Map([[30, {}], [31, {}], [67, {}]]);
  assert.equal(answersUseOnlyAllowedQuestions([
    { question_id: 30, answer: [100] },
    { question_id: '67', answer: [200] }
  ], allowed), true);
  assert.equal(answersUseOnlyAllowedQuestions([{ question_id: 7, answer: 1 }], allowed), false);
  assert.equal(answersUseOnlyAllowedQuestions([{ question_id: 'not-a-number', answer: 1 }], allowed), false);
});

test('only forms with an unanswered visible required question are reopened', () => {
  const registrationForm = {
    id: 4,
    questions: [
      { id: 11, question_type: 'text', required: 'Y' },
      { id: 12, question_type: 'text', required: 'N' }
    ]
  };
  const surveyForm = {
    id: 6,
    questions: [
      { id: 30, question_type: 'multiple', required: 'Y' },
      { id: 31, question_type: 'single', required: 'Y' },
      { id: 32, question_type: 'notice', required: 'N' }
    ]
  };
  const result = pendingRequiredState(
    [registrationForm, surveyForm],
    new Set([12, 30, 31, 32]),
    new Set([31])
  );
  assert.deepEqual(result.pendingForms.map(form => form.id), [6]);
  assert.deepEqual(result.pendingQuestions.map(question => question.id), [30]);
});
