'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { selectPendingSurveyQuestions } = require('./pendingSurveyQuestions');

const question = (id, extra = {}) => ({ id, required: 'Y', answer_type_id: 3, ...extra });

test('label, order, option and location edits do not erase a saved answer', () => {
  const questions = [question(1, { name_en: 'Edited wording', order: 9, available_since: '2099-01-01', answers: [{ id: 2 }, { id: 3 }] })];
  assert.deepEqual(selectPendingSurveyQuestions(questions, new Map([[1, 2]])), []);
});

test('a new required child uses saved parent context without asking the parent again', () => {
  const questions = [question(1), question(2, { depends_on_question_id: 1, depends_on_answer_id: 10 })];
  const result = selectPendingSurveyQuestions(questions, new Map([[1, 10]]));
  assert.equal(result.length, 2);
  assert.equal(result[0].previously_answered, true);
  assert.equal(result[0].requires_reanswer, false);
  assert.equal(result[0].stored_answer, 10);
  assert.equal(result[1].previously_answered, false);
});

test('saved parent answers keep unrelated conditional branches out of the queue', () => {
  const questions = [question(1), question(2, { depends_on_question_id: 1, depends_on_answer_id: 10 }),
    question(3, { depends_on_question_id: 2, depends_on_answer_id: 20 })];
  assert.deepEqual(selectPendingSurveyQuestions(questions, new Map([[1, 11]])), []);
});

test('unanswered required parents retain their possible children in one form', () => {
  const questions = [question(1), question(2, { depends_on_question_id: 1, depends_on_answer_id: 10 })];
  assert.deepEqual(selectPendingSurveyQuestions(questions, new Map()).map(item => item.id), [1, 2]);
});

test('skipping optional registration questions does not prompt them again at the QR gate', () => {
  const questions = [question(1, { required: 'N' }), question(2, { depends_on_question_id: 1, depends_on_answer_id: 10 })];
  assert.deepEqual(selectPendingSurveyQuestions(questions, new Map()), []);
  assert.equal(selectPendingSurveyQuestions(questions, new Map([[1, 10]]))[1].id, 2);
});

test('multi-choice saved parents unlock only selected branches and cycles do not loop', () => {
  const questions = [question(1, { answer_type_id: 4 }), question(2, { depends_on_question_id: 1, depends_on_answer_id: 10 }),
    question(3, { depends_on_question_id: 1, depends_on_answer_id: 11 }),
    question(4, { depends_on_question_id: 5, depends_on_answer_id: 40 }), question(5, { depends_on_question_id: 4, depends_on_answer_id: 50 })];
  assert.deepEqual(selectPendingSurveyQuestions(questions, new Map([[1, [10]]])).map(item => item.id), [1, 2]);
});
