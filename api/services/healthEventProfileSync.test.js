'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  PROFILE_MAPS_TO,
  isProfileMap,
  isCatalogProfileMap,
  catalogTableFor,
  hasProfileMappings,
  computeProfileUpdates,
  applyProfileAnswers
} = require('./healthEventProfileSync');

const genderQuestion = {
  id: 14, maps_to: 'profile_gender', question_type: 'single',
  options: [
    { id: 28, name_en: 'Female', is_other: 'N', profile_option_id: 2 },
    { id: 29, name_en: 'Male', is_other: 'N', profile_option_id: 3 },
    { id: 32, name_en: 'Non-binary', is_other: 'N', profile_option_id: null }
  ]
};
const ethnicityQuestion = {
  id: 15, maps_to: 'profile_ethnicity', question_type: 'single',
  options: [
    { id: 36, name_en: 'Hispanic/Latino', is_other: 'N', profile_option_id: 2 },
    { id: 44, name_en: 'Other', is_other: 'Y', profile_option_id: 1 }
  ]
};
const zipQuestion = { id: 17, maps_to: 'profile_zipcode', question_type: 'single', options: [{ id: 50, name_en: '92220', is_other: 'N' }] };
const plainQuestion = { id: 9, maps_to: null, question_type: 'single', options: [{ id: 1, name_en: 'Yes', is_other: 'N' }] };
const emptyProfile = { gender_id: null, ethnicity_id: null, second_ethnicity_id: null, other_ethnicity: null, other_second_ethnicity: null, language_id: null, other_language: null, zipcode: null };

test('mapping helpers know the profile targets', () => {
  assert.deepEqual([...PROFILE_MAPS_TO], ['profile_gender', 'profile_ethnicity', 'profile_second_ethnicity', 'profile_language', 'profile_zipcode']);
  assert.equal(isProfileMap('profile_gender'), true);
  assert.equal(isProfileMap('attend_date'), false);
  assert.equal(isCatalogProfileMap('profile_zipcode'), false);
  assert.equal(catalogTableFor('profile_second_ethnicity'), 'ethnicity');
  assert.equal(catalogTableFor('profile_language'), 'language');
  assert.equal(catalogTableFor('attend_date'), null);
  assert.equal(hasProfileMappings([plainQuestion]), false);
  assert.equal(hasProfileMappings([plainQuestion, zipQuestion]), true);
});

test('copies mapped catalogue answers and zip code into empty profile fields', () => {
  const { updates, applied } = computeProfileUpdates({
    questions: [genderQuestion, ethnicityQuestion, zipQuestion, plainQuestion],
    answers: [
      { question_id: 14, answer: '28' },
      { question_id: 15, answer: 36 },
      { question_id: 17, answer: '50' },
      { question_id: 9, answer: 1 }
    ],
    profile: emptyProfile
  });
  assert.deepEqual(updates, { gender_id: 2, ethnicity_id: 2, zipcode: '92220' });
  assert.equal(applied.length, 3);
});

test('never overwrites a profile value that already exists', () => {
  const { updates } = computeProfileUpdates({
    questions: [genderQuestion, ethnicityQuestion],
    answers: [{ question_id: 14, answer: 29 }, { question_id: 15, answer: 36 }],
    profile: { ...emptyProfile, gender_id: 2 }
  });
  assert.deepEqual(updates, { ethnicity_id: 2 });
});

test('unmapped options and unknown answers are ignored', () => {
  const { updates } = computeProfileUpdates({
    questions: [genderQuestion],
    answers: [{ question_id: 14, answer: 32 }, { question_id: 14, answer: 999 }, { question_id: 77, answer: 1 }],
    profile: emptyProfile
  });
  assert.deepEqual(updates, {});
});

test('"other" options carry their free text into the other_* column', () => {
  const { updates } = computeProfileUpdates({
    questions: [ethnicityQuestion],
    answers: [{ question_id: 15, answer: 44, other_text: '  Middle Eastern / North African, from a very long description that exceeds the column  ' }],
    profile: emptyProfile
  });
  assert.equal(updates.ethnicity_id, 1);
  assert.equal(updates.other_ethnicity.length, 45);
  assert.equal(updates.other_ethnicity.startsWith('Middle Eastern'), true);
});

test('multiple-choice answers use the first mapped option; text zip codes are trimmed', () => {
  const multi = { ...ethnicityQuestion, question_type: 'multiple' };
  const zipText = { id: 18, maps_to: 'profile_zipcode', question_type: 'text', options: [] };
  const { updates } = computeProfileUpdates({
    questions: [multi, zipText],
    answers: [{ question_id: 15, answer: [44, 36] }, { question_id: 18, answer: '  92223 ' }],
    profile: emptyProfile
  });
  assert.deepEqual(updates, { ethnicity_id: 1, zipcode: '92223' });
});

test('applyProfileAnswers locks the user, validates catalogue ids and updates only what changed', async () => {
  const calls = [];
  const connection = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (/FROM user WHERE id = \?/.test(sql)) return [[{ ...emptyProfile }]];
      if (/FROM gender WHERE id = \?/.test(sql)) return [[{ id: params[0] }]];
      if (/FROM ethnicity WHERE id = \?/.test(sql)) return [[]]; // catalogue row disabled
      if (/^UPDATE user SET/.test(sql)) return [{ affectedRows: 1 }];
      throw new Error(`unexpected query ${sql}`);
    }
  };
  const questionsById = new Map([[14, genderQuestion], [15, ethnicityQuestion]]);
  const result = await applyProfileAnswers(connection, 42, questionsById, [
    { question_id: 14, answer: 28 }, { question_id: 15, answer: 44, other_text: 'Berber' }
  ]);
  assert.deepEqual(result.updates, { gender_id: 2 });
  assert.match(calls[0].sql, /FOR UPDATE/);
  const update = calls.find(call => /^UPDATE user SET/.test(call.sql));
  assert.equal(update.sql, "UPDATE user SET gender_id = ? WHERE id = ? AND deleted = 'N'");
  assert.deepEqual(update.params, [2, 42]);
});

test('applyProfileAnswers is a no-op without mapped questions', async () => {
  const connection = { async query() { throw new Error('must not query'); } };
  const result = await applyProfileAnswers(connection, 42, new Map([[9, plainQuestion]]), [{ question_id: 9, answer: 1 }]);
  assert.equal(result, null);
});
