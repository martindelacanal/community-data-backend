'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Exercise the actual monolithic router's handler and authorization resolver
// without importing its live database, S3, Mailchimp or background services.
const source = fs.readFileSync(path.join(__dirname, 'user.js'), 'utf8');
function functionSource(name) {
  const expression = new RegExp(`(?:async )?function ${name}\\(`);
  const start = source.search(expression);
  assert.ok(start >= 0, `${name} exists`);
  const end = source.indexOf('\n}', start);
  return source.slice(start, end + 2);
}

function fixture({ onboarded = true, targetRole = 'beneficiary' } = {}) {
  const statements = [];
  let committed = false;
  let rolledBack = false;
  let handler;
  const connection = {
    beginTransaction: async () => {},
    commit: async () => { committed = true; },
    rollback: async () => { rolledBack = true; },
    release: () => {},
    query: async (sql, params = []) => {
      statements.push({ sql, params });
      if (/FOR UPDATE/.test(sql)) return [[{ id: params[0] }]];
      if (/FROM delivery_log|FROM client_location/.test(sql)) return [[]];
      if (/select q.id, q.answer_type_id/i.test(sql)) return [[{ id: params[0], answer_type_id: 1 }]];
      if (/insert into user_question\(/i.test(sql)) return [{ affectedRows: 1, insertId: 500 }];
      throw new Error(`Unexpected SQL in isolated route test: ${sql}`);
    },
  };
  const pool = {
    getConnection: async () => connection,
    query: async (sql, params) => {
      statements.push({ sql, params });
      if (/r.name = 'delivery'/.test(sql)) return [onboarded && params[1] === 7 ? [{ id: 42 }] : []];
      if (/r.name = 'beneficiary'/.test(sql)) return [targetRole === 'beneficiary' ? [{ id: 99 }] : []];
      throw new Error(`Unexpected authorization SQL: ${sql}`);
    },
  };
  const context = vm.createContext({
    router: { post: (_path, _auth, action) => { handler = action; } },
    verifyToken: () => {},
    mysqlConnection: { promise: () => pool },
    logger: { error: error => { throw error; } }, console,
    buildOnboardPendingQuestions: async (_user, _location, _genealogy, executor) => {
      assert.equal(executor, connection, 'pending state uses the locked transaction');
      assert.ok(statements.some(item => /FOR UPDATE/.test(item.sql)), 'locks beneficiary before checking pending state');
      return [{ id: 1, previously_answered: true }, { id: 2, previously_answered: false }];
    },
    getLatestUserQuestionRow: async () => null,
    getUserQuestionSnapshot: async () => null,
    insertBeneficiaryAnswerHistory: async () => {},
    clearUnreachableOnboardAnswers: async () => {},
    validateRequiredSurveyAnswersForUser: async () => null,
    BENEFICIARY_ANSWER_SOURCE_DEFAULT: 'beneficiary-onboard-ui',
    BENEFICIARY_ANSWER_SOURCE_AUTO_CLEAR: 'beneficiary-onboard-auto-clear',
  });
  const helpers = ['hasSurveyValue', 'hasMeaningfulSubmittedSurveyAnswer', 'parseSurveyPositiveInt',
    'parseSurveyNullablePositiveInt', 'parseSurveyBoolean', 'parseSurveyClientDate', 'parseSurveySource',
    'resolveOnboardSurveyUser'].map(functionSource).join('\n');
  const start = source.indexOf("router.post('/onBoard/answers'");
  const end = source.indexOf("router.post('/onBoard',", start);
  assert.ok(start > 0 && end > start);
  vm.runInContext(helpers + '\n' + source.slice(start, end), context);

  async function request({ role = 'delivery', questionId = 2, locationId = 7 } = {}) {
    const res = {
      statusCode: 200, payload: null,
      status(code) { this.statusCode = code; return this; },
      json(payload) { this.payload = payload; return this; },
    };
    await handler({
      data: { data: JSON.stringify({ id: 42, role }) },
      query: { beneficiary_id: '99', location_id: String(locationId) },
      body: [{ question_id: questionId, answer_type_id: 1, answer: 'Saved with assistance' }],
    }, res);
    return res;
  }
  return { request, statements, get committed() { return committed; }, get rolledBack() { return rolledBack; } };
}

test('onboarded delivery saves a pending answer for the selected beneficiary', async () => {
  const app = fixture();
  assert.equal((await app.request()).statusCode, 200);
  assert.equal(app.committed, true);
  const insert = app.statements.find(item => /insert into user_question\(/i.test(item.sql));
  assert.deepEqual(Array.from(insert.params), [99, 2, 1, 'Saved with assistance', null]);
});

test('delivery cannot overwrite a previously answered question or submit an unoffered one', async () => {
  for (const questionId of [1, 345]) {
    const app = fixture();
    assert.equal((await app.request({ questionId })).statusCode, 403);
    assert.equal(app.committed, false);
    assert.equal(app.rolledBack, true);
    assert.equal(app.statements.some(item => /insert into user_question\(|update user_question/i.test(item.sql)), false);
  }
});

test('unonboarded delivery and cross-location assistance are rejected before writes', async () => {
  const unonboarded = fixture({ onboarded: false });
  assert.equal((await unonboarded.request()).statusCode, 403);
  const elsewhere = fixture();
  assert.equal((await elsewhere.request({ locationId: 8 })).statusCode, 403);
  assert.equal(unonboarded.committed || elsewhere.committed, false);
});

test('delegation does not expose staff targets or allow another staff role', async () => {
  const staffTarget = fixture({ targetRole: 'admin' });
  assert.equal((await staffTarget.request()).statusCode, 404);
  const unauthorized = fixture();
  assert.equal((await unauthorized.request({ role: 'stocker' })).statusCode, 401);
});

test('beneficiary submissions always apply to self even if another beneficiary ID is supplied', async () => {
  const app = fixture();
  assert.equal((await app.request({ role: 'beneficiary' })).statusCode, 200);
  const insert = app.statements.find(item => /insert into user_question\(/i.test(item.sql));
  assert.equal(insert.params[0], 42);
});
