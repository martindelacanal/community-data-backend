'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { parseLocationQuestionIds, replaceLocationSurveyQuestions } = require('../services/locationSurveyQuestions');

const source = fs.readFileSync(path.join(__dirname, 'user.js'), 'utf8').replace(/\r\n/g, '\n');
const location = { organization: 'Test', community_city: 'Test', address: '1 Test St', coordinates: '33, -117', client_ids: [3] };
const questionRows = [
  { id: 1, enabled: 'Y', depends_on_question_id: null },
  { id: 2, enabled: 'Y', depends_on_question_id: 1 },
  { id: 3, enabled: 'Y', depends_on_question_id: 2 },
  { id: 4, enabled: 'N', depends_on_question_id: null },
];
function fixture({ failAssignment = false, exists = true, questions = questionRows } = {}) {
  const handlers = new Map();
  const events = [], statements = [];
  const connection = {
    async getConnection() { return connection; },
    async beginTransaction() { events.push('begin'); },
    async commit() { events.push('commit'); },
    async rollback() { events.push('rollback'); },
    release() { events.push('release'); },
    async query(sql, params = []) {
      statements.push({ sql, params: Array.from(params) });
      if (/SELECT id FROM location/.test(sql)) return [exists ? [{ id: 7 }] : []];
      if (/SELECT id, enabled, depends_on_question_id FROM question/.test(sql)) return [questions];
      if (/SELECT question_id FROM question_location/.test(sql)) return [[{ question_id: 1 }, { question_id: 4 }]];
      if (/SELECT q.id, q.name/.test(sql)) return [questions.filter(question => question.enabled === 'Y')];
      if (failAssignment && /INSERT INTO question_location/.test(sql)) throw new Error('Synthetic assignment failure');
      if (/INSERT INTO location/.test(sql)) return [{ insertId: 7, affectedRows: 1 }];
      return [{ affectedRows: 1 }];
    }
  };
  const router = Object.fromEntries(['get', 'post', 'put'].map(method => [method, (route, _auth, handler) => handlers.set(`${method} ${route}`, handler)]));
  const routes = [['post', '/new/location'], ['put', '/new/location/:id'], ['get', '/survey/location-questions'], ['put', '/survey/location/:id/questions']];
  for (const [method, route] of routes) {
    const start = source.indexOf(`router.${method}('${route}',`);
    assert.ok(start >= 0);
    const end = source.indexOf('\n});', start) + 4;
    vm.runInNewContext(source.slice(start, end), {
      router, verifyToken() {}, mysqlConnection: { promise: () => connection },
      parseLocationQuestionIds, replaceLocationSurveyQuestions,
      lockVolunteerNotificationRecipientSettings: async () => {},
      addVolunteerNotificationRecipientsForLocation: async () => {},
      console: { log() {} }, logger: { error() {} },
    });
  }
  async function request(method, route, { role = 'admin', body = {}, id = '7', query = {} } = {}) {
    const response = { statusCode: 200, body: null, status(code) { this.statusCode = code; return this; }, json(value) { this.body = value; return this; } };
    await handlers.get(`${method} ${route}`)({ data: { data: JSON.stringify({ role }) }, body, params: { id }, query }, response);
    return response;
  }
  return { request, statements, events, connection };
}

test('omitted questions preserve optional existing location workflows; non-admin assignments fail before writes', async () => {
  assert.equal(parseLocationQuestionIds({}, 'opsmanager'), undefined);
  const f = fixture();
  const denied = await f.request('post', '/new/location', { role: 'opsmanager', body: { ...location, question_ids: [1] } });
  assert.equal(denied.statusCode, 403);
  assert.equal(f.statements.length, 0);
  const allowed = await f.request('post', '/new/location', { role: 'opsmanager', body: location });
  assert.equal(allowed.statusCode, 200);
  assert.ok(!f.statements.some(item => /question/.test(item.sql)));
});

test('creation saves selected questions and their parents in the location transaction', async () => {
  const f = fixture();
  const response = await f.request('post', '/new/location', { body: { ...location, question_ids: [3, 3] } });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body.question_ids, [1, 2, 3]);
  assert.deepEqual(f.events, ['begin', 'commit', 'release']);
  assert.deepEqual(f.statements.filter(item => /INSERT INTO question_location/.test(item.sql)).map(item => item.params), [[2, 7], [3, 7]]);
});

test('a question persistence failure rolls back the entire location create and edit', async () => {
  for (const method of ['post', 'put']) {
    const f = fixture({ failAssignment: true });
    const response = await f.request(method, method === 'post' ? '/new/location' : '/new/location/:id', { body: { ...location, question_ids: [2] } });
    assert.equal(response.statusCode, 500);
    assert.deepEqual(f.events, ['begin', 'rollback', 'release']);
  }
});

test('editing can clear active assignments without deleting history or other locations', async () => {
  const f = fixture();
  const response = await f.request('put', '/survey/location/:id/questions', { body: { question_ids: [] } });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body.question_ids, []);
  const clear = f.statements.find(item => /UPDATE question_location ql/.test(item.sql));
  assert.match(clear.sql, /ql.location_id = \? AND q.enabled = 'Y'/);
  assert.deepEqual(clear.params, [7]);
  assert.ok(!f.statements.some(item => /DELETE FROM|UPDATE location/.test(item.sql)));
  const optional = f.statements.find(item => /UPDATE question q SET/.test(item.sql));
  assert.deepEqual(optional.params, [1]);
  assert.match(optional.sql, /NOT EXISTS/);
});

test('missing locations, unavailable questions, dependency cycles and malformed IDs never commit', async () => {
  for (const setup of [
    { options: { exists: false }, ids: [1], status: 404 },
    { ids: [99], status: 400 }, { ids: [4], status: 400 }, { ids: [true], status: 400 },
    { ids: null, status: 400 }, { ids: [-1], status: 400 },
    { options: { questions: [{ id: 1, enabled: 'Y', depends_on_question_id: 2 }, { id: 2, enabled: 'Y', depends_on_question_id: 1 }] }, ids: [1], status: 400 }
  ]) {
    const f = fixture(setup.options);
    const response = await f.request('put', '/survey/location/:id/questions', { body: { question_ids: setup.ids } });
    assert.equal(response.statusCode, setup.status);
    assert.ok(!f.events.includes('commit'));
  }
});

test('removing a parent also disables hidden descendants at this location to preserve survey dependency validity', async () => {
  const f = fixture({ questions: questionRows.map(question => question.id === 4 ? { ...question, depends_on_question_id: 1 } : question) });
  await f.request('put', '/survey/location/:id/questions', { body: { question_ids: [] } });
  const clearHidden = f.statements.find(item => /UPDATE question_location SET enabled = 'N'/.test(item.sql));
  assert.deepEqual(clearHidden.params, [7, 4]);
});

test('only survey admins can fetch or update location questions', async () => {
  for (const role of ['opsmanager', 'client', 'stocker', 'beneficiary']) {
    const f = fixture();
    assert.equal((await f.request('get', '/survey/location-questions', { role })).statusCode, 403);
    assert.equal((await f.request('put', '/survey/location/:id/questions', { role, body: { question_ids: [] } })).statusCode, 403);
    assert.equal(f.statements.length, 0);
  }
});
