'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const moment = require('moment-timezone');
const { createBoundedAsyncCache } = require('../services/boundedAsyncCache');
const { normalizeInteractionMetricFilters } = require('../services/interactionMetrics');

// Run the deployed handlers/helpers with a fake database. Importing the entire
// monolith would initialize unrelated mail/S3/production database dependencies.
const source = fs.readFileSync(path.join(__dirname, 'user.js'), 'utf8').replace(/\r\n/g, '\n');
function functionSource(name, text = source) {
  const start = text.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, name);
  return text.slice(start, text.indexOf('\n}', start) + 2);
}
function routeSource(route) {
  const start = source.indexOf(`router.post('${route}',`);
  assert.ok(start >= 0, route);
  return source.slice(start, source.indexOf('\n});', start) + 4);
}
const metricFetchers = {
  summary: 'fetchInteractionSummary', routes: 'fetchInteractionRoutes',
  content: 'fetchInteractionContent', users: 'fetchInteractionUsers',
  actions: 'fetchInteractionActions', audience: 'fetchInteractionAudience',
  acquisition: 'fetchInteractionAcquisition'
};

function fixture() {
  const handlers = new Map();
  const calls = [];
  const sqlCalls = [];
  const healthCalls = [];
  const healthSource = fs.readFileSync(path.join(__dirname, '../services/healthMetrics.js'), 'utf8').replace(/\r\n/g, '\n');
  const healthNormalizer = vm.createContext({});
  vm.runInContext(['parseSurveyDateOnly', 'normalizeHealthMetricIntList', 'normalizeHealthMetricAge',
    'normalizeHealthMetricAnswerFilters', 'normalizeHealthMetricFilters']
    .map(name => functionSource(name, healthSource)).join('\n'), healthNormalizer);
  const globals = {
    normalizeInteractionMetricFilters,
    normalizeHealthMetricFiltersService: healthNormalizer.normalizeHealthMetricFilters,
    async getHealthMetricQuestionCatalogService(cabecera, language) {
      healthCalls.push({ type: 'catalog', cabecera, language });
      return { questionIds: [9], questions: [{ id: 9, question: language === 'es' ? 'Cobertura' : 'Coverage',
        traffic_light_enabled: true, traffic_light_direction: 'ascending',
        answers: [{ answer_id: 90, answer: 'Yes', order: 1 }] }] };
    },
    async getHealthMetricUserIdsService(cabecera, filters) {
      healthCalls.push({ type: 'users', cabecera, filters });
      return [cabecera.client_id || 100];
    },
    async getHealthMetricAnswerCountMapService(userIds) {
      healthCalls.push({ type: 'counts', userIds });
      return new Map([['9:90', userIds[0]]]);
    },
    participantMetricsCache: createBoundedAsyncCache(),
    PARTICIPANT_METRICS_CACHE_TTL_MS: 30000,
    verifyToken() {},
    router: { post(route, _auth, handler) { handlers.set(route, handler); } },
    mysqlConnection: { promise() { return { async query(sql, params) {
      sqlCalls.push({ sql, params: Array.from(params) });
      return [[{ name: 2, total: 5 }]];
    } }; } },
    console, logger: { error() {} }
  };
  for (const [metric, fetcher] of Object.entries(metricFetchers)) {
    globals[fetcher] = async (_db, language, filters) => {
      calls.push({ metric, language, filters });
      return { metric, calculation: calls.length };
    };
  }
  const context = vm.createContext(globals);
  vm.runInContext([
    ...['buildParticipantMetricsCacheKey', 'getCachedParticipantMetrics', 'getCachedInteractionMetric',
      'ensureAdminMetricsAccess', 'normalizeInteractionLanguage', 'buildDemographicWhere', 'demographicMetric'].map(name => functionSource(name)),
    ...Object.keys(metricFetchers).map(metric => routeSource(`/metrics/interaction/${metric}`)),
    routeSource('/metrics/health/questions')
  ].join('\n'), context);
  async function request(metric, { role = 'admin', filters = {}, language = 'en' } = {}) {
    const response = { statusCode: 200, body: null, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
    await handlers.get(`/metrics/interaction/${metric}`)({
      data: { data: JSON.stringify({ role }) }, body: filters, query: { language }
    }, response);
    return response;
  }
  async function healthRequest({ role = 'client', clientId = 1, filters = {}, language = 'en' } = {}) {
    const response = { statusCode: 200, body: null, status(code) { this.statusCode = code; return this; }, json(body) { this.body = body; return this; } };
    await handlers.get('/metrics/health/questions')({
      data: { data: JSON.stringify({ role, client_id: clientId }) }, body: filters, query: { language }
    }, response);
    return response;
  }
  return { context, calls, sqlCalls, request, healthRequest, healthCalls };
}

test('health chart deduplicates normalized filters and keeps auth/client/language boundaries', async () => {
  const f = fixture();
  const base = { filters: { locations: ['7', 7], from_date: '2026-09-01', client_id: 99 } };
  const [first, same] = await Promise.all([
    f.healthRequest(base),
    f.healthRequest({ filters: { locations: [7], from_date: '2026-09-01T00:00:00Z', client_id: 88 } })
  ]);
  assert.equal(JSON.stringify(first.body), JSON.stringify(same.body));
  assert.equal(first.body[0].answers[0].total, 1);
  assert.equal(f.healthCalls.length, 3, 'catalog/user scope/count run once for concurrent equivalent filters');
  for (const role of ['beneficiary', 'opsmanager', 'delivery']) {
    assert.equal((await f.healthRequest({ ...base, role })).statusCode, 403);
  }
  assert.equal((await f.healthRequest({ ...base, clientId: null })).statusCode, 400);
  assert.equal(f.healthCalls.length, 3, 'permissions are checked before any cache lookup/computation');
  const otherClient = await f.healthRequest({ ...base, clientId: 2 });
  assert.equal(otherClient.body[0].answers[0].total, 2);
  const translated = await f.healthRequest({ ...base, language: 'es' });
  assert.equal(translated.body[0].question, 'Cobertura');
  await f.healthRequest({ filters: { ...base.filters, answer_filters: { 9: [90] } } });
  await f.healthRequest({ filters: { ...base.filters, from_date: '2026-08-01' } });
  await f.healthRequest({ ...base, role: 'admin', clientId: null });
  await f.healthRequest({ ...base, role: 'director', clientId: null });
  assert.equal(f.healthCalls.length, 21);
  const userScopes = f.healthCalls.filter(call => call.type === 'users');
  assert.equal(userScopes[0].cabecera.client_id, 1, 'request client_id cannot override authenticated client');
  assert.equal(userScopes[3].filters.answerFilters[0].questionId, 9);
});

test('all seven interaction routes enforce admin access even when a result is cached', async () => {
  const f = fixture();
  for (const metric of Object.keys(metricFetchers)) {
    assert.equal((await f.request(metric)).statusCode, 200);
    assert.equal((await f.request(metric)).statusCode, 200);
    for (const role of ['client', 'director', 'beneficiary', 'delivery', 'opsmanager']) {
      assert.equal((await f.request(metric, { role })).statusCode, 401);
    }
  }
  assert.equal(f.calls.length, 7, 'authorized identical requests share results; rejected requests never compute');
});

test('interaction cache separates auth scope, dates, platform, language and metric', async () => {
  const f = fixture();
  const base = { filters: { from_date: '2026-09-01', to_date: '2026-09-30' } };
  const initial = await f.request('summary', base);
  assert.deepEqual((await f.request('summary', base)).body, initial.body);
  await f.request('summary', { ...base, language: 'es' });
  await f.request('summary', { filters: { ...base.filters, auth_scope: 'anonymous' } });
  await f.request('summary', { filters: { ...base.filters, platform: 'app' } });
  await f.request('summary', { filters: { ...base.filters, from_date: '2026-08-01' } });
  await f.request('summary', { filters: { ...base.filters, page_types: ['article_detail'] } });
  await f.request('routes', base);
  assert.equal(f.calls.length, 7);
});

test('demographic cache preserves client scope and leaves cached numeric buckets unmodified', async () => {
  const f = fixture();
  const filters = { from_date: '2026-09-01', to_date: '2026-09-30', locations: [3], client_id: 99 };
  const first = await f.context.demographicMetric('household', { role: 'client', client_id: 1 }, filters, 'en');
  const repeated = await f.context.demographicMetric('household', { role: 'client', client_id: 1 }, filters, 'en');
  assert.equal(JSON.stringify(first), JSON.stringify(repeated));
  assert.equal(first.average, 2);
  assert.equal(first.data[0].name, '2');
  await f.context.demographicMetric('household', { role: 'client', client_id: 2 }, filters, 'en');
  await f.context.demographicMetric('household', { role: 'admin' }, filters, 'en');
  assert.equal(f.sqlCalls.length, 3);
  assert.equal(f.sqlCalls[0].params.at(-1), 1);
  assert.equal(f.sqlCalls[1].params.at(-1), 2);
  assert.match(f.sqlCalls[0].sql, /cu.client_id = \?/);
  assert.doesNotMatch(f.sqlCalls[2].sql, /cu.client_id = \?/);
});

test('demographic date boundaries remain California calendar days across DST', () => {
  const f = fixture();
  for (const day of ['2026-03-08', '2026-11-01']) {
    const where = f.context.buildDemographicWhere({ role: 'client', client_id: 7 }, { from_date: day, to_date: day });
    assert.doesNotMatch(where.whereSql, /CONVERT_TZ\((?:u|db_r)\.creation_date/);
    assert.match(where.whereSql, /creation_date >= CONVERT_TZ\(\?,'America\/Los_Angeles','\+00:00'\)/);
    const start = moment.tz(where.params[0], 'America/Los_Angeles');
    const end = moment.tz(where.params[1], 'America/Los_Angeles');
    assert.equal(end.diff(start, 'hours'), day.includes('03-08') ? 23 : 25);
    for (const timestamp of [start.valueOf() - 1, start.valueOf(), end.valueOf() - 1, end.valueOf()]) {
      const wasInDay = moment(timestamp).tz('America/Los_Angeles').format('YYYY-MM-DD') === day;
      const inUtcRange = timestamp >= start.valueOf() && timestamp < end.valueOf();
      assert.equal(inUtcRange, wasInDay);
    }
  }
});
