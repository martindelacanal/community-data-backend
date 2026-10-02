'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { fetchInteractionSummary, fetchInteractionAcquisition, fetchInteractionRoutes } = require('./interactionMetrics');

function databaseWithResults(results) {
  const statements = [];
  return {
    statements,
    async query(sql, params) {
      statements.push({ sql, params });
      assert.ok(results.length, 'no unexpected extra database scan');
      return [results.shift()];
    }
  };
}

test('summary returns the same totals/timeline without recounting all events', async () => {
  const db = databaseWithResults([
    [{ total_views: 7, unique_visitors: 4, total_active_duration_seconds: 90 }],
    [{ metric_date: '2026-09-01', total: '7' }],
    [{ metric_date: '2026-09-01', total: '3' }, { metric_date: '2026-09-02', total: '4' }],
    [{ page_type: 'articles_list', route_group: 'articles', total_views: 7, avg_active_duration_seconds: 10 }]
  ]);
  const result = await fetchInteractionSummary(db, 'en', {
    from_date: '2026-09-01', to_date: '2026-09-30', auth_scope: 'authenticated', platform: 'app_ios'
  });
  assert.equal(result.totalActions, 7);
  assert.equal(result.totalViews, 7);
  assert.deepEqual(result.timeline, { categories: ['2026-09-01', '2026-09-02'], views: [7, 0], actions: [3, 4] });
  assert.equal(db.statements.length, 4);
  for (const { sql, params } of db.statements) {
    assert.doesNotMatch(sql, /DATE\([se]\.(started_at|occurred_at)\)\s*[<>]/);
    assert.match(sql, /< DATE_ADD\(\?, INTERVAL 1 DAY\)/);
    assert.deepEqual(params, ['2026-09-01', '2026-09-30', 'capacitor_ios']);
  }
  assert.match(db.statements[0].sql, /s.is_authenticated = 1/);
  assert.match(db.statements[2].sql, /e.user_id IS NOT NULL/);
});

test('open date filters remain optional and preserve anonymous/page/platform scope', async () => {
  const db = databaseWithResults([[], []]);
  await fetchInteractionRoutes(db, 'es', { auth_scope: 'anonymous', page_types: ['article_detail'], platform: 'web' });
  for (const { sql, params } of db.statements) {
    assert.doesNotMatch(sql, /started_at\s*[<>]/);
    assert.match(sql, /s.is_authenticated = 0/);
    assert.match(sql, /s.page_type IN \(\?\)/);
    assert.deepEqual(params, ['article_detail', 'web_desktop', 'web_mobile']);
  }
});

test('acquisition shares first-touch ranking, preserves cohort scope and output', async () => {
  const db = databaseWithResults([
    [
      { breakdown: 'timeline', metric_date: '2026-09-02', metric_key: 'web_desktop', total_new: 3 },
      { breakdown: 'timeline', metric_date: '2026-09-01', metric_key: 'capacitor_ios', total_new: 2 },
      { breakdown: 'source', metric_key: 'newsletter', total_new: 2 },
      { breakdown: 'source', metric_key: 'direct', total_new: 3 }
    ],
    [{ active_visitors: 8 }]
  ]);
  const result = await fetchInteractionAcquisition(db, 'es', {
    from_date: '2026-09-01', to_date: '2026-09-30', auth_scope: 'authenticated', platform: 'app'
  });
  assert.equal(db.statements.length, 2);
  const { sql, params } = db.statements[0];
  assert.equal((sql.match(/ROW_NUMBER\(\)/g) || []).length, 1);
  assert.ok(sql.indexOf('WHERE rn = 1') < sql.indexOf('fs.first_seen_at >= ?'), 'first touch is ranked across history before date filtering');
  assert.match(sql, /ORDER BY s.started_at ASC, s.id ASC/);
  assert.match(sql, /fs.first_authenticated = 1/);
  assert.deepEqual(params, ['2026-09-01', '2026-09-30', 'capacitor_android', 'capacitor_ios']);
  assert.equal(result.totalNewVisitors, 5);
  assert.equal(result.newAppVisitors, 2);
  assert.equal(result.returningVisitors, 3);
  assert.deepEqual(result.timeline.categories, ['2026-09-01', '2026-09-02']);
  assert.deepEqual(result.timeline.series.map(row => row.data), [[2, 0], [0, 3]]);
  assert.deepEqual(result.acquisitionSources.map(row => row.totalNewVisitors), [3, 2]);
  assert.equal(result.acquisitionSources[0].label, 'Directo / sin origen');
});

test('empty acquisition and summary keep valid zero-valued response contracts', async () => {
  const acquisition = await fetchInteractionAcquisition(databaseWithResults([[], [{ active_visitors: 0 }]]));
  assert.equal(acquisition.totalNewVisitors, 0);
  assert.equal(acquisition.returningVisitors, 0);
  assert.deepEqual(acquisition.timeline, { categories: [], series: [] });
  const summary = await fetchInteractionSummary(databaseWithResults([[{}], [], [], []]));
  assert.equal(summary.totalActions, 0);
  assert.deepEqual(summary.timeline, { categories: [], views: [], actions: [] });
});
