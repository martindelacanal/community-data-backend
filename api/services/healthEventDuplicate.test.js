'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { duplicateHealthEvent, HealthEventDuplicateError } = require('./healthEventDuplicate');

function fixture({ missing = false, taken = false, failure = '', objectJson = false } = {}) {
  const statements = [], writes = [], copies = [], deletions = [], lifecycle = [];
  const landing = { hero: { title_en: 'Banning', title_es: 'Banning' }, gallery: { title_en: 'Photos' } };
  const source = { id: 7, slug: 'banning', timezone: 'America/Los_Angeles', enabled: 'Y',
    landing_json: objectJson ? landing : JSON.stringify(landing), promo_json: JSON.stringify({ text_en: 'Clinic', link_url: '/banning' }) };
  const images = [{ id: 11, health_event_id: 7, section_key: 'hero', s3_key: 'old/hero photo.jpg',
    s3_key_small: 'old/hero__small', s3_key_medium: 'old/hero__medium', mime_type: 'image/jpeg',
    original_filename: 'hero.jpg', alt_en: 'Clinic', alt_es: 'Clínica', link_url: null, display_order: 1, enabled: 'Y' },
  { id: 12, health_event_id: 7, section_key: 'promo_dialog', s3_key: 'old/promo.png',
    s3_key_small: null, s3_key_medium: null, mime_type: 'image/png', original_filename: 'promo.png',
    alt_en: null, alt_es: null, link_url: 'https://example.org', display_order: 2, enabled: 'N' }];
  const connection = {
    async beginTransaction() { lifecycle.push('begin'); },
    async commit() { lifecycle.push('commit'); },
    async rollback() { lifecycle.push('rollback'); },
    release() { lifecycle.push('release'); },
    async query(sql, params) {
      statements.push(sql);
      if (sql.startsWith('SELECT * FROM health_event WHERE')) return [missing ? [] : [source]];
      if (sql.startsWith('SELECT id FROM health_event WHERE slug')) return [taken ? [{ id: 8 }] : []];
      if (sql.startsWith('SELECT * FROM health_event_image')) return [images];
      if (sql.startsWith('INSERT INTO')) {
        if (failure === 'database' && sql.startsWith('INSERT INTO health_event_image')) throw new Error('insert image failed');
        if (failure === 'slug-race') throw Object.assign(new Error('duplicate'), { code: 'ER_DUP_ENTRY' });
        const [, table, columns] = sql.match(/INSERT INTO (\w+)\(([^)]+)\)/);
        writes.push({ table, value: Object.fromEntries(columns.split(',').map((column, index) => [column.trim(), params[index]])) });
        return [{ insertId: 42 }];
      }
      throw new Error(`Unexpected SQL ${sql}`);
    }
  };
  const details = { slug: 'san-jacinto', name_en: 'San Jacinto', name_es: 'San Jacinto',
    location_id: 88, client_id: 9, start_date: '2027-02-20', end_date: '2027-02-21',
    enabled: 'Y', landing_enabled: 'Y', public_home_visible: 'Y' };
  const options = { pool: { async getConnection() { lifecycle.push('acquire'); return connection; } },
    sourceId: 7, userId: 3, details,
    async copyObject(from, to) { copies.push({ from, to }); if (failure === 'storage' && copies.length === 2) throw new Error('copy failed'); },
    async deleteObjects(keys) { deletions.push(...keys); }, logger: { error() {} } };
  return { options, source, images, statements, writes, copies, deletions, lifecycle };
}

test('copies bilingual landing and all image variants to an independently editable disabled draft', async () => {
  const f = fixture();
  const sourceBefore = JSON.stringify(f.source), imagesBefore = JSON.stringify(f.images);
  assert.deepEqual(await duplicateHealthEvent(f.options), { id: 42 });
  const event = f.writes.find(row => row.table === 'health_event').value;
  assert.equal(event.slug, 'san-jacinto');
  assert.equal(event.location_id, 88);
  assert.equal(event.created_by_user_id, 3);
  assert.equal(event.landing_json, f.source.landing_json);
  assert.equal(event.promo_json, f.source.promo_json);
  for (const flag of ['enabled', 'landing_enabled', 'public_home_visible', 'promo_dialog_enabled']) assert.equal(event[flag], 'N', flag);
  assert.equal(event.registration_opens_at, null);
  assert.equal(event.registration_closes_at, null);
  assert.equal(event.timezone, f.source.timezone);
  const copiedImages = f.writes.filter(row => row.table === 'health_event_image').map(row => row.value);
  assert.equal(copiedImages.length, 2);
  assert.equal(copiedImages[1].enabled, 'N', 'disabled image settings preserved');
  assert.equal(copiedImages[0].alt_es, 'Clínica');
  assert.equal(copiedImages[1].s3_key_small, null);
  assert.equal(copiedImages[0].health_event_id, 42);
  assert.equal(f.copies.length, 4);
  assert.equal(new Set(f.copies.map(copy => copy.to)).size, 4);
  for (const copy of f.copies) { assert.match(copy.to, /^health-events\/42\/copy-/); assert.notEqual(copy.to, copy.from); }
  assert.equal(JSON.stringify(f.source), sourceBefore);
  assert.equal(JSON.stringify(f.images), imagesBefore);
  assert.ok(f.statements.every(sql => !/health_event_(registration|answer|scan|slot|stand|form|notification|wristband)/.test(sql)), 'operational and participant data never read or copied');
  assert.deepEqual(f.lifecycle, ['acquire', 'begin', 'commit', 'release']);
  assert.deepEqual(f.deletions, []);
});

test('handles JSON already parsed by the database driver', async () => {
  const f = fixture({ objectJson: true });
  await duplicateHealthEvent(f.options);
  assert.deepEqual(JSON.parse(f.writes[0].value.landing_json), f.source.landing_json);
});

for (const [options, code, status] of [[{ missing: true }, 'NOT_FOUND', 404], [{ taken: true }, 'SLUG_TAKEN', 409], [{ failure: 'slug-race' }, 'SLUG_TAKEN', 409]]) {
  test(`rejects ${JSON.stringify(options)} atomically`, async () => {
    const f = fixture(options);
    await assert.rejects(duplicateHealthEvent(f.options), error => error instanceof HealthEventDuplicateError && error.code === code && error.status === status);
    assert.deepEqual(f.lifecycle, ['acquire', 'begin', 'rollback', 'release']);
    assert.equal(f.copies.length, 0);
  });
}

for (const failure of ['storage', 'database']) {
  test(`rolls back and cleans only new image objects after ${failure} failure`, async () => {
    const f = fixture({ failure });
    await assert.rejects(duplicateHealthEvent(f.options), /failed/);
    assert.deepEqual(f.lifecycle, ['acquire', 'begin', 'rollback', 'release']);
    assert.deepEqual(f.deletions, f.copies.map(copy => copy.to));
    assert.ok(f.deletions.every(key => key.startsWith('health-events/42/copy-')));
  });
}

for (const dates of [{ start_date: '2027-02-30' }, { start_date: 'invalid' }, { end_date: '2027-02-19' }]) {
  test(`rejects invalid new dates before opening a transaction ${JSON.stringify(dates)}`, async () => {
    const f = fixture(); Object.assign(f.options.details, dates);
    await assert.rejects(duplicateHealthEvent(f.options), { code: 'INVALID_DATES', status: 400 });
    assert.deepEqual(f.lifecycle, []);
  });
}
