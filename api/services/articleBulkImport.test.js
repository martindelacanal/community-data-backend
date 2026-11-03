'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const sharp = require('sharp');
const { createHash } = require('node:crypto');
const { COLUMNS, parseCsv, validateRows, validateImageManifest, findDatabaseConflicts, inspectImages, saveImport, BulkImportError } = require('./articleBulkImport');

const catalogs = Object.fromEntries(['categories', 'filters', 'statuses', 'tags', 'genders', 'ethnicities', 'locations'].map(key => [key, [{ id: 1 }, { id: 2 }]]));
const raw = (overrides = {}) => ({ row: 1, titleEnglish: 'Food services', titleSpanish: 'Servicios alimentarios', contentEnglish: 'English body', contentSpanish: 'Contenido español', author: 'Test author', date: '2026-11-03T12:30:00-03:00', categoryIds: '1|2', article_status_id: 'draft', ...overrides });
const csv = (rows, delimiter = ',') => {
  const keys = COLUMNS.map(col => col.key);
  return [keys.join(delimiter), ...rows.map(row => keys.map(key => `"${String(row[key] || '').replace(/"/g, '""')}"`).join(delimiter))].join('\r\n');
};

test('UTF-8 BOM, quoted multiline bodies, escaped commas and Excel semicolons preserve row numbers', () => {
  for (const delimiter of [',', ';']) {
    const parsed = parseCsv(`\uFEFF${csv([raw({ contentEnglish: 'First "quoted" line,\nSecond line' }), raw({ titleEnglish: 'Second', titleSpanish: 'Segundo' })], delimiter)}`);
    assert.deepEqual(parsed.errors, []);
    assert.equal(parsed.rows[0].contentEnglish, 'First "quoted" line,\nSecond line');
    assert.equal(parsed.rows[1].row, 2);
  }
});

test('bad CSV headers and malformed quoting never silently shift article fields', () => {
  assert.ok(parseCsv('titleEnglish,titleEnglish\na,b').errors.some(error => error.code === 'duplicate_column'));
  assert.ok(parseCsv('titleEnglish\na').errors.some(error => error.column === 'date' && error.code === 'missing_column'));
  assert.ok(parseCsv('titleEnglish\n"unfinished').errors.some(error => error.code === 'csv_format'));
  assert.ok(parseCsv('titleEnglish\na,b').errors.some(error => error.code === 'csv_format'));
});

test('valid bilingual article normalizes explicit status, UTC date and optional targeting', () => {
  const result = validateRows([raw({ article_status_id: 'publicado', ageRanges: '18-35|65-100', tags: '2', showImageSpanish: 'false' })], catalogs);
  assert.deepEqual(result.errors, []);
  assert.equal(result.rows[0].article_status_id, 2);
  assert.equal(result.rows[0].date, '2026-11-03 15:30:00');
  assert.equal(result.rows[0].showImageEnglish, true);
  assert.equal(result.rows[0].showImageSpanish, false);
  assert.deepEqual(result.rows[0].tags, [2]);
  assert.deepEqual(result.rows[0].ageRanges, [{ min: 18, max: 35 }, { min: 65, max: 100 }]);
});

test('reports the exact article and field for missing, invalid, unknown and inconsistent values', () => {
  const result = validateRows([raw({ row: 3, titleSpanish: '', author: '', date: '2026-02-30T12:30:00Z', categoryIds: '8', filter: '1|1', priority_filter_id: '2', ageRanges: '18-35|20-40', article_status_id: '', showImageSpanish: 'maybe' })], catalogs);
  for (const key of ['titleSpanish', 'author', 'date', 'categoryIds', 'filter', 'priority_filter_id', 'ageRanges', 'article_status_id', 'showImageSpanish']) assert.ok(result.errors.some(error => error.column === key && error.row === 3), key);
  assert.ok(validateRows([raw({ date: '2026-11-03T12:30:00' })], catalogs).errors.some(error => error.column === 'date'));
  assert.ok(validateRows([raw({ contentEnglish: '<p>&nbsp;</p>' })], catalogs).errors.some(error => error.column === 'contentEnglish'));
});

test('duplicate names, URL collisions and repeated priorities are detected within CSV', () => {
  const result = validateRows([raw({ priority: '3' }), raw({ row: 2, titleEnglish: 'FOOD services', titleSpanish: 'Servicios alimentarios!', priority: '3' })], catalogs);
  assert.ok(result.errors.some(error => error.code === 'duplicate_title' && error.row === 2));
  assert.ok(result.errors.some(error => error.code === 'invalid_priority' && error.row === 2));
});

test('cross-language URLs cannot shadow another imported article, but matching translations of one article are allowed', () => {
  const first = raw({ titleEnglish: 'Hello', titleSpanish: 'Hola' });
  for (const second of [
    raw({ row: 2, titleEnglish: 'Hola!', titleSpanish: 'Otro' }),
    raw({ row: 2, titleEnglish: 'Other', titleSpanish: 'Hello!' })
  ]) {
    const result = validateRows([first, second], catalogs);
    assert.ok(result.errors.some(error => error.code === 'duplicate_title' && error.row === 2));
  }
  assert.deepEqual(validateRows([raw({ titleEnglish: 'Community', titleSpanish: 'Community' })], catalogs).errors, []);
});

test('existing article URLs are checked across both language columns in both directions', async () => {
  const rows = validateRows([raw({ titleEnglish: 'Hello', titleSpanish: 'Hola' })], catalogs).rows;
  for (const [existing, expectedColumn] of [
    [{ id: 3, title_en: 'Hola!', title_es: 'Otro', slug_en: 'hola', slug_es: 'otro' }, 'titleSpanish'],
    [{ id: 4, title_en: 'Other', title_es: 'Hello!', slug_en: 'other', slug_es: 'hello' }, 'titleEnglish']
  ]) {
    const db = { query: async (_sql, params) => {
      assert.deepEqual(params[2], ['hello', 'hola']);
      assert.deepEqual(params[3], ['hello', 'hola']);
      return [params[2].includes(existing.slug_en) || params[3].includes(existing.slug_es) ? [existing] : []];
    } };
    const errors = await findDatabaseConflicts(db, rows);
    assert.ok(errors.some(error => error.code === 'existing_title' && error.column === expectedColumn && error.row === 1));
  }
});

test('one exact numbered image per CSV article, matching MIME and distinct contents are required', () => {
  const images = [{ name: '1.jpg', type: 'image/jpeg', size: 120, hash: 'a'.repeat(64) }, { name: '2.png', type: 'image/png', size: 120, hash: 'b'.repeat(64) }];
  assert.deepEqual(validateImageManifest(images, 2), []);
  assert.ok(validateImageManifest([images[0], { ...images[1], name: '1.png' }], 2).some(error => error.code === 'duplicate_image_name'));
  assert.ok(validateImageManifest([images[0]], 2).some(error => error.code === 'missing_image' && error.row === 2));
  assert.ok(validateImageManifest([images[0], { ...images[1], hash: images[0].hash }], 2).some(error => error.code === 'duplicate_image_content'));
  assert.ok(validateImageManifest([{ ...images[0], type: 'image/png' }], 1).some(error => error.code === 'image_type'));
  assert.ok(validateImageManifest([null], 1).some(error => error.code === 'invalid_image_manifest'));
});

async function imageFixture(t, invalid = false) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'bulk-article-test-'));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const filePath = path.join(directory, '1.png');
  const buffer = invalid ? Buffer.from('not a PNG') : await sharp({ create: { width: 5, height: 5, channels: 3, background: '#00aaff' } }).png().toBuffer();
  await fs.writeFile(filePath, buffer);
  return { path: filePath, originalname: '1.png', mimetype: 'image/png', buffer };
}

test('actual bytes are decoded before an upload can create articles', async t => {
  const valid = await imageFixture(t);
  const result = await inspectImages([valid]);
  assert.deepEqual(result.errors, []);
  assert.equal(result.images[0].hash, createHash('sha256').update(valid.buffer).digest('hex'));
  const invalid = await imageFixture(t, true);
  assert.equal((await inspectImages([invalid])).errors[0].code, 'invalid_image');
});

async function saveFixture(t, failure) {
  const file = await imageFixture(t); const { images } = await inspectImages([file]);
  const rows = validateRows([raw()], catalogs).rows; const calls = []; let nextId = 7;
  const connection = {
    query: async (sql, params) => {
      calls.push({ sql, params });
      if (sql.includes('GET_LOCK')) return [[{ acquired: 1 }]];
      if (sql.includes('RELEASE_LOCK')) return [[{ released: 1 }]];
      if (sql.startsWith('SELECT id, title')) return [failure === 'conflict' ? [{ id: 1, title_en: 'Food services' }] : []];
      if (sql.startsWith('INSERT INTO article_images') && failure === 'database') throw new Error('synthetic DB failure');
      return [{ insertId: nextId++ }];
    },
    beginTransaction: async () => calls.push('begin'), commit: async () => { calls.push('commit'); if (failure === 'commit') throw new Error('lost commit response'); },
    rollback: async () => calls.push('rollback'), release: () => calls.push('release')
  };
  const deps = { pool: { getConnection: async () => connection, query: async () => [[{ article_id: 7 }]] }, rows, images,
    bucketName: 'synthetic-bucket', logger: { error() {} },
    uploadImageWithVariants: async ({ originalKey }) => {
      calls.push({ upload: originalKey }); if (failure === 'upload') throw new Error('synthetic S3 failure');
      return { originalKey, smallKey: `${originalKey}__small`, mediumKey: `${originalKey}__medium` };
    }, deleteS3Objects: async keys => calls.push({ cleanup: keys }) };
  return { deps, calls };
}

test('all image uploads precede atomic article/relations writes and use independent language keys', async t => {
  const { deps, calls } = await saveFixture(t);
  const result = await saveImport(deps);
  assert.deepEqual(result, { createdCount: 1, articleIds: [7] });
  assert.equal(calls.filter(call => call.upload).length, 2);
  assert.ok(calls.findIndex(call => call === 'begin') > calls.findIndex(call => call.upload?.endsWith('/es')));
  assert.equal(calls.filter(call => call.sql?.startsWith('INSERT INTO article_images')).length, 2);
  assert.ok(calls.includes('commit')); assert.ok(!calls.includes('rollback')); assert.equal(calls.at(-1), 'release');
});

test('failed S3 upload creates no article and cleans even partially uploaded variants', async t => {
  const { deps, calls } = await saveFixture(t, 'upload');
  await assert.rejects(saveImport(deps), /synthetic S3 failure/);
  assert.ok(!calls.includes('begin'));
  assert.equal(calls.find(call => call.cleanup).cleanup.length, 3);
});

test('a metadata failure rolls back every article and deletes uploaded images', async t => {
  const { deps, calls } = await saveFixture(t, 'database');
  await assert.rejects(saveImport(deps), /synthetic DB failure/);
  assert.ok(calls.includes('rollback')); assert.ok(!calls.includes('commit'));
  assert.equal(calls.find(call => call.cleanup).cleanup.length, 6);
  assert.equal(calls.at(-1), 'release');
});

test('duplicate recheck under import lock prevents a repeated or competing import', async t => {
  const { deps, calls } = await saveFixture(t, 'conflict');
  await assert.rejects(saveImport(deps), error => error instanceof BulkImportError && error.status === 409);
  assert.ok(!calls.some(call => call.sql?.startsWith('INSERT')));
  assert.ok(calls.includes('rollback')); assert.ok(calls.some(call => call.cleanup));
});

test('a lost commit response is reconciled without deleting committed images', async t => {
  const { deps, calls } = await saveFixture(t, 'commit');
  assert.deepEqual(await saveImport(deps), { createdCount: 1, articleIds: [7] });
  assert.ok(!calls.some(call => call.cleanup));
});
