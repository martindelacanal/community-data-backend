'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { createHash } = require('node:crypto');
const express = require('express');
const sharp = require('sharp');
const { registerArticleBulkImportRoutes } = require('./articleBulkImport');

const header = 'titleEnglish,titleSpanish,contentEnglish,contentSpanish,author,date,categoryIds,article_status_id';
const csv = `${header}\nFood news,Noticias de alimentos,English text,Texto español,Author,2026-11-03T12:00:00Z,1,draft`;
async function fixture(t, failure) {
  const calls = []; let currentId = 20;
  const query = async (sql, params) => {
    calls.push({ sql, params });
    if (sql.includes('GET_LOCK')) return [[{ acquired: 1 }]];
    if (sql.includes('RELEASE_LOCK')) return [[{ released: 1 }]];
    if (sql.startsWith('SELECT id, title')) return [[]];
    if (sql.startsWith('SELECT id,')) return [[{ id: 1, nameEnglish: 'Test', nameSpanish: 'Prueba' }, { id: 2, nameEnglish: 'Published', nameSpanish: 'Publicado' }]];
    if (sql.startsWith('INSERT INTO article_images') && failure) throw new Error('synthetic write failure');
    return [{ insertId: currentId++ }];
  };
  const connection = { query, beginTransaction: async () => calls.push('begin'), commit: async () => calls.push('commit'), rollback: async () => calls.push('rollback'), release: () => calls.push('release') };
  const app = express(); app.use(express.json({ limit: '5mb' })); const router = express.Router();
  registerArticleBulkImportRoutes(router, { pool: { query, getConnection: async () => connection }, logger: { error() {} }, bucketName: 'synthetic-bucket',
    verifyToken: (req, res, next) => {
      if (!req.headers.authorization) return res.status(401).end();
      req.data = { data: JSON.stringify({ role: req.headers.authorization, id: 99 }) }; next();
    },
    uploadImageWithVariants: async ({ originalKey }) => { calls.push({ upload: originalKey }); return { originalKey, smallKey: `${originalKey}__small`, mediumKey: `${originalKey}__medium` }; },
    deleteS3Objects: async keys => calls.push({ cleanup: keys }) });
  app.use(router); const listener = app.listen(0, '127.0.0.1'); await once(listener, 'listening');
  t.after(() => new Promise(resolve => { listener.closeAllConnections(); listener.close(resolve); }));
  const request = (route, { role = 'admin', body, form } = {}) => fetch(`http://127.0.0.1:${listener.address().port}${route}`, {
    method: form || body ? 'POST' : 'GET', headers: { ...(role ? { Authorization: role } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    body: form || (body ? JSON.stringify(body) : undefined), signal: AbortSignal.timeout(10000)
  });
  const image = await sharp({ create: { width: 4, height: 4, channels: 3, background: '#ff00aa' } }).png().toBuffer();
  const form = (csvText = csv, files = [{ name: '1.png', buffer: image }]) => {
    const data = new FormData(); data.append('csv', new Blob([csvText], { type: 'text/csv' }), 'articles.csv');
    for (const file of files) data.append('images', new Blob([file.buffer], { type: 'image/png' }), file.name);
    return data;
  };
  return { request, calls, image, form };
}

test('anonymous and non-editor users cannot read schema, validate or upload', async t => {
  const { request, calls } = await fixture(t);
  for (const role of [null, 'beneficiary', 'volunteer', 'operationsmanager']) {
    for (const route of ['/article/bulk/schema', '/article/bulk/validate', '/article/bulk']) {
      const response = await request(route, { role, ...(route.endsWith('schema') ? {} : { body: {} }) });
      assert.equal(response.status, role ? 403 : 401);
    }
  }
  assert.equal(calls.length, 0);
});

test('schema includes all authoring fields and bilingual column instructions for content managers', async t => {
  const { request } = await fixture(t);
  const response = await request('/article/bulk/schema', { role: 'contentmanager' });
  assert.equal(response.status, 200); const schema = await response.json();
  assert.equal(schema.columns.length, 24);
  assert.ok(schema.columns.every(col => col.descriptionEn && col.descriptionEs));
  assert.ok(schema.columns.find(col => col.key === 'article_status_id').required);
  assert.equal(schema.limits.maxRows, 100);
});

test('preflight reports fields and duplicate image content without S3 or database writes', async t => {
  const { request, calls, image } = await fixture(t);
  const response = await request('/article/bulk/validate', { body: {
    csv: csv.replace(',Author,', ',,'), images: [{ name: '1.png', size: image.length, type: 'image/png', hash: createHash('sha256').update(image).digest('hex') }]
  } });
  const result = await response.json(); assert.equal(response.status, 200); assert.equal(result.valid, false);
  assert.ok(result.errors.some(error => error.row === 1 && error.column === 'author'));
  assert.ok(!calls.some(call => call.upload || call.sql?.startsWith('INSERT')));
  assert.equal(result.rows, undefined);
});

test('binary duplicate and corrupt files are refused before S3 or article writes', async t => {
  const { request, calls, form, image } = await fixture(t);
  const doubleCsv = `${csv}\nSecond article,Segundo artículo,English,Español,Author,2026-11-03T12:00:00Z,1,published`;
  const duplicate = await request('/article/bulk', { form: form(doubleCsv, [{ name: '1.png', buffer: image }, { name: '2.png', buffer: image }]) });
  assert.equal(duplicate.status, 400); assert.ok((await duplicate.json()).errors.some(error => error.code === 'duplicate_image_content'));
  const corrupt = await request('/article/bulk', { form: form(csv, [{ name: '1.png', buffer: Buffer.from('corrupt') }]) });
  assert.equal(corrupt.status, 400); assert.ok((await corrupt.json()).errors.some(error => error.code === 'invalid_image'));
  assert.ok(!calls.some(call => call.upload || call.sql?.startsWith('INSERT')));
});

test('a valid content-manager import creates one draft and both language preview images atomically', async t => {
  const { request, calls, form } = await fixture(t);
  const response = await request('/article/bulk', { role: 'contentmanager', form: form() });
  assert.equal(response.status, 201); assert.equal((await response.json()).createdCount, 1);
  const article = calls.find(call => call.sql?.startsWith('INSERT INTO article ('));
  assert.equal(article.params[10], 1);
  assert.equal(calls.filter(call => call.sql?.startsWith('INSERT INTO article_images')).length, 2);
  assert.ok(calls.includes('commit')); assert.ok(!calls.includes('rollback'));
});

test('failed image metadata insertion rolls back and returns a bilingual error', async t => {
  const { request, calls, form } = await fixture(t, true);
  const response = await request('/article/bulk', { form: form() });
  assert.equal(response.status, 500); const result = await response.json();
  assert.ok(result.errors[0].messageEn); assert.ok(result.errors[0].messageEs);
  assert.ok(calls.includes('rollback')); assert.ok(!calls.includes('commit')); assert.ok(calls.some(call => call.cleanup));
});
