const { test } = require('node:test');
const assert = require('node:assert/strict');
const sharp = require('sharp');
const {
  DEFAULT_MODEL, normalizeProductName, buildExtractionPrompt, prepareImage,
  validateExtraction, matchExtraction, createTicketAnalysisService
} = require('./ticketAnalysis');

const catalog = {
  products: [{ id: 11, name: 'Fresh Apples', product_type_id: 2 }, { id: 12, name: 'Apple Juice', product_type_id: 3 }],
  productTypes: [{ id: 2, name: 'Produce', name_es: 'Frutas y verduras' }, { id: 3, name: 'Beverages', name_es: 'Bebidas' }],
  language: 'en'
};
const row = (values = {}) => ({ name: 'Fresh Apples', quantity: 10, unit: 'lb', product_type_id: 2, source_image_indexes: [1], uncertain: false, warnings: [], ...values });
const result = (items = [row()]) => ({ items, warnings: [] });
const image = () => sharp({ create: { width: 16, height: 16, channels: 3, background: '#fff' } }).png().toBuffer();

test('catalog matching is exact after Unicode, case and whitespace normalization, never fuzzy', () => {
  assert.equal(normalizeProductName('  FRESH\u00a0  Apples  '), 'freshapples');
  const draft = matchExtraction(result([row({ name: ' fresh  APPLES ', product_type_id: 3 }), row({ name: 'Fresh Apple' }), row({ name: 'New Food', product_type_id: 999 })]), catalog);
  assert.equal(draft.items[0].product_id, 11);
  assert.equal(draft.items[0].name, 'Fresh Apples');
  assert.equal(draft.items[0].product_type_id, 2);
  assert.equal(draft.items[1].product_id, null);
  assert.equal(draft.items[1].name, 'Fresh Apple');
  assert.equal(draft.items[2].product_type_id, null);
  assert.ok(draft.items[2].warnings.some((warning) => warning.includes('category')));
});

test('exact names take priority and same-category legacy duplicates choose a stable ID; differing categories stay unresolved', () => {
  const duplicateCatalog = { ...catalog, products: [...catalog.products, { id: 99, name: 'FRESH APPLES', product_type_id: 2 }] };
  assert.equal(matchExtraction(result([row({ name: 'FRESH APPLES' })]), duplicateCatalog).items[0].product_id, 99);
  assert.equal(matchExtraction(result([row({ name: 'fresh apples' })]), duplicateCatalog).items[0].product_id, 11);
  const draft = matchExtraction(result(), { ...catalog, products: [...catalog.products, { id: 99, name: 'Fresh Apples', product_type_id: 3 }] });
  assert.equal(draft.items[0].product_id, null);
  assert.equal(draft.items[0].product_type_id, null);
  assert.ok(draft.items[0].warnings.some((warning) => warning.includes('different categories')));
});

test('converts only explicit weight units, preserves null weights, and localizes warnings', () => {
  const draft = matchExtraction(result([
    row({ quantity: 2, unit: 'kg' }), row({ quantity: 16, unit: 'oz' }), row({ quantity: 1000, unit: 'g' }),
    row({ quantity: 3, unit: 'unknown' }), row({ quantity: null, uncertain: true }), row({ quantity: 0 })
  ]), { ...catalog, language: 'es' });
  assert.deepEqual(draft.items.map(({ quantity }) => quantity), [4.409, 1, 2.205, null, null, 0]);
  assert.equal(draft.items[0].original_quantity, 2);
  assert.equal(draft.items[0].original_unit, 'kg');
  assert.ok(draft.items[4].warnings.includes('Lectura dudosa. Compara esta fila con la foto.'));
});

test('rejects malformed model JSON, extra fields, invalid source pages and numerical coercion', () => {
  const invalid = [
    null, { items: [], warnings: [], secret: 'ignored' }, result([row({ quantity: '10' })]),
    result([row({ quantity: -1 })]), result([row({ quantity: Infinity })]), result([row({ product_type_id: '2' })]),
    result([row({ source_image_indexes: [0] })]), result([row({ source_image_indexes: [2] })]),
    result([row({ source_image_indexes: [1, 1] })]), result([row({ name: '' })]), result([row({ quantity: undefined })]),
    result([row({ unit: 'cases' })]), result([row({ instructions: 'write database' })]),
    result([row({ uncertain: undefined })]), result([row({ warnings: ['x'.repeat(301)] })])
  ];
  for (const value of invalid) assert.throws(() => validateExtraction(value, 1), { code: 'ai_invalid_response' });
  assert.deepEqual(validateExtraction(result([]), 1), result([]));
  assert.equal(validateExtraction(result([row({ source_image_indexes: [1, 2] })]), 2).items.length, 1);
});

test('prompt preserves literal product names, distinguishes cases from pounds, overlap and untrusted image instructions', () => {
  const prompt = buildExtractionPrompt({ ...catalog, imageCount: 2 });
  for (const text of ['untrusted document data', 'Do not translate', 'NOT price, cases', 'Do NOT convert units', 'SAME ticket/page twice', 'Do not merge or sum separate rows', 'Fresh Apples']) assert.ok(prompt.includes(text), text);
});

test('image preparation decodes actual content and emits bounded JPEG data without metadata', async () => {
  const prepared = await prepareImage(await image());
  assert.equal(prepared.inlineData.mimeType, 'image/jpeg');
  const metadata = await sharp(Buffer.from(prepared.inlineData.data, 'base64')).metadata();
  assert.equal(metadata.format, 'jpeg');
  assert.equal(metadata.exif, undefined);
  await assert.rejects(prepareImage(Buffer.from('<svg><script/></svg>')), { code: 'ai_invalid_image' });
  await assert.rejects(prepareImage(Buffer.alloc(10 * 1024 * 1024 + 1)), { code: 'ai_image_too_large' });
});

test('sends multiple ordered photos and structured schema with API key only in private headers, returns draft', async () => {
  const calls = [];
  const service = createTicketAnalysisService({
    env: { GEMINI_API_KEY: 'private-test-key' },
    http: { post: async (...args) => {
      calls.push(args);
      return { data: { candidates: [{ finishReason: 'STOP', content: { parts: [{ text: JSON.stringify(result()) }] } }] } };
    } }
  });
  const draft = await service.analyze({ ...catalog, images: [await image(), await image()] });
  assert.equal(draft.draft, true);
  assert.equal(draft.items[0].product_id, 11);
  assert.equal(draft.model, DEFAULT_MODEL);
  const [url, body, config] = calls[0];
  assert.equal(calls.length, 1);
  assert.equal(url, `https://generativelanguage.googleapis.com/v1beta/models/${DEFAULT_MODEL}:generateContent`);
  assert.equal(config.headers['x-goog-api-key'], 'private-test-key');
  assert.equal(config.maxRedirects, 0);
  assert.equal(body.generationConfig.responseMimeType, 'application/json');
  assert.equal(Object.hasOwn(body.generationConfig, 'temperature'), false);
  assert.equal(body.generationConfig.responseJsonSchema.additionalProperties, false);
  assert.ok(!JSON.stringify(body.generationConfig.responseJsonSchema).includes('maxItems'));
  assert.deepEqual(body.contents[0].parts.filter((part) => part.text?.startsWith('PHOTO')).map(({ text }) => text), ['PHOTO 1', 'PHOTO 2']);
  assert.equal(body.contents[0].parts.filter(({ inlineData }) => inlineData).length, 2);
  assert.ok(!JSON.stringify(draft).includes('private-test-key'));
});

test('disabled analysis does not call provider, and errors or truncated replies never become partial drafts', async () => {
  let calls = 0;
  const disabled = createTicketAnalysisService({ env: { GEMINI_API_KEY: 'private-test-key', GEMINI_TICKET_ANALYSIS_ENABLED: 'false' }, http: { post: async () => { calls++; } } });
  await assert.rejects(disabled.analyze({ ...catalog, images: [await image()] }), { code: 'ai_not_configured' });
  assert.equal(calls, 0);
  for (const [providerError, expected] of [
    [{ code: 'ECONNABORTED' }, 'ai_timeout'], [{ response: { status: 429 } }, 'ai_rate_limited'], [{ message: 'private data', config: { key: 'secret' } }, 'ai_provider_unavailable']
  ]) {
    const service = createTicketAnalysisService({ env: { GEMINI_API_KEY: 'key' }, http: { post: async () => { throw providerError; } } });
    await assert.rejects(service.analyze({ ...catalog, images: [await image()] }), { code: expected, message: expected });
  }
  for (const [finishReason, text] of [['MAX_TOKENS', JSON.stringify(result())], ['STOP', '{broken'], ['SAFETY', '']]) {
    const service = createTicketAnalysisService({ env: { GEMINI_API_KEY: 'key' }, http: { post: async () => ({ data: { candidates: [{ finishReason, content: { parts: [{ text }] } }] } }) } });
    await assert.rejects(service.analyze({ ...catalog, images: [await image()] }), { code: 'ai_invalid_response' });
  }
});
