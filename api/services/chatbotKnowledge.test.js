'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { publicAddress, validateUrl, fetchPage, ingestSource, splitText, plain, retrieveContext, extractPdf } = require('./chatbotKnowledge');
const { MAX_TOTAL_CHUNKS } = require('./chatbotLimits');

test('source URL policy excludes internal addresses, alternate IP formats, ports and credentials', () => {
  for (const url of ['http://www.cdc.gov', 'https://localhost/a', 'https://127.0.0.1/', 'https://2130706433/',
    'https://0x7f000001/', 'https://169.254.169.254/latest/meta-data', 'https://[::1]/',
    'https://[::ffff:127.0.0.1]/', 'https://user:secret@cdc.gov/', 'https://cdc.gov:444/']) {
    assert.throws(() => validateUrl(url), { code: 'chatbot_invalid_url' }, url);
  }
  for (const address of ['10.0.0.1', '172.16.0.1', '192.168.0.1', '100.64.0.1', '224.0.0.1', '::1', 'fc00::1', 'fe80::1', '::ffff:10.0.0.1']) assert.equal(publicAddress(address), false);
  assert.equal(publicAddress('8.8.8.8'), true);
  assert.equal(validateUrl('https://www.cdc.gov/diabetes/#a').href, 'https://www.cdc.gov/diabetes/');
});
test('fetch pins public DNS and checks redirects and every returned address', async () => {
  let requested = 0;
  const request = async (url, options) => {
    requested++;
    const pinned = await new Promise((resolve, reject) => options.httpsAgent.options.lookup('www.cdc.gov', { all: true }, (e, x) => e ? reject(e) : resolve(x)));
    assert.deepEqual(pinned, [{ address: '8.8.8.8', family: 4 }]);
    assert.equal(options.proxy, false); assert.equal(options.maxRedirects, 0);
    return { status: 302, headers: { location: 'https://169.254.169.254/' }, data: '' };
  };
  await assert.rejects(fetchPage('https://www.cdc.gov/', { lookup: async () => [{ address: '8.8.8.8', family: 4 }], request }), { code: 'chatbot_invalid_url' });
  assert.equal(requested, 1);
  await assert.rejects(fetchPage('https://www.cdc.gov/', { lookup: async () => [{ address: '8.8.8.8', family: 4 }, { address: '127.0.0.1', family: 4 }], request }), { code: 'chatbot_invalid_url' });
  assert.equal(requested, 1);
});
test('ingestion removes active HTML, chunks whole document and indexes only once', async () => {
  let calls = 0;
  const body = '<main><h1>Nutrition</h1><p>' + 'Vegetables provide fiber. '.repeat(150) + '</p><script>steal()</script></main>';
  const result = await ingestSource({ url: 'https://www.cdc.gov/nutrition/' }, {
    fetcher: async () => ({ buffer: Buffer.from(body), type: 'text/html', url: 'https://www.cdc.gov/nutrition/' }),
    embedder: async texts => { calls++; return texts.map(() => Array(768).fill(.1)); }
  });
  assert.equal(calls, 1); assert.ok(result.chunks.length > 1);
  assert.ok(result.chunks.every(c => c.text.length <= 1400 && !c.text.includes('steal')));
  assert.equal(result.title, 'Nutrition'); assert.equal(result.sha256.length, 64);
  assert.equal(result.metadata.embeddingDimensions, 768);
  assert.equal(plain('<script>x</script><p>A &amp; B</p>'), 'A & B');
  assert.ok(splitText('abcd '.repeat(1000)).every(s => s.length <= 1400));
});
test('retrieval uses only ready enabled sources, excludes nonpublic internal content and bounds model context', async () => {
  const queries = [];
  const db = { query: async sql => {
    queries.push(sql);
    if (sql.includes('chatbot_search')) throw Object.assign(new Error('legacy schema'), { code: 'ER_NO_SUCH_TABLE' });
    if (sql.includes('chatbot_chunk')) return [Array.from({ length: 15 }, (_, i) => ({ id: i, source_id: 'trusted', title: 'Health', content: 'Nutrition facts '.repeat(200), embedding: Array(768).fill(.1), url: 'https://www.cdc.gov/nutrition/' }))];
    return [[]];
  } };
  const result = await retrieveContext({ db, question: 'Salud y nutrición', locale: 'es', embedder: async () => [Array(768).fill(.1)] });
  assert.equal(result.length, 3); // Diversity cap per uploaded source.
  assert.ok(result.every(c => c.text.length <= 1400 && !('embedding' in c)));
  assert.ok(queries.some(q => q.includes("s.enabled=1 AND s.status='ready'")));
  assert.ok(queries.some(q => q.includes('article_status_id=2')));
  assert.ok(queries.some(q => q.includes('is_active=1')));
  assert.ok(queries.every(q => !q.includes('FROM user') && !q.includes('health_event_answer')));
});
function pagedKnowledgeFixture(total, relevantOrdinal, fragmentsPerSource = 40) {
  const reads = [];
  const db = { query: async (sql, params = []) => {
    if (sql.includes('chatbot_search')) throw Object.assign(new Error('legacy schema'), { code: 'ER_NO_SUCH_TABLE' });
    if (!sql.includes('chatbot_chunk')) return [[]];
    assert.match(sql, /s.enabled=1 AND s.status='ready'/);
    assert.match(sql, /c.id>\? ORDER BY c.id LIMIT 500/);
    const after = params[0];
    reads.push(after);
    // Leave gaps in IDs, as deletion/reindexing does, to exercise keyset paging.
    const start = after / 2;
    return [Array.from({ length: Math.min(500, total - start) }, (_, index) => {
      const ordinal = start + index + 1;
      return { id: ordinal * 2, source_id: `reference-${Math.ceil(ordinal / fragmentsPerSource)}`,
        title: 'Reference', content: ordinal === relevantOrdinal ? 'Targettopic evidence' : 'Unrelated passage',
        embedding: null, url: 'https://www.cdc.gov/' };
    })];
  } };
  return { db, reads };
}

test('retrieval finds evidence after 500 sources and the former 20,000-fragment cutoff', async () => {
  const f = pagedKnowledgeFixture(24001, 24001);
  let embeddingCalls = 0;
  const result = await retrieveContext({ db: f.db, question: 'Targettopic', embedder: async () => {
    embeddingCalls++;
    throw new Error('Temporary embedding outage');
  } });
  assert.deepEqual(result.map(source => source.id), ['source:reference-601:48002']);
  assert.equal(result[0].text, 'Targettopic evidence');
  assert.equal(embeddingCalls, 1);
  assert.equal(f.reads.length, 49);
  assert.equal(f.reads.at(-1), 48000);
  assert.ok(f.reads.every((after, index) => after === index * 1000));
});

test('retrieval covers the expanded corpus limit without fetching an unused extra batch', async () => {
  const f = pagedKnowledgeFixture(MAX_TOTAL_CHUNKS + 1, MAX_TOTAL_CHUNKS, 200);
  const result = await retrieveContext({ db: f.db, question: 'Targettopic', embedder: async () => [[1, 0]] });
  assert.equal(result.length, 1);
  assert.equal(result[0].id, `source:reference-${Math.ceil(MAX_TOTAL_CHUNKS / 200)}:${MAX_TOTAL_CHUNKS * 2}`);
  assert.equal(f.reads.length, MAX_TOTAL_CHUNKS / 500);
  assert.equal(f.reads.at(-1), (MAX_TOTAL_CHUNKS - 500) * 2);
  assert.ok(result.every(source => source.text.length <= 1400 && !('embedding' in source)));
});

function fixturePdf() {
  const text = 'Healthy meals include vegetables, whole grains and a variety of foods. Ask your healthcare professional for individual guidance.';
  const stream = `BT /F1 12 Tf 40 700 Td (${text}) Tj ET`;
  const objects = ['<< /Type /Catalog /Pages 2 0 R >>', '<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
    '<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>',
    '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>', `<< /Length ${Buffer.byteLength(stream)} >>\nstream\n${stream}\nendstream`];
  let pdf = '%PDF-1.4\n'; const offsets = [0];
  objects.forEach((value, i) => { offsets.push(Buffer.byteLength(pdf)); pdf += `${i + 1} 0 obj\n${value}\nendobj\n`; });
  const start = Buffer.byteLength(pdf);
  pdf += `xref\n0 6\n0000000000 65535 f \n${offsets.slice(1).map(o => String(o).padStart(10, '0') + ' 00000 n ').join('\n')}\ntrailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n${start}\n%%EOF`;
  return Buffer.from(pdf);
}
test('PDF extraction handles actual searchable PDF bytes in isolated worker', async () => {
  const result = await extractPdf(fixturePdf());
  assert.match(result.text, /Healthy meals include vegetables/);
  assert.equal(result.pages, 1);
  assert.throws(() => extractPdf(Buffer.from('not a pdf')), { code: 'chatbot_invalid_pdf' });
});
module.exports = { fixturePdf };

function internalFixture({ articles = [], resources = [], calendar = [], external = [] } = {}) {
  const calls = [];
  return { calls, query: async (sql, params = []) => {
    calls.push({ sql, params });
    if (sql.includes('chatbot_search')) throw Object.assign(new Error('legacy schema'), { code: 'ER_NO_SUCH_TABLE' });
    if (sql.includes('chatbot_chunk')) return [external];
    const rows = sql.includes('FROM article a') ? articles : sql.includes('FROM trusted_resources r') ? resources : calendar;
    return [rows.filter(row => row.id > params.at(-1)).slice(0, 200)];
  } };
}

test('default distribution questions list every published date beyond eight passages and 120 days, including cancellations', async () => {
  const calendar = Array.from({ length: 425 }, (_, i) => ({ id: i + 1, date: `2027-01-${String(i % 25 + 1).padStart(2, '0')}`, no_distribution: i % 25 === 24 ? '1' : '0', time: '9:00 AM' }));
  for (const question of ['¿Cuáles son las próximas distribuciones?', 'What are the upcoming distributions?']) {
    const db = internalFixture({ calendar });
    const { directAnswer } = await retrieveContext({ db, question, locale: 'es', embedder: () => assert.fail('calendar must not use external search') });
    for (let day = 1; day <= 25; day++) assert.ok(directAnswer.content.includes(`2027-01-${String(day).padStart(2, '0')}`));
    assert.match(directAnswer.content, /2027-01-25: 17 avisos sin distribución/);
    assert.equal(directAnswer.validationSources.length, 425);
    assert.equal(directAnswer.sources[0].url, '/calendar');
    assert.equal(db.calls.length, 3);
    assert.ok(db.calls.every(({ sql }) => !sql.includes('120 DAY') && !sql.includes('CURDATE()')));
  }
});

test('calendar uses California date at the UTC midnight boundary', async () => {
  const { calendarRows } = require('./chatbotKnowledge');
  const db = internalFixture();
  await calendarRows(db, new Date('2026-10-31T01:00:00Z'));
  assert.equal(db.calls[0].params[0], '2026-10-30');
});

test('resource search covers more than 600 resources and uses city and service metadata', async () => {
  const resources = Array.from({ length: 1075 }, (_, i) => ({ id: i + 1, title_english: `Center ${i}`, city: i === 1074 ? 'Los Angeles - Downtown/East Side' : 'Banning',
    services: i === 1074 ? 'Mental health counseling Salud mental' : 'Housing Vivienda', information_english: 'Public community services.' }));
  const db = internalFixture({ resources });
  const result = await retrieveContext({ db, question: 'Mental health resources in Los Angeles', location: 'Los Angeles' });
  assert.equal(result.length, 1);
  assert.match(result[0].id, /^resource:1075:/);
  assert.equal(result[0].url, '/trusted-resource/1075');
  assert.match(result[0].text, /Mental health counseling/);
  assert.equal(db.calls.filter(c => c.sql.includes('FROM trusted_resources r')).length, 6);
  assert.ok(db.calls.every(c => !c.sql.includes('chatbot_chunk')));
});

test('resource details after the first 1400 characters remain searchable', async () => {
  const db = internalFixture({ resources: [{ id: 1, title_english: 'Community center', information_english: 'General introduction. '.repeat(150) + 'Specialized audiology services.', city: 'Los Angeles' }] });
  const result = await retrieveContext({ db, question: 'audiology resources' });
  assert.equal(result.length, 1);
  assert.match(result[0].text, /Specialized audiology/);
  assert.ok(result[0].text.length <= 1400);
});

test('unknown cities and ZIPs do not silently return options from a different city', async () => {
  const db = internalFixture({ resources: [{ id: 1, title_english: 'Health clinic', city: 'Banning', address: '123 Main St 92220' }] });
  for (const location of ['Los Angeles', '99999']) {
    const result = await retrieveContext({ db, question: `Health resources in ${location}`, location });
    assert.equal(result[0].id, 'navigation:resources');
    assert.equal(result[0].url, '/trusted-resources');
    assert.match(result[0].text, /No matching active Community resources/);
  }
});

test('internal articles matched by tags remain available ahead of high-scoring external sources', async () => {
  const db = internalFixture({ articles: [{ id: 1, title_en: 'Healthy living', slug_en: 'healthy-living', tags: 'diabetes', content_en: 'A balanced diet can support general health.' }],
    external: Array.from({ length: 10 }, (_, i) => ({ id: i + 1, source_id: `s${i}`, title: 'Diabetes health', content: 'Diabetes health information.', embedding: [1, 0] })) });
  const result = await retrieveContext({ db, question: 'Tell me about diabetes', embedder: async () => [[1, 0]] });
  assert.equal(result[0].id, 'article:1:0');
  assert.equal(result[0].url, '/article/healthy-living');
  assert.match(result[0].text, /diabetes/);
  assert.equal(result.length, 8);
});

test('changing from calendar to default health resources does not inherit calendar intent', async () => {
  const db = internalFixture({ resources: [{ id: 1, title_english: 'Health services', information_english: 'Community health resources.' }] });
  const result = await retrieveContext({ db, question: 'Where can I find health resources?', history: [{ role: 'user', text: 'What are the upcoming distributions?' }] });
  assert.match(result[0].id, /^resource:/);
  assert.ok(db.calls.every(c => !c.sql.includes('calendar_event')));
});

test('a city answer continues a resource search after the assistant requests location', async () => {
  const db = internalFixture({ resources: [{ id: 1, title_english: 'Health services', city: 'Los Angeles' }] });
  const result = await retrieveContext({ db, question: 'Los Angeles', location: 'Los Angeles', history: [
    { role: 'user', text: 'Health resources near me' }, { role: 'assistant', text: 'Which city or ZIP code would you like to search?' }
  ] });
  assert.match(result[0].id, /^resource:/);
  assert.ok(db.calls.every(c => !c.sql.includes('chatbot_chunk')));
});

test('explicit article discovery cites internal articles without consulting external pages', async () => {
  const db = internalFixture({ articles: [{ id: 1, title_en: 'Healthy living', slug_en: 'healthy-living', tags: 'diabetes', content_en: 'General educational content about diabetes.' }] });
  const result = await retrieveContext({ db, question: 'What articles do you have about diabetes?' });
  assert.match(result[0].id, /^article:/);
  assert.ok(db.calls.every(c => !c.sql.includes('chatbot_chunk')));
});

test('food received at a distribution retrieves cooking evidence instead of calendar dates or pharmacies', async () => {
  const db = internalFixture({ calendar: [{ id: 1, date: '2026-11-03', no_distribution: 0 }],
    resources: [{ id: 1, title_spanish: 'Farmacia', information_spanish: 'Recetas médicas.' }],
    external: [{ id: 1, source_id: 'fruit', title: 'Fruit recipes', content: 'Strawberries in a fruit salad.', embedding: null }] });
  const result = await retrieveContext({ db, question: 'Me dieron muchas fresas en el último reparto. ¿Qué puedo hacer con ellas?', embedder: async () => { throw Error('offline'); } });
  assert.ok(result.some(source => source.id === 'source:fruit:1'));
  assert.ok(result.every(source => !/^(calendar|resource):/.test(source.id)));
  assert.ok(db.calls.every(call => !call.sql.includes('calendar_event')));
  assert.ok(db.calls.every(call => !call.sql.includes('FROM trusted_resources')));
});

test('regional ingredient aliases retrieve exact ingredient evidence without an embedding provider', async () => {
  const db = internalFixture({ external: [
    { id: 1, source_id: 'pineapple', title: 'Pineapple recipes', content: 'Pineapple fruit salad.', embedding: null },
    { id: 2, source_id: 'banana', title: 'Banana recipes', content: 'Banana bread recipe.', embedding: null }
  ] });
  for (const question of ['recetas con ananá', 'recetas con piña']) {
    const result = await retrieveContext({ db, question, embedder: async () => { throw Error('offline'); } });
    assert.equal(result[0].id, 'source:pineapple:1');
  }
});

test('a new food question does not inherit a previous calendar location prompt', async () => {
  const db = internalFixture({ external: [{ id: 1, source_id: 'eggplant', title: 'Eggplant dip', content: 'Eggplant recipe preparation.', embedding: null }] });
  const result = await retrieveContext({ db, question: 'Gracias, recibimos muchas berenjenas. ¿Cómo podemos prepararlas o conservarlas?', history: [
    { role: 'user', text: '¿Cuáles son las próximas distribuciones?' },
    { role: 'assistant', text: 'Consulta el calendario. ¿En qué ciudad o código postal buscas?' }
  ], embedder: async () => { throw Error('offline'); } });
  assert.equal(result[0].id, 'source:eggplant:1');
  assert.ok(db.calls.every(call => !call.sql.includes('calendar_event')));
});

test('parks, senior centers and educational centers use the internal directory', async () => {
  for (const [question, title, services] of [
    ['Qué parques gratuitos hay en San Bernardino?', 'Central Park', 'Recreation park'],
    ['recomendación de centros para adultos mayores', 'Senior Center', 'Senior activities'],
    ['un listado de centros educativos', 'Community School', 'Educational programs']
  ]) {
    const db = internalFixture({ resources: [{ id: 1, title_english: title, services, city: 'San Bernardino' }] });
    const result = await retrieveContext({ db, question });
    assert.match(result[0].id, /^resource:/);
    assert.ok(db.calls.every(call => !call.sql.includes('chatbot_chunk')));
  }
});

test('a request for actual senior-center locations keeps the previous directory topic', async () => {
  const db = internalFixture({ resources: [{ id: 1, title_english: 'Senior Center', services: 'Senior activities' }] });
  const result = await retrieveContext({ db, question: 'pero donde me recomendaste ir? dame opciones de lugares', history: [
    { role: 'user', text: 'recomendación de centros para adultos mayores' },
    { role: 'assistant', text: '¿En qué ciudad o código postal buscas?' }
  ] });
  assert.match(result[0].id, /^resource:/);
});

test('a new health topic after a location question does not inherit resource discovery', async () => {
  const db = internalFixture({ external: [{ id: 1, source_id: 'influenza', title: 'Influenza', content: 'Influenza general education.', embedding: null }] });
  const result = await retrieveContext({ db, question: 'Información general sobre influenza', history: [
    { role: 'user', text: '¿Dónde puedo encontrar recursos de salud?' },
    { role: 'assistant', text: '¿En qué ciudad o código postal buscas?' }
  ], embedder: async () => { throw Error('offline'); } });
  assert.equal(result[0].id, 'source:influenza:1');
});

test('food storage follow-ups preserve the ingredient while a new recipe ingredient replaces it', () => {
  const { searchQuestion } = require('./chatbotNavigation');
  const history = [{ role: 'user', text: 'recetas con fresas' }, { role: 'assistant', text: '¿Quieres saber cómo guardarlas?' }];
  assert.match(searchQuestion('¿Y cómo conservarlas?', history), /fresas/);
  assert.match(searchQuestion('¿Cómo guardarlas?', history), /fresas/);
  assert.match(searchQuestion('Más recetas', history), /fresas/);
  assert.equal(searchQuestion('Y recetas con ananá', history), 'Y recetas con ananá');
});


test('indexed retrieval uses translated search terms, reranks candidates and never scans the corpus', async () => {
  const calls=[];
  const db={query:async(sql,params)=>{
    calls.push({sql,params});
    if(sql.includes('MATCH('))return[[{chunk_id:300001,source_id:'pdf-late'}]];
    if(sql.includes('c.embedding')){
      assert.match(sql,/c.id IN/);
      return[[{id:300001,source_id:'pdf-late',title:'Guide',content:'Dietary fiber supports nutrition.',embedding:[1,0],url:null}]];
    }
    return[[]];
  }};
  const result=await retrieveContext({db,question:'Información sobre fibra',searchTerms:'fiber fibra dietary',embedder:async()=>[[1,0]]});
  assert.equal(result[0].id,'source:pdf-late:300001');
  assert.match(calls.find(call=>call.sql.includes('MATCH(')).params[0],/fiber/);
  assert.equal(calls.filter(call=>call.sql.includes('c.embedding')).length,1);
});


test('an indexed query without matches avoids a corpus scan and embedding request', async () => {
  let queries=0;
  const db={query:async sql=>{queries++;assert.ok(!sql.includes('c.embedding'));return[[]];}};
  const result=await retrieveContext({db,question:'Unmatchedtopic',embedder:()=>assert.fail('no candidates need embeddings')});
  assert.deepEqual(result,[]);assert.ok(queries>0);
});
