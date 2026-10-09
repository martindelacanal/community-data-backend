'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { publicAddress, validateUrl, fetchPage, ingestSource, splitText, plain, retrieveContext, extractPdf } = require('./chatbotKnowledge');

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
    if (sql.includes('chatbot_chunk')) return [Array.from({ length: 15 }, (_, i) => ({ id: i, source_id: 'trusted', title: 'Health', content: 'Nutrition facts '.repeat(200), embedding: Array(768).fill(.1), url: 'https://www.cdc.gov/nutrition/' }))];
    return [[]];
  } };
  const result = await retrieveContext({ db, question: 'Salud y nutrición', locale: 'es', embedder: async () => [Array(768).fill(.1)] });
  assert.equal(result.length, 3); // Diversity cap per uploaded source.
  assert.ok(result.every(c => c.text.length <= 1400 && !('embedding' in c)));
  assert.ok(queries[0].includes("s.enabled=1 AND s.status='ready'"));
  assert.ok(queries.some(q => q.includes('article_status_id=2')));
  assert.ok(queries.some(q => q.includes('is_active=1')));
  assert.ok(queries.every(q => !q.includes('FROM user') && !q.includes('health_event_answer')));
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
