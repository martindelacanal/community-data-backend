'use strict';

const crypto = require('node:crypto');
const dns = require('node:dns').promises;
const https = require('node:https');
const path = require('node:path');
const { fork } = require('node:child_process');
const axios = require('axios');
const cheerio = require('cheerio');
const ipaddr = require('ipaddr.js');

const MAX_BYTES = 10 * 1024 * 1024;
const MAX_TEXT = 1000000;
const EMBEDDING_MODEL = 'gemini-embedding-001';
const SITE = 'https://bienestarcommunity.org';
function fail(code, status = 400) { return Object.assign(new Error(code), { code, status }); }
function plain(value) {
  const $ = cheerio.load(String(value || ''));
  $('script,style,noscript,iframe,svg,nav,footer,header,form,[hidden]').remove();
  $('br,p,div,li,h1,h2,h3,h4,tr,section').append('\n');
  return $.root().text().replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').replace(/[ \t]+/g, ' ').replace(/\n\s*\n/g, '\n').trim();
}
function publicAddress(address) {
  try {
    let ip = ipaddr.parse(address);
    if (ip.kind() === 'ipv6' && ip.isIPv4MappedAddress()) ip = ip.toIPv4Address();
    return ip.range() === 'unicast';
  } catch { return false; }
}
function validateUrl(raw) {
  let url;
  try { url = new URL(raw); } catch { throw fail('chatbot_invalid_url'); }
  const host = url.hostname.replace(/^\[|\]$/g, '').toLowerCase();
  if (url.protocol !== 'https:' || url.username || url.password || (url.port && url.port !== '443')
    || !host.includes('.') || host.endsWith('.') || /(^|\.)(localhost|local|internal|test|invalid|example)$/.test(host)
    || (ipaddr.isValid(host) && !publicAddress(host)) || raw.length > 2048) throw fail('chatbot_invalid_url');
  url.hash = '';
  return url;
}
async function fetchPage(raw, { lookup = dns.lookup, request = axios.get } = {}, redirects = 0) {
  const url = validateUrl(raw);
  let addresses;
  try { addresses = await lookup(url.hostname, { all: true, verbatim: true }); } catch { throw fail('chatbot_source_unreachable'); }
  if (!addresses.length || addresses.some(({ address }) => !publicAddress(address))) throw fail('chatbot_invalid_url');
  const selected = addresses[0];
  // Pin the approved resolution for the TLS socket: no second DNS lookup/rebinding.
  const agent = new https.Agent({ keepAlive: false, lookup: (host, options, cb) => {
    if (options?.all) cb(null, [selected]); else cb(null, selected.address, selected.family);
  } });
  let response;
  try {
    response = await request(url.href, { httpsAgent: agent, proxy: false, maxRedirects: 0,
      timeout: 12000, maxContentLength: MAX_BYTES, maxBodyLength: MAX_BYTES, responseType: 'arraybuffer',
      headers: { 'User-Agent': 'BienestarCommunity-Knowledge/1.0', Accept: 'text/html,application/pdf,text/plain' },
      validateStatus: status => status >= 200 && status < 400 });
  } catch { throw fail('chatbot_source_unreachable'); }
  finally { agent.destroy(); }
  if (response.status >= 300) {
    if (redirects >= 3 || !response.headers.location) throw fail('chatbot_source_unreachable');
    return fetchPage(new URL(response.headers.location, url).href, { lookup, request }, redirects + 1);
  }
  const buffer = Buffer.from(response.data);
  if (buffer.length > MAX_BYTES) throw fail('chatbot_source_too_large', 413);
  const type = String(response.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
  if (!['text/html', 'application/xhtml+xml', 'application/pdf', 'text/plain'].includes(type)) throw fail('chatbot_source_type');
  return { buffer, type, url: url.href };
}
function extractPdf(buffer) {
  if (buffer.subarray(0, 5).toString() !== '%PDF-') throw fail('chatbot_invalid_pdf');
  return new Promise((resolve, reject) => {
    const worker = fork(path.join(__dirname, 'chatbotPdfWorker.js'), [], {
      execArgv: ['--max-old-space-size=192'], stdio: ['ignore', 'ignore', 'ignore', 'ipc'], serialization: 'advanced',
      windowsHide: true
    });
    let settled = false;
    const finish = (error, value) => {
      if (settled) return; settled = true; clearTimeout(timer); worker.kill();
      if (error) reject(error); else resolve(value);
    };
    const timer = setTimeout(() => finish(fail('chatbot_pdf_timeout')), 30000);
    worker.once('message', result => result.error ? finish(fail(result.error)) : finish(null, result));
    worker.once('error', () => finish(fail('chatbot_invalid_pdf')));
    worker.once('exit', () => { if (!settled) finish(fail('chatbot_invalid_pdf')); });
    worker.send({ buffer, maxText: MAX_TEXT });
  });
}
function splitText(text, size = 1400, overlap = 180) {
  const chunks = [];
  for (let start = 0; start < text.length;) {
    let end = Math.min(start + size, text.length);
    if (end < text.length) { const boundary = text.lastIndexOf(' ', end); if (boundary > start + size / 2) end = boundary; }
    const value = text.slice(start, end).trim(); if (value) chunks.push(value);
    if (end === text.length) break;
    start = end - overlap;
  }
  return chunks;
}
async function embed(texts, taskType, env = process.env, http = axios) {
  const key = env.CHATBOT_GEMINI_API_KEY || env.GEMINI_API_KEY;
  if (!key) throw fail('chatbot_not_configured', 503);
  const result = [];
  // Batch indexing embeds each fragment once; chat only embeds the short question.
  for (let i = 0; i < texts.length; i += 100) {
    try {
      const response = await http.post(`https://generativelanguage.googleapis.com/v1beta/models/${EMBEDDING_MODEL}:batchEmbedContents`, {
        requests: texts.slice(i, i + 100).map(text => ({ model: `models/${EMBEDDING_MODEL}`,
          content: { parts: [{ text }] }, taskType, outputDimensionality: 768 }))
      }, { headers: { 'x-goog-api-key': key }, timeout: 25000, maxContentLength: 4 * 1024 * 1024 });
      const vectors = response.data.embeddings?.map(e => e.values);
      if (!Array.isArray(vectors) || vectors.length !== Math.min(100, texts.length - i)
        || vectors.some(v => !Array.isArray(v) || v.length !== 768 || v.some(x => !Number.isFinite(x)))) throw Error();
      result.push(...vectors);
    } catch { throw fail('chatbot_index_unavailable', 503); }
  }
  return result;
}
async function ingestSource({ file, url, title }, { env = process.env, fetcher = fetchPage, embedder = embed } = {}) {
  let buffer, type, sourceUrl = null, filename = null, detectedTitle;
  if (file) {
    if (url || !Buffer.isBuffer(file.buffer) || file.buffer.length > MAX_BYTES) throw fail('chatbot_source_too_large', 413);
    buffer = file.buffer; type = 'application/pdf'; filename = path.basename(file.originalname || 'document.pdf').slice(0, 255);
  } else {
    const page = await fetcher(url); ({ buffer, type } = page); sourceUrl = page.url;
  }
  let text, pages;
  if (type === 'application/pdf') { const pdf = await extractPdf(buffer); text = pdf.text; pages = pdf.pages; }
  else if (type === 'text/plain') text = buffer.toString('utf8');
  else {
    const $ = cheerio.load(buffer.toString('utf8'));
    detectedTitle = $('h1').first().text() || $('title').text();
    const main = $('main,article,[role="main"]').first();
    text = plain(main.length ? main.html() : $.html());
  }
  text = String(text || '').trim();
  if (text.length < 80) throw fail('chatbot_source_no_text');
  if (text.length > MAX_TEXT) throw fail('chatbot_source_too_large', 413);
  const pieces = splitText(text);
  const vectors = await embedder(pieces, 'RETRIEVAL_DOCUMENT', env);
  return { title: plain(title || detectedTitle || filename || new URL(sourceUrl).hostname).slice(0, 200),
    kind: type === 'application/pdf' ? 'pdf' : 'url', url: sourceUrl, filename,
    sha256: crypto.createHash('sha256').update(buffer).digest('hex'),
    chunks: pieces.map((content, index) => ({ text: content, embedding: vectors[index] })),
    metadata: { pages: pages || null, characters: text.length, bytes: buffer.length,
      embeddingModel: EMBEDDING_MODEL, embeddingDimensions: 768, fetchedAt: new Date().toISOString() } };
}
const STOP = new Set('what when where how which with from this that have does about there their please can could would should que como donde cuando cual para del los las una unos unas por con mis sus hay quiero saber sobre puedes puedo necesito me the and for are is to en de el la un a y mi do i'.split(' '));
function termsFor(question) {
  const terms = String(question).normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase().match(/[a-z0-9]{3,}/g) || [];
  const groups = ['food comida alimentos alimentacion nutrition nutricion', 'health salud medical medico',
    'distribution distribucion reparto despensa calendar calendario upcoming proximas proxima next',
    'resource recursos resources ayuda help', 'diabetes diabetic diabetico', 'children ninos infancia',
    'blood pressure presion hipertension hypertension', 'exercise ejercicio actividad'];
  const found = new Set(terms.filter(t => !STOP.has(t)));
  for (const group of groups) { const values = group.split(' '); if (values.some(v => found.has(v))) values.forEach(v => found.add(v)); }
  return [...found].slice(0, 40);
}
function lexical(text, terms) {
  const value = String(text).normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase();
  return terms.reduce((n, term) => n + (value.includes(term) ? 1 : 0), 0) / Math.max(terms.length, 1);
}
function cosine(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return 0;
  let dot = 0, aa = 0, bb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; aa += a[i] ** 2; bb += b[i] ** 2; }
  return dot / (Math.sqrt(aa * bb) || 1);
}
async function internalContext(db, locale, terms) {
  const es = locale === 'es';
  const [[articles], [resources], [calendar]] = await Promise.all([
    db.query(`SELECT id,title_en,title_es,subtitle_en,subtitle_es,LEFT(content_en,60000) content_en,
      LEFT(content_es,60000) content_es,slug_en,slug_es FROM article WHERE article_status_id=2
      AND (publication_date IS NULL OR publication_date<=NOW()) ORDER BY publication_date DESC LIMIT 400`),
    db.query(`SELECT id,title_english,title_spanish,description_english,description_spanish,
      information_english,information_spanish,address,phone_number,website FROM trusted_resources WHERE is_active=1 ORDER BY id LIMIT 600`),
    db.query(`SELECT ce.id,DATE_FORMAT(ce.date,'%Y-%m-%d') date,ce.time,ce.no_distribution,ce.message_en,ce.message_es,
      l.organization,l.community_city,l.address FROM calendar_event ce JOIN location l ON l.id=ce.location_id
      WHERE ce.enabled='Y' AND l.enabled='Y' AND ce.date>=CURDATE() AND ce.date<=DATE_ADD(CURDATE(),INTERVAL 120 DAY)
      ORDER BY ce.date,ce.time LIMIT 400`)
  ]);
  const result = [];
  for (const a of articles) {
    const title = (es ? a.title_es : a.title_en) || a.title_en || a.title_es;
    const text = plain([a.title_en, a.title_es, es ? (a.content_es || a.content_en) : (a.content_en || a.content_es)].join('\n'));
    const chunks = splitText(text).map((text, i) => ({ id: `article:${a.id}:${i}`, title,
      url: `${SITE}/article/${encodeURIComponent((es ? a.slug_es : a.slug_en) || a.slug_en || a.slug_es)}`, text,
      score: lexical(text, terms) + lexical(title, terms) * .5 }));
    result.push(...chunks.sort((a, b) => b.score - a.score).slice(0, 2));
  }
  for (const r of resources) {
    const title = (es ? r.title_spanish : r.title_english) || r.title_english || r.title_spanish;
    const text = plain([r.title_english, r.title_spanish,
      es ? (r.description_spanish || r.description_english) : (r.description_english || r.description_spanish),
      es ? (r.information_spanish || r.information_english) : (r.information_english || r.information_spanish),
      r.address, r.phone_number, r.website].filter(Boolean).join('\n')).slice(0, 1400);
    result.push({ id: `resource:${r.id}`, title, url: `${SITE}/trusted-resource/${r.id}`, text,
      score: lexical(text, terms) + lexical(title, terms) * .5 });
  }
  const calendarIntent = terms.some(t => ['calendar', 'calendario', 'distribution', 'distribucion', 'reparto', 'proxima', 'upcoming'].includes(t));
  for (const c of calendar) {
    const title = `${c.organization} · ${c.date}`;
    const text = `${c.no_distribution ? 'NO DISTRIBUTION / SIN REPARTO' : 'Food distribution / Distribución de alimentos'}\n${c.date} ${c.time || ''} (America/Los_Angeles)\n${c.organization}\n${c.community_city}\n${c.address}\n${c.message_es || ''}\n${c.message_en || ''}`;
    result.push({ id: `calendar:${c.id}`, title, url: `${SITE}/calendar`, text: text.slice(0, 1400),
      score: lexical(text, terms) + (calendarIntent ? 2 : 0) });
  }
  if (calendarIntent && !calendar.length) result.push({ id: 'calendar:current', title: es ? 'Calendario de distribuciones' : 'Distribution calendar',
    url: `${SITE}/calendar`, text: 'The current published calendar has no enabled distributions from today through the next 120 days. Do not infer dates or claim there will never be distributions. Invite the user to check the calendar again.', score: 2 });
  return result;
}
async function retrieveContext({ db, question, history = [], locale = 'en', env = process.env, embedder = embed }) {
  // Short follow-ups ("and where is it?") need the preceding user topic to retrieve the same evidence.
  const previous = [...history].reverse().find(m => m.role === 'user');
  const search = question.length < 100 && previous ? `${String(previous.text || previous.content || '').slice(0, 1200)}\n${question}` : question;
  const terms = termsFor(search);
  const readBatch = after => db.query(`SELECT c.id,c.source_id,c.content,c.embedding,s.title,s.url FROM chatbot_chunk c
    JOIN chatbot_source s ON s.id=c.source_id WHERE s.enabled=1 AND s.status='ready' AND c.id>? ORDER BY c.id LIMIT 500`, [after]);
  let [stored] = await readBatch(0);
  const internal = await internalContext(db, locale, terms);
  const resourceIntent = /\b(recursos?|resources?|servicios?|services?)\b/i.test(search);
  if (resourceIntent) internal.forEach(c => { if (c.id.startsWith('resource:')) c.score += 1; });
  let vector;
  if (stored.length) { try { [vector] = await embedder([search], 'RETRIEVAL_QUERY', env); } catch { /* Lexical retrieval remains available on a temporary embedding outage. */ } }
  let external = [], scanned = 0;
  while (stored.length && scanned < 20000) {
  const ranked = stored.map(c => {
    let embedding = c.embedding;
    if (typeof embedding === 'string') { try { embedding = JSON.parse(embedding); } catch { embedding = null; } }
    const semantic = vector ? cosine(vector, embedding) : 0;
    return { id: `source:${c.source_id}:${c.id}`, sourceId: c.source_id, title: c.title,
      url: c.url, text: c.content.slice(0, 1400),
      score: lexical(`${c.title} ${c.content}`, terms) + (semantic >= .45 ? semantic * .8 : 0) };
  });
  // Never load the full vector corpus into RAM: bounded pages plus the best diverse passages.
  const perSource = new Map();
  external = [...external, ...ranked].sort((a,b) => b.score-a.score).filter(c => {
    const count = perSource.get(c.sourceId) || 0;
    perSource.set(c.sourceId,count+1); return count<3;
  }).slice(0,64);
  scanned += stored.length;
  if (stored.length < 500) break;
  [stored] = await readBatch(stored[stored.length-1].id);
  }
  const selected = [], counts = new Map();
  for (const candidate of [...internal, ...external].filter(c => c.score > 0).sort((a, b) => b.score - a.score)) {
    const key = candidate.sourceId || candidate.id.split(':').slice(0, 2).join(':');
    if ((counts.get(key) || 0) >= 3) continue;
    counts.set(key, (counts.get(key) || 0) + 1);
    const { score, ...safe } = candidate; selected.push(safe); if (selected.length === 8) break;
  }
  return selected;
}

module.exports = { MAX_BYTES, MAX_TEXT, ingestSource, retrieveContext, publicAddress, validateUrl, fetchPage,
  splitText, plain, embed, cosine, termsFor, internalContext, extractPdf };
