'use strict';
const { normalize, matchesLocation } = require('./chatbotNavigation');
const { publicHomeHealthEvents, toSqlDateString } = require('./healthEventPublicHome');

const EVENT_TYPE = '(?:health (?:fairs?|events?)|(?:ferias?|eventos?|jornadas?) de (?:la )?salud)';
const EVENT_PREFIX = '(?:(?:any|upcoming|next|when is|where is|what is the next|show me|are there|is there|when and where is|hay|habra|cual es|cuando es|donde es|muestrame|tienen|algun|alguna|una|an?|the|la|el|un|proxim[oa]s?) )*';
const PLACE = '([a-z0-9]+(?: [a-z0-9]+){0,4})';
const LISTING = new RegExp('^' + EVENT_PREFIX + EVENT_TYPE + '(?: (?:in|en) ' + PLACE + ')?$');
const NAMED_FAIR = new RegExp('^' + PLACE + ' health fair$');

function healthFairRequest(question) {
  const text = normalize(question).replace(/^(?:hola|hello|hi) /, '').replace(/ (?:please|por favor)$/, '')
    .replace(/ (?:de |of |at )bienestar(?: community)?$/, '');
  const nearby = / (?:near me|nearby|in my area|cerca de mi|en mi zona)$/;
  const listing = LISTING.exec(text.replace(nearby, ''));
  if (listing) {
    const place = listing[1] || '';
    if (/\b(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|january|february|march|april|may|june|july|august|september|october|november|december|week|month|year|mes|semana|ano|spring|summer|winter|fall|20\d\d)\b/.test(place)) return null;
    return { place, needsLocation:nearby.test(text) && !place };
  }
  // A bare title such as "Riverside Health Fair" is a listing lookup. Explanations and
  // questions about personal care must keep their normal scope and retrieval checks.
  const named = NAMED_FAIR.exec(text);
  if (named && /^[A-ZÀ-Þ]/u.test(question.trim()) &&
      !/\b(?:what|how|why|my|your|is|are|do|did|can|the|a|at|to|about|missed|cancelled|free|register|attend|organize|plan)\b/u.test(named[1])) {
    return {place:named[1], named:true};
  }
  return null;
}

async function healthFairContext(db, place, locale, listing) {
  const es = locale === 'es';
  const events = (await publicHomeHealthEvents(db)).filter(event =>
    typeof event.slug === 'string' && /^[a-z0-9][a-z0-9_-]*$/i.test(event.slug) &&
    (matchesLocation(event.community_city, event.address, place) ||
      (listing?.named && [event.name_es, event.name_en].some(name => (' '+normalize(name)+' ').includes(' '+normalize(place)+' ')))));
  if (!events.length) {
    const title = es ? 'Portada de Bienestar' : 'Bienestar home';
    const content = place
      ? (es ? 'No encontré una feria de salud publicada en la portada que coincida con ese lugar o nombre.' : 'I found no health fair on the public home page matching that place or name.')
      : (es ? 'En este momento no hay eventos de salud vigentes anunciados en la portada.' : 'There are currently no upcoming or ongoing health events listed on the public home page.');
    return {directAnswer:{content, status:'complete', sources:[{id:'navigation:health-events',url:'/home',title}], citationIds:['navigation:health-events']}};
  }
  const shown = events.slice(0, 6);
  const sources = shown.map(ev => ({id:'health-event:'+ev.id,url:'/health-event/'+encodeURIComponent(ev.slug),
    title:(es ? ev.name_es : ev.name_en) || ev.name_en || ev.name_es || (es ? 'Feria de salud' : 'Health fair')}));
  if (!listing) {
    // These fields are public on the event landing page. Keep account, booking and form data out of model inputs.
    return shown.map((ev, i) => ({...sources[i], text:JSON.stringify({
      name:sources[i].title, startDate:toSqlDateString(ev.start_date), endDate:toSqlDateString(ev.end_date),
      startTime:ev.start_time, endTime:ev.end_time, timezone:ev.timezone, location:ev.community_city, address:ev.address
    })}));
  }
  const lines = shown.map((ev, i) => {
    const start = toSqlDateString(ev.start_date), end = toSqlDateString(ev.end_date);
    const date = start === end ? start : [start,end].filter(Boolean).join(' – ');
    const hours = [ev.start_time, ev.end_time].filter(Boolean).map(t => String(t).slice(0,5)).join(' – ');
    return [sources[i].title, date, hours && [hours,ev.timezone].filter(Boolean).join(' '),
      ev.community_city, ev.address].filter(Boolean).join(' · ');
  });
  const intro = es ? 'Eventos de salud anunciados en la portada de Bienestar' : 'Health events listed on the Bienestar home page';
  const more = events.length > shown.length ? (es ? ' (primeros 6)' : ' (first 6)') : '';
  return {directAnswer:{content:intro+more+'\n'+lines.join('\n'), status:'complete', sources, citationIds:sources.map(s=>s.id)}};
}

function asksAboutProduceTotal(question, history = []) {
  const q = normalize(question).replace(/\b(?:distirbuted|distrbuted)\b/g,'distributed')
    .replace(/ (?:in total|en total|so far|hasta ahora|to date|historically|historicamente)$/,'');
  if (!/\b(?:pounds?|lbs?|libras)\b/u.test(q) || !/\b(?:food|produce|fruit|vegetables|comida|alimentos|frutas?|verduras?|productos)\b/u.test(q)) return false;
  // A cumulative organization milestone cannot answer personal allotments, other
  // organizations, individual sites or time-limited questions.
  if (/\b(?:per|each|family|familia|household|caja|box|recibo|receive|weekly|week|month|year|annually|daily|today|ayer|semana|semanal|mes|ano|anual|hoy|por|in|en|at|between|during|durante|20\d\d)\b/u.test(q)) return false;
  const subject = '(?:(?:bienestar(?: (?:community|wellbeing|is well being|is wellbeing))?|you|your organization|ustedes) )?';
  const food = '(?:(?:free|fresh|frescos?|frescas?|gratis|de|of|and|y) )*(?:food|produce|alimentos|comida|productos|frutas?|verduras?)(?: (?:y verduras|and vegetables|frescos|frescas))?';
  const unit = '(?:pounds?|lbs?|libras)(?: of| de)? ';
  const verb = '(?:distributed|distributed by bienestar(?: wellbeing)?|distribuido|distribuidos|distribuidas|repartido|repartidos|entregado)';
  const total = new RegExp('^(?:(?:how many|cuantas?|number of|total) )?'+unit+food+' (?:(?:has|have|ha|han) )?'+subject+verb+'$');
  const spanish = new RegExp('^(?:cuantas? )?'+unit+food+' (?:ha|han) (?:distribuido|repartido|entregado) (?:bienestar|ustedes)$');
  if (total.test(q) || spanish.test(q)) return true;
  // An abbreviated follow-up inherits only the immediately preceding user's topic.
  const previous = history.findLast(m=>m.role==='user');
  return Boolean(previous && new RegExp('^'+unit+food+'$').test(q) && asksAboutProduceTotal(previous.content || previous.text || ''));
}

function asksAboutWeeklyProduce(question) {
  const q = normalize(question);
  return /^(?:cuantas libras de (?:comida|alimentos|verduras|frutas) (?:reparten|distribuyen|reparte|distribuye) (?:por semana|semanalmente)(?: en bienestar(?: community)?)?|how many pounds of (?:food|produce) (?:does bienestar(?: community)?|do you) (?:give out|distribute) (?:weekly|each week|per week))$/.test(q);
}

// The bilingual story is published with the web app. Read its current copy from
// the fixed first-party origin so a new public milestone does not require a backend release.
const impactCache = new Map();
async function impactReply(locale, {fetcher = fetch, weekly = false} = {}) {
  const lang = locale === 'es' ? 'es' : 'en';
  const es = lang === 'es';
  const source = {id:'navigation:impact',url:'/who-we-are',title:es ? 'Quiénes somos · Nuestra historia' : 'Who we are · Our story'};
  if (weekly) {
    const content = es ? 'La sección «Nuestra historia» informa el total histórico de alimentos distribuidos. Esa fuente no confirma cuántas libras se reparten por semana.'
      : 'The “Our story” page reports the cumulative amount of food distributed. That source does not establish how many pounds are distributed per week.';
    return {content,status:'complete',sources:[source],citationIds:[source.id]};
  }
  let quote;
  const hit = fetcher === fetch && impactCache.get(lang);
  if (hit && hit.expires > Date.now()) quote = hit.text;
  else {
    try {
      const r = await fetcher('https://bienestarcommunity.org/assets/i18n/'+lang+'.json',{redirect:'error',signal:AbortSignal.timeout(2500)});
      if (r.ok) {
        const text = (await r.json()).who_we_are_our_story_highlight;
        if (typeof text === 'string' && text.length <= 500 && /\b(?:pounds|libras)\b/iu.test(text) && !/[<>]|https?:\/\//iu.test(text)) {
          quote = text.trim();
          if (fetcher === fetch) impactCache.set(lang,{text:quote,expires:Date.now()+900_000});
        }
      }
    } catch { /* The verified source link remains available when its published text cannot be read. */ }
  }
  const content = quote ? (es ? 'La sección «Nuestra historia» de Bienestar dice: ' : 'Bienestar’s “Our story” page reports: ')+quote
    : (es ? 'Puedes consultar la cifra publicada por Bienestar en la sección «Nuestra historia».' : 'You can find Bienestar’s published figure in the “Our story” section.');
  return {content,status:'complete',sources:[source],citationIds:[source.id]};
}

module.exports = { healthFairRequest, healthFairContext, asksAboutProduceTotal, asksAboutWeeklyProduce, impactReply };
