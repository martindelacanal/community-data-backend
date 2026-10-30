'use strict';

const normalize = value => String(value || '').normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
function foodPreparationIntent(value) {
  const text = normalize(value);
  if (/\b(prescriptions?|medicamentos|receta medica|recetas medicas)\b/.test(text)) return false;
  return /\b(recetas?|recipes?|cocinar|cocinarlas|cocinarlos|cooking|cook|conservar(?:la|lo|las|los)?|guardar|almacenar|storage|storing)\b/.test(text)
    || (/\b(frutas?|verduras?|fresas?|frutillas?|anan[aá]s?|pina|pineapple|berenjenas?|eggplants?|alimentos?|comida|food|strawberries|vegetables?)\b/.test(text)
      && /\b(preparar(?:la|lo|las|los)?|prepare|hacer|do with|aprovechar|duren|last longer)\b/.test(text));
}
const calendarIntent = value => !foodPreparationIntent(value) && /\b(distributions?|distribuciones?|distribucion|repartos?|calendario|calendar|entregan alimentos|proxima entrega|next delivery)\b/.test(normalize(value));
const resourceIntent = value => !foodPreparationIntent(value) && /\b(recursos?|resources?|servicios?|services?|clinicas?|clinics?|parques?|parks?|guarderias?|childcare|daycare|senior centers?|centros (?:educativos|para adultos mayores)|escuelas?|schools?|bibliotecas?|libraries|transporte publico|public transportation)\b/.test(normalize(value));
const articleIntent = value => /\b(articles?|articulos?|publicaciones)\b/.test(normalize(value));
const nearbyIntent = value => /\b(near me|nearby|closest|nearest|around here|my area|my location|my neighborhood|local resources|local services|cerca de mi|cercanos?|cercanas?|mas cerca|por aqui|por aca|mi zona|mi barrio|mi ubicacion|recursos locales|servicios locales)\b/.test(normalize(value));

function calendarOverview(question) {
  return /^(?:(?:cuales son|muestra(?:me)?|dime|ver|quiero ver|todas|todas las|las|proximas|el|what are|show(?: me)?|list|all|the|upcoming|next|available|todas las fechas|fechas disponibles|de|del|of|dates|please|por favor)\s+)*(?:distribuciones|distribuciones disponibles|distributions|distribution dates|calendario(?: de distribuciones)?|distribution calendar)(?:\s+(?:proximas|disponibles|upcoming|available|please|por favor))*$/.test(normalize(question));
}

function searchQuestion(question, history = [], location = '') {
  const previous = [...history].reverse().find(m => m.role === 'user');
  const last = history.at(-1);
  const current = normalize(question);
  const previousText = String(previous?.text || previous?.content || '');
  const foodFollowup = foodPreparationIntent(previousText) &&
    (/\b(conservarlas|conservarlos|guardarlas|guardarlos|prepararlas|prepararlos|cocinarlas|cocinarlos|them|it)\b/.test(current)
      || /^(?:(?:y|and) )?(?:mas recetas|more recipes)$/.test(current));
  if (/\b(otra consulta|otra pregunta|cambiando de tema|another question|different topic)\b/.test(current)
    || (foodPreparationIntent(question) && !foodFollowup)) return question;
  const continuing = /^(?:and\b|y\b|what about\b|que hay\b|where is (?:it|that)\b|donde (?:queda|esta)\b|pero donde\b|but where\b|no hay en otras\b|hay en otras\b|more\b|mas\b)/.test(current);
  // A previous city prompt must not pull an unrelated new question back to its topic.
  const locationOnly = location && [normalize(location), `en ${normalize(location)}`, `in ${normalize(location)}`, `vivo en ${normalize(location)}`, `i live in ${normalize(location)}`].includes(current);
  const answeringLocation = locationOnly && last?.role === 'assistant' && /city or zip|ciudad o codigo postal/i.test(normalize(last.text || last.content));
  return previous && (continuing || answeringLocation || foodFollowup)
    ? `${previousText.slice(0, 1200)}\n${question}` : question;
}

// Only use a location actually supplied by the user. Never trust IP, VPN, UI locale,
// an assistant's earlier guess, or an invented classifier location.
function explicitLocation(value, question, history = []) {
  if (typeof value !== 'string' || !value.trim() || value.length > 100) return '';
  const candidate = normalize(value);
  if (candidate.length < 3) return '';
  const supplied = [question, ...history.filter(m => m.role === 'user').map(m => m.text || m.content)];
  return supplied.some(text => ` ${normalize(text)} `.includes(` ${candidate} `)) ? value.trim() : '';
}

function calendarReply(rows, locale) {
  const es = locale === 'es';
  const days = new Map();
  for (const row of rows) {
    const day = days.get(row.date) || { active: 0, cancelled: 0 };
    if (Number(row.no_distribution) === 1) day.cancelled++; else day.active++;
    days.set(row.date, day);
  }
  const visible = [...days.entries()].sort(([a], [b]) => a.localeCompare(b)).slice(0, 120);
  const lines = visible.map(([date, day]) => {
    const labels = [];
    if (day.active) labels.push(`${day.active} ${es ? (day.active === 1 ? 'distribución' : 'distribuciones') : (day.active === 1 ? 'distribution' : 'distributions')}`);
    if (day.cancelled) labels.push(`${day.cancelled} ${es ? (day.cancelled === 1 ? 'aviso sin distribución' : 'avisos sin distribución') : (day.cancelled === 1 ? 'no-distribution notice' : 'no-distribution notices')}`);
    return `• ${date}: ${labels.join('; ')}`;
  });
  const intro = days.size
    ? (es ? `El calendario publicado tiene ${days.size} fechas desde hoy (hora de California):` : `The published calendar has ${days.size} dates from today (California time):`)
    : (es ? 'No hay fechas futuras publicadas en el calendario en este momento.' : 'There are no upcoming dates published in the calendar right now.');
  if (days.size > visible.length) lines.push(es ? `Mostrando las primeras ${visible.length} fechas; las restantes están en el calendario.` : `Showing the first ${visible.length} dates; the remaining dates are in the calendar.`);
  const end = es ? 'Abre el calendario de Community para consultar todas las sedes, los horarios, las direcciones y los avisos de cada fecha.' : 'Open the Community calendar for every location, time, address and notice on each date.';
  const cited = rows.filter(row => visible.some(([date]) => date === row.date));
  return { content: [intro, ...lines, end].join('\n'), status: 'complete',
    sources: [{ id: 'calendar:current', title: es ? 'Calendario de distribuciones' : 'Distribution calendar', url: '/calendar' }],
    citationIds: cited.map(row => `calendar:${row.id}`),
    validationSources: cited.map(row => ({ id: `calendar:${row.id}` })) };
}

module.exports = { normalize, calendarIntent, resourceIntent, articleIntent, nearbyIntent, foodPreparationIntent, calendarOverview, searchQuestion, explicitLocation, calendarReply };
