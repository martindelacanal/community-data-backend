'use strict';

const { normalize } = require('./chatbotNavigation');

// Match a complete count request. Qualifiers not encoded here must never turn
// into an unfiltered total. A captured place is verified against the calendar.
function calendarCountRequest(question) {
  const text = normalize(question).replace(/^(?:please|por favor) /, '').replace(/ (?:please|por favor)$/, '')
    .replace(/^(?:(?:can|could) you tell me|tell me|quiero saber|(?:me )?(?:puedes|podrias) decir(?:me)?|dime|decime) /, '')
    .replace(/^(?:what is (?:the )?)?(?:total number|number) of /, 'how many ')
    .replace(/^(?:cual es )?(?:la cantidad|el total) de /, 'cuantas ')
    .replace(/\bdistrbution(s?)\b/g, 'distribution$1')
    .replace(/\bdistrbucion(es)?\b/g, 'distribucion$1');
  const en = text.match(/^how many (?:upcoming |scheduled )?(?:food )?(?:distributions?|pickups?)(?: (?:are there|are scheduled|will there be|are planned))?(?: (in|at|for) (.{1,100}))?$/);
  const es = text.match(/^cuant[oa]s (?:proxim[oa]s )?(?:distribuciones|repartos|entregas)(?: de (?:comida|alimentos|despensa))?(?: (?:hay|habra|estan programad[oa]s|se haran|se realizan|se realizaran|tienen|tenemos))?(?: (en|para) (.{1,100}))?$/);
  const venue = text.match(/^how many (?:upcoming |scheduled )?(?:food )?distributions? does (.{1,100}) have$/)
    || text.match(/^cuant[oa]s (?:distribuciones|repartos|entregas)(?: de (?:comida|alimentos))? tiene (.{1,100})$/);
  const regular = en || es;
  if (!regular && !venue) return null;
  let place = regular ? regular[2] || '' : venue[1];
  // An upcoming-calendar total can't answer a date, interval or recurrence.
  if (/\b(?:hoy|manana|ayer|semana|mes|ano|today|tomorrow|yesterday|week|month|year|since|until|entre|desde|hasta|before|after|between|cancelad[oa]s?|cancelled|canceled|\d{4})\b/.test(place)
    || /\b(?:on|el|los) (?:weekends?|sundays?|mondays?|tuesdays?|wednesdays?|thursdays?|fridays?|saturdays?|lunes|martes|miercoles|jueves|viernes|sabados?|domingos?)\b/.test(place)
    || /(?:^| (?:en|in|during|durante) )(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|january|february|march|april|may|june|july|august|september|october|november|december)$/.test(place)) return null;
  let kind = regular?.[1] === 'at' ? 'venue' : null;
  if (/^(?:(?:the |la )?(?:city of|ciudad de)) /.test(place)) {
    place = place.replace(/^(?:the |la )?(?:city of|ciudad de) /, '');
    kind = 'city';
  } else if (/^(?:the |la |el )?(?:sede|sitio|venue|site) (?:de |of )?/.test(place)) {
    place = place.replace(/^(?:the |la |el )?(?:sede|sitio|venue|site) (?:de |of )?/, '');
    kind = 'venue';
  }
  return { place, kind };
}

function placeKey(value) {
  return normalize(value).replace(/\bhighschool\b/g, 'high school').replace(/\bsaint\b/g, 'st');
}

function resolveCountScope(request, history, location) {
  if (request.place) return request;
  if (location) return { ...request, place: location };
  if (!history.length) return request;
  const users = history.filter(message => message.role === 'user').reverse();
  for (const user of users) {
    const previous = calendarCountRequest(user.content || user.text);
    if (!previous) return null;
    if (previous.place) return previous;
  }
  // SQL retains at most six messages. Six visible messages can hide an
  // earlier named site, so bare repeats alone cannot prove a global scope.
  return users.length && history.length < 6 ? request : null;
}

function matchCountPlace(rows, request) {
  const wanted = placeKey(request.place);
  if (!wanted) return { rows };
  const venueTarget = wanted.replace(/^(?:the|el|la) /, '');
  const cities = [], venues = [], venueIds = new Set();
  for (const row of rows) {
    const [rawCity, ...venueParts] = String(row.community_city || '').split(':');
    const city = placeKey(rawCity);
    if (request.kind !== 'venue' && city && wanted === city) cities.push(row);
    if (request.kind === 'city') continue;
    const aliases = [placeKey(venueParts.join(' ')), placeKey(row.organization)].filter(Boolean);
    const matches = aliases.some(venue => [venue, ...(city ? [
      venue + ' ' + city, venue + ' in ' + city, venue + ' en ' + city, venue + ' de ' + city,
      city + ' ' + venue, city + ' at ' + venue, city + ' en ' + venue
    ] : [])].some(label => label === wanted || label === venueTarget));
    if (!matches) continue;
    venues.push(row);
    venueIds.add(row.location_id ?? [city, placeKey(row.address), aliases[0]].join('|'));
  }
  if (cities.length && (!venues.length || (venues.length === cities.length && venues.every((row, i) => row === cities[i])))) {
    return { rows: cities, name: String(cities[0].community_city).split(':')[0].trim() };
  }
  if (venues.length && !cities.length && venueIds.size === 1) {
    const [city, ...parts] = String(venues[0].community_city || '').split(':');
    const site = parts.join(' ').trim() || venues[0].organization;
    return { rows: venues, name: [site, city.trim()].filter(Boolean).join(', ') };
  }
  if (cities.length || venues.length) return { ambiguous: true, rows: [...new Set([...cities, ...venues])] };
  return null;
}

function calendarCountReply(request, rows, locale) {
  const match = matchCountPlace(rows, request);
  if (!match) return null;
  const es = locale === 'es';
  const selected = match.rows;
  const sources = [{ id: 'calendar:current', title: es ? 'Calendario de distribuciones' : 'Distribution calendar', url: '/calendar' }];
  const validationSources = selected.map(row => ({ id: 'calendar:' + row.id }));
  const citationIds = validationSources.length ? validationSources.map(source => source.id) : ['calendar:current'];
  if (match.ambiguous) {
    const content = es ? 'El nombre coincide con más de una ubicación del calendario. ¿Puedes indicar la sede y la ciudad o consultar la dirección en el calendario?' :
      'That name matches multiple calendar locations. Can you give both the site and the city, or check the address in the calendar?';
    return { content, sources, citationIds, validationSources, status: 'complete' };
  }
  const active = selected.filter(row => Number(row.no_distribution) !== 1);
  const dates = [...new Set(active.map(row => row.date))].sort();
  const cancelled = selected.length - active.length;
  const count = active.length;
  const label = match.name && /^[\p{L}\p{M}\d .,'’()&-]{1,140}$/u.test(match.name) ? match.name : '';
  const scope = label ? (es ? ' en ' : ' in ') + label : '';
  const messages = [es
    ? 'El calendario publicado desde hoy (hora de California) muestra ' + count + (count === 1 ? ' distribución' : ' distribuciones') + scope + '.'
    : 'The published calendar from today (California time) shows ' + count + (count === 1 ? ' distribution' : ' distributions') + scope + '.'];
  if (dates.length) {
    const visible = dates.slice(0, 24);
    const prefix = visible.length < dates.length
      ? (es ? 'Las primeras 24 fechas son ' : 'The first 24 dates are ')
      : (es ? (dates.length === 1 ? 'La fecha es ' : 'Las fechas son ') : (dates.length === 1 ? 'The date is ' : 'The dates are '));
    messages.push(prefix + visible.join(', ') + '.');
  }
  if (cancelled) {
    messages.push(es
      ? 'El calendario también incluye ' + cancelled + (cancelled === 1 ? ' aviso sin reparto.' : ' avisos sin reparto.')
      : 'The calendar also lists ' + cancelled + (cancelled === 1 ? ' no-distribution notice.' : ' no-distribution notices.'));
  }
  return { content: messages.join(' '), sources, citationIds, validationSources, status: 'complete' };
}

module.exports = { calendarCountRequest, resolveCountScope, calendarCountReply };
