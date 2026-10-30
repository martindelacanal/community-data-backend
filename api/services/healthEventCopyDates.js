'use strict';

const DAY = 86400000;
function dateString(value) {
  // mysql2 materializes DATE in the connection's local timezone by default.
  if (value instanceof Date) return `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, '0')}-${String(value.getDate()).padStart(2, '0')}`;
  return value == null ? null : String(value).slice(0, 10);
}
function timestamp(value) {
  const date = dateString(value);
  const ms = Date.parse(`${date}T00:00:00Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '') || !Number.isFinite(ms) || new Date(ms).toISOString().slice(0, 10) !== date) {
    throw new Error('Invalid event configuration date');
  }
  return ms;
}

function dateLabels(ms, locale) {
  const date = new Date(ms), year = date.getUTCFullYear(), month = date.getUTCMonth() + 1, day = date.getUTCDate();
  const format = options => new Intl.DateTimeFormat(locale, { ...options, timeZone: 'UTC' }).format(date);
  const weekday = format({ weekday: 'long' });
  const numeric = locale === 'en' ? `${month}/${day}` : `${day}/${month}`;
  const short = `${numeric}/${String(year).slice(-2)}`, long = `${numeric}/${year}`;
  const words = format({ month: 'long', day: 'numeric' });
  const full = format({ year: 'numeric', month: 'long', day: 'numeric' });
  return [date.toISOString().slice(0, 10), full, words, long, short,
    ...[full, words, long, short].flatMap(label => [`${weekday}, ${label}`, `${weekday} ${label}`])];
}

/** Move structured dates and recognizable English/Spanish event date labels together. */
function createEventDateCopy(source, target) {
  const start = timestamp(source.start_date), end = timestamp(source.end_date);
  const targetStart = timestamp(target.start_date), targetEnd = timestamp(target.end_date);
  if (end < start || targetEnd < targetStart) throw new Error('Invalid event date range');
  const offset = targetStart - start;
  const shift = value => value == null ? null : new Date(timestamp(value) + offset).toISOString().slice(0, 10);
  const inRange = value => value == null || (timestamp(value) >= targetStart && timestamp(value) <= targetEnd);
  const translators = new Map();
  function text(value, locale) {
    if (!value || offset === 0) return value;
    if (!translators.has(locale)) {
      const replacements = new Map();
      // Include the setup day. Bound text matching for unusually long events; structured dates always move.
      for (let ms = start - DAY; ms <= Math.min(end, start + 366 * DAY); ms += DAY) {
        const before = dateLabels(ms, locale), after = dateLabels(ms + offset, locale);
        before.forEach((label, index) => replacements.set(label.toLocaleLowerCase(locale), after[index]));
      }
      const escaped = [...replacements.keys()].sort((a, b) => b.length - a.length).map(s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
      translators.set(locale, { replacements, pattern: new RegExp(`(?<![\\p{L}\\d])(?:${escaped.join('|')})(?![\\p{L}\\d])`, 'giu') });
    }
    const { replacements, pattern } = translators.get(locale);
    return value.replace(pattern, match => {
      const replacement = replacements.get(match.toLocaleLowerCase(locale));
      return match[0] === match[0].toLocaleUpperCase(locale)
        ? replacement[0].toLocaleUpperCase(locale) + replacement.slice(1) : replacement;
    });
  }
  return { shift, inRange, text };
}

module.exports = { createEventDateCopy, dateString };
