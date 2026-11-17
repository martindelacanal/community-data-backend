'use strict';

/** Current wall-clock in the event timezone as 'YYYY-MM-DD HH:mm:ss' (lexicographically comparable). */
function nowInTimezone(timeZone) {
  try {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: timeZone || 'America/Los_Angeles',
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    }).formatToParts(new Date());
    const get = (type) => (parts.find(p => p.type === type) || {}).value || '00';
    const hour = get('hour') === '24' ? '00' : get('hour');
    return `${get('year')}-${get('month')}-${get('day')} ${hour}:${get('minute')}:${get('second')}`;
  } catch (error) {
    return new Date().toISOString().slice(0, 19).replace('T', ' ');
  }
}

function toSqlDateString(value) {
  if (!value) return null;
  if (value instanceof Date) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`;
  }
  return String(value).slice(0, 10);
}

/** True once the event's last day is over in the event's own timezone. */
function hasEventEnded(event) {
  const endDate = toSqlDateString(event.end_date);
  if (!endDate) return false;
  const today = nowInTimezone(event.timezone).slice(0, 10);
  return today > endDate;
}

async function publicHomeHealthEvents(db) {
  // The server clock is UTC. Fetch one extra day and apply the cutoff in the event's timezone.
  const [events] = await db.query(
    `SELECT he.*, l.organization, l.community_city, l.address
     FROM health_event he INNER JOIN location l ON l.id = he.location_id
     WHERE he.enabled = 'Y' AND he.landing_enabled = 'Y' AND he.public_home_visible = 'Y'
       AND he.end_date >= (CURDATE() - INTERVAL 1 DAY)
     ORDER BY he.start_date ASC`);
  return events.filter(event => !hasEventEnded(event));
}

module.exports = { nowInTimezone, toSqlDateString, hasEventEnded, publicHomeHealthEvents };
