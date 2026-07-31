const DEFAULT_DAILY_QR_TIME_ZONE = 'America/Los_Angeles';
const COMPACT_DAILY_QR_PATTERN = /^B([1-9]\d*)\.(\d{8})$/;

function formatDailyQrDate(date = new Date(), timeZone = DEFAULT_DAILY_QR_TIME_ZONE) {
  const resolvedTimeZone = timeZone || DEFAULT_DAILY_QR_TIME_ZONE;
  try {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: resolvedTimeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).formatToParts(date);
    const getPart = (type) => (parts.find(part => part.type === type) || {}).value || '';
    const year = getPart('year');
    const month = getPart('month');
    const day = getPart('day');
    if (year && month && day) return `${year}${month}${day}`;
  } catch (error) {
    // Invalid/unavailable IANA timezone: use the server's calendar date.
  }

  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}`;
}

/** Parse the compact protocol and legacy JSON objects into one identity shape. */
function parseDailyBeneficiaryQr(raw) {
  if (raw == null) return null;

  if (typeof raw === 'string') {
    const text = raw.trim();
    const compact = COMPACT_DAILY_QR_PATTERN.exec(text);
    if (compact) {
      return {
        id: Number.parseInt(compact[1], 10),
        role: 'beneficiary',
        date: compact[2],
        approved: 'N',
        compact: true
      };
    }
    try {
      raw = JSON.parse(text);
    } catch (error) {
      return null;
    }
  }

  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const normalizedId = String(raw.id == null ? '' : raw.id).trim();
  if (!/^[1-9]\d*$/.test(normalizedId) || raw.role !== 'beneficiary' || !raw.date) return null;
  const id = Number.parseInt(normalizedId, 10);
  return {
    ...raw,
    id,
    role: 'beneficiary',
    date: String(raw.date),
    // Approval state is controlled by the authenticated delivery route, never
    // by the identity parser (health scans also use this function).
    approved: 'N',
    compact: false
  };
}

function isValidCalendarDate(year, month, day) {
  const candidate = new Date(Date.UTC(year, month - 1, day));
  return candidate.getUTCFullYear() === year
    && candidate.getUTCMonth() === month - 1
    && candidate.getUTCDate() === day;
}

function addCalendarCandidate(candidates, year, month, day) {
  if (isValidCalendarDate(year, month, day)) {
    candidates.add(`${String(year).padStart(4, '0')}${String(month).padStart(2, '0')}${String(day).padStart(2, '0')}`);
  }
}

/**
 * Normalizes both the new YYYYMMDD date and legacy browser toLocaleString()
 * values. Ambiguous slash dates retain both valid day/month interpretations so
 * currently-issued legacy apps continue working during rollout.
 */
function getQrDateCandidates(value, timeZone = DEFAULT_DAILY_QR_TIME_ZONE) {
  const text = String(value == null ? '' : value).trim();
  const candidates = new Set();

  let match = /^(\d{4})(\d{2})(\d{2})$/.exec(text);
  if (match) {
    addCalendarCandidate(candidates, Number(match[1]), Number(match[2]), Number(match[3]));
    return candidates;
  }

  match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (match) {
    addCalendarCandidate(candidates, Number(match[1]), Number(match[2]), Number(match[3]));
    return candidates;
  }

  // ISO timestamps carry an unambiguous instant (used by the short-lived PIN
  // fallback when it advances through the same approval endpoint).
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) {
    const instant = new Date(text);
    if (!Number.isNaN(instant.getTime())) {
      candidates.add(formatDailyQrDate(instant, timeZone));
      return candidates;
    }
  }

  // Some WebViews format locale dates as YYYY-MM-DD HH:mm:ss without an
  // offset. Preserve its explicit calendar portion rather than assuming UTC.
  match = /^(\d{4})-(\d{2})-(\d{2})(?:\s|,)/.exec(text);
  if (match) {
    addCalendarCandidate(candidates, Number(match[1]), Number(match[2]), Number(match[3]));
    return candidates;
  }

  // Old beneficiary apps emitted locale-dependent M/D/YYYY or D/M/YYYY.
  match = /^(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{4})(?:\D|$)/.exec(text);
  if (match) {
    const first = Number(match[1]);
    const second = Number(match[2]);
    const year = Number(match[3]);
    addCalendarCandidate(candidates, year, first, second);
    addCalendarCandidate(candidates, year, second, first);
  }

  return candidates;
}

function isCurrentDailyBeneficiaryQr(
  qr,
  timeZone = DEFAULT_DAILY_QR_TIME_ZONE,
  now = new Date()
) {
  if (!qr || qr.role !== 'beneficiary' || !qr.date) return false;
  return getQrDateCandidates(qr.date, timeZone).has(formatDailyQrDate(now, timeZone));
}

module.exports = {
  DEFAULT_DAILY_QR_TIME_ZONE,
  formatDailyQrDate,
  getQrDateCandidates,
  isCurrentDailyBeneficiaryQr,
  parseDailyBeneficiaryQr
};
