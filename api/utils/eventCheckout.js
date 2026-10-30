'use strict';

function visibleQuestions(questions, answers) {
  const byId = new Map(questions.map(q => [q.id, q]));
  const visible = (q, seen = new Set()) => {
    if (!q.depends_on_question_id) return true;
    if (seen.has(q.id)) return false;
    seen.add(q.id);
    const parent = byId.get(q.depends_on_question_id);
    if (!parent || !visible(parent, seen)) return false;
    const value = answers.get(parent.id)?.answer;
    return (Array.isArray(value) ? value : [value]).map(Number).includes(Number(q.depends_on_option_id));
  };
  return questions.filter(q => visible(q));
}
function validAnswer(q, item) {
  if (!item) return false;
  const a = item.answer;
  if (a == null || (typeof a === 'string' && !a.trim())) return false;
  switch (q.question_type) {
    case 'text': return typeof a === 'string' && a.trim().length <= 5000;
    case 'number': return (typeof a === 'number' || typeof a === 'string') && Number.isFinite(Number(a));
    case 'date': { const d = new Date(`${a}T12:00:00Z`); return /^\d{4}-\d{2}-\d{2}$/.test(a) && Number.isFinite(d.getTime()) && d.toISOString().slice(0, 10) === a; }
    case 'consent': return a === true || a === 1 || a === '1' || a === 'Y';
    case 'single':
    case 'multiple': {
      const values = q.question_type === 'multiple' ? a : [a];
      if (!Array.isArray(values) || !values.length) return false;
      return values.every(v => {
        const option = q.options.find(o => Number.isInteger(Number(v)) && o.id === Number(v));
        return option && (option.is_other !== 'Y' || !!String(item.other_text || '').trim());
      });
    }
    default: return false;
  }
}
function checkoutValidation(questions, items) {
  const answers = new Map(items.map(a => [Number(a.question_id), a]));
  const visible = visibleQuestions(questions, answers).filter(q => !['appointment', 'notice'].includes(q.question_type));
  const missing = visible.filter(q => q.required === 'Y' && !validAnswer(q, answers.get(q.id))).map(q => q.id);
  const invalid = visible.filter(q => answers.has(q.id) && !validAnswer(q, answers.get(q.id))).map(q => q.id);
  return { missing, invalid, answers: visible.filter(q => validAnswer(q, answers.get(q.id))).map(q => answers.get(q.id)) };
}
async function readRegistrationAnswers(connection, registrationId) {
  const [rows] = await connection.query(`SELECT a.*, DATE_FORMAT(a.answer_date, '%Y-%m-%d') answer_date_iso, GROUP_CONCAT(o.option_id) option_ids
    FROM health_event_answer a LEFT JOIN health_event_answer_option o ON o.answer_id = a.id
    WHERE a.registration_id = ? GROUP BY a.id`, [registrationId]);
  return rows.map(a => ({ question_id: a.question_id, answer: a.option_ids ? a.option_ids.split(',').map(Number)
    : a.answer_number != null ? a.answer_number : a.answer_date_iso || a.answer_text, other_text: a.other_text }));
}
async function findEventEntry(connection, eventId, userId, timezone) {
  const [rows] = await connection.query(`SELECT s.id, (SELECT c.id FROM health_event_scan c WHERE c.paired_scan_id=s.id LIMIT 1) checkout_id
    FROM health_event_scan s INNER JOIN health_event_stand st ON st.id=s.stand_id
    WHERE s.health_event_id=? AND s.scanned_user_id=? AND s.scan_type='checkin' AND st.is_entry='Y'
      AND DATE(CONVERT_TZ(s.scanned_at, @@session.time_zone, ?)) = DATE(CONVERT_TZ(NOW(), @@session.time_zone, ?))
    ORDER BY s.scanned_at DESC, s.id DESC LIMIT 1 FOR UPDATE`, [eventId, userId, timezone, timezone]);
  return rows[0] || null;
}
module.exports = { visibleQuestions, validAnswer, checkoutValidation, readRegistrationAnswers, findEventEntry };
