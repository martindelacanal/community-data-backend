'use strict';

/**
 * Event form -> participant profile sync.
 *
 * An event form question can be mapped (health_event_question.maps_to) to a
 * field of the Bienestar profile; catalogue answers additionally map each form
 * option to an app option (health_event_question_option.profile_option_id).
 * When someone registers for the event (or answers pending questions later),
 * the mapped answers are copied into `user` — but ONLY into fields that are
 * still empty: a profile the person filled in themselves is never overwritten
 * by an event form.
 *
 * Banning (2026-08) showed why this matters: 323 accounts created by the
 * Jotform import / web registration had gender and ethnicity as form answers
 * only, so the app-wide demographics and the "Gender / Ethnicity" export
 * columns missed them until a backfill.
 */

const PROFILE_FIELDS = Object.freeze({
  profile_gender: { column: 'gender_id', catalog: 'gender' },
  profile_ethnicity: { column: 'ethnicity_id', catalog: 'ethnicity', otherColumn: 'other_ethnicity', otherMax: 45 },
  profile_second_ethnicity: { column: 'second_ethnicity_id', catalog: 'ethnicity', otherColumn: 'other_second_ethnicity', otherMax: 45 },
  profile_language: { column: 'language_id', catalog: 'language', otherColumn: 'other_language', otherMax: 45 },
  profile_zipcode: { column: 'zipcode', text: true, max: 30 }
});

const PROFILE_MAPS_TO = Object.freeze(Object.keys(PROFILE_FIELDS));
const PROFILE_COLUMNS = Object.freeze(Array.from(new Set(
  Object.values(PROFILE_FIELDS).flatMap(spec => [spec.column, spec.otherColumn].filter(Boolean))
)));
// Whitelisted catalogue tables (interpolated into SQL, never user input).
const CATALOG_TABLES = Object.freeze({ gender: 'gender', ethnicity: 'ethnicity', language: 'language' });

function isProfileMap(mapsTo) {
  return typeof mapsTo === 'string' && Object.prototype.hasOwnProperty.call(PROFILE_FIELDS, mapsTo);
}

function isCatalogProfileMap(mapsTo) {
  return isProfileMap(mapsTo) && !!PROFILE_FIELDS[mapsTo].catalog;
}

/** Catalogue table backing a profile mapping ('gender' | 'ethnicity' | 'language'), or null. */
function catalogTableFor(mapsTo) {
  return isCatalogProfileMap(mapsTo) ? CATALOG_TABLES[PROFILE_FIELDS[mapsTo].catalog] : null;
}

function isEmpty(value) {
  return value == null || String(value).trim() === '';
}

function findOption(question, value) {
  const id = Number.parseInt(value, 10);
  if (!Number.isInteger(id)) return null;
  return (question.options || []).find(option => Number(option.id) === id) || null;
}

function hasProfileMappings(questions) {
  return Array.from(questions || []).some(question => isProfileMap(question && question.maps_to));
}

/**
 * Pure decision: which profile columns to set from these answers.
 *
 * @param {object} args
 * @param {Array}  args.questions  form questions ({ id, maps_to, question_type, options[{ id, name_en, is_other, profile_option_id }] })
 * @param {Array}  args.answers    submitted items ({ question_id, answer, other_text })
 * @param {object} args.profile    current `user` row (only PROFILE_COLUMNS matter)
 * @returns {{ updates: object, applied: Array }} updates = { column: value }; applied = human-readable trail
 */
function computeProfileUpdates({ questions, answers, profile }) {
  const byId = new Map(Array.from(questions || []).map(question => [Number(question.id), question]));
  const current = { ...(profile || {}) };
  const updates = {};
  const applied = [];

  for (const item of answers || []) {
    const question = byId.get(Number(item && item.question_id));
    if (!question || !isProfileMap(question.maps_to)) continue;
    const spec = PROFILE_FIELDS[question.maps_to];
    if (!isEmpty(current[spec.column])) continue; // never overwrite an existing profile value

    if (spec.text) {
      let value = null;
      if (question.question_type === 'single' || question.question_type === 'multiple') {
        const raw = Array.isArray(item.answer) ? item.answer[0] : item.answer;
        const option = findOption(question, raw);
        value = option ? (option.is_other === 'Y' && !isEmpty(item.other_text) ? item.other_text : option.name_en) : null;
      } else if (!Array.isArray(item.answer)) {
        value = item.answer;
      }
      value = isEmpty(value) ? '' : String(value).trim().slice(0, spec.max);
      if (!value) continue;
      updates[spec.column] = value;
      current[spec.column] = value;
      applied.push({ maps_to: question.maps_to, question_id: Number(question.id), column: spec.column, value });
      continue;
    }

    const raw = Array.isArray(item.answer) ? item.answer : [item.answer];
    const options = raw.map(value => findOption(question, value)).filter(Boolean);
    const mapped = options.find(option => option.profile_option_id != null && Number.isInteger(Number(option.profile_option_id)));
    if (!mapped) continue;
    const catalogId = Number(mapped.profile_option_id);
    updates[spec.column] = catalogId;
    current[spec.column] = catalogId;
    applied.push({ maps_to: question.maps_to, question_id: Number(question.id), option_id: Number(mapped.id), column: spec.column, value: catalogId });

    if (spec.otherColumn && mapped.is_other === 'Y' && !isEmpty(item.other_text) && isEmpty(current[spec.otherColumn])) {
      const otherText = String(item.other_text).trim().slice(0, spec.otherMax);
      updates[spec.otherColumn] = otherText;
      current[spec.otherColumn] = otherText;
      applied.push({ maps_to: question.maps_to, question_id: Number(question.id), column: spec.otherColumn, value: otherText });
    }
  }

  return { updates, applied };
}

/**
 * Copies mapped answers into the participant's profile inside the caller's
 * transaction. Returns null when nothing had to change.
 */
async function applyProfileAnswers(connection, userId, questionsById, answers) {
  const questions = Array.from(questionsById instanceof Map ? questionsById.values() : (questionsById || []));
  if (!Number.isInteger(Number(userId)) || !hasProfileMappings(questions) || !Array.isArray(answers) || !answers.length) {
    return null;
  }
  const [rows] = await connection.query(
    `SELECT ${PROFILE_COLUMNS.join(', ')} FROM user WHERE id = ? AND deleted = 'N' LIMIT 1 FOR UPDATE`, [Number(userId)]);
  if (!rows.length) return null;

  const { updates, applied } = computeProfileUpdates({ questions, answers, profile: rows[0] });

  // A mapping may point at a catalogue row that was disabled/removed after the
  // form was configured: skip those instead of failing the registration.
  for (const spec of Object.values(PROFILE_FIELDS)) {
    if (!spec.catalog || updates[spec.column] === undefined) continue;
    const [catalogRows] = await connection.query(
      `SELECT id FROM ${CATALOG_TABLES[spec.catalog]} WHERE id = ? AND enabled = 'Y' LIMIT 1`, [updates[spec.column]]);
    if (!catalogRows.length) {
      delete updates[spec.column];
      if (spec.otherColumn) delete updates[spec.otherColumn];
    }
  }

  const columns = Object.keys(updates);
  if (!columns.length) return null;
  await connection.query(
    `UPDATE user SET ${columns.map(column => `${column} = ?`).join(', ')} WHERE id = ? AND deleted = 'N'`,
    [...columns.map(column => updates[column]), Number(userId)]);
  return { updates, applied: applied.filter(entry => columns.includes(entry.column)) };
}

module.exports = {
  PROFILE_FIELDS,
  PROFILE_MAPS_TO,
  PROFILE_COLUMNS,
  isProfileMap,
  isCatalogProfileMap,
  catalogTableFor,
  hasProfileMappings,
  computeProfileUpdates,
  applyProfileAnswers
};
