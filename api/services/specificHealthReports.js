const mysqlConnection = require('../connection/connection');
const logger = require('../utils/logger');
const { buildHealthMetricsCsv } = require('./healthMetrics');
const { EXCLUDED_REPORT_USER_IDS } = require('./rawDataReport');

// ---------------------------------------------------------------------------
// "Specific" health reports (Eligibility + Members Exclusive).
//
// Both reports reuse the standard Health Metrics CSV pipeline (same columns as
// the regular health-metrics.csv download) and add a participant-level filter:
//
//   Report 1 - Eligibility
//     Required (BOTH):
//       Q2  (active Medi-Cal coverage?) = "No" OR "Not sure"
//       Q18 (may staff contact you?)    = "Yes"
//     Qualifying (at least ONE):
//       Q12 (contact me - if "No")      = "Yes"
//       Q21 (contact me - if "Not sure")= "Yes"
//       Q7  (assistance social services)= "Yes"
//
//   Report 2 - Members Exclusive
//     Required (ALL):
//       Q1  (any other health insurance?) = "Yes"
//       Q3  (which health plan?)          = client's plan (IEHP / Molina)
//       Q18 (may staff contact you?)      = "Yes"
//     Qualifying (at least ONE):
//       Q13 (help navigating care?)       = "Yes"
//       Q7  (assistance social services)  = "Yes"
//
// Answer ids are resolved at runtime (by name) so the logic is independent of
// the exact ids stored in each environment.
// ---------------------------------------------------------------------------

const QUESTION = {
  OTHER_HEALTH_INSURANCE: 1,
  ACTIVE_MEDICAL_COVERAGE: 2,
  WHICH_HEALTH_PLAN: 3,
  SOCIAL_SERVICES_ASSISTANCE: 7,
  CONTACT_IF_NO: 12,
  HELP_NAVIGATING_CARE: 13,
  MAY_STAFF_CONTACT: 18,
  CONTACT_IF_NOT_SURE: 21
};

const REPORT_CONDITION_QUESTION_IDS = Object.values(QUESTION);

// Each client gets their own pair of reports. `plan` is the exact (case
// insensitive) answer name expected for Q3 "which health plan do you have?".
const SPECIFIC_REPORT_CLIENTS = [
  { clientId: 1, shortName: 'IEHP', plan: 'IEHP' },
  { clientId: 2, shortName: 'Molina', plan: 'Molina' }
];

const REPORT_TYPE = {
  ELIGIBILITY: 'eligibility',
  MEMBERS: 'members-exclusive'
};

function normalizeAnswerName(name) {
  return String(name == null ? '' : name).trim().toLowerCase();
}

function isYesAnswerName(name) {
  // Affirmative answers: "Yes", "Yes, please", "Yes, thank you", "Sí, ...", etc.
  // Anchored to whole-word/phrase forms so unrelated words that merely begin
  // with "si" (e.g. "Single", "Site visit") are NOT treated as "yes".
  const normalized = normalizeAnswerName(name);
  if (normalized === '') {
    return false;
  }
  return (
    normalized === 'yes' || normalized === 'si' || normalized === 'sí' ||
    normalized.startsWith('yes,') || normalized.startsWith('yes ') ||
    normalized.startsWith('si,') || normalized.startsWith('si ') ||
    normalized.startsWith('sí,') || normalized.startsWith('sí ')
  );
}

/**
 * Resolve, per question id, the answer ids that satisfy each condition we use.
 * Returns a map: questionId -> { yes: number[], notYes: number[], byName: Map }.
 */
async function resolveReportAnswerIds() {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT id, question_id, name, name_es
       FROM answer
      WHERE question_id IN (?)
        AND enabled = 'Y'`,
    [REPORT_CONDITION_QUESTION_IDS]
  );

  const byQuestion = new Map();
  for (const row of rows) {
    if (!byQuestion.has(row.question_id)) {
      byQuestion.set(row.question_id, { yes: [], notYes: [], byName: new Map() });
    }
    const entry = byQuestion.get(row.question_id);
    // Fall back to the Spanish name so an answer that only has name_es is still
    // classified instead of being silently dropped into "notYes".
    const displayName = (row.name != null && String(row.name).trim() !== '') ? row.name : row.name_es;
    if (isYesAnswerName(displayName)) {
      entry.yes.push(row.id);
    } else {
      entry.notYes.push(row.id);
    }
    entry.byName.set(normalizeAnswerName(displayName), row.id);
  }

  return byQuestion;
}

function answerIdsForName(answerIdsByQuestion, questionId, targetName) {
  const entry = answerIdsByQuestion.get(questionId);
  if (!entry) {
    return [];
  }
  const id = entry.byName.get(normalizeAnswerName(targetName));
  return id === undefined ? [] : [id];
}

function yesIds(answerIdsByQuestion, questionId) {
  const entry = answerIdsByQuestion.get(questionId);
  return entry ? entry.yes : [];
}

function notYesIds(answerIdsByQuestion, questionId) {
  const entry = answerIdsByQuestion.get(questionId);
  return entry ? entry.notYes : [];
}

function userAnsweredAny(answerStateByUserQuestion, userId, questionId, acceptableAnswerIds) {
  if (!Array.isArray(acceptableAnswerIds) || acceptableAnswerIds.length === 0) {
    return false;
  }
  const state = answerStateByUserQuestion.get(`${userId}:${questionId}`);
  if (!state || !Array.isArray(state.answer_ids) || state.answer_ids.length === 0) {
    return false;
  }
  return state.answer_ids.some(answerId => acceptableAnswerIds.includes(answerId));
}

/**
 * Build the participant-level predicate for a given report type / client.
 * The predicate signature matches healthMetrics' `userFilter` option:
 *   (user, answerStateByUserQuestion) => boolean
 */
function buildReportUserFilter(reportType, answerIdsByQuestion, planAnswerIds) {
  const excludedUserIds = new Set(EXCLUDED_REPORT_USER_IDS.map(id => Number(id)));

  if (reportType === REPORT_TYPE.ELIGIBILITY) {
    const q2NotYes = notYesIds(answerIdsByQuestion, QUESTION.ACTIVE_MEDICAL_COVERAGE);
    const q18Yes = yesIds(answerIdsByQuestion, QUESTION.MAY_STAFF_CONTACT);
    const q12Yes = yesIds(answerIdsByQuestion, QUESTION.CONTACT_IF_NO);
    const q21Yes = yesIds(answerIdsByQuestion, QUESTION.CONTACT_IF_NOT_SURE);
    const q7Yes = yesIds(answerIdsByQuestion, QUESTION.SOCIAL_SERVICES_ASSISTANCE);

    return (user, answerStateByUserQuestion) => {
      if (excludedUserIds.has(Number(user.user_id))) {
        return false;
      }
      const userId = user.user_id;
      const requiredNoMedical = userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.ACTIVE_MEDICAL_COVERAGE, q2NotYes);
      const requiredContact = userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.MAY_STAFF_CONTACT, q18Yes);
      if (!requiredNoMedical || !requiredContact) {
        return false;
      }
      const qualifying =
        userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.CONTACT_IF_NO, q12Yes) ||
        userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.CONTACT_IF_NOT_SURE, q21Yes) ||
        userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.SOCIAL_SERVICES_ASSISTANCE, q7Yes);
      return qualifying;
    };
  }

  if (reportType === REPORT_TYPE.MEMBERS) {
    const q1Yes = yesIds(answerIdsByQuestion, QUESTION.OTHER_HEALTH_INSURANCE);
    const q18Yes = yesIds(answerIdsByQuestion, QUESTION.MAY_STAFF_CONTACT);
    const q13Yes = yesIds(answerIdsByQuestion, QUESTION.HELP_NAVIGATING_CARE);
    const q7Yes = yesIds(answerIdsByQuestion, QUESTION.SOCIAL_SERVICES_ASSISTANCE);

    return (user, answerStateByUserQuestion) => {
      if (excludedUserIds.has(Number(user.user_id))) {
        return false;
      }
      const userId = user.user_id;
      const requiredOtherInsurance = userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.OTHER_HEALTH_INSURANCE, q1Yes);
      const requiredPlan = userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.WHICH_HEALTH_PLAN, planAnswerIds);
      const requiredContact = userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.MAY_STAFF_CONTACT, q18Yes);
      if (!requiredOtherInsurance || !requiredPlan || !requiredContact) {
        return false;
      }
      const qualifying =
        userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.HELP_NAVIGATING_CARE, q13Yes) ||
        userAnsweredAny(answerStateByUserQuestion, userId, QUESTION.SOCIAL_SERVICES_ASSISTANCE, q7Yes);
      return qualifying;
    };
  }

  throw new Error(`Unknown specific report type: ${reportType}`);
}

/**
 * Build a single specific report CSV for one client.
 * @returns {{ fileName: string, csvData: string, rowCount: number }}
 */
async function buildSpecificHealthReportCsv({ clientId, reportType, filters = {}, language = 'en', answerIdsByQuestion = null }) {
  const clientConfig = SPECIFIC_REPORT_CLIENTS.find(client => Number(client.clientId) === Number(clientId));
  if (!clientConfig) {
    throw new Error(`Specific reports are not configured for client_id ${clientId}`);
  }

  const resolvedAnswerIds = answerIdsByQuestion || (await resolveReportAnswerIds());
  const planAnswerIds = answerIdsForName(resolvedAnswerIds, QUESTION.WHICH_HEALTH_PLAN, clientConfig.plan);

  if (reportType === REPORT_TYPE.MEMBERS && planAnswerIds.length === 0) {
    logger.warn(`Specific reports: could not resolve Q3 plan answer "${clientConfig.plan}" for client ${clientId}.`);
  }

  const userFilter = buildReportUserFilter(reportType, resolvedAnswerIds, planAnswerIds);
  const fileName = `${clientConfig.shortName}-${reportType}.csv`;

  const { csvData, rowCount } = await buildHealthMetricsCsv({
    cabecera: { role: 'client', client_id: clientConfig.clientId },
    filters,
    language,
    userFilter,
    extraAnswerQuestionIds: REPORT_CONDITION_QUESTION_IDS,
    fileName
  });

  return { fileName, csvData, rowCount };
}

/**
 * Build both specific reports (eligibility + members-exclusive) for one client.
 * @returns {Array<{ fileName: string, csvData: string, rowCount: number }>}
 */
async function buildSpecificReportsForClient({ clientId, filters = {}, language = 'en', answerIdsByQuestion = null }) {
  const resolvedAnswerIds = answerIdsByQuestion || (await resolveReportAnswerIds());

  const reports = [];
  for (const reportType of [REPORT_TYPE.ELIGIBILITY, REPORT_TYPE.MEMBERS]) {
    reports.push(
      await buildSpecificHealthReportCsv({
        clientId,
        reportType,
        filters,
        language,
        answerIdsByQuestion: resolvedAnswerIds
      })
    );
  }

  return reports;
}

/**
 * Build the specific reports for every configured client (IEHP + Molina).
 * Used by the admin "Download specific report" action.
 * @returns {Array<{ fileName: string, csvData: string, rowCount: number }>}
 */
async function buildAllSpecificReports({ filters = {}, language = 'en' }) {
  const answerIdsByQuestion = await resolveReportAnswerIds();

  const allReports = [];
  for (const client of SPECIFIC_REPORT_CLIENTS) {
    const clientReports = await buildSpecificReportsForClient({
      clientId: client.clientId,
      filters,
      language,
      answerIdsByQuestion
    });
    allReports.push(...clientReports);
  }

  return allReports;
}

module.exports = {
  REPORT_TYPE,
  SPECIFIC_REPORT_CLIENTS,
  REPORT_CONDITION_QUESTION_IDS,
  resolveReportAnswerIds,
  buildSpecificHealthReportCsv,
  buildSpecificReportsForClient,
  buildAllSpecificReports
};
