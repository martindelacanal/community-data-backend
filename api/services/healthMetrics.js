const mysqlConnection = require('../connection/connection');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const moment = require('moment-timezone');

const HEALTH_METRICS_CHUNK_SIZE = 1000;
const SURVEY_TRAFFIC_LIGHT_DIRECTION_ASCENDING = 'ascending';
const SURVEY_TRAFFIC_LIGHT_DIRECTION_DESCENDING = 'descending';

function chunkArray(items, chunkSize = HEALTH_METRICS_CHUNK_SIZE) {
  if (!Array.isArray(items) || items.length === 0) {
    return [];
  }

  const chunks = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    chunks.push(items.slice(i, i + chunkSize));
  }

  return chunks;
}

function parseSurveyDateOnly(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null;
  }

  const [year, month, day] = value.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }

  return value;
}

function normalizeSurveyTrafficLightEnabled(value) {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'number') {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'y'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'n'].includes(normalized)) {
      return false;
    }
  }

  return null;
}

function normalizeSurveyTrafficLightDirection(value) {
  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  if (
    normalized !== SURVEY_TRAFFIC_LIGHT_DIRECTION_ASCENDING &&
    normalized !== SURVEY_TRAFFIC_LIGHT_DIRECTION_DESCENDING
  ) {
    return null;
  }

  return normalized;
}

function getSurveyTrafficLightConfig(answerTypeId, rawEnabled, rawDirection) {
  if (answerTypeId !== 3 && answerTypeId !== 4) {
    return {
      traffic_light_enabled: false,
      traffic_light_direction: SURVEY_TRAFFIC_LIGHT_DIRECTION_ASCENDING
    };
  }

  const normalizedEnabled = normalizeSurveyTrafficLightEnabled(rawEnabled);
  const normalizedDirection = normalizeSurveyTrafficLightDirection(rawDirection);
  return {
    traffic_light_enabled: normalizedEnabled === null ? false : normalizedEnabled,
    traffic_light_direction: normalizedDirection || SURVEY_TRAFFIC_LIGHT_DIRECTION_ASCENDING
  };
}

function normalizeHealthMetricIntList(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  const normalized = [];
  const seen = new Set();
  for (let i = 0; i < value.length; i++) {
    const parsed = Number(value[i]);
    if (!Number.isInteger(parsed) || parsed <= 0 || seen.has(parsed)) {
      continue;
    }

    seen.add(parsed);
    normalized.push(parsed);
  }

  return normalized;
}

function normalizeHealthMetricAge(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return Math.max(0, Math.trunc(parsed));
}

function normalizeHealthMetricAnswerFilters(rawValue) {
  if (!rawValue || typeof rawValue !== 'object' || Array.isArray(rawValue)) {
    return [];
  }

  const normalized = [];
  for (const [questionIdRaw, answerIdsRaw] of Object.entries(rawValue)) {
    const questionId = Number(questionIdRaw);
    if (!Number.isInteger(questionId) || questionId <= 0) {
      continue;
    }

    const answerIds = normalizeHealthMetricIntList(
      Array.isArray(answerIdsRaw) ? answerIdsRaw : [answerIdsRaw]
    );

    if (answerIds.length === 0) {
      continue;
    }

    normalized.push({ questionId, answerIds });
  }

  normalized.sort((a, b) => a.questionId - b.questionId);
  return normalized;
}

function normalizeHealthMetricFilters(rawFilters = {}) {
  const rawFromDate = typeof rawFilters.from_date === 'string'
    ? rawFilters.from_date.slice(0, 10)
    : '';
  const rawToDate = typeof rawFilters.to_date === 'string'
    ? rawFilters.to_date.slice(0, 10)
    : '';
  const zipcode = rawFilters.zipcode == null ? null : String(rawFilters.zipcode).trim();

  return {
    locations: normalizeHealthMetricIntList(rawFilters.locations),
    genders: normalizeHealthMetricIntList(rawFilters.genders),
    ethnicities: normalizeHealthMetricIntList(rawFilters.ethnicities),
    minAge: normalizeHealthMetricAge(rawFilters.min_age),
    maxAge: normalizeHealthMetricAge(rawFilters.max_age),
    zipcode: zipcode || null,
    laFromDate: parseSurveyDateOnly(rawFromDate) || '1970-01-01',
    laToDate: parseSurveyDateOnly(rawToDate) || '2100-01-01',
    answerFilters: normalizeHealthMetricAnswerFilters(
      rawFilters.answer_filters ?? rawFilters.register_form
    )
  };
}

function getHealthMetricBirthDateRange(minAge, maxAge) {
  if (minAge === null && maxAge === null) {
    return null;
  }

  const today = moment.tz('America/Los_Angeles').startOf('day');

  return {
    minBirthDate: maxAge === null
      ? null
      : today.clone().subtract(maxAge + 1, 'years').add(1, 'day').format('YYYY-MM-DD'),
    maxBirthDate: minAge === null
      ? null
      : today.clone().subtract(minAge, 'years').format('YYYY-MM-DD')
  };
}

function buildHealthMetricLatestAnswerFilterClause(answerFilters, params) {
  if (!Array.isArray(answerFilters) || answerFilters.length === 0) {
    return '';
  }

  const filterQuestionIds = answerFilters.map(item => item.questionId);
  const answerConditions = [];

  params.push(filterQuestionIds);
  for (let i = 0; i < answerFilters.length; i++) {
    const answerFilter = answerFilters[i];
    answerConditions.push('(uqf.question_id = ? AND uqaf.answer_id IN (?))');
    params.push(answerFilter.questionId, answerFilter.answerIds);
  }
  params.push(answerFilters.length);

  return `u.id IN (
    SELECT matched.user_id
    FROM (
      SELECT uqf.user_id, uqf.question_id
      FROM user_question uqf
      INNER JOIN (
        SELECT user_id, question_id, MAX(id) AS latest_id
        FROM user_question
        WHERE question_id IN (?)
        GROUP BY user_id, question_id
      ) latest_uqf ON latest_uqf.latest_id = uqf.id
      INNER JOIN user_question_answer uqaf ON uqaf.user_question_id = uqf.id
      WHERE ${answerConditions.join(' OR ')}
      GROUP BY uqf.user_id, uqf.question_id
    ) matched
    GROUP BY matched.user_id
    HAVING COUNT(*) = ?
  )`;
}

function buildHealthMetricUserScope(cabecera, filters) {
  const joinClauses = [];
  const whereClauses = ['u.role_id = 5'];
  const params = [];

  if (cabecera.role === 'client') {
    joinClauses.push('INNER JOIN client_user cu ON u.id = cu.user_id');
  }

  whereClauses.push(`(
    (
      u.creation_date >= CONVERT_TZ(CONCAT(?, ' 00:00:00'), 'America/Los_Angeles', '+00:00')
      AND u.creation_date < CONVERT_TZ(CONCAT(DATE_ADD(?, INTERVAL 1 DAY), ' 00:00:00'), 'America/Los_Angeles', '+00:00')
    )
    OR EXISTS (
      SELECT 1
      FROM delivery_beneficiary db_range
      WHERE db_range.receiving_user_id = u.id
        AND db_range.creation_date >= CONVERT_TZ(CONCAT(?, ' 00:00:00'), 'America/Los_Angeles', '+00:00')
        AND db_range.creation_date < CONVERT_TZ(CONCAT(DATE_ADD(?, INTERVAL 1 DAY), ' 00:00:00'), 'America/Los_Angeles', '+00:00')
        ${filters.locations.length > 0 ? 'AND db_range.location_id IN (?)' : ''}
    )
  )`);
  params.push(filters.laFromDate, filters.laToDate, filters.laFromDate, filters.laToDate);
  if (filters.locations.length > 0) {
    params.push(filters.locations);
  }

  if (filters.locations.length > 0) {
    whereClauses.push(`(
      u.first_location_id IN (?)
      OR EXISTS (
        SELECT 1
        FROM delivery_beneficiary db_location
        WHERE db_location.receiving_user_id = u.id
          AND db_location.location_id IN (?)
      )
    )`);
    params.push(filters.locations, filters.locations);
  }

  if (filters.genders.length > 0) {
    whereClauses.push('u.gender_id IN (?)');
    params.push(filters.genders);
  }

  if (filters.ethnicities.length > 0) {
    whereClauses.push('u.ethnicity_id IN (?)');
    params.push(filters.ethnicities);
  }

  const birthDateRange = getHealthMetricBirthDateRange(filters.minAge, filters.maxAge);
  if (birthDateRange?.minBirthDate) {
    whereClauses.push('u.date_of_birth >= ?');
    params.push(birthDateRange.minBirthDate);
  }
  if (birthDateRange?.maxBirthDate) {
    whereClauses.push('u.date_of_birth <= ?');
    params.push(birthDateRange.maxBirthDate);
  }

  if (filters.zipcode) {
    whereClauses.push('u.zipcode = ?');
    params.push(filters.zipcode);
  }

  if (cabecera.role === 'client') {
    whereClauses.push('cu.client_id = ?');
    params.push(cabecera.client_id);
  }

  const latestAnswerFilterClause = buildHealthMetricLatestAnswerFilterClause(filters.answerFilters, params);
  if (latestAnswerFilterClause) {
    whereClauses.push(latestAnswerFilterClause);
  }

  return {
    joinSql: joinClauses.join('\n      '),
    whereSql: whereClauses.join('\n        AND '),
    params
  };
}

async function getHealthMetricQuestionCatalog(cabecera, language) {
  const localizedQuestionName = language === 'es'
    ? 'COALESCE(q.name_es, q.name)'
    : 'COALESCE(q.name, q.name_es)';
  const localizedAnswerName = language === 'es'
    ? 'COALESCE(a.name_es, a.name)'
    : 'COALESCE(a.name, a.name_es)';

  let query = `
    SELECT
      q.id AS question_id,
      ${localizedQuestionName} AS question,
      q.\`order\` AS question_order,
      q.answer_type_id,
      q.traffic_light_enabled,
      q.traffic_light_direction,
      a.id AS answer_id,
      ${localizedAnswerName} AS answer,
      a.\`order\` AS answer_order
    FROM question q
    LEFT JOIN answer a ON a.question_id = q.id
    WHERE q.enabled = 'Y'
      AND q.answer_type_id IN (3, 4)
  `;
  const params = [];

  if (cabecera.role === 'client') {
    query += `
      AND EXISTS (
        SELECT 1
        FROM question_location ql
        INNER JOIN client_location cl ON cl.location_id = ql.location_id
        WHERE ql.question_id = q.id
          AND ql.enabled = 'Y'
          AND cl.client_id = ?
      )
    `;
    params.push(cabecera.client_id);
  }

  query += `
    ORDER BY q.\`order\` ASC, q.id ASC, a.\`order\` ASC, a.id ASC
  `;

  const [rows] = await mysqlConnection.promise().query(query, params);
  const questions = [];
  const questionById = new Map();

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    if (!questionById.has(row.question_id)) {
      const trafficLightConfig = getSurveyTrafficLightConfig(
        row.answer_type_id,
        row.traffic_light_enabled,
        row.traffic_light_direction
      );
      const question = {
        id: row.question_id,
        question: row.question,
        traffic_light_enabled: trafficLightConfig.traffic_light_enabled,
        traffic_light_direction: trafficLightConfig.traffic_light_direction,
        answers: []
      };
      questionById.set(row.question_id, question);
      questions.push(question);
    }

    if (row.answer_id !== null && row.answer_id !== undefined) {
      questionById.get(row.question_id).answers.push({
        answer_id: row.answer_id,
        answer: row.answer,
        order: row.answer_order
      });
    }
  }

  return {
    questions,
    questionIds: questions.map(question => question.id)
  };
}

async function getHealthMetricUserIds(cabecera, filters) {
  const { joinSql, whereSql, params } = buildHealthMetricUserScope(cabecera, filters);
  const query = `
    SELECT u.id AS user_id
    FROM user u
      ${joinSql}
    WHERE ${whereSql}
    ORDER BY u.id
  `;

  const [rows] = await mysqlConnection.promise().query(query, params);
  return rows.map(row => row.user_id);
}

async function getHealthMetricUsers(cabecera, filters) {
  const { joinSql, whereSql, params } = buildHealthMetricUserScope(cabecera, filters);
  const query = `
    SELECT
      u.id AS user_id,
      u.username,
      u.email,
      u.firstname,
      u.lastname,
      u.language,
      DATE_FORMAT(u.date_of_birth, '%m/%d/%Y') AS date_of_birth,
      TIMESTAMPDIFF(YEAR, u.date_of_birth, DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', 'America/Los_Angeles'))) AS age,
      u.phone,
      u.zipcode,
      u.household_size,
      g.name AS gender,
      eth.name AS ethnicity,
      u.other_ethnicity,
      first_loc.community_city AS first_location_visited,
      loc.community_city AS last_location_visited,
      DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS registration_date,
      DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles'), '%T') AS registration_time
    FROM user u
      ${joinSql}
      LEFT JOIN gender g ON g.id = u.gender_id
      LEFT JOIN ethnicity eth ON eth.id = u.ethnicity_id
      LEFT JOIN location first_loc ON first_loc.id = u.first_location_id
      LEFT JOIN location loc ON loc.id = u.location_id
    WHERE ${whereSql}
    ORDER BY u.id
  `;

  const [rows] = await mysqlConnection.promise().query(query, params);
  return rows;
}

async function getHealthMetricDeliverySummaryByUserIds(userIds, filters) {
  const summaryByUserId = new Map();
  if (!Array.isArray(userIds) || userIds.length === 0) {
    return summaryByUserId;
  }

  const userIdChunks = chunkArray(userIds);
  for (let i = 0; i < userIdChunks.length; i++) {
    const userIdChunk = userIdChunks[i];
    const [rows] = await mysqlConnection.promise().query(
      `SELECT
         db.receiving_user_id AS user_id,
         GROUP_CONCAT(DISTINCT loc.community_city ORDER BY loc.community_city SEPARATOR ', ') AS locations_visited,
         COUNT(*) AS delivery_count,
         SUM(CASE WHEN db.delivering_user_id IS NOT NULL THEN 1 ELSE 0 END) AS delivery_count_scanned,
         SUM(CASE WHEN db.delivering_user_id IS NULL THEN 1 ELSE 0 END) AS delivery_count_not_scanned,
         SUM(
           CASE
             WHEN db.creation_date >= CONVERT_TZ(CONCAT(?, ' 00:00:00'), 'America/Los_Angeles', '+00:00')
              AND db.creation_date < CONVERT_TZ(CONCAT(DATE_ADD(?, INTERVAL 1 DAY), ' 00:00:00'), 'America/Los_Angeles', '+00:00')
             THEN 1
             ELSE 0
           END
         ) AS delivery_count_between_dates,
         SUM(
           CASE
             WHEN db.delivering_user_id IS NOT NULL
              AND db.creation_date >= CONVERT_TZ(CONCAT(?, ' 00:00:00'), 'America/Los_Angeles', '+00:00')
              AND db.creation_date < CONVERT_TZ(CONCAT(DATE_ADD(?, INTERVAL 1 DAY), ' 00:00:00'), 'America/Los_Angeles', '+00:00')
             THEN 1
             ELSE 0
           END
         ) AS delivery_count_between_dates_scanned,
         SUM(
           CASE
             WHEN db.delivering_user_id IS NULL
              AND db.creation_date >= CONVERT_TZ(CONCAT(?, ' 00:00:00'), 'America/Los_Angeles', '+00:00')
              AND db.creation_date < CONVERT_TZ(CONCAT(DATE_ADD(?, INTERVAL 1 DAY), ' 00:00:00'), 'America/Los_Angeles', '+00:00')
             THEN 1
             ELSE 0
           END
         ) AS delivery_count_between_dates_not_scanned
       FROM delivery_beneficiary db
       LEFT JOIN location loc ON loc.id = db.location_id
       WHERE db.receiving_user_id IN (?)
       GROUP BY db.receiving_user_id`,
      [
        filters.laFromDate,
        filters.laToDate,
        filters.laFromDate,
        filters.laToDate,
        filters.laFromDate,
        filters.laToDate,
        userIdChunk
      ]
    );

    for (let j = 0; j < rows.length; j++) {
      const row = rows[j];
      summaryByUserId.set(row.user_id, {
        locations_visited: row.locations_visited || '',
        delivery_count: Number(row.delivery_count || 0),
        delivery_count_scanned: Number(row.delivery_count_scanned || 0),
        delivery_count_not_scanned: Number(row.delivery_count_not_scanned || 0),
        delivery_count_between_dates: Number(row.delivery_count_between_dates || 0),
        delivery_count_between_dates_scanned: Number(row.delivery_count_between_dates_scanned || 0),
        delivery_count_between_dates_not_scanned: Number(row.delivery_count_between_dates_not_scanned || 0)
      });
    }
  }

  return summaryByUserId;
}

async function getHealthMetricAnswerValueMap(userIds, questionIds, language) {
  const answerValueByUserQuestion = new Map();
  if (!Array.isArray(userIds) || userIds.length === 0 || !Array.isArray(questionIds) || questionIds.length === 0) {
    return answerValueByUserQuestion;
  }

  const localizedAnswerName = language === 'es'
    ? 'COALESCE(a.name_es, a.name)'
    : 'COALESCE(a.name, a.name_es)';

  const userIdChunks = chunkArray(userIds);
  for (let i = 0; i < userIdChunks.length; i++) {
    const userIdChunk = userIdChunks[i];
    const [rows] = await mysqlConnection.promise().query(
      `SELECT
         uq.user_id,
         uq.question_id,
         GROUP_CONCAT(${localizedAnswerName} ORDER BY a.\`order\` ASC, a.id ASC SEPARATOR ', ') AS answer_value
       FROM user_question uq
       INNER JOIN (
         SELECT user_id, question_id, MAX(id) AS latest_id
         FROM user_question
         WHERE user_id IN (?) AND question_id IN (?)
         GROUP BY user_id, question_id
       ) latest_uq ON latest_uq.latest_id = uq.id
       INNER JOIN user_question_answer uqa ON uqa.user_question_id = uq.id
       LEFT JOIN answer a ON a.question_id = uq.question_id AND a.id = uqa.answer_id
       GROUP BY uq.id, uq.user_id, uq.question_id`,
      [userIdChunk, questionIds]
    );

    for (let j = 0; j < rows.length; j++) {
      const row = rows[j];
      answerValueByUserQuestion.set(
        `${row.user_id}:${row.question_id}`,
        row.answer_value || ''
      );
    }
  }

  return answerValueByUserQuestion;
}

async function getHealthMetricAnswerCountMap(userIds, questionIds) {
  const answerCountByKey = new Map();
  if (!Array.isArray(userIds) || userIds.length === 0 || !Array.isArray(questionIds) || questionIds.length === 0) {
    return answerCountByKey;
  }

  const userIdChunks = chunkArray(userIds);
  for (let i = 0; i < userIdChunks.length; i++) {
    const userIdChunk = userIdChunks[i];
    const [rows] = await mysqlConnection.promise().query(
      `SELECT
         uq.question_id,
         uqa.answer_id,
         COUNT(*) AS total
       FROM user_question uq
       INNER JOIN (
         SELECT user_id, question_id, MAX(id) AS latest_id
         FROM user_question
         WHERE user_id IN (?) AND question_id IN (?)
         GROUP BY user_id, question_id
       ) latest_uq ON latest_uq.latest_id = uq.id
       INNER JOIN user_question_answer uqa ON uqa.user_question_id = uq.id
       GROUP BY uq.question_id, uqa.answer_id`,
      [userIdChunk, questionIds]
    );

    for (let j = 0; j < rows.length; j++) {
      const row = rows[j];
      const key = `${row.question_id}:${row.answer_id}`;
      const currentTotal = answerCountByKey.get(key) || 0;
      answerCountByKey.set(key, currentTotal + Number(row.total || 0));
    }
  }

  return answerCountByKey;
}

function baseHeaders() {
  return [
    { id: 'user_id', title: 'User ID' },
    { id: 'username', title: 'Username' },
    { id: 'email', title: 'Email' },
    { id: 'firstname', title: 'Firstname' },
    { id: 'lastname', title: 'Lastname' },
    { id: 'language', title: 'Language' },
    { id: 'date_of_birth', title: 'Date of birth' },
    { id: 'age', title: 'Age' },
    { id: 'phone', title: 'Phone' },
    { id: 'zipcode', title: 'Zipcode' },
    { id: 'household_size', title: 'Household size' },
    { id: 'gender', title: 'Gender' },
    { id: 'ethnicity', title: 'Ethnicity' },
    { id: 'other_ethnicity', title: 'Other ethnicity' },
    { id: 'first_location_visited', title: 'First location visited' },
    { id: 'last_location_visited', title: 'Last location visited' },
    { id: 'locations_visited', title: 'Locations visited' },
    { id: 'delivery_count', title: 'Delivery Count' },
    { id: 'delivery_count_scanned', title: 'D.C. scanned' },
    { id: 'delivery_count_not_scanned', title: 'D.C. not scanned' },
    { id: 'delivery_count_between_dates', title: 'D.C. between dates' },
    { id: 'delivery_count_between_dates_scanned', title: 'D.C. between dates scanned' },
    { id: 'delivery_count_between_dates_not_scanned', title: 'D.C. between dates not scanned' },
    { id: 'registration_date', title: 'Registration date' },
    { id: 'registration_time', title: 'Registration time' }
  ];
}

function assertHealthMetricsScope(cabecera) {
  if (!cabecera || !['admin', 'client', 'director'].includes(cabecera.role)) {
    const error = new Error('Forbidden');
    error.statusCode = 403;
    throw error;
  }

  if (cabecera.role === 'client' && !cabecera.client_id) {
    const error = new Error('client_id requerido para role=client');
    error.statusCode = 400;
    throw error;
  }
}

async function buildHealthMetricsCsv({ cabecera, filters = {}, language = 'en' }) {
  assertHealthMetricsScope(cabecera);

  const normalizedFilters = normalizeHealthMetricFilters(filters);
  const [questionCatalog, metricUsers] = await Promise.all([
    getHealthMetricQuestionCatalog(cabecera, language),
    getHealthMetricUsers(cabecera, normalizedFilters)
  ]);
  const { questions, questionIds } = questionCatalog;
  const headers = baseHeaders().concat(
    questions.map(question => ({
      id: String(question.id),
      title: question.question
    }))
  );

  const csvStringifier = createCsvStringifier({
    header: headers,
    fieldDelimiter: ';'
  });

  if (metricUsers.length === 0) {
    return {
      csvData: csvStringifier.getHeaderString(),
      rowCount: 0,
      fileName: 'health-metrics.csv'
    };
  }

  const metricUserIds = metricUsers.map(user => user.user_id);
  const [deliverySummaryByUserId, answerValueByUserQuestion] = await Promise.all([
    getHealthMetricDeliverySummaryByUserIds(metricUserIds, normalizedFilters),
    getHealthMetricAnswerValueMap(metricUserIds, questionIds, language)
  ]);

  const rowsForCsv = metricUsers.map(user => {
    const deliverySummary = deliverySummaryByUserId.get(user.user_id) || {
      locations_visited: '',
      delivery_count: 0,
      delivery_count_scanned: 0,
      delivery_count_not_scanned: 0,
      delivery_count_between_dates: 0,
      delivery_count_between_dates_scanned: 0,
      delivery_count_between_dates_not_scanned: 0
    };

    const row = {
      user_id: user.user_id,
      username: user.username,
      email: user.email,
      firstname: user.firstname,
      lastname: user.lastname,
      language: user.language,
      date_of_birth: user.date_of_birth,
      age: user.age,
      phone: user.phone,
      zipcode: user.zipcode,
      household_size: user.household_size,
      gender: user.gender,
      ethnicity: user.ethnicity,
      other_ethnicity: user.other_ethnicity,
      first_location_visited: user.first_location_visited,
      last_location_visited: user.last_location_visited,
      locations_visited: deliverySummary.locations_visited,
      delivery_count: deliverySummary.delivery_count,
      delivery_count_scanned: deliverySummary.delivery_count_scanned,
      delivery_count_not_scanned: deliverySummary.delivery_count_not_scanned,
      delivery_count_between_dates: deliverySummary.delivery_count_between_dates,
      delivery_count_between_dates_scanned: deliverySummary.delivery_count_between_dates_scanned,
      delivery_count_between_dates_not_scanned: deliverySummary.delivery_count_between_dates_not_scanned,
      registration_date: user.registration_date,
      registration_time: user.registration_time
    };

    for (let i = 0; i < questions.length; i++) {
      const question = questions[i];
      row[String(question.id)] = answerValueByUserQuestion.get(`${user.user_id}:${question.id}`) || '';
    }

    return row;
  });

  let csvData = csvStringifier.getHeaderString();
  csvData += csvStringifier.stringifyRecords(rowsForCsv);

  return {
    csvData,
    rowCount: rowsForCsv.length,
    fileName: 'health-metrics.csv'
  };
}

module.exports = {
  buildHealthMetricsCsv,
  getHealthMetricAnswerCountMap,
  getHealthMetricQuestionCatalog,
  getHealthMetricUserIds,
  normalizeHealthMetricFilters
};
