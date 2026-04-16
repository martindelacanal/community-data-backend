const mysqlConnection = require('../connection/connection.js');
const moment = require('moment-timezone');
const XLSX = require('xlsx-js-style');

const REPORT_TIMEZONE = 'America/Los_Angeles';
const EVENT_TIME_UNMATCHED_THRESHOLD_HOURS = 1;
const EXCLUDED_REPORT_USER_IDS = [5, 23, 24, 35, 43125];

function chunkArray(items, size = 1000) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function buildUserBaseRow(row) {
  return {
    user_id: row.user_id,
    username: row.username,
    email: row.email,
    firstname: row.firstname,
    lastname: row.lastname,
    date_of_birth: row.date_of_birth,
    phone: row.phone,
    zipcode: row.zipcode,
    household_size: row.household_size,
    gender: row.gender,
    ethnicity: row.ethnicity,
    other_ethnicity: row.other_ethnicity,
    last_location_visited: row.last_location_visited,
    locations_visited: row.locations_visited,
    registration_date: row.registration_date,
    registration_time: row.registration_time,
    registered_at_client_location: row.registered_at_client_location_flag ? '1' : '0'
  };
}

function getAnswerValueFromRow(row) {
  switch (row.answer_type_id) {
    case 1:
      return row.answer_text;
    case 2:
      return row.answer_number;
    case 3:
    case 4:
      return row.answer_value;
    default:
      return '';
  }
}

function parseLaDateTime(dateTimeValue) {
  if (!dateTimeValue) {
    return null;
  }

  const parsedValue = moment.tz(dateTimeValue, 'YYYY-MM-DD HH:mm:ss', true, REPORT_TIMEZONE);
  return parsedValue.isValid() ? parsedValue : null;
}

function parseLaDate(dateValue) {
  if (!dateValue) {
    return null;
  }

  const parsedValue = moment.tz(dateValue, 'MM/DD/YYYY', true, REPORT_TIMEZONE);
  return parsedValue.isValid() ? parsedValue : null;
}

function toUtcDateTime(localDateTime) {
  return moment.tz(localDateTime, 'YYYY-MM-DD HH:mm:ss', REPORT_TIMEZONE).utc().format('YYYY-MM-DD HH:mm:ss');
}

function computeEventTimeValue(eventRow, scannedWindow) {
  if (eventRow.approved === 'Y') {
    return eventRow.event_time;
  }

  if (!scannedWindow) {
    return 'Unmatched';
  }

  const eventDateTime = moment.tz(eventRow.event_datetime_la, 'YYYY-MM-DD HH:mm:ss', REPORT_TIMEZONE);
  const earliestAllowed = scannedWindow.first.clone().subtract(EVENT_TIME_UNMATCHED_THRESHOLD_HOURS, 'hour');
  const latestAllowed = scannedWindow.last.clone().add(EVENT_TIME_UNMATCHED_THRESHOLD_HOURS, 'hour');

  if (eventDateTime.isBefore(earliestAllowed) || eventDateTime.isAfter(latestAllowed)) {
    return 'Unmatched';
  }

  return eventRow.event_time;
}

function buildRawDataSortMetadata({ registrationDateTimeLa, eventDateTimeLa, eventDateValue, eventTimeValue }) {
  const registrationMoment = parseLaDateTime(registrationDateTimeLa);
  const eventMoment = parseLaDateTime(eventDateTimeLa);

  if (eventTimeValue === 'Unmatched') {
    const unmatchedDateMoment = eventMoment || parseLaDate(eventDateValue) || registrationMoment;
    const registrationDateKey = registrationMoment ? registrationMoment.format('YYYY-MM-DD') : null;
    const unmatchedDateKey = unmatchedDateMoment ? unmatchedDateMoment.format('YYYY-MM-DD') : null;

    if (registrationDateKey && unmatchedDateKey && registrationDateKey === unmatchedDateKey) {
      return {
        sortDateKey: registrationDateKey,
        sortBucket: 0,
        sortDateTimeValue: registrationMoment.valueOf()
      };
    }

    return {
      sortDateKey: unmatchedDateMoment ? unmatchedDateMoment.format('YYYY-MM-DD') : '9999-12-31',
      sortBucket: 1,
      sortDateTimeValue: eventMoment ? eventMoment.valueOf() : Number.MAX_SAFE_INTEGER
    };
  }

  const effectiveMoment = eventMoment || registrationMoment;
  return {
    sortDateKey: effectiveMoment ? effectiveMoment.format('YYYY-MM-DD') : '9999-12-31',
    sortBucket: 0,
    sortDateTimeValue: effectiveMoment ? effectiveMoment.valueOf() : Number.MAX_SAFE_INTEGER
  };
}

async function fetchQuestionCatalog(clientId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT DISTINCT
        q.id AS question_id,
        q.answer_type_id,
        q.name AS question,
        q.\`order\` AS question_order
      FROM question AS q
      INNER JOIN question_location AS ql
        ON ql.question_id = q.id
       AND ql.enabled = 'Y'
      INNER JOIN client_location AS cl
        ON cl.location_id = ql.location_id
       AND cl.client_id = ?
      WHERE q.enabled = 'Y'
      ORDER BY q.\`order\`, q.id`,
    [clientId]
  );

  return rows;
}

async function fetchParticipantBaseRows(fromDate, toDate, clientId) {
  const fromDateUtc = toUtcDateTime(fromDate);
  const toDateUtc = toUtcDateTime(toDate);

  const [rows] = await mysqlConnection.promise().query(
    `SELECT
        u.id AS user_id,
        u.username,
        u.email,
        u.firstname,
        u.lastname,
        DATE_FORMAT(u.date_of_birth, '%m/%d/%Y') AS date_of_birth,
        u.phone,
        u.zipcode,
        u.household_size,
        g.name AS gender,
        eth.name AS ethnicity,
        u.other_ethnicity,
        loc.community_city AS last_location_visited,
        (
          SELECT GROUP_CONCAT(DISTINCT loc_visited.community_city ORDER BY loc_visited.community_city SEPARATOR ', ')
          FROM delivery_beneficiary AS db_visited
          LEFT JOIN location AS loc_visited ON db_visited.location_id = loc_visited.id
          WHERE db_visited.receiving_user_id = u.id
        ) AS locations_visited,
        DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', ?), '%m/%d/%Y') AS registration_date,
        DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', ?), '%T') AS registration_time,
        DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', ?), '%Y-%m-%d %H:%i:%s') AS registration_datetime_la,
        EXISTS (
          SELECT 1
          FROM client_location cl_first
          WHERE cl_first.location_id = u.first_location_id
            AND cl_first.client_id = cu.client_id
        ) AS registered_at_client_location_flag
      FROM user u
      INNER JOIN client_user cu ON u.id = cu.user_id
      LEFT JOIN gender AS g ON u.gender_id = g.id
      LEFT JOIN ethnicity AS eth ON u.ethnicity_id = eth.id
      LEFT JOIN location AS loc ON u.location_id = loc.id
      WHERE u.role_id = 5
        AND cu.client_id = ?
        AND u.id NOT IN (?)
        AND (
          u.creation_date BETWEEN ? AND ?
          OR EXISTS (
            SELECT 1
            FROM delivery_beneficiary db3
            INNER JOIN client_location cl3
              ON cl3.location_id = db3.location_id
             AND cl3.client_id = cu.client_id
            WHERE db3.receiving_user_id = u.id
              AND db3.creation_date BETWEEN ? AND ?
          )
        )
      ORDER BY u.id`,
    [
      REPORT_TIMEZONE,
      REPORT_TIMEZONE,
      REPORT_TIMEZONE,
      clientId,
      EXCLUDED_REPORT_USER_IDS,
      fromDateUtc,
      toDateUtc,
      fromDateUtc,
      toDateUtc
    ]
  );

  return rows;
}

async function fetchLatestAnswersByUserQuestion(userIds, questionCatalog) {
  const answerMap = new Map();

  if (!Array.isArray(userIds) || userIds.length === 0 || !Array.isArray(questionCatalog) || questionCatalog.length === 0) {
    return answerMap;
  }

  const questionIds = questionCatalog.map(question => question.question_id);
  const userIdChunks = chunkArray(userIds);

  for (let i = 0; i < userIdChunks.length; i++) {
    const userIdChunk = userIdChunks[i];
    const [rows] = await mysqlConnection.promise().query(
      `SELECT
          uq.user_id,
          uq.question_id,
          q.answer_type_id,
          uq.answer_text,
          uq.answer_number,
          GROUP_CONCAT(a.name ORDER BY a.\`order\`, a.id SEPARATOR ', ') AS answer_value
        FROM user_question AS uq
        INNER JOIN (
          SELECT user_id, question_id, MAX(id) AS latest_id
          FROM user_question
          WHERE user_id IN (?)
            AND question_id IN (?)
          GROUP BY user_id, question_id
        ) AS latest_uq
          ON latest_uq.latest_id = uq.id
        INNER JOIN question AS q ON q.id = uq.question_id
        LEFT JOIN user_question_answer AS uqa ON uq.id = uqa.user_question_id
        LEFT JOIN answer AS a
          ON a.question_id = uq.question_id
         AND a.id = uqa.answer_id
        GROUP BY uq.id, uq.user_id, uq.question_id, q.answer_type_id, uq.answer_text, uq.answer_number`,
      [userIdChunk, questionIds]
    );

    rows.forEach(row => {
      answerMap.set(
        `${row.user_id}:${row.question_id}`,
        getAnswerValueFromRow(row)
      );
    });
  }

  return answerMap;
}

async function fetchDeliveryEvents(fromDate, toDate, clientId) {
  const fromDateUtc = toUtcDateTime(fromDate);
  const toDateUtc = toUtcDateTime(toDate);

  const [rows] = await mysqlConnection.promise().query(
    `SELECT
        db.id AS delivery_beneficiary_id,
        db.receiving_user_id AS user_id,
        db.location_id,
        loc.community_city AS event_location,
        db.approved,
        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', ?), '%m/%d/%Y') AS event_date,
        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', ?), '%T') AS event_time,
        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', ?), '%Y-%m-%d') AS event_date_key,
        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', ?), '%Y-%m-%d %H:%i:%s') AS event_datetime_la
      FROM delivery_beneficiary AS db
      INNER JOIN client_location AS cl
        ON cl.location_id = db.location_id
       AND cl.client_id = ?
      LEFT JOIN location AS loc ON loc.id = db.location_id
      WHERE db.receiving_user_id NOT IN (?)
        AND db.creation_date BETWEEN ? AND ?
      ORDER BY db.receiving_user_id, db.creation_date, db.id`,
    [
      REPORT_TIMEZONE,
      REPORT_TIMEZONE,
      REPORT_TIMEZONE,
      REPORT_TIMEZONE,
      clientId,
      EXCLUDED_REPORT_USER_IDS,
      fromDateUtc,
      toDateUtc
    ]
  );

  return rows;
}

async function buildRawDataReport({ from_date, to_date, client_id }) {
  const [questionCatalog, participantBaseRows, deliveryEvents] = await Promise.all([
    fetchQuestionCatalog(client_id),
    fetchParticipantBaseRows(from_date, to_date, client_id),
    fetchDeliveryEvents(from_date, to_date, client_id)
  ]);

  const participantUserIds = participantBaseRows.map(row => row.user_id);
  const answersByUserQuestion = await fetchLatestAnswersByUserQuestion(participantUserIds, questionCatalog);

  const participantByUserId = new Map();
  participantBaseRows.forEach(row => {
    participantByUserId.set(row.user_id, {
      ...buildUserBaseRow(row),
      registration_datetime_la: row.registration_datetime_la
    });
  });

  const eventsByUserId = new Map();
  const scannedWindowByLocationDate = new Map();

  deliveryEvents.forEach(eventRow => {
    if (!participantByUserId.has(eventRow.user_id)) {
      return;
    }

    const userEvents = eventsByUserId.get(eventRow.user_id) || [];
    userEvents.push(eventRow);
    eventsByUserId.set(eventRow.user_id, userEvents);

    if (eventRow.approved !== 'Y') {
      return;
    }

    const windowKey = `${eventRow.location_id}:${eventRow.event_date_key}`;
    const eventDateTime = moment.tz(eventRow.event_datetime_la, 'YYYY-MM-DD HH:mm:ss', REPORT_TIMEZONE);
    const scannedWindow = scannedWindowByLocationDate.get(windowKey);

    if (!scannedWindow) {
      scannedWindowByLocationDate.set(windowKey, {
        first: eventDateTime,
        last: eventDateTime
      });
      return;
    }

    if (eventDateTime.isBefore(scannedWindow.first)) {
      scannedWindow.first = eventDateTime;
    }

    if (eventDateTime.isAfter(scannedWindow.last)) {
      scannedWindow.last = eventDateTime;
    }
  });

  const headers = [
    'User ID', 'Username', 'Email', 'Firstname', 'Lastname', 'Date of birth',
    'Phone', 'Zipcode', 'Household size', 'Gender', 'Ethnicity', 'Other ethnicity',
    'Last location visited', 'Locations visited', 'Registration date', 'Registration time',
    'Event Date', 'Event Time', 'Event Location', 'Registered at Client Location'
  ];

  questionCatalog.forEach(question => {
    headers.push(question.question);
  });

  const rowEntries = [];
  let originalOrder = 0;

  participantBaseRows.forEach(participantRow => {
    const baseRow = participantByUserId.get(participantRow.user_id);
    const participantEvents = eventsByUserId.get(participantRow.user_id) || [];

    if (participantEvents.length === 0) {
      const excelRow = [
        baseRow.user_id,
        baseRow.username,
        baseRow.email,
        baseRow.firstname,
        baseRow.lastname,
        baseRow.date_of_birth,
        baseRow.phone,
        baseRow.zipcode,
        baseRow.household_size,
        baseRow.gender,
        baseRow.ethnicity,
        baseRow.other_ethnicity,
        baseRow.last_location_visited,
        baseRow.locations_visited,
        baseRow.registration_date,
        baseRow.registration_time,
        '',
        '',
        '',
        baseRow.registered_at_client_location
      ];

      questionCatalog.forEach(question => {
        const answerValue = answersByUserQuestion.get(`${baseRow.user_id}:${question.question_id}`);
        excelRow.push(answerValue ?? '');
      });

      rowEntries.push({
        excelRow,
        originalOrder: originalOrder++,
        ...buildRawDataSortMetadata({
          registrationDateTimeLa: baseRow.registration_datetime_la,
          eventDateTimeLa: null,
          eventDateValue: null,
          eventTimeValue: ''
        })
      });
      return;
    }

    participantEvents.forEach(eventRow => {
      const windowKey = `${eventRow.location_id}:${eventRow.event_date_key}`;
      const scannedWindow = scannedWindowByLocationDate.get(windowKey);
      const eventTimeValue = computeEventTimeValue(eventRow, scannedWindow);

      const excelRow = [
        baseRow.user_id,
        baseRow.username,
        baseRow.email,
        baseRow.firstname,
        baseRow.lastname,
        baseRow.date_of_birth,
        baseRow.phone,
        baseRow.zipcode,
        baseRow.household_size,
        baseRow.gender,
        baseRow.ethnicity,
        baseRow.other_ethnicity,
        baseRow.last_location_visited,
        baseRow.locations_visited,
        baseRow.registration_date,
        baseRow.registration_time,
        eventRow.event_date,
        eventTimeValue,
        eventRow.event_location,
        baseRow.registered_at_client_location
      ];

      questionCatalog.forEach(question => {
        const answerValue = answersByUserQuestion.get(`${baseRow.user_id}:${question.question_id}`);
        excelRow.push(answerValue ?? '');
      });

      rowEntries.push({
        excelRow,
        originalOrder: originalOrder++,
        ...buildRawDataSortMetadata({
          registrationDateTimeLa: baseRow.registration_datetime_la,
          eventDateTimeLa: eventRow.event_datetime_la,
          eventDateValue: eventRow.event_date,
          eventTimeValue
        })
      });
    });
  });

  rowEntries.sort((left, right) => {
    if (left.sortDateKey !== right.sortDateKey) {
      return left.sortDateKey.localeCompare(right.sortDateKey);
    }

    if (left.sortBucket !== right.sortBucket) {
      return left.sortBucket - right.sortBucket;
    }

    if (left.sortDateTimeValue !== right.sortDateTimeValue) {
      return left.sortDateTimeValue - right.sortDateTimeValue;
    }

    return left.originalOrder - right.originalOrder;
  });

  const excelData = [headers, ...rowEntries.map(rowEntry => rowEntry.excelRow)];

  const workbook = XLSX.utils.book_new();
  const worksheet = XLSX.utils.aoa_to_sheet(excelData);
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Raw Data');

  return {
    headers,
    excelData,
    workbook,
    excelBuffer: XLSX.write(workbook, { bookType: 'xlsx', type: 'buffer' })
  };
}

async function getRawDataExcel(from_date, to_date, client_id) {
  const report = await buildRawDataReport({ from_date, to_date, client_id });
  return report.excelBuffer;
}

module.exports = {
  EXCLUDED_REPORT_USER_IDS,
  REPORT_TIMEZONE,
  EVENT_TIME_UNMATCHED_THRESHOLD_HOURS,
  buildRawDataReport,
  getRawDataExcel
};
