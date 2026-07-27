const { Readable } = require('stream');
const mysqlConnection = require('../connection/connection');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;

const ALL_DATA_FROM_DATE = '1970-01-01';
const ALL_DATA_TO_DATE = '2100-01-01';
const CSV_STREAM_BATCH_SIZE = 500;

function createCsvReadableFromRows(header, rows) {
  const csvStringifier = createCsvStringifier({
    header,
    fieldDelimiter: ';'
  });

  async function* generator() {
    yield Buffer.from(csvStringifier.getHeaderString(), 'utf8');
    for (let i = 0; i < rows.length; i += CSV_STREAM_BATCH_SIZE) {
      const batch = rows.slice(i, i + CSV_STREAM_BATCH_SIZE);
      yield Buffer.from(csvStringifier.stringifyRecords(batch), 'utf8');
    }
  }

  return Readable.from(generator(), { objectMode: false });
}

function parseUsDate(value) {
  const [month, day, year] = String(value || '').split('/').map(Number);
  if (!year || !month || !day) {
    return 0;
  }

  return Date.UTC(year, month - 1, day);
}

function addTicketDestinationCsvColumns(row) {
  let destinations = row.destinations_json;
  if (Buffer.isBuffer(destinations)) {
    destinations = destinations.toString('utf8');
  }
  if (typeof destinations === 'string') {
    try {
      destinations = JSON.parse(destinations);
    } catch (error) {
      destinations = [];
    }
  }
  if (!Array.isArray(destinations)) {
    destinations = [];
  }

  destinations.sort((left, right) => (
    Number(left.display_order) - Number(right.display_order)
    || Number(left.relation_id) - Number(right.relation_id)
  ));

  return {
    ...row,
    destination_ids: destinations
      .map((destination) => destination.location_id)
      .join(', '),
    destinations: destinations
      .map((destination) => destination.location || `#${destination.location_id}`)
      .join(' | '),
    destination_weights: destinations
      .map((destination) => {
        const location = destination.location || `#${destination.location_id}`;
        const weight = destination.total_weight === null
          || destination.total_weight === undefined
          ? ''
          : destination.total_weight;
        return `${location} [${destination.location_id}]: ${weight} lb`;
      })
      .join(' | ')
  };
}

async function generateVolunteerTableCsv() {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT
        v.id,
        v.firstname,
        v.lastname,
        DATE_FORMAT(v.date_of_birth, '%m/%d/%Y') AS date_of_birth,
        v.email,
        v.phone,
        v.zipcode,
        g.name AS gender,
        e.name AS ethnicity,
        v.other_ethnicity,
        l.community_city AS location,
        DATE_FORMAT(CONVERT_TZ(v.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
        DATE_FORMAT(CONVERT_TZ(v.creation_date, '+00:00', 'America/Los_Angeles'), '%T') AS creation_time
      FROM volunteer AS v
      INNER JOIN ethnicity AS e ON v.ethnicity_id = e.id
      INNER JOIN gender AS g ON v.gender_id = g.id
      INNER JOIN location AS l ON v.location_id = l.id
      WHERE CONVERT_TZ(v.creation_date, '+00:00', 'America/Los_Angeles') >= ?
        AND CONVERT_TZ(v.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
      ORDER BY v.id`,
    [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE]
  );

  const headers = [
    { id: 'id', title: 'ID' },
    { id: 'firstname', title: 'Firstname' },
    { id: 'lastname', title: 'Lastname' },
    { id: 'date_of_birth', title: 'Date of birth' },
    { id: 'email', title: 'Email' },
    { id: 'phone', title: 'Phone' },
    { id: 'zipcode', title: 'Zipcode' },
    { id: 'gender', title: 'Gender' },
    { id: 'ethnicity', title: 'Ethnicity' },
    { id: 'other_ethnicity', title: 'Other ethnicity' },
    { id: 'location', title: 'Location' },
    { id: 'creation_date', title: 'Creation date' },
    { id: 'creation_time', title: 'Creation time' }
  ];

  return {
    body: createCsvReadableFromRows(headers, rows),
    getRowCount: () => rows.length,
    fileName: 'volunteers-table.csv'
  };
}

async function generateWorkerTableCsv() {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT
        dl.id,
        dl.user_id,
        u.username,
        u.firstname,
        u.lastname,
        l.community_city,
        DATE_FORMAT(CONVERT_TZ(dl.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y %T') AS onboarding_date,
        DATE_FORMAT(CONVERT_TZ(dl.offboarding_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y %T') AS offboarding_date
      FROM delivery_log AS dl
      INNER JOIN user AS u ON dl.user_id = u.id
      LEFT JOIN location AS l ON dl.location_id = l.id
      WHERE u.enabled = 'Y'
        AND dl.operation_id = 3
      ORDER BY dl.id`
  );

  const headers = [
    { id: 'id', title: 'ID' },
    { id: 'user_id', title: 'User ID' },
    { id: 'username', title: 'Username' },
    { id: 'firstname', title: 'First Name' },
    { id: 'lastname', title: 'Last Name' },
    { id: 'community_city', title: 'Location' },
    { id: 'onboarding_date', title: 'Onboarding Date' },
    { id: 'offboarding_date', title: 'Offboarding Date' }
  ];

  return {
    body: createCsvReadableFromRows(headers, rows),
    getRowCount: () => rows.length,
    fileName: 'workers-table.csv'
  };
}

async function generateTicketTableCsvs() {
  const [ticketRows, ticketFoodRows] = await Promise.all([
    mysqlConnection.promise().query(
      `SELECT
        dt.id,
        dt.donation_id,
        dt.total_weight,
        dtl.location_id,
        dtl.total_weight AS location_total_weight,
        dtl.display_order AS location_display_order,
        dtl.id AS location_relation_id,
        p.id AS provider_id,
        p.name AS provider,
        loc.community_city AS location,
        DATE_FORMAT(dt.date, '%m/%d/%Y') AS date,
        db.name AS delivered_by,
        tb.name AS transported_by,
        as1.name AS audit_status,
        u.id AS created_by_id,
        u.username AS created_by_username,
        DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
        DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', 'America/Los_Angeles'), '%T') AS creation_time
      FROM donation_ticket AS dt
      LEFT JOIN stocker_log AS sl ON dt.id = sl.donation_ticket_id AND sl.operation_id = 5
      LEFT JOIN delivered_by AS db ON dt.delivered_by = db.id
      LEFT JOIN transported_by AS tb ON dt.transported_by_id = tb.id
      LEFT JOIN provider AS p ON dt.provider_id = p.id
      LEFT JOIN audit_status AS as1 ON dt.audit_status_id = as1.id
      INNER JOIN donation_ticket_location AS dtl ON dt.id = dtl.donation_ticket_id
      INNER JOIN location AS loc ON dtl.location_id = loc.id
      LEFT JOIN user AS u ON sl.user_id = u.id
      WHERE dt.enabled = 'Y'
      ORDER BY dt.date, dt.id, dtl.display_order, dtl.id`
    ),
    mysqlConnection.promise().query(
      `WITH visible_ticket_destinations AS (
         SELECT
           dtl.donation_ticket_id,
           JSON_ARRAYAGG(
             JSON_OBJECT(
               'location_id', dtl.location_id,
               'location', loc.community_city,
               'total_weight', dtl.total_weight,
               'display_order', dtl.display_order,
               'relation_id', dtl.id
             )
           ) AS destinations_json
         FROM donation_ticket_location AS dtl
         INNER JOIN location AS loc ON dtl.location_id = loc.id
         GROUP BY dtl.donation_ticket_id
       ),
       ticket_products AS (
         SELECT
           donation_ticket_id,
           product_id,
           SUM(quantity) AS quantity,
           MIN(id) AS sort_id
         FROM product_donation_ticket
         GROUP BY donation_ticket_id, product_id
       )
       SELECT
         dt.id,
         dt.donation_id,
         dt.total_weight,
         destinations.destinations_json,
         p.id AS provider_id,
         p.name AS provider,
         DATE_FORMAT(dt.date, '%m/%d/%Y') AS date,
         db.name AS delivered_by,
         tb.name AS transported_by,
         as1.name AS audit_status,
         u.id AS created_by_id,
         u.username AS created_by_username,
         DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
         DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', 'America/Los_Angeles'), '%T') AS creation_time,
         product.id AS product_id,
         product.name AS product,
         pt.name AS product_type,
         ticket_product.quantity
       FROM donation_ticket AS dt
       LEFT JOIN stocker_log AS sl ON dt.id = sl.donation_ticket_id AND sl.operation_id = 5
       LEFT JOIN delivered_by AS db ON dt.delivered_by = db.id
       LEFT JOIN transported_by AS tb ON dt.transported_by_id = tb.id
       LEFT JOIN provider AS p ON dt.provider_id = p.id
       LEFT JOIN audit_status AS as1 ON dt.audit_status_id = as1.id
       INNER JOIN visible_ticket_destinations AS destinations
         ON dt.id = destinations.donation_ticket_id
       LEFT JOIN user AS u ON sl.user_id = u.id
       LEFT JOIN ticket_products AS ticket_product
         ON dt.id = ticket_product.donation_ticket_id
       LEFT JOIN product AS product ON ticket_product.product_id = product.id
       LEFT JOIN product_type AS pt ON product.product_type_id = pt.id
       WHERE dt.enabled = 'Y'
       ORDER BY dt.date, dt.id, ticket_product.sort_id`
    )
  ]);

  const uniqueTicketLocations = new Set();
  const ticketsWithGeneralWeight = new Set();
  const rows = ticketRows[0].reduce((result, row) => {
    const key = `${row.id}:${row.location_id}`;
    if (uniqueTicketLocations.has(key)) {
      return result;
    }

    uniqueTicketLocations.add(key);
    const isFirstDestination = !ticketsWithGeneralWeight.has(row.id);
    ticketsWithGeneralWeight.add(row.id);
    result.push({
      ...row,
      total_weight: isFirstDestination ? row.total_weight : ''
    });
    return result;
  }, []);

  const uniqueTicketProducts = new Set();
  const foodRows = ticketFoodRows[0].map(addTicketDestinationCsvColumns).filter((row) => {
    const key = `${row.id}:${row.product_id ?? 'no-product'}`;
    if (uniqueTicketProducts.has(key)) {
      return false;
    }

    uniqueTicketProducts.add(key);
    return true;
  });

  const ticketHeaders = [
    { id: 'id', title: 'ID' },
    { id: 'donation_id', title: 'Donation ID' },
    { id: 'total_weight', title: 'Total weight' },
    { id: 'location_id', title: 'Location ID' },
    { id: 'location_total_weight', title: 'Location total weight' },
    { id: 'provider_id', title: 'Provider ID' },
    { id: 'provider', title: 'Provider' },
    { id: 'location', title: 'Location' },
    { id: 'date', title: 'Date' },
    { id: 'delivered_by', title: 'Delivered by' },
    { id: 'transported_by', title: 'Transported by' },
    { id: 'audit_status', title: 'Audit status' },
    { id: 'created_by_id', title: 'Created by ID' },
    { id: 'created_by_username', title: 'Created by username' },
    { id: 'creation_date', title: 'Creation date' },
    { id: 'creation_time', title: 'Creation time' }
  ];

  const foodHeaders = [
    { id: 'id', title: 'ID' },
    { id: 'donation_id', title: 'Donation ID' },
    { id: 'total_weight', title: 'Total weight' },
    { id: 'destination_ids', title: 'Destination IDs' },
    { id: 'destinations', title: 'Destinations' },
    { id: 'destination_weights', title: 'Destination weights' },
    { id: 'provider_id', title: 'Provider ID' },
    { id: 'provider', title: 'Provider' },
    { id: 'date', title: 'Date' },
    { id: 'delivered_by', title: 'Delivered by' },
    { id: 'transported_by', title: 'Transported by' },
    { id: 'audit_status', title: 'Audit status' },
    { id: 'created_by_id', title: 'Created by ID' },
    { id: 'created_by_username', title: 'Created by username' },
    { id: 'creation_date', title: 'Creation date' },
    { id: 'creation_time', title: 'Creation time' },
    { id: 'product_id', title: 'Product ID' },
    { id: 'product', title: 'Product' },
    { id: 'product_type', title: 'Product type' },
    { id: 'quantity', title: 'Quantity' }
  ];

  return {
    tickets: {
      body: createCsvReadableFromRows(ticketHeaders, rows),
      getRowCount: () => rows.length,
      fileName: 'tickets.csv'
    },
    ticketsWithFood: {
      body: createCsvReadableFromRows(foodHeaders, foodRows),
      getRowCount: () => foodRows.length,
      fileName: 'tickets-with-food.csv'
    }
  };
}

async function generateBeneficiarySummaryCsv() {
  const query1 = `SELECT
      loc.id AS location_id,
      loc.community_city,
      DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
      SUM(
        IF(
          NOT EXISTS (
            SELECT 1
            FROM delivery_beneficiary db1
            WHERE db1.receiving_user_id = db.receiving_user_id
              AND CONVERT_TZ(db1.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles')
          )
          AND DATE(db.creation_date) = DATE(u.creation_date),
          1,
          0
        )
      ) AS count_beneficiaries_creation_date
    FROM delivery_beneficiary AS db
    INNER JOIN location AS loc ON db.location_id = loc.id
    INNER JOIN user AS u ON db.receiving_user_id = u.id
    WHERE CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') >= ?
      AND CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
    GROUP BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')
    ORDER BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')`;

  const query2 = `SELECT
      loc.id AS location_id,
      loc.community_city,
      DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
      COUNT(
        DISTINCT IF(
          NOT EXISTS (
            SELECT 1
            FROM delivery_beneficiary db2
            WHERE db2.receiving_user_id = db.receiving_user_id
              AND db2.location_id != db.location_id
              AND CONVERT_TZ(db2.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles')
          )
          AND EXISTS (
            SELECT 1
            FROM delivery_beneficiary db3
            WHERE db3.receiving_user_id = db.receiving_user_id
              AND db3.location_id = db.location_id
              AND CONVERT_TZ(db3.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles')
          )
          AND DATE(db.creation_date) > DATE(u.creation_date),
          db.receiving_user_id,
          NULL
        )
      ) AS count_beneficiaries_same_location
    FROM delivery_beneficiary AS db
    INNER JOIN location AS loc ON db.location_id = loc.id
    INNER JOIN user AS u ON db.receiving_user_id = u.id
    WHERE CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') >= ?
      AND CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
    GROUP BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')
    ORDER BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')`;

  const query3 = `SELECT
      loc.id AS location_id,
      loc.community_city,
      DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
      SUM(
        IF(
          EXISTS (
            SELECT 1
            FROM delivery_beneficiary db1
            WHERE db1.receiving_user_id = db.receiving_user_id
              AND CONVERT_TZ(db1.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles')
            GROUP BY db1.receiving_user_id
            HAVING COUNT(DISTINCT db1.location_id) > 1
          ),
          1,
          0
        )
      ) AS count_beneficiaries_same_and_other_location
    FROM delivery_beneficiary AS db
    INNER JOIN location AS loc ON db.location_id = loc.id
    WHERE CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') >= ?
      AND CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
    GROUP BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')
    ORDER BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')`;

  const query4 = `SELECT
      loc.id AS location_id,
      loc.community_city,
      DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
      SUM(
        IF(
          NOT EXISTS (
            SELECT 1
            FROM delivery_beneficiary db1
            WHERE db1.receiving_user_id = db.receiving_user_id
              AND db1.location_id = db.location_id
              AND CONVERT_TZ(db1.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles')
          )
          AND EXISTS (
            SELECT 1
            FROM delivery_beneficiary db2
            WHERE db2.receiving_user_id = db.receiving_user_id
              AND db2.location_id != db.location_id
              AND CONVERT_TZ(db2.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles')
          ),
          1,
          0
        )
      ) AS count_beneficiaries_first_time
    FROM delivery_beneficiary AS db
    INNER JOIN location AS loc ON db.location_id = loc.id
    WHERE CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') >= ?
      AND CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
    GROUP BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')
    ORDER BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')`;

  const query5 = `SELECT
      loc.id AS location_id,
      loc.community_city,
      DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
      SUM(
        IF(
          NOT EXISTS (
            SELECT 1
            FROM delivery_beneficiary db1
            WHERE db1.receiving_user_id = db.receiving_user_id
              AND CONVERT_TZ(db1.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles')
          )
          AND DATE(db.creation_date) > DATE(u.creation_date),
          1,
          0
        )
      ) AS count_beneficiaries_already_registered_first_time
    FROM delivery_beneficiary AS db
    INNER JOIN location AS loc ON db.location_id = loc.id
    INNER JOIN user AS u ON db.receiving_user_id = u.id
    WHERE CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') >= ?
      AND CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
    GROUP BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')
    ORDER BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')`;

  const query6 = `SELECT
      loc.id AS location_id,
      loc.community_city,
      DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
      COUNT(DISTINCT db.receiving_user_id) AS total_beneficiaries
    FROM delivery_beneficiary AS db
    INNER JOIN location AS loc ON db.location_id = loc.id
    WHERE CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') >= ?
      AND CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
    GROUP BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')
    ORDER BY loc.id, DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y')`;

  const [
    [countByCreationDate],
    [countSameLocation],
    [countSameAndOtherLocation],
    [countFirstTime],
    [countAlreadyRegisteredFirstTime],
    [totalBeneficiaries]
  ] = await Promise.all([
    mysqlConnection.promise().query(query1, [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE]),
    mysqlConnection.promise().query(query2, [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE]),
    mysqlConnection.promise().query(query3, [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE]),
    mysqlConnection.promise().query(query4, [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE]),
    mysqlConnection.promise().query(query5, [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE]),
    mysqlConnection.promise().query(query6, [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE])
  ]);

  const rowsByKey = new Map();
  const ensureRow = sourceRow => {
    const key = `${sourceRow.location_id}:${sourceRow.creation_date}`;
    if (!rowsByKey.has(key)) {
      rowsByKey.set(key, {
        location_id: sourceRow.location_id,
        community_city: sourceRow.community_city,
        count_beneficiaries_creation_date: 0,
        count_beneficiaries_same_location: 0,
        count_beneficiaries_same_and_other_location: 0,
        count_beneficiaries_first_time: 0,
        count_beneficiaries_already_registered_first_time: 0,
        total_beneficiaries: 0,
        creation_date: sourceRow.creation_date
      });
    }

    return rowsByKey.get(key);
  };

  for (let i = 0; i < countByCreationDate.length; i++) {
    const row = ensureRow(countByCreationDate[i]);
    row.count_beneficiaries_creation_date = countByCreationDate[i].count_beneficiaries_creation_date;
  }
  for (let i = 0; i < countSameLocation.length; i++) {
    const row = ensureRow(countSameLocation[i]);
    row.count_beneficiaries_same_location = countSameLocation[i].count_beneficiaries_same_location;
  }
  for (let i = 0; i < countSameAndOtherLocation.length; i++) {
    const row = ensureRow(countSameAndOtherLocation[i]);
    row.count_beneficiaries_same_and_other_location = countSameAndOtherLocation[i].count_beneficiaries_same_and_other_location;
  }
  for (let i = 0; i < countFirstTime.length; i++) {
    const row = ensureRow(countFirstTime[i]);
    row.count_beneficiaries_first_time = countFirstTime[i].count_beneficiaries_first_time;
  }
  for (let i = 0; i < countAlreadyRegisteredFirstTime.length; i++) {
    const row = ensureRow(countAlreadyRegisteredFirstTime[i]);
    row.count_beneficiaries_already_registered_first_time = countAlreadyRegisteredFirstTime[i].count_beneficiaries_already_registered_first_time;
  }
  for (let i = 0; i < totalBeneficiaries.length; i++) {
    const row = ensureRow(totalBeneficiaries[i]);
    row.total_beneficiaries = totalBeneficiaries[i].total_beneficiaries;
  }

  const rows = Array.from(rowsByKey.values()).sort((left, right) => {
    const locationDiff = Number(left.location_id) - Number(right.location_id);
    if (locationDiff !== 0) {
      return locationDiff;
    }

    return parseUsDate(left.creation_date) - parseUsDate(right.creation_date);
  });

  const headers = [
    { id: 'location_id', title: 'Location ID' },
    { id: 'community_city', title: 'Community city' },
    { id: 'count_beneficiaries_creation_date', title: 'Beneficiaries who registered in that location and scanned QR' },
    { id: 'count_beneficiaries_same_location', title: 'Beneficiaries who always go to the same location' },
    { id: 'count_beneficiaries_same_and_other_location', title: 'Beneficiaries who have already gone to the location and have gone to others' },
    { id: 'count_beneficiaries_first_time', title: 'Beneficiaries who are going for the first time but have already gone to another location' },
    { id: 'count_beneficiaries_already_registered_first_time', title: 'Beneficiaries who are going for the first time and have not gone to another location (already registered)' },
    { id: 'total_beneficiaries', title: 'Total beneficiaries' },
    { id: 'creation_date', title: 'Date' }
  ];

  return {
    body: createCsvReadableFromRows(headers, rows),
    getRowCount: () => rows.length,
    fileName: 'beneficiary-summary.csv'
  };
}

async function generateDeliverySummaryCsv() {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT
        db.id,
        db.delivering_user_id,
        u1.username AS delivery_username,
        db.receiving_user_id,
        u2.username AS beneficiary_username,
        u2.firstname AS beneficiary_firstname,
        u2.lastname AS beneficiary_lastname,
        db.location_id,
        l.community_city,
        db.approved,
        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS creation_date,
        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles'), '%T') AS creation_time
      FROM delivery_beneficiary AS db
      INNER JOIN location AS l ON db.location_id = l.id
      INNER JOIN user AS u2 ON db.receiving_user_id = u2.id
      LEFT JOIN user AS u1 ON db.delivering_user_id = u1.id
      WHERE CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') >= ?
        AND CONVERT_TZ(db.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
      ORDER BY db.id`,
    [ALL_DATA_FROM_DATE, ALL_DATA_TO_DATE]
  );

  const headers = [
    { id: 'id', title: 'ID' },
    { id: 'delivering_user_id', title: 'Delivering user ID' },
    { id: 'delivery_username', title: 'Delivery username' },
    { id: 'receiving_user_id', title: 'Receiving user ID' },
    { id: 'beneficiary_username', title: 'Beneficiary username' },
    { id: 'beneficiary_firstname', title: 'Beneficiary firstname' },
    { id: 'beneficiary_lastname', title: 'Beneficiary lastname' },
    { id: 'location_id', title: 'Location ID' },
    { id: 'community_city', title: 'Community city' },
    { id: 'approved', title: 'Approved' },
    { id: 'creation_date', title: 'Creation date' },
    { id: 'creation_time', title: 'Creation time' }
  ];

  return {
    body: createCsvReadableFromRows(headers, rows),
    getRowCount: () => rows.length,
    fileName: 'delivery-summary.csv'
  };
}

module.exports = {
  generateBeneficiarySummaryCsv,
  generateDeliverySummaryCsv,
  generateTicketTableCsvs,
  generateVolunteerTableCsv,
  generateWorkerTableCsv
};
