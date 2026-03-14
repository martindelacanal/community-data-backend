const mysqlConnection = require('../connection/connection');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;

const ALL_DATA_FROM_DATE = '1970-01-01';
const ALL_DATA_TO_DATE = '2100-01-01';

function stringifyCsv(header, rows) {
  const csvStringifier = createCsvStringifier({
    header,
    fieldDelimiter: ';'
  });

  let csvData = csvStringifier.getHeaderString();
  csvData += csvStringifier.stringifyRecords(rows);
  return csvData;
}

function parseUsDate(value) {
  const [month, day, year] = String(value || '').split('/').map(Number);
  if (!year || !month || !day) {
    return 0;
  }

  return Date.UTC(year, month - 1, day);
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
    csvData: stringifyCsv(headers, rows),
    rowCount: rows.length,
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
    csvData: stringifyCsv(headers, rows),
    rowCount: rows.length,
    fileName: 'workers-table.csv'
  };
}

async function generateTicketTableCsvs() {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT
        dt.id,
        dt.donation_id,
        dt.total_weight,
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
        DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', 'America/Los_Angeles'), '%T') AS creation_time,
        product.id AS product_id,
        product.name AS product,
        pt.name AS product_type,
        pdt.quantity AS quantity
      FROM donation_ticket AS dt
      LEFT JOIN stocker_log AS sl ON dt.id = sl.donation_ticket_id AND sl.operation_id = 5
      LEFT JOIN delivered_by AS db ON dt.delivered_by = db.id
      LEFT JOIN transported_by AS tb ON dt.transported_by_id = tb.id
      LEFT JOIN provider AS p ON dt.provider_id = p.id
      LEFT JOIN audit_status AS as1 ON dt.audit_status_id = as1.id
      LEFT JOIN location AS loc ON dt.location_id = loc.id
      LEFT JOIN user AS u ON sl.user_id = u.id
      LEFT JOIN product_donation_ticket AS pdt ON dt.id = pdt.donation_ticket_id
      LEFT JOIN product AS product ON pdt.product_id = product.id
      LEFT JOIN product_type AS pt ON product.product_type_id = pt.id
      WHERE dt.enabled = 'Y'
      ORDER BY dt.date, dt.id, pdt.id`
  );

  const headers = [
    { id: 'id', title: 'ID' },
    { id: 'donation_id', title: 'Donation ID' },
    { id: 'total_weight', title: 'Total weight' },
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
    { id: 'creation_time', title: 'Creation time' },
    { id: 'product_id', title: 'Product ID' },
    { id: 'product', title: 'Product' },
    { id: 'product_type', title: 'Product type' },
    { id: 'quantity', title: 'Quantity' }
  ];

  const headersWithoutProduct = headers.filter(
    header => !['product_id', 'product', 'product_type', 'quantity'].includes(header.id)
  );

  const uniqueTicketIds = new Set();
  const uniqueRows = rows.filter(row => {
    if (uniqueTicketIds.has(row.id)) {
      return false;
    }

    uniqueTicketIds.add(row.id);
    return true;
  });

  return {
    tickets: {
      csvData: stringifyCsv(headersWithoutProduct, uniqueRows),
      rowCount: uniqueRows.length,
      fileName: 'tickets.csv'
    },
    ticketsWithFood: {
      csvData: stringifyCsv(headers, rows),
      rowCount: rows.length,
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
    csvData: stringifyCsv(headers, rows),
    rowCount: rows.length,
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
    csvData: stringifyCsv(headers, rows),
    rowCount: rows.length,
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
