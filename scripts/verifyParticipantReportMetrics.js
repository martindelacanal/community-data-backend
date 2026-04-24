require('dotenv').config({ path: './.env' });

const XLSX = require('xlsx-js-style');
const moment = require('moment-timezone');
const mysqlConnection = require('../api/connection/connection');
const {
  REPORT_TIMEZONE,
  getRawDataExcel
} = require('../api/services/rawDataReport');
const {
  getParticipantRegisterSummary
} = require('../api/services/participantRegistrationMetrics');

function getArgValue(name) {
  const prefix = `--${name}=`;
  const arg = process.argv.find(value => value.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : null;
}

function parseClientIds() {
  const rawClientIds = getArgValue('client-ids') || getArgValue('client-id') || '1,2';
  return rawClientIds
    .split(',')
    .map(value => Number(value.trim()))
    .filter(value => Number.isFinite(value) && value > 0);
}

function resolveDateRange() {
  const fromArg = getArgValue('from');
  const toArg = getArgValue('to');

  if (fromArg && toArg) {
    return {
      fromDate: fromArg,
      toDate: toArg
    };
  }

  const today = moment().tz(REPORT_TIMEZONE);
  const lastMonday = today.clone().subtract(1, 'week').startOf('isoWeek');
  const lastSunday = lastMonday.clone().endOf('isoWeek');

  return {
    fromDate: lastMonday.format('YYYY-MM-DD'),
    toDate: lastSunday.format('YYYY-MM-DD')
  };
}

async function fetchReportLocations(clientId) {
  let locationQuery = `
    SELECT l.id, l.community_city AS name
    FROM location l
    INNER JOIN client_location cl ON l.id = cl.location_id
    WHERE cl.client_id = ?`;

  if (Number(clientId) === 2) {
    locationQuery += ' AND l.id != 32';
  }

  locationQuery += ' ORDER BY l.id';

  const [locations] = await mysqlConnection.promise().query(locationQuery, [clientId]);
  return locations;
}

function getUniqueRawRowsByUserId(excelRawData) {
  const workbook = XLSX.read(excelRawData, { type: 'buffer' });
  const worksheet = workbook.Sheets['Raw Data'];
  const rows = XLSX.utils.sheet_to_json(worksheet);
  const rowsByUserId = new Map();

  rows.forEach(row => {
    const userId = Number(row['User ID']);
    if (Number.isFinite(userId) && !rowsByUserId.has(userId)) {
      rowsByUserId.set(userId, row);
    }
  });

  return rowsByUserId;
}

function sumLocationMap(locationMap, locations) {
  return locations.reduce((total, location) => total + Number(locationMap[location.id] || 0), 0);
}

async function verifyClient(clientId, fromDate, toDate) {
  const locations = await fetchReportLocations(clientId);
  const locationIds = locations.map(location => Number(location.id));

  const metrics = await getParticipantRegisterSummary(
    { role: 'client', client_id: clientId },
    { from_date: fromDate, to_date: toDate, locations: locationIds },
    { includeDetails: true, clientId }
  );

  const excelRawData = await getRawDataExcel(
    `${fromDate} 00:00:00`,
    `${toDate} 23:59:59`,
    clientId
  );
  const rawRowsByUserId = getUniqueRawRowsByUserId(excelRawData);
  const newRegistrationUniqueCount = metrics.newUserIds.filter(userId => rawRowsByUserId.has(Number(userId))).length;
  const locationNewTotal = sumLocationMap(metrics.newPerLocationMap, locations);
  const locationRecurringTotal = sumLocationMap(metrics.recurringPerLocationMap, locations);

  const checks = {
    newMatchesLocationTotal: Number(metrics.new) === locationNewTotal,
    recurringMatchesLocationTotal: Number(metrics.recurring) === locationRecurringTotal,
    newMatchesRegistrationFile: Number(metrics.new) === newRegistrationUniqueCount
  };

  const result = {
    clientId,
    fromDate,
    toDate,
    new: Number(metrics.new),
    recurring: Number(metrics.recurring),
    locationNewTotal,
    locationRecurringTotal,
    newRegistrationUniqueCount,
    checks
  };

  console.log(JSON.stringify(result, null, 2));

  if (!Object.values(checks).every(Boolean)) {
    throw new Error(`Participant report verification failed for client_id=${clientId}`);
  }
}

async function main() {
  const clientIds = parseClientIds();
  const { fromDate, toDate } = resolveDateRange();

  for (const clientId of clientIds) {
    await verifyClient(clientId, fromDate, toDate);
  }
}

main()
  .catch(error => {
    console.error(error.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mysqlConnection.end();
  });
