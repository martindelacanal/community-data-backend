const fs = require('fs');
const path = require('path');
const moment = require('moment-timezone');

require('dotenv').config({
  path: path.resolve(__dirname, '../.env')
});

const mysqlConnection = require('../api/connection/connection.js');
const {
  buildRawDataReport,
  REPORT_TIMEZONE
} = require('../api/services/rawDataReport');

function parseCliArgs(argv) {
  const args = {};

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (!token.startsWith('--')) {
      continue;
    }

    const key = token.slice(2);
    const nextToken = argv[i + 1];
    if (!nextToken || nextToken.startsWith('--')) {
      args[key] = true;
      continue;
    }

    args[key] = nextToken;
    i += 1;
  }

  return args;
}

function normalizeDateTimeInput(value, boundary) {
  if (!value) {
    return null;
  }

  const trimmedValue = String(value).trim();
  const acceptedFormats = ['YYYY-MM-DD HH:mm:ss', 'YYYY-MM-DD'];

  for (let i = 0; i < acceptedFormats.length; i++) {
    const format = acceptedFormats[i];
    const parsed = moment.tz(trimmedValue, format, true, REPORT_TIMEZONE);
    if (!parsed.isValid()) {
      continue;
    }

    if (format === 'YYYY-MM-DD') {
      if (boundary === 'start') {
        return parsed.startOf('day').format('YYYY-MM-DD HH:mm:ss');
      }

      return parsed.endOf('day').format('YYYY-MM-DD HH:mm:ss');
    }

    return parsed.format('YYYY-MM-DD HH:mm:ss');
  }

  throw new Error(`Invalid date format "${trimmedValue}". Use YYYY-MM-DD or YYYY-MM-DD HH:mm:ss.`);
}

function sanitizeFilenameSegment(value) {
  const normalizedValue = String(value || '').trim();
  if (!normalizedValue) {
    return '';
  }

  return normalizedValue
    .replace(/[<>:"/\\|?*\x00-\x1F]+/g, '-')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

async function fetchClientFileLabel(clientId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT short_name
     FROM client
     WHERE id = ?
     LIMIT 1`,
    [clientId]
  );

  return sanitizeFilenameSegment(rows[0] && rows[0].short_name) || `client-${clientId}`;
}

async function main() {
  const args = parseCliArgs(process.argv.slice(2));

  if (args.help) {
    console.log('Usage: node scripts/generateHistoricalRawDataReport.js [--client-id 1] [--from 1970-01-01] [--to 2026-04-16] [--output generated-reports/report.xlsx]');
    return;
  }

  const clientId = Number.parseInt(args['client-id'] || '1', 10);
  if (!Number.isInteger(clientId) || clientId <= 0) {
    throw new Error('client-id must be a positive integer.');
  }

  const fromDate = normalizeDateTimeInput(args.from, 'start') || '1970-01-01 00:00:00';
  const toDate = normalizeDateTimeInput(args.to, 'end')
    || moment().tz(REPORT_TIMEZONE).endOf('day').format('YYYY-MM-DD HH:mm:ss');

  const clientFileLabel = await fetchClientFileLabel(clientId);
  const defaultOutputPath = path.resolve(
    __dirname,
    '..',
    'generated-reports',
    `historical-raw-data-${clientFileLabel}.xlsx`
  );
  const outputPath = path.resolve(args.output || defaultOutputPath);

  const report = await buildRawDataReport({
    from_date: fromDate,
    to_date: toDate,
    client_id: clientId
  });

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, report.excelBuffer);

  console.log(`Historical raw-data report generated.`);
  console.log(`Client ID: ${clientId}`);
  console.log(`Range (${REPORT_TIMEZONE}): ${fromDate} -> ${toDate}`);
  console.log(`Rows: ${Math.max(report.excelData.length - 1, 0)}`);
  console.log(`Output: ${outputPath}`);
}

main()
  .catch(error => {
    console.error(`Historical raw-data report failed: ${error.message}`);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await mysqlConnection.end();
    } catch (closeError) {
      console.error(`Error closing database connection: ${closeError.message}`);
    }
  });
