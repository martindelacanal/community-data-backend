const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { streamHealthMetricsCsv } = require('./healthMetrics');
const { getGoogleDriveFileMetadata, updateGoogleDriveFile } = require('./googleDrive');
const {
  generateBeneficiarySummaryCsv,
  generateDeliverySummaryCsv,
  generateTicketTableCsvs,
  generateVolunteerTableCsv,
  generateWorkerTableCsv
} = require('./tableCsvExports');

const ALERT_DEDUP_FILE = path.join(__dirname, '..', '..', '.drive-sync-last-alert.json');
const ALERT_DEDUP_WINDOW_MS = 12 * 60 * 60 * 1000;

function parseBooleanEnv(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') {
    return defaultValue;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
    return false;
  }

  return defaultValue;
}

function isHealthMetricsDriveSyncEnabled() {
  return parseBooleanEnv(process.env.HEALTH_METRICS_DRIVE_SYNC_ENABLED, false);
}

function getDriveSyncFolderId() {
  return (process.env.HEALTH_METRICS_DRIVE_FOLDER_ID || '').trim();
}

function getHealthMetricsDriveSyncConfig() {
  const language = (process.env.HEALTH_METRICS_DRIVE_LANGUAGE || 'en').trim().toLowerCase();

  return {
    folderId: getDriveSyncFolderId(),
    fileId: (process.env.HEALTH_METRICS_DRIVE_FILE_ID || '').trim(),
    fileName: (process.env.HEALTH_METRICS_DRIVE_FILE_NAME || 'health-metrics.csv').trim(),
    language: language === 'es' ? 'es' : 'en'
  };
}

function getScheduledDriveCsvSyncConfig() {
  return {
    folderId: getDriveSyncFolderId(),
    volunteers: {
      fileId: (process.env.VOLUNTEERS_DRIVE_FILE_ID || '').trim(),
      fileName: (process.env.VOLUNTEERS_DRIVE_FILE_NAME || 'volunteers-table.csv').trim()
    },
    workers: {
      fileId: (process.env.WORKERS_DRIVE_FILE_ID || '').trim(),
      fileName: (process.env.WORKERS_DRIVE_FILE_NAME || 'workers-table.csv').trim()
    },
    tickets: {
      fileId: (process.env.TICKETS_DRIVE_FILE_ID || '').trim(),
      fileName: (process.env.TICKETS_DRIVE_FILE_NAME || 'tickets.csv').trim()
    },
    ticketsWithFood: {
      fileId: (process.env.TICKETS_WITH_FOOD_DRIVE_FILE_ID || '').trim(),
      fileName: (process.env.TICKETS_WITH_FOOD_DRIVE_FILE_NAME || 'tickets-with-food.csv').trim()
    },
    beneficiarySummary: {
      fileId: (process.env.BENEFICIARY_SUMMARY_DRIVE_FILE_ID || '').trim(),
      fileName: (process.env.BENEFICIARY_SUMMARY_DRIVE_FILE_NAME || 'beneficiary-summary.csv').trim()
    },
    deliverySummary: {
      fileId: (process.env.DELIVERY_SUMMARY_DRIVE_FILE_ID || '').trim(),
      fileName: (process.env.DELIVERY_SUMMARY_DRIVE_FILE_NAME || 'delivery-summary.csv').trim()
    }
  };
}

function validateCsvDriveSyncConfig(label, config) {
  const missing = [];
  if (!config.folderId) {
    missing.push('HEALTH_METRICS_DRIVE_FOLDER_ID');
  }
  if (!config.fileId) {
    missing.push(`${label}.fileId`);
  }

  if (missing.length > 0) {
    throw new Error(`Missing Drive sync config for ${label}: ${missing.join(', ')}`);
  }
}

async function syncCsvStreamToDrive({ label, folderId, fileId, fileName, body, getRowCount }) {
  validateCsvDriveSyncConfig(label, { folderId, fileId });
  const existingFile = await getGoogleDriveFileMetadata(fileId);
  const parents = Array.isArray(existingFile.parents) ? existingFile.parents : [];
  if (parents.length > 0 && !parents.includes(folderId)) {
    throw new Error(
      `Google Drive file ${fileId} is not inside folder ${folderId}.`
    );
  }

  const updatedFile = await updateGoogleDriveFile({
    fileId,
    fileName,
    body,
    mimeType: 'text/csv; charset=utf-8'
  });

  const rowCount = typeof getRowCount === 'function' ? getRowCount() : 0;
  logger.info(
    `${label} Drive sync completed. fileId=${updatedFile.id || fileId}, rows=${rowCount}`
  );

  return {
    label,
    skipped: false,
    fileId: updatedFile.id || fileId,
    fileName: updatedFile.name || fileName,
    rowCount,
    webViewLink: updatedFile.webViewLink || existingFile.webViewLink || null
  };
}

async function syncHealthMetricsCsvToDrive({ ignoreEnabledFlag = false } = {}) {
  if (!ignoreEnabledFlag && !isHealthMetricsDriveSyncEnabled()) {
    logger.info('Health metrics Drive sync is disabled. Skipping execution.');
    return {
      skipped: true,
      reason: 'disabled'
    };
  }

  const config = getHealthMetricsDriveSyncConfig();
  const { body, getRowCount, fileName } = streamHealthMetricsCsv({
    cabecera: { role: 'admin' },
    filters: {},
    language: config.language
  });

  return syncCsvStreamToDrive({
    label: 'Health metrics',
    folderId: config.folderId,
    fileId: config.fileId,
    fileName: config.fileName || fileName,
    body,
    getRowCount
  });
}

async function runDriveSyncTask(label, taskFn, results, errors) {
  try {
    const result = await taskFn();
    results.push(result);
  } catch (error) {
    logger.error(`${label} Drive sync failed: ${error.message}`);
    errors.push({ label, message: error.message });
  }
}

function readLastDriveSyncAlert() {
  try {
    return JSON.parse(fs.readFileSync(ALERT_DEDUP_FILE, 'utf8'));
  } catch (err) {
    return null;
  }
}

function writeLastDriveSyncAlert(payload) {
  try {
    fs.writeFileSync(ALERT_DEDUP_FILE, JSON.stringify(payload));
  } catch (err) {
    logger.warn(`Could not persist Drive sync alert dedup state: ${err.message}`);
  }
}

async function maybeSendDriveSyncAlert({ errors, notifyOnError }) {
  if (!notifyOnError || !notifyOnError.sendEmail || !Array.isArray(notifyOnError.emails) || notifyOnError.emails.length === 0) {
    return;
  }
  if (!Array.isArray(errors) || errors.length === 0) {
    return;
  }

  const signature = errors
    .map(e => `${e.label}:${e.message}`)
    .sort()
    .join('\n');
  const last = readLastDriveSyncAlert();
  const now = Date.now();
  if (last && last.signature === signature && now - last.timestamp < ALERT_DEDUP_WINDOW_MS) {
    logger.info(`Drive sync alert email suppressed (same errors within ${ALERT_DEDUP_WINDOW_MS / 3600000}h).`);
    return;
  }

  const subject = `[community-data] Drive CSV sync had ${errors.length} error(s)`;
  const body = [
    `Drive CSV sync run finished with ${errors.length} error(s) at ${new Date().toISOString()}.`,
    '',
    'Errors:',
    ...errors.map(e => `  - ${e.label}: ${e.message}`),
    '',
    'Drive folder: https://drive.google.com/drive/folders/1qpLUheUhPtQYgwT0YuaK3Yv4JLCtEv2e',
    'Diagnostic on EC2: cd /home/ubuntu/community-data-backend && node scripts/diagnoseDriveSync.js',
    '',
    'This alert is deduped: same errors will not re-send within 12 hours.'
  ].join('\n');

  try {
    await notifyOnError.sendEmail(subject, body, notifyOnError.emails);
    writeLastDriveSyncAlert({ signature, timestamp: now });
    logger.info(`Drive sync alert email sent to ${notifyOnError.emails.join(',')}.`);
  } catch (err) {
    logger.error(`Failed to send Drive sync alert email: ${err.message}`);
  }
}

async function syncScheduledDriveCsvsToDrive({ ignoreEnabledFlag = false, notifyOnError = null } = {}) {
  if (!ignoreEnabledFlag && !isHealthMetricsDriveSyncEnabled()) {
    logger.info('Scheduled Drive CSV sync is disabled. Skipping execution.');
    return {
      skipped: true,
      reason: 'disabled',
      results: [],
      errors: []
    };
  }

  const config = getScheduledDriveCsvSyncConfig();
  const results = [];
  const errors = [];

  await runDriveSyncTask(
    'Health metrics',
    () => syncHealthMetricsCsvToDrive({ ignoreEnabledFlag: true }),
    results,
    errors
  );

  const singleFileTasks = [
    {
      label: 'Volunteers',
      config: config.volunteers,
      generator: generateVolunteerTableCsv
    },
    {
      label: 'Workers',
      config: config.workers,
      generator: generateWorkerTableCsv
    },
    {
      label: 'Beneficiary summary',
      config: config.beneficiarySummary,
      generator: generateBeneficiarySummaryCsv
    },
    {
      label: 'Delivery summary',
      config: config.deliverySummary,
      generator: generateDeliverySummaryCsv
    }
  ];

  for (let i = 0; i < singleFileTasks.length; i++) {
    const task = singleFileTasks[i];
    await runDriveSyncTask(
      task.label,
      async () => {
        const exportData = await task.generator();
        return syncCsvStreamToDrive({
          label: task.label,
          folderId: config.folderId,
          fileId: task.config.fileId,
          fileName: task.config.fileName || exportData.fileName,
          body: exportData.body,
          getRowCount: exportData.getRowCount
        });
      },
      results,
      errors
    );
  }

  let ticketExports = null;
  try {
    ticketExports = await generateTicketTableCsvs();
  } catch (error) {
    logger.error(`Tickets export generation failed: ${error.message}`);
    errors.push({ label: 'Tickets export generation', message: error.message });
  }

  if (ticketExports) {
    await runDriveSyncTask(
      'Tickets',
      () => syncCsvStreamToDrive({
        label: 'Tickets',
        folderId: config.folderId,
        fileId: config.tickets.fileId,
        fileName: config.tickets.fileName || ticketExports.tickets.fileName,
        body: ticketExports.tickets.body,
        getRowCount: ticketExports.tickets.getRowCount
      }),
      results,
      errors
    );

    await runDriveSyncTask(
      'Tickets with food',
      () => syncCsvStreamToDrive({
        label: 'Tickets with food',
        folderId: config.folderId,
        fileId: config.ticketsWithFood.fileId,
        fileName: config.ticketsWithFood.fileName || ticketExports.ticketsWithFood.fileName,
        body: ticketExports.ticketsWithFood.body,
        getRowCount: ticketExports.ticketsWithFood.getRowCount
      }),
      results,
      errors
    );
  }

  await maybeSendDriveSyncAlert({ errors, notifyOnError });

  return {
    skipped: false,
    results,
    errors
  };
}

module.exports = {
  getHealthMetricsDriveSyncConfig,
  getScheduledDriveCsvSyncConfig,
  isHealthMetricsDriveSyncEnabled,
  parseBooleanEnv,
  syncHealthMetricsCsvToDrive,
  syncScheduledDriveCsvsToDrive
};
