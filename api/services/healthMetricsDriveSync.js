const logger = require('../utils/logger');
const { buildHealthMetricsCsv } = require('./healthMetrics');
const { getGoogleDriveFileMetadata, updateGoogleDriveFile } = require('./googleDrive');
const {
  generateBeneficiarySummaryCsv,
  generateDeliverySummaryCsv,
  generateTicketTableCsvs,
  generateVolunteerTableCsv,
  generateWorkerTableCsv
} = require('./tableCsvExports');

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

async function syncCsvContentToDrive({ label, folderId, fileId, fileName, csvData, rowCount }) {
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
    content: csvData,
    mimeType: 'text/csv; charset=utf-8'
  });

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
  const { csvData, rowCount, fileName } = await buildHealthMetricsCsv({
    cabecera: { role: 'admin' },
    filters: {},
    language: config.language
  });

  return syncCsvContentToDrive({
    label: 'Health metrics',
    folderId: config.folderId,
    fileId: config.fileId,
    fileName: config.fileName || fileName,
    csvData,
    rowCount
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

async function syncScheduledDriveCsvsToDrive({ ignoreEnabledFlag = false } = {}) {
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
        return syncCsvContentToDrive({
          label: task.label,
          folderId: config.folderId,
          fileId: task.config.fileId,
          fileName: task.config.fileName || exportData.fileName,
          csvData: exportData.csvData,
          rowCount: exportData.rowCount
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
      () => syncCsvContentToDrive({
        label: 'Tickets',
        folderId: config.folderId,
        fileId: config.tickets.fileId,
        fileName: config.tickets.fileName || ticketExports.tickets.fileName,
        csvData: ticketExports.tickets.csvData,
        rowCount: ticketExports.tickets.rowCount
      }),
      results,
      errors
    );

    await runDriveSyncTask(
      'Tickets with food',
      () => syncCsvContentToDrive({
        label: 'Tickets with food',
        folderId: config.folderId,
        fileId: config.ticketsWithFood.fileId,
        fileName: config.ticketsWithFood.fileName || ticketExports.ticketsWithFood.fileName,
        csvData: ticketExports.ticketsWithFood.csvData,
        rowCount: ticketExports.ticketsWithFood.rowCount
      }),
      results,
      errors
    );
  }

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
