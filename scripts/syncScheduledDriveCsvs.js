const path = require('path');

require('dotenv').config({
  path: path.resolve(__dirname, '../.env')
});

const { syncScheduledDriveCsvsToDrive } = require('../api/services/healthMetricsDriveSync');

syncScheduledDriveCsvsToDrive({ ignoreEnabledFlag: true })
  .then(summary => {
    if (summary.skipped) {
      console.log(`Scheduled Drive CSV sync skipped: ${summary.reason}`);
      process.exit(0);
    }

    summary.results.forEach(result => {
      if (Array.isArray(result.items) && result.items.length > 0) {
        result.items.forEach(item => {
          console.log(`OK ${item.label}: fileId=${item.fileId} rows=${item.rowCount}`);
        });
        return;
      }

      console.log(`OK ${result.label}: fileId=${result.fileId} rows=${result.rowCount}`);
    });

    if (summary.errors.length > 0) {
      summary.errors.forEach(error => {
        console.error(`ERROR ${error.label}: ${error.message}`);
      });
      process.exit(1);
    }

    process.exit(0);
  })
  .catch(error => {
    console.error(`Scheduled Drive CSV sync failed: ${error.message}`);
    process.exit(1);
  });
