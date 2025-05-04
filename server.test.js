// filepath: c:\Users\marti\Desktop\TRABAJO\PROYECTOS\COMMUNITY_DATA\BACKEND\server.test.js
require('dotenv').config({path: './.env'}); // Load environment variables
const mysqlConnection = require('./api/connection/connection.js');
const email = require('./api/email/email.js');
const moment = require('moment-timezone');

// Import functions from server.js
// Ensure these functions are exported from server.js using module.exports
const {
    getRawData,
    getNewRegistrations,
    getNewRegistrationsWithoutHealthInsurance,
    getSummary
} = require('./server.js'); // Adjust path if necessary

// Test function to manually run the weekly report generation
async function testWeeklyReports() {
    try {
        console.log('Starting test for weekly reports...');
        const password = 'bienestarcommunity'; // Consider moving to .env
        const customEmail = 'martin.delacanalerbetta@gmail.com'; // Consider moving to .env or config

        // Get current date in LA timezone
        const today = moment().tz("America/Los_Angeles");

        // Calculate the last complete week (Monday to Sunday)
        let lastMonday = today.clone().subtract(1, 'week').startOf('isoWeek'); // Previous Monday 00:00:00
        let lastSunday = lastMonday.clone().endOf('isoWeek'); // Previous Sunday 23:59:59

        // Format dates for database query (YYYY-MM-DD HH:mm:ss)
        let from_date = lastMonday.format("YYYY-MM-DD HH:mm:ss");
        let to_date = lastSunday.format("YYYY-MM-DD HH:mm:ss");

        // Format dates for display/filenames (MM-DD-YYYY)
        let formatted_from_date = lastMonday.format("MM-DD-YYYY");
        let formatted_to_date = lastSunday.format("MM-DD-YYYY");
        let report_date_label = moment().tz("America/Los_Angeles").format("MM-DD-YYYY"); // Date for the report filename/subject

        console.log(`Testing weekly reports for: ${formatted_from_date} to ${formatted_to_date} (Range: ${from_date} to ${to_date} LA Time)`);

        // Get all active clients
        const [rows_clients] = await mysqlConnection.promise().query(
            `SELECT id, name FROM client WHERE enabled = 'Y' ORDER BY id`
        );

        if (rows_clients.length === 0) {
            console.log('No active clients found');
            return;
        }

        // Generate reports for each client
        for (const client of rows_clients) {
            console.log(`Processing client: ${client.id} - ${client.name}`);
            const subject = `TEST - Bienestar Community report for ${client.name} - ${report_date_label}`;
            const message = `This is a TEST report.\n\nAttached you will find the Bienestar Community report for ${report_date_label}. The report covers the period from ${formatted_from_date} to ${formatted_to_date}. The file is password protected.`;

            // Generate data for this client using the imported function
            const csvRawData = await getRawData(from_date, to_date, client.id);

            // Check if raw data has more than just the header row
            if (csvRawData && csvRawData.trim().split('\n').length > 1) {
                console.log(`Generated raw data for client: ${client.name}`);

                // Use YYYY-MM-DD format for helper functions as they parse based on date part
                const date_only_from = lastMonday.format("YYYY-MM-DD");
                const date_only_to = lastSunday.format("YYYY-MM-DD");

                // Generate derived reports using imported functions
                const csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, date_only_from, date_only_to);
                const csvAllNewRegistrations = await getNewRegistrations(csvRawData, date_only_from, date_only_to);
                const csvSummary = await getSummary(date_only_from, date_only_to, csvRawData);

                console.log(`Sending email for client: ${client.name} to: ${customEmail}`);

                // Send email with the generated reports
                await email.sendEmailWithAttachment(
                    subject,
                    message,
                    csvRawData,
                    csvNewRegistrations,
                    csvSummary,
                    csvAllNewRegistrations,
                    password,
                    [customEmail] // Send only to the test email address
                );

                console.log(`Email sent successfully for client: ${client.name}`);
            } else {
                console.log(`No data available for client: ${client.name} in the specified date range. Skipping email.`);
            }
        }

        console.log('Weekly report test completed successfully');
    } catch (error) {
        console.error('Error running weekly report test:', error);
        throw error; // Re-throw error to be caught by the final catch block
    }
}

// Run the test
testWeeklyReports()
    .then(() => console.log('Test completed'))
    .catch(err => {
        console.error('Test failed:', err);
        process.exitCode = 1; // Indicate failure
    })
    .finally(async () => {
        try {
            await mysqlConnection.end(); // Close the database connection
            console.log('Database connection closed.');
        } catch (closeErr) {
            console.error('Error closing database connection:', closeErr);
        }
        process.exit(); // Ensure the script exits
    });
