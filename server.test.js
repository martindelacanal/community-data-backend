require('dotenv').config({ path: './.env' }); // Load environment variables
const mysqlConnection = require('./api/connection/connection.js');
const email = require('./api/email/email.js');
const moment = require('moment-timezone');

// Import functions from server.js - ACTUALIZAR A LAS NUEVAS FUNCIONES EXCEL
const {
    getRawDataExcel,
    getNewRegistrationsExcel,
    getNewRegistrationsWithoutHealthInsuranceExcel,
    getSummaryExcel
} = require('./server.js'); // Adjust path if necessary

// Test function to manually run the weekly reports generation
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

            // CAMBIO: Usar la nueva función Excel en lugar de CSV
            const excelRawData = await getRawDataExcel(from_date, to_date, client.id);

            // CAMBIO: Verificar que excelRawData sea un buffer válido
            if (excelRawData && excelRawData.length > 0) {
                console.log(`Generated raw Excel data for client: ${client.name}`);

                // Use YYYY-MM-DD format for helper functions as they parse based on date part
                const date_only_from = lastMonday.format("YYYY-MM-DD");
                const date_only_to = lastSunday.format("YYYY-MM-DD");

                // CAMBIO: Usar las nuevas funciones Excel
                const summaryObject = await getSummaryExcel(
                    date_only_from,
                    date_only_to,
                    client.id,
                    excelRawData
                );

                // CAMBIO: Verificar que summaryObject tenga la estructura correcta
                if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                    const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, date_only_from, date_only_to);
                    const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, date_only_from, date_only_to);

                    console.log(`Sending email for client: ${client.name} to: ${customEmail}`);

                    // CAMBIO: Usar la nueva función de email para Excel
                    await email.sendEmailWithExcelAttachment(
                        subject,
                        message,
                        excelRawData,
                        excelNewRegistrations,
                        summaryObject,
                        excelAllNewRegistrations,
                        password,
                        [customEmail] // Send only to the test email address
                    );

                    console.log(`Email sent successfully for client: ${client.name}`);
                } else {
                    console.log(`No summary data generated for client: ${client.name}. Skipping email.`);
                }
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

// Test function to manually run the monthly report generation
async function testMonthlyReports() {
    try {
        console.log('Starting test for monthly reports...');
        const password = 'bienestarcommunity'; // Consider moving to .env
        const customEmail = 'martin.delacanalerbetta@gmail.com'; // Consider moving to .env or config

        const jobRunDate = moment.tz("2025-05-05", "America/Los_Angeles");

        // Calculate previous month based on the job run date
        let prevMonth = jobRunDate.clone().subtract(1, 'month'); // This will be April 2025

        // Find the first day of the previous month
        let firstDayOfPrevMonth = prevMonth.clone().startOf('month');

        // Find the Monday before or on the first day of the previous month
        let reportFirstMonday = firstDayOfPrevMonth.clone();
        while (reportFirstMonday.day() !== 1) { // 1 is Monday (moment.js day: Sun=0, Mon=1, ..., Sat=6)
            reportFirstMonday.subtract(1, 'day');
        }
        reportFirstMonday = reportFirstMonday.startOf('day'); // Set time to 00:00:00

        // Find the last day of the previous month
        let lastDayOfPrevMonth = prevMonth.clone().endOf('month');

        // Find the Sunday after or on the last day of the previous month
        let reportLastSunday = lastDayOfPrevMonth.clone();
        while (reportLastSunday.day() !== 0) { // 0 is Sunday
            reportLastSunday.add(1, 'day');
        }
        reportLastSunday = reportLastSunday.endOf('day'); // Set time to 23:59:59

        // Format dates for database query (YYYY-MM-DD HH:mm:ss)
        let from_date_db = reportFirstMonday.format("YYYY-MM-DD HH:mm:ss");
        let to_date_db = reportLastSunday.format("YYYY-MM-DD HH:mm:ss");

        // Format dates for display and helper functions (YYYY-MM-DD for helpers, MM-DD-YYYY for display)
        let formatted_from_date_display = reportFirstMonday.format("MM-DD-YYYY");
        let formatted_to_date_display = reportLastSunday.format("MM-DD-YYYY");

        let monthName = prevMonth.format("MMMM");
        let year = prevMonth.format("YYYY");

        console.log(`Testing monthly reports for ${monthName} ${year}: ${formatted_from_date_display} to ${formatted_to_date_display} (DB Range: ${from_date_db} to ${to_date_db} LA Time)`);

        const [rows_clients] = await mysqlConnection.promise().query(
            `SELECT id, name FROM client WHERE enabled = 'Y' ORDER BY id`
        );

        if (rows_clients.length === 0) {
            console.log('No active clients found');
            return;
        }

        for (const client of rows_clients) {
            console.log(`Processing client: ${client.id} - ${client.name}`);
            const subject = `TEST - Monthly Bienestar Community report for ${client.name} - ${monthName} ${year}`;
            const message = `This is a TEST monthly report.\n\nAttached you will find the Bienestar Community report for ${monthName} ${year}. The report covers the period from ${formatted_from_date_display} to ${formatted_to_date_display}. The file is password protected.`;

            // CAMBIO: Usar la nueva función Excel
            const excelRawData = await getRawDataExcel(from_date_db, to_date_db, client.id);

            // CAMBIO: Verificar que excelRawData sea un buffer válido
            if (excelRawData && excelRawData.length > 0) {
                console.log(`Generated raw Excel data for client: ${client.name}`);

                const date_only_from = reportFirstMonday.format("YYYY-MM-DD");
                const date_only_to = reportLastSunday.format("YYYY-MM-DD");

                // CAMBIO: Usar las nuevas funciones Excel
                const summaryObject = await getSummaryExcel(
                    date_only_from,
                    date_only_to,
                    client.id,
                    excelRawData
                );

                // CAMBIO: Verificar que summaryObject tenga la estructura correcta
                if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                    const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, date_only_from, date_only_to);
                    const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, date_only_from, date_only_to);

                    console.log(`Sending email for client: ${client.name} to: ${customEmail}`);

                    // CAMBIO: Usar la nueva función de email para Excel
                    await email.sendEmailWithExcelAttachment(
                        subject,
                        message,
                        excelRawData,
                        excelNewRegistrations,
                        summaryObject,
                        excelAllNewRegistrations,
                        password,
                        [customEmail]
                    );
                    console.log(`Email sent successfully for client: ${client.name}`);
                } else {
                    console.log(`No summary data generated for client: ${client.name}. Skipping email.`);
                }
            } else {
                console.log(`No data available for client: ${client.name} in the specified date range. Skipping email.`);
            }
        }
        console.log('Monthly report test completed successfully');
    } catch (error) {
        console.error('Error running monthly report test:', error);
        throw error;
    }
}

// Run the test 
// You can choose to run one or both tests:
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

// testMonthlyReports()
//     .then(() => console.log('Test completed'))
//     .catch(err => {
//         console.error('Test failed:', err);
//         process.exitCode = 1;
//     })
//     .finally(async () => {
//         try {
//             await mysqlConnection.end();
//             console.log('Database connection closed.');
//         } catch (closeErr) {
//             console.error('Error closing database connection:', closeErr);
//         }
//         process.exit();
//     });