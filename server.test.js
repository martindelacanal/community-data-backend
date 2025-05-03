require('dotenv').config({path: './.env'}); // Load environment variables
const mysqlConnection = require('./api/connection/connection.js');
const email = require('./api/email/email.js');
const moment = require('moment-timezone');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const { parse } = require('csv-parse/sync');


// filepath: c:\Users\marti\Desktop\TRABAJO\PROYECTOS\COMMUNITY_DATA\BACKEND\server.test.js


// Import functions from server.js
// Since these functions aren't exported, we need to redefine them here
async function getRawData(from_date, to_date, client_id) {
    // Ensure from_date and to_date include time for accurate BETWEEN comparison with CONVERT_TZ
    const params = [
        client_id,
        from_date, // Expected format: 'YYYY-MM-DD HH:mm:ss'
        to_date,   // Expected format: 'YYYY-MM-DD HH:mm:ss'
        from_date, // Expected format: 'YYYY-MM-DD HH:mm:ss'
        to_date,   // Expected format: 'YYYY-MM-DD HH:mm:ss'
    ];

    const [rows] = await mysqlConnection.promise().query(
        `SELECT
                u.id as user_id,
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
                (SELECT GROUP_CONCAT(DISTINCT loc_visited.community_city)
                        FROM delivery_beneficiary AS db_visited
                        LEFT JOIN location AS loc_visited ON db_visited.location_id = loc_visited.id
                        WHERE db_visited.receiving_user_id = u.id) AS locations_visited,
                DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles'), '%m/%d/%Y') AS registration_date,
                DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles'), '%T') AS registration_time,
                q.id AS question_id,
                at.id AS answer_type_id,
                q.name AS question,
                a.name AS answer,
                uq.answer_text AS answer_text,
                uq.answer_number AS answer_number
        FROM user u
        INNER JOIN client_user cu ON u.id = cu.user_id
        INNER JOIN gender AS g ON u.gender_id = g.id
        INNER JOIN ethnicity AS eth ON u.ethnicity_id = eth.id
        LEFT JOIN location AS loc ON u.location_id = loc.id
        CROSS JOIN question AS q
        LEFT JOIN answer_type as at ON q.answer_type_id = at.id
        LEFT JOIN user_question AS uq ON u.id = uq.user_id AND uq.question_id = q.id
        LEFT JOIN user_question_answer AS uqa ON uq.id = uqa.user_question_id
        LEFT JOIN answer as a ON a.id = uqa.answer_id and a.question_id = q.id
        WHERE u.role_id = 5
          AND q.enabled = 'Y'
          AND cu.client_id = ?
          AND (
              CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ?
              OR
              u.id IN (
                  SELECT db3.receiving_user_id
                  FROM delivery_beneficiary db3
                  INNER JOIN location loc3 ON db3.location_id = loc3.id
                  INNER JOIN client_location cl3 ON loc3.id = cl3.location_id
                  WHERE CONVERT_TZ(db3.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ?
                    AND cl3.client_id = cu.client_id
              )
          )
          AND EXISTS (
              SELECT 1
              FROM question_location ql
              INNER JOIN client_location cl ON ql.location_id = cl.location_id
              WHERE ql.question_id = q.id AND cl.client_id = cu.client_id AND ql.enabled = 'Y'
          )
        GROUP BY u.id, q.id, a.id
        ORDER BY u.id, q.id, a.id`,
        params
    );


    // Process raw data
    var question_id_array = [];
    if (rows.length > 0) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const id_repetido = question_id_array.some(obj => obj.question_id === row.question_id);
            if (!id_repetido) {
                question_id_array.push({ question_id: row.question_id, question: row.question });
            }
        }
    }

    var rows_filtered = [];
    var row_filtered = {};
    for (let i = 0; i < rows.length; i++) {
        if (!row_filtered["username"]) {
            row_filtered["user_id"] = rows[i].user_id;
            row_filtered["username"] = rows[i].username;
            row_filtered["email"] = rows[i].email;
            row_filtered["firstname"] = rows[i].firstname;
            row_filtered["lastname"] = rows[i].lastname;
            row_filtered["date_of_birth"] = rows[i].date_of_birth;
            row_filtered["phone"] = rows[i].phone;
            row_filtered["zipcode"] = rows[i].zipcode;
            row_filtered["household_size"] = rows[i].household_size;
            row_filtered["gender"] = rows[i].gender;
            row_filtered["ethnicity"] = rows[i].ethnicity;
            row_filtered["other_ethnicity"] = rows[i].other_ethnicity;
            row_filtered["last_location_visited"] = rows[i].last_location_visited;
            row_filtered["locations_visited"] = rows[i].locations_visited;
            row_filtered["registration_date"] = rows[i].registration_date;
            row_filtered["registration_time"] = rows[i].registration_time;
        }
        
        if (!row_filtered[rows[i].question_id]) {
            switch (rows[i].answer_type_id) {
                case 1:
                    row_filtered[rows[i].question_id] = rows[i].answer_text;
                    break;
                case 2:
                    row_filtered[rows[i].question_id] = rows[i].answer_number;
                    break;
                case 3:
                    row_filtered[rows[i].question_id] = rows[i].answer;
                    break;
                case 4:
                    row_filtered[rows[i].question_id] = rows[i].answer;
                    break;
                default:
                    break;
            }
        } else {
            row_filtered[rows[i].question_id] = row_filtered[rows[i].question_id] + ', ' + rows[i].answer;
        }
        
        if (i < rows.length - 1) {
            if (rows[i].username !== rows[i + 1].username) {
                rows_filtered.push(row_filtered);
                row_filtered = {};
            }
        } else {
            rows_filtered.push(row_filtered);
            row_filtered = {};
        }
    }

    // Create headers array
    var headers_array = [
        { id: 'user_id', title: 'User ID' },
        { id: 'username', title: 'Username' },
        { id: 'email', title: 'Email' },
        { id: 'firstname', title: 'Firstname' },
        { id: 'lastname', title: 'Lastname' },
        { id: 'date_of_birth', title: 'Date of birth' },
        { id: 'phone', title: 'Phone' },
        { id: 'zipcode', title: 'Zipcode' },
        { id: 'household_size', title: 'Household size' },
        { id: 'gender', title: 'Gender' },
        { id: 'ethnicity', title: 'Ethnicity' },
        { id: 'other_ethnicity', title: 'Other ethnicity' },
        { id: 'last_location_visited', title: 'Last location visited' },
        { id: 'locations_visited', title: 'Locations visited' },
        { id: 'registration_date', title: 'Registration date' },
        { id: 'registration_time', title: 'Registration time' }
    ];

    for (let i = 0; i < question_id_array.length; i++) {
        const question_id = question_id_array[i].question_id;
        const question = question_id_array[i].question;
        headers_array.push({ id: question_id, title: question });
    }

    const csvStringifier = createCsvStringifier({
        header: headers_array,
        fieldDelimiter: ';'
    });

    let csvData = csvStringifier.getHeaderString();
    csvData += csvStringifier.stringifyRecords(rows_filtered);

    return csvData;
}

async function getNewRegistrations(csvRawData, from_date, to_date) {
    const records = parse(csvRawData, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ';'
    });

    if (records.length === 0) {
        return '';
    }

    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD").endOf('day');

    const filteredRecords = records.filter(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        return registrationDate.isBetween(fromDate, toDate, 'day', '[]');
    });

    const csvStringifier = createCsvStringifier({
        header: Object.keys(records[0]).map(key => ({ id: key, title: key })),
        fieldDelimiter: ';'
    });

    let csvData = csvStringifier.getHeaderString();
    csvData += csvStringifier.stringifyRecords(filteredRecords);

    return csvData;
}

async function getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date) {
    const records = parse(csvRawData, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ';'
    });

    if (records.length === 0) {
        return '';
    }

    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD").endOf('day');

    const filteredRecords = records.filter(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        return registrationDate.isBetween(fromDate, toDate, 'day', '[]') && 
            (record['Health Insurance?'] === 'No' || record['Health Insurance?'] === '');
    });

    const csvStringifier = createCsvStringifier({
        header: Object.keys(records[0]).map(key => ({ id: key, title: key })),
        fieldDelimiter: ';'
    });

    let csvData = csvStringifier.getHeaderString();
    csvData += csvStringifier.stringifyRecords(filteredRecords);

    return csvData;
}

async function getSummary(from_date, to_date, csvRawData) {
    const records = parse(csvRawData, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ';'
    });

    if (records.length === 0) {
        return '';
    }

    let newCount = 0;
    let recurringCount = 0;
    let newHealthPlanYes = 0;
    let newHealthPlanNo = 0;

    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD").endOf('day');

    records.forEach(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        const isNew = registrationDate.isBetween(fromDate, toDate, 'day', '[]');

        if (isNew) {
            newCount++;
            if (record['Health Insurance?'] === 'Yes') {
                newHealthPlanYes++;
            } else if (record['Health Insurance?'] === 'No' || record['Health Insurance?'] === '') {
                newHealthPlanNo++;
            }
        } else {
            recurringCount++;
        }
    });

    const totalNewRecurring = newCount + recurringCount;
    const totalNewHealthPlan = newHealthPlanYes + newHealthPlanNo;

    const summaryData = [
        {
            New: newCount,
            Recurring: recurringCount,
            'Total New+Recurring': totalNewRecurring,
            '(New) Health Plan YES': newHealthPlanYes,
            '(New) Health Plan NO': newHealthPlanNo,
            'Total (New) Health Plan': totalNewHealthPlan
        }
    ];

    const csvStringifier = createCsvStringifier({
        header: [
            { id: 'New', title: 'New' },
            { id: 'Recurring', title: 'Recurring' },
            { id: 'Total New+Recurring', title: 'Total New+Recurring' },
            { id: '(New) Health Plan YES', title: '(New) Health Plan YES' },
            { id: '(New) Health Plan NO', title: '(New) Health Plan NO' },
            { id: 'Total (New) Health Plan', title: 'Total (New) Health Plan' }
        ],
        fieldDelimiter: ';'
    });

    let csvData = csvStringifier.getHeaderString();
    csvData += csvStringifier.stringifyRecords(summaryData);

    return csvData;
}

// Test function to manually run the weekly report generation
async function testWeeklyReports() {
    try {
        console.log('Starting test for weekly reports...');
        const password = 'bienestarcommunity';
        const customEmail = 'martin.delacanalerbetta@gmail.com';

        // Get current date in LA timezone
        const today = moment().tz("America/Los_Angeles");

        // Calculate the last complete week (Monday to Sunday)
        // Find last Monday
        let lastMonday = today.clone();
        while (lastMonday.day() !== 1) { // 1 is Monday
            lastMonday.subtract(1, 'days');
        }
        lastMonday.subtract(7, 'days'); // Go back to previous Monday
        lastMonday = lastMonday.startOf('day'); // Set time to 00:00:00 LA time

        // Find last Sunday
        let lastSunday = lastMonday.clone().add(6, 'days').endOf('day'); // Set time to 23:59:59 LA time

        // Format dates for database query (including time)
        let from_date = lastMonday.format("YYYY-MM-DD HH:mm:ss");
        let to_date = lastSunday.format("YYYY-MM-DD HH:mm:ss");

        // Format dates for display (optional, can keep as MM-DD-YYYY)
        let formatted_from_date = lastMonday.format("MM-DD-YYYY");
        let formatted_to_date = lastSunday.format("MM-DD-YYYY");

        console.log(`Testing weekly reports for: ${formatted_from_date} to ${formatted_to_date} (Range: ${from_date} to ${to_date} LA Time)`);

        // Get all clients
        const [rows_clients] = await mysqlConnection.promise().query(
            `SELECT id, name FROM client WHERE enabled = 'Y' ORDER BY id`
        );

        // ... rest of the test function remains the same ...
        if (rows_clients.length === 0) {
            console.log('No active clients found');
            return;
        }

        // Generate reports for each client
        let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");

        for (const client of rows_clients) {
            console.log(`Processing client: ${client.name}`);
            const subject = `TEST - Bienestar Community report for ${client.name} - ${date}`;
            const message = `This is a TEST report.\n\nAttached you will find the Bienestar Community report for ${date}. The report covers the period from ${formatted_from_date} to ${formatted_to_date}. The file is password protected.`;

            // Generate data for this client using the full date-time range
            const csvRawData = await getRawData(from_date, to_date, client.id);

            if (csvRawData && csvRawData.split('\n').length > 2) {
                console.log(`Generated raw data for client: ${client.name}`);
                // Pass YYYY-MM-DD format to helper functions as they parse based on date part only
                const csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));
                const csvAllNewRegistrations = await getNewRegistrations(csvRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));
                const csvSummary = await getSummary(lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"), csvRawData);

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
                    [customEmail] // Use custom email instead of client emails
                );

                console.log(`Email sent successfully for client: ${client.name}`);
            } else {
                console.log(`No data available for client: ${client.name} in the specified date range`);
            }
        }

        console.log('Weekly report test completed successfully');
    } catch (error) {
        console.error('Error running weekly report test:', error);
    }
}

// Run the test
testWeeklyReports()
    .then(() => console.log('Test completed'))
    .catch(err => console.error('Test failed:', err))
    .finally(() => process.exit());