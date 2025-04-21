const http = require('http');
const app = require('./app');

const port = process.env.PORT || 3000;

const server = http.createServer(app);
const mysqlConnection = require('./api/connection/connection.js');
const schedule = require('node-schedule');
const { RecurrenceRule } = require('node-schedule');

const logger = require('./api/utils/logger.js');
const email = require('./api/email/email.js');
const moment = require('moment-timezone');

const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const { parse } = require('csv-parse/sync');

schedule.scheduleJob('0 * * * *', async () => { // Se ejecuta cada hora
    // Modificar todos los delivery_log con operation_id = 3 y offboarding_date = null que hayan sido creados hace más de 5 horas y agregarle la fecha actual
    const [rows] = await mysqlConnection.promise().query(`
        UPDATE delivery_log dl
        JOIN user u ON dl.user_id = u.id
        SET dl.offboarding_date = NOW(),
            u.user_status_id = 4,
            u.location_id = NULL
        WHERE dl.operation_id = 3
          AND dl.offboarding_date IS NULL
          AND dl.creation_date < DATE_SUB(NOW(), INTERVAL 5 HOUR)
    `);
});

async function getRawData(from_date, to_date, client_id) {

    // The client_id needs to be passed multiple times in the query parameters.
    const params = [
        client_id, // For cu.client_id = ?
        from_date, // For u.creation_date BETWEEN ? AND ?
        to_date,   // For u.creation_date BETWEEN ? AND ?
        from_date, // For db3.creation_date BETWEEN ? AND ?
        to_date,   // For db3.creation_date BETWEEN ? AND ?
        // client_id is implicitly used in the subquery via cu.client_id
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
        -- No need for LEFT JOIN delivery_beneficiary AS db here, it's handled in the WHERE clause subquery if needed.
        WHERE u.role_id = 5
          AND q.enabled = 'Y'
          AND cu.client_id = ? -- Ensure the user is associated with the target client
          AND (
              -- Condition 1: User registered within the date range
              CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ?
              OR
              -- Condition 2: User had a delivery within the date range AT A LOCATION associated with THIS client
              u.id IN (
                  SELECT db3.receiving_user_id
                  FROM delivery_beneficiary db3
                  INNER JOIN location loc3 ON db3.location_id = loc3.id
                  INNER JOIN client_location cl3 ON loc3.id = cl3.location_id
                  WHERE CONVERT_TZ(db3.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ?
                    AND cl3.client_id = cu.client_id -- Filter deliveries by the client associated with the user in the outer query
              )
          )
          -- Ensure the question is relevant to at least one location associated with this client
          AND EXISTS (
              SELECT 1
              FROM question_location ql
              INNER JOIN client_location cl ON ql.location_id = cl.location_id
              WHERE ql.question_id = q.id AND cl.client_id = cu.client_id AND ql.enabled = 'Y'
          )
        GROUP BY u.id, q.id, a.id
        ORDER BY u.id, q.id, a.id`,
        params // Use the updated parameters array
    );
    // agregar a headers las preguntas de la encuesta, iterar el array rows y agregar el campo question hasta que se vuelva a repetir el question_id 
    var question_id_array = [];
    if (rows.length > 0) {
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            // si el id de la pregunta no esta en el question_id_array, agregarlo
            const id_repetido = question_id_array.some(obj => obj.question_id === row.question_id);
            if (!id_repetido) {
                question_id_array.push({ question_id: row.question_id, question: row.question });
            }
        }
    }

    /* iterar el array rows y agregar los campos username, email, firstname, lastname, date_of_birth, phone, zipcode, household_size, gender, ethnicity, other_ethnicity, location, registration_date, registration_time
    y que cada pregunta sea una columna, si el question_id se repite entonces agregar el campo answer a la columna correspondiente agregando al final del campo texto separando el valor por coma, si no se repite entonces agregar el campo answer a la columna correspondiente y agregar el objeto a rows_filtered
    */
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
            // es un answer_type_id = 4, agregar el campo answer al final del campo texto separando el valor por coma
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
    // iterar el array headers y convertirlo en un array de objetos con id y title para csvWriter
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
    // Parsear el CSV recibido
    const records = parse(csvRawData, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ';'
    });

    if (records.length === 0) {
        return '';
    }

    // Convertir from_date y to_date a objetos de fecha
    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD");

    // Filtrar las filas según registration_date dentro del rango
    const filteredRecords = records.filter(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        return registrationDate.isBetween(fromDate, toDate, 'day', '[)'); // Include start, exclude end
    });

    // Generar un nuevo CSV con las filas filtradas
    const csvStringifier = createCsvStringifier({
        header: Object.keys(records[0]).map(key => ({ id: key, title: key })),
        fieldDelimiter: ';'
    });

    let csvData = csvStringifier.getHeaderString();
    csvData += csvStringifier.stringifyRecords(filteredRecords);

    return csvData;
}

async function getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date) {
    // Parsear el CSV recibido
    const records = parse(csvRawData, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ';'
    });

    if (records.length === 0) {
        return '';
    }

    // Convertir from_date y to_date a objetos de fecha
    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD");

    // Filtrar las filas sin seguro de salud y con registration_date dentro del rango
    const filteredRecords = records.filter(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        return registrationDate.isBetween(fromDate, toDate, 'day', '[)') && // Include start, exclude end
            (record['Health Insurance?'] === 'No' || record['Health Insurance?'] === '');
    });

    // Generar un nuevo CSV con las filas filtradas
    const csvStringifier = createCsvStringifier({
        header: Object.keys(records[0]).map(key => ({ id: key, title: key })),
        fieldDelimiter: ';'
    });

    let csvData = csvStringifier.getHeaderString();
    csvData += csvStringifier.stringifyRecords(filteredRecords);

    return csvData;
}

async function getSummary(from_date, to_date, csvRawData) {
    // Parsear el CSV recibido
    const records = parse(csvRawData, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ';'
    });

    if (records.length === 0) {
        return '';
    }

    // Inicializar contadores
    let newCount = 0;
    let recurringCount = 0;
    let newHealthPlanYes = 0;
    let newHealthPlanNo = 0;

    // Convertir from_date y to_date a objetos de fecha
    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD");

    // Calcular las sumas correspondientes
    records.forEach(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        // Use '[]' to include both fromDate and toDate in the check for "New"
        const isNew = registrationDate.isBetween(fromDate, toDate, 'day', '[]'); 

        if (isNew) {
            newCount++;
            // ... (health plan logic remains the same)
            if (record['Health Insurance?'] === 'Yes') {
                newHealthPlanYes++;
            } else if (record['Health Insurance?'] === 'No' || record['Health Insurance?'] === '') {
                newHealthPlanNo++;
            }
        } else {
            recurringCount++;
        }
    });

    // ... (rest of the function remains the same)
    const totalNewRecurring = newCount + recurringCount;
    const totalNewHealthPlan = newHealthPlanYes + newHealthPlanNo;

    // Crear el CSV con la información resumida
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

// schedule.scheduleJob('*/5 * * * *', async () => { // Se ejecuta cada 5 minutos
// schedule.scheduleJob('* * * * *', async () => { // Se ejecuta cada minuto
// schedule.scheduleJob('0 0 * * 1', async () => { // Se ejecuta cada lunes a medianoche
// Create a rule for Mondays at midnight in Los Angeles time
const rule = new RecurrenceRule();
rule.dayOfWeek = 1; // Monday (0 is Sunday)
rule.hour = 0;      // Midnight
rule.minute = 0;    
rule.tz = 'America/Los_Angeles';

// New rule for Sunday at 6:00 PM for administration email
const adminRule = new RecurrenceRule();
adminRule.dayOfWeek = 0; // Sunday (0 is Sunday)
adminRule.hour = 18;     // 6:00 PM
adminRule.minute = 0;
adminRule.tz = 'America/Los_Angeles';

// Schedule for administration email (Sunday 6:00 PM)
schedule.scheduleJob(adminRule, async () => {
    const adminEmail = 'administration@bienestariswellbeing.org';
    const password = 'bienestarcommunity';
    
    const [adminClients] = await mysqlConnection.promise().query(
        `SELECT ce.client_id, c.name AS client_name
         FROM client_email AS ce
         INNER JOIN client AS c ON ce.client_id = c.id
         WHERE ce.email = ? AND ce.enabled = 'Y'
         ORDER BY ce.client_id`,
        [adminEmail]
    );

    if (adminClients.length > 0) {
        // Calculate date range (Corrected)
        let today = moment().tz("America/Los_Angeles"); // Job runs on Sunday
        let lastMonday = today.clone().subtract(6, 'days'); // Sunday - 6 days = Monday
        let lastSunday = today.clone(); // Sunday (today)
        
        let from_date = lastMonday.format("YYYY-MM-DD");
        let to_date = lastSunday.format("YYYY-MM-DD");
        
        let formatted_from_date = lastMonday.format("MM-DD-YYYY");
        let formatted_to_date = lastSunday.format("MM-DD-YYYY");
        
        // Send an email for EACH client
        for (const client of adminClients) {
            let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY"); // Use current date for report name consistency
            
            // Message for email
            const message = `Dear recipient,\n\nAttached you will find the Bienestar Community report for ${date}. The report covers the period from ${formatted_from_date} to ${formatted_to_date}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;
            const subject = `Bienestar Community report for ${client.client_name} - ${date}`;
            
            // Generate reports for this specific client
            const csvRawData = await getRawData(from_date, to_date, client.client_id);
            
            if (csvRawData && csvRawData.split('\n').length > 2) {
                const csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date);
                const csvAllNewRegistrations = await getNewRegistrations(csvRawData, from_date, to_date);
                const csvSummary = await getSummary(from_date, to_date, csvRawData);
                
                // Send admin email for this client
                await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password, [adminEmail]);
            }
        }
    }
});

schedule.scheduleJob(rule, async () => {
    const password = 'bienestarcommunity';
    const [rows_emails] = await mysqlConnection.promise().query(
        `SELECT ce.email, ce.client_id, c.name as client_name
        FROM client_email AS ce
        INNER JOIN client AS c ON ce.client_id = c.id
        WHERE ce.enabled = 'Y' AND ce.email != 'administration@bienestariswellbeing.org'
        ORDER BY ce.client_id`
    );
    if (rows_emails.length > 0) {
        // Calcular from_date y to_date
        let today = moment().tz("America/Los_Angeles");
        // If running on Monday, we want last Monday through last Sunday
        let lastMonday = today.clone().subtract(7, 'days'); // Last Monday
        let lastSunday = today.clone().subtract(1, 'days'); // Yesterday (Sunday)
        
        let from_date = lastMonday.format("YYYY-MM-DD");
        let to_date = lastSunday.format("YYYY-MM-DD");  // Include the entire Sunday
        
        // Formatear las fechas para el mensaje
        let formatted_from_date = lastMonday.format("MM-DD-YYYY");
        let formatted_to_date = lastSunday.format("MM-DD-YYYY");
        // un cliente puede tener varios emails
        // recorrer los emails de un cliente, guardarlos en variable emails y enviar el correo
        const emails = [];
        const client_id = [];
        var csvRawData = null;
        var csvNewRegistrations = null;
        var csvAllNewRegistrations = null;
        var csvSummary = null;
        var subject = '';
        var message = '';

        let date = today.format("MM-DD-YYYY");

        // add to message that it is a zip file and need to be unzipped with password
        message = `Dear recipient,\n\nAttached you will find the Bienestar Community report for ${date}. The report covers the period from ${formatted_from_date} to ${formatted_to_date}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;

        for (let i = 0; i < rows_emails.length; i++) {
            if (i === 0) {
                emails.push(rows_emails[i].email);
                client_id.push(rows_emails[i].client_id);
                subject = `Bienestar Community report for ${rows_emails[i].client_name} - ${date}`;
                csvRawData = await getRawData(from_date, to_date, client_id[0]);
                if (csvRawData && csvRawData.split('\n').length > 2) { // tiene 2 saltos de linea el csv vacio
                    csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date);
                    csvAllNewRegistrations = await getNewRegistrations(csvRawData, from_date, to_date);
                    csvSummary = await getSummary(from_date, to_date, csvRawData);
                } else {
                    csvRawData = null;
                }
            } else if (client_id.includes(rows_emails[i].client_id)) {
                emails.push(rows_emails[i].email);
            } else {
                if (csvRawData) {
                    await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password, emails);
                }
                emails.length = 0;
                emails.push(rows_emails[i].email);
                client_id.length = 0;
                client_id.push(rows_emails[i].client_id);
                subject = `Bienestar Community report for ${rows_emails[i].client_name} - ${date}`;
                csvRawData = await getRawData(from_date, to_date, client_id[0]);
                if (csvRawData && csvRawData.split('\n').length > 2) { // tiene 2 saltos de linea el csv vacio
                    csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date);
                    csvAllNewRegistrations = await getNewRegistrations(csvRawData, from_date, to_date);
                    csvSummary = await getSummary(from_date, to_date, csvRawData);
                } else {
                    csvRawData = null;
                }
            }
        }
        if (csvRawData) {
            await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password, emails);
        }
    }
});

// Monthly client reports - Runs every Monday, checks if it's first Monday of month
schedule.scheduleJob('0 0 * * 1', async () => {
    const today = moment().tz("America/Los_Angeles");
    const dayOfMonth = today.date();
    
    // Check if this is the first Monday of the month (days 1-7)
    if (dayOfMonth <= 7) {
        const password = 'bienestarcommunity';
        const [rows_emails] = await mysqlConnection.promise().query(
            `SELECT ce.email, ce.client_id, c.name as client_name
            FROM client_email AS ce
            INNER JOIN client AS c ON ce.client_id = c.id
            WHERE ce.enabled = 'Y' AND ce.email != 'administration@bienestariswellbeing.org'
            ORDER BY ce.client_id`
        );
        
        if (rows_emails.length > 0) {
            // Calculate previous month's date range (first Monday to last Sunday)
            let prevMonth = today.clone().subtract(1, 'month');
            
            // Find the first Monday of the previous month
            let firstMonday = prevMonth.clone().startOf('month');
            while (firstMonday.day() !== 1) {
                firstMonday.add(1, 'day');
            }
            
            // Find the last Sunday of the previous month
            let lastSunday = prevMonth.clone().endOf('month');
            while (lastSunday.day() !== 0) {
                lastSunday.subtract(1, 'day');
            }
            
            let from_date = firstMonday.format("YYYY-MM-DD");
            let to_date = lastSunday.format("YYYY-MM-DD");
            
            // Format dates for the message
            let formatted_from_date = firstMonday.format("MM-DD-YYYY");
            let formatted_to_date = lastSunday.format("MM-DD-YYYY");
            
            // Variables for email
            const emails = [];
            const client_id = [];
            var csvRawData = null;
            var csvNewRegistrations = null;
            var csvAllNewRegistrations = null;
            var csvSummary = null;
            var subject = '';
            var message = '';
            
            let monthName = prevMonth.format("MMMM");
            let year = prevMonth.format("YYYY");
            let date = today.format("MM-DD-YYYY");
            
            // Email message
            message = `Dear recipient,\n\nAttached you will find the monthly Bienestar Community report for ${monthName} ${year}. The report covers the period from ${formatted_from_date} to ${formatted_to_date}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;
            
            // Process each client's emails
            for (let i = 0; i < rows_emails.length; i++) {
                if (i === 0) {
                    emails.push(rows_emails[i].email);
                    client_id.push(rows_emails[i].client_id);
                    subject = `Monthly Bienestar Community report for ${rows_emails[i].client_name} - ${monthName} ${year}`;
                    csvRawData = await getRawData(from_date, to_date, client_id[0]);
                    if (csvRawData && csvRawData.split('\n').length > 2) {
                        csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date);
                        csvAllNewRegistrations = await getNewRegistrations(csvRawData, from_date, to_date);
                        csvSummary = await getSummary(from_date, to_date, csvRawData);
                    } else {
                        csvRawData = null;
                    }
                } else if (client_id.includes(rows_emails[i].client_id)) {
                    emails.push(rows_emails[i].email);
                } else {
                    if (csvRawData) {
                        await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password, emails);
                    }
                    emails.length = 0;
                    emails.push(rows_emails[i].email);
                    client_id.length = 0;
                    client_id.push(rows_emails[i].client_id);
                    subject = `Monthly Bienestar Community report for ${rows_emails[i].client_name} - ${monthName} ${year}`;
                    csvRawData = await getRawData(from_date, to_date, client_id[0]);
                    if (csvRawData && csvRawData.split('\n').length > 2) {
                        csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date);
                        csvAllNewRegistrations = await getNewRegistrations(csvRawData, from_date, to_date);
                        csvSummary = await getSummary(from_date, to_date, csvRawData);
                    } else {
                        csvRawData = null;
                    }
                }
            }
            
            // Send final email if there's data
            if (csvRawData) {
                await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password, emails);
            }
        }
    }
});

// Monthly admin reports rule
const monthlyAdminRule = new RecurrenceRule();
monthlyAdminRule.dayOfWeek = 0; // Sunday
monthlyAdminRule.hour = 18;     // 6:00 PM
monthlyAdminRule.minute = 0;
monthlyAdminRule.tz = 'America/Los_Angeles';

// Monthly admin reports - Runs every Sunday, checks if it's last Sunday of month
schedule.scheduleJob(monthlyAdminRule, async () => {
    const today = moment().tz("America/Los_Angeles");
    const nextWeek = today.clone().add(7, 'days');
    
    // If next Sunday is in a different month, then this is the last Sunday
    if (nextWeek.month() !== today.month()) {
        const adminEmail = 'administration@bienestariswellbeing.org';
        const password = 'bienestarcommunity';
        
        const [adminClients] = await mysqlConnection.promise().query(
            `SELECT ce.client_id, c.name AS client_name
             FROM client_email AS ce
             INNER JOIN client AS c ON ce.client_id = c.id
             WHERE ce.email = ? AND ce.enabled = 'Y'
             ORDER BY ce.client_id`,
            [adminEmail]
        );
        
        if (adminClients.length > 0) {
            // Calculate current month's date range (first Monday to last Sunday)
            // First Monday of the month
            let firstMonday = today.clone().startOf('month');
            while (firstMonday.day() !== 1) {
                firstMonday.add(1, 'day');
            }
            
            // Last Sunday is today
            let lastSunday = today.clone();
            
            let from_date = firstMonday.format("YYYY-MM-DD");
            let to_date = lastSunday.format("YYYY-MM-DD");
            
            let formatted_from_date = firstMonday.format("MM-DD-YYYY");
            let formatted_to_date = lastSunday.format("MM-DD-YYYY");
            
            let monthName = today.format("MMMM");
            let year = today.format("YYYY");
            
            // Process each client for admin report
            for (const client of adminClients) {
                const message = `Dear recipient,\n\nAttached you will find the monthly Bienestar Community report for ${monthName} ${year}. The report covers the period from ${formatted_from_date} to ${formatted_to_date}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;
                const subject = `Monthly Bienestar Community report for ${client.client_name} - ${monthName} ${year}`;
                
                // Generate reports for this specific client
                const csvRawData = await getRawData(from_date, to_date, client.client_id);
                
                if (csvRawData && csvRawData.split('\n').length > 2) {
                    const csvNewRegistrations = await getNewRegistrationsWithoutHealthInsurance(csvRawData, from_date, to_date);
                    const csvAllNewRegistrations = await getNewRegistrations(csvRawData, from_date, to_date);
                    const csvSummary = await getSummary(from_date, to_date, csvRawData);
                    
                    // Send admin email for this client
                    await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password, [adminEmail]);
                }
            }
        }
    }
});

server.listen(port, () => logger.info(`Server running on port ${port}`));