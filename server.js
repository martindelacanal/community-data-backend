const http = require('http');
const app = require('./app');

const port = process.env.PORT || 3000;

const server = http.createServer(app);
const mysqlConnection = require('./api/connection/connection.js');
const schedule = require('node-schedule');

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
        LEFT JOIN delivery_beneficiary AS db ON u.id = db.receiving_user_id
        WHERE u.role_id = 5 AND q.enabled = 'Y' 
        AND (CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ? 
        OR u.id IN (SELECT db3.receiving_user_id FROM delivery_beneficiary db3 
                     WHERE CONVERT_TZ(db3.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ?))
        AND EXISTS (SELECT 1 FROM question_location ql INNER JOIN client_location cl ON ql.location_id = cl.location_id WHERE ql.question_id = q.id AND cl.client_id = cu.client_id AND ql.enabled = \'Y\')
        and cu.client_id = ?
        GROUP BY u.id, q.id, a.id
        ORDER BY u.id, q.id, a.id`,
        [from_date, to_date, from_date, to_date, client_id]
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

async function getNewRegistrations(csvRawData) {
    // Parsear el CSV recibido
    const records = parse(csvRawData, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ';'
    });

    if (records.length === 0) {
        return '';
    }

    // Filtrar las filas según la condición especificada
    const filteredRecords = records.filter(record => {
        return record['Health Insurance?'] === 'No' || record['Health Insurance?'] === '';
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
        const isNew = registrationDate.isBetween(fromDate, toDate, null, '[]'); // Inclusivo en ambos extremos

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

schedule.scheduleJob('*/5 * * * *', async () => { // Se ejecuta cada 5 minutos
// schedule.scheduleJob('* * * * *', async () => { // Se ejecuta cada minuto
// schedule.scheduleJob('0 0 * * 1', async () => { // Se ejecuta cada lunes a medianoche

    const password = 'bienestarcommunity';
    const [rows_emails] = await mysqlConnection.promise().query(
        `select ce.email, ce.client_id, c.name as client_name
        from client_email as ce
        inner join client as c on ce.client_id = c.id
        where ce.enabled = 'Y'
        order by ce.client_id`
    );
    if (rows_emails.length > 0) {
        // Calcular from_date y to_date
        let today = moment().tz("America/Los_Angeles");
        let lastSaturday = today.clone().day(-1); // último sábado
        let previousSaturday = lastSaturday.clone().subtract(1, 'weeks'); // sábado anterior al último sábado

        let from_date = previousSaturday.format("YYYY-MM-DD");
        let to_date = lastSaturday.format("YYYY-MM-DD");
        // un cliente puede tener varios emails
        // recorrer los emails de un cliente, guardarlos en variable emails y enviar el correo
        const emails = [];
        const client_id = [];
        var csvRawData = null;
        var csvNewRegistrations = null;
        var csvSummary = null;
        var subject = '';
        var message = '';

        let date = today.format("MM-DD-YYYY");

        // add to message that it is a zip file and need to be unzipped with password
        message = `Dear recipient,\n\nAttached you will find the Bienestar Community report for ${date}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;

        for (let i = 0; i < rows_emails.length; i++) {
            if (i === 0) {
                emails.push(rows_emails[i].email);
                client_id.push(rows_emails[i].client_id);
                subject = `Bienestar Community report for ${rows_emails[i].client_name} - ${date}`;
                csvRawData = await getRawData(from_date, to_date, client_id[0]);
                if (csvRawData && csvRawData.split('\n').length > 1) {
                    csvNewRegistrations = await getNewRegistrations(csvRawData);
                    csvSummary = await getSummary(from_date, to_date, csvRawData);
                } else {
                    csvRawData = null;
                }
            } else if (client_id.includes(rows_emails[i].client_id)) {
                emails.push(rows_emails[i].email);
            } else {
                if (csvRawData) {
                    await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, password, emails);
                }
                emails.length = 0;
                emails.push(rows_emails[i].email);
                client_id.length = 0;
                client_id.push(rows_emails[i].client_id);
                subject = `Bienestar Community report for ${rows_emails[i].client_name} - ${date}`;
                csvRawData = await getRawData(from_date, to_date, client_id[0]);
                if (csvRawData && csvRawData.split('\n').length > 1) {
                    csvNewRegistrations = await getNewRegistrations(csvRawData);
                    csvSummary = await getSummary(from_date, to_date, csvRawData);
                } else {
                    csvRawData = null;
                }
            }
        }
        if (csvRawData) {
            await email.sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, password, emails);
        }
    }
});

server.listen(port, () => logger.info(`Server running on port ${port}`));