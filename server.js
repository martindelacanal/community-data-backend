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

const XLSX = require('xlsx');

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

async function getRawDataExcel(from_date, to_date, client_id) {
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
                uq.answer_number AS answer_number,
                EXISTS (
                    SELECT 1
                    FROM client_location cl_first
                    WHERE cl_first.location_id = u.first_location_id
                      AND cl_first.client_id = cu.client_id
                ) AS registered_at_client_location_flag
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
            row_filtered["registered_at_client_location"] = rows[i].registered_at_client_location_flag ? '1' : '0';
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
    // En lugar de usar csvStringifier, crear un workbook de Excel
    const workbook = XLSX.utils.book_new();

    // Crear las cabeceras
    const headers = [
        'User ID', 'Username', 'Email', 'Firstname', 'Lastname', 'Date of birth',
        'Phone', 'Zipcode', 'Household size', 'Gender', 'Ethnicity', 'Other ethnicity',
        'Last location visited', 'Locations visited', 'Registration date', 'Registration time',
        'Registered at Client Location'
    ];

    // Agregar las preguntas como headers
    for (let i = 0; i < question_id_array.length; i++) {
        headers.push(question_id_array[i].question);
    }

    // Crear los datos para Excel
    const excelData = [];
    excelData.push(headers); // Primera fila con headers

    // Agregar los datos
    rows_filtered.forEach(row => {
        const excelRow = [
            row.user_id,
            row.username,
            row.email,
            row.firstname,
            row.lastname,
            row.date_of_birth,
            row.phone,
            row.zipcode,
            row.household_size,
            row.gender,
            row.ethnicity,
            row.other_ethnicity,
            row.last_location_visited,
            row.locations_visited,
            row.registration_date,
            row.registration_time,
            row.registered_at_client_location
        ];

        // Agregar las respuestas de las preguntas
        for (let i = 0; i < question_id_array.length; i++) {
            const questionId = question_id_array[i].question_id;
            excelRow.push(row[questionId] || '');
        }

        excelData.push(excelRow);
    });

    // Crear worksheet
    const worksheet = XLSX.utils.aoa_to_sheet(excelData);
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Raw Data');

    // Convertir a buffer
    const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'buffer' });

    return excelBuffer;
}

async function getNewRegistrationsExcel(excelRawData, from_date, to_date) {
    // Leer el Excel buffer
    const workbook = XLSX.read(excelRawData, { type: 'buffer' });
    const worksheet = workbook.Sheets['Raw Data'];
    const jsonData = XLSX.utils.sheet_to_json(worksheet);

    if (jsonData.length === 0) {
        return Buffer.alloc(0);
    }

    // Convertir from_date y to_date a objetos de fecha
    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD").endOf('day');

    // Filtrar las filas según registration_date dentro del rango
    const filteredRecords = jsonData.filter(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        return registrationDate.isBetween(fromDate, toDate, 'day', '[]');
    });

    // Crear nuevo workbook con datos filtrados
    const newWorkbook = XLSX.utils.book_new();
    const newWorksheet = XLSX.utils.json_to_sheet(filteredRecords);
    XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, 'New Registrations');

    return XLSX.write(newWorkbook, { bookType: 'xlsx', type: 'buffer' });
}


async function getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, from_date, to_date) {
    // Leer el Excel buffer
    const workbook = XLSX.read(excelRawData, { type: 'buffer' });
    const worksheet = workbook.Sheets['Raw Data'];
    const jsonData = XLSX.utils.sheet_to_json(worksheet);

    if (jsonData.length === 0) {
        return Buffer.alloc(0);
    }

    // Convertir from_date y to_date a objetos de fecha
    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD").endOf('day');

    // Filtrar las filas sin seguro de salud y con registration_date dentro del rango
    const filteredRecords = jsonData.filter(record => {
        const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
        return registrationDate.isBetween(fromDate, toDate, 'day', '[]') &&
            (record['Health Insurance?'] === 'No' || record['Health Insurance?'] === '');
    });

    // Crear nuevo workbook con datos filtrados
    const newWorkbook = XLSX.utils.book_new();
    const newWorksheet = XLSX.utils.json_to_sheet(filteredRecords);
    XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, 'No Health Insurance');

    return XLSX.write(newWorkbook, { bookType: 'xlsx', type: 'buffer' });
}

async function getSummaryExcel(from_date, to_date, client_id, excelRawData) {
    // Leer el Excel buffer en lugar de parsear CSV
    const workbook = XLSX.read(excelRawData, { type: 'buffer' });
    const worksheet = workbook.Sheets['Raw Data'];
    const records = XLSX.utils.sheet_to_json(worksheet);

    // Fetch client name
    let clientName = 'Unknown Client';
    try {
        const [clientRows] = await mysqlConnection.promise().query(
            `SELECT name FROM client WHERE id = ?`,
            [client_id]
        );
        if (clientRows.length > 0) {
            clientName = clientRows[0].name;
        }
    } catch (error) {
        logger.error(`Error fetching client name for client_id ${client_id}:`, error);
    }

    // Format dates for display and DB
    const displayFromDate = moment(from_date, "YYYY-MM-DD").format("MM/DD/YYYY");
    const displayToDate = moment(to_date, "YYYY-MM-DD").format("MM/DD/YYYY");
    const dateRangeDisplay = `${displayFromDate} - ${displayToDate}`;

    const from_date_db_start = moment(from_date, "YYYY-MM-DD").format("YYYY-MM-DD 00:00:00");
    const to_date_db_end = moment(to_date, "YYYY-MM-DD").format("YYYY-MM-DD 23:59:59");

    // Overall Summary Calculations (from csvRawData)
    let newCount = 0;
    let newHealthPlanYes = 0;
    let newHealthPlanNo = 0;
    let recurringCount = 0; // Initialize recurringCount

    if (records.length > 0) {
        const filterFromDateMoment = moment(from_date, "YYYY-MM-DD");
        const filterToDateMoment = moment(to_date, "YYYY-MM-DD").endOf('day');

        records.forEach(record => {
            const registrationDate = moment(record['Registration date'], "MM/DD/YYYY");
            const isWithinDateRange = registrationDate.isBetween(filterFromDateMoment, filterToDateMoment, 'day', '[]');
            const wasRegisteredAtThisClientLocation = record['Registered at Client Location'] === '1';

            if (isWithinDateRange && wasRegisteredAtThisClientLocation) {
                newCount++;
                if (record['Health Insurance?'] === 'Yes') {
                    newHealthPlanYes++;
                } else if (record['Health Insurance?'] === 'No' || record['Health Insurance?'] === '') {
                    newHealthPlanNo++;
                }
            }
        });
    }

    try {
        const recurringParams = [
            client_id,
            client_id,
            from_date_db_start,
            to_date_db_end,
            from_date_db_start,
            from_date_db_start
        ];
        const [recurringRows] = await mysqlConnection.promise().query(
            `
            SELECT COUNT(DISTINCT u.id) AS recurring
            FROM user u
            INNER JOIN client_user cu ON u.id = cu.user_id AND cu.client_id = ?
            INNER JOIN delivery_beneficiary db_range ON u.id = db_range.receiving_user_id
            INNER JOIN client_location cl_range ON db_range.location_id = cl_range.location_id AND cl_range.client_id = ?
            WHERE u.role_id = 5 AND u.enabled = 'Y'
              AND CONVERT_TZ(db_range.creation_date,'+00:00','America/Los_Angeles') BETWEEN ? AND ?
              AND (
                    EXISTS (
                        SELECT 1
                        FROM delivery_beneficiary db_prev
                        INNER JOIN client_location cl_prev ON db_prev.location_id = cl_prev.location_id AND cl_prev.client_id = cu.client_id
                        WHERE db_prev.receiving_user_id = u.id
                          AND CONVERT_TZ(db_prev.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles')
                    )
                    OR
                    (
                        CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') < ? 
                        AND EXISTS ( 
                            SELECT 1
                            FROM client_location cl_first_check
                            WHERE cl_first_check.location_id = u.first_location_id AND cl_first_check.client_id = cu.client_id
                        )
                        AND NOT EXISTS ( 
                            SELECT 1
                            FROM delivery_beneficiary db_no_past
                            WHERE db_no_past.receiving_user_id = u.id
                              AND CONVERT_TZ(db_no_past.creation_date, '+00:00', 'America/Los_Angeles') < ? 
                        )
                    )
              )
            `,
            recurringParams
        );
        recurringCount = (recurringRows[0] && recurringRows[0].recurring) || 0;
    } catch (error) {
        logger.error(`Error fetching overall recurring count for client_id ${client_id}:`, error);
    }

    const totalNewRecurring = newCount + recurringCount;
    const totalNewHealthPlan = newHealthPlanYes + newHealthPlanNo;

    const summaryPartRows = [
        { col1: 'Client Name', col2: clientName, col3: '', col4: '', col5: '' },
        { col1: 'Date Range', col2: dateRangeDisplay, col3: '', col4: '', col5: '' },
        { col1: '', col2: '', col3: '', col4: '', col5: '' },
        { col1: 'New', col2: newCount, col3: '', col4: '', col5: '' },
        { col1: 'Recurring', col2: recurringCount, col3: '', col4: '', col5: '' },
        { col1: 'Total', col2: totalNewRecurring, col3: '', col4: '', col5: '' },
        { col1: '', col2: '', col3: '', col4: '', col5: '' },
        { col1: '(New) Health Plan', col2: '', col3: '', col4: '', col5: '' },
        { col1: '  YES', col2: newHealthPlanYes, col3: '', col4: '', col5: '' },
        { col1: '  NO', col2: newHealthPlanNo, col3: '', col4: '', col5: '' },
        { col1: '  Total', col2: totalNewHealthPlan, col3: '', col4: '', col5: '' }
    ];

    let allClientLocations = [];
    try {
        [allClientLocations] = await mysqlConnection.promise().query(
            `SELECT l.id, l.community_city as name FROM location l JOIN client_location cl ON l.id = cl.location_id WHERE cl.client_id = ? ORDER BY l.id`,
            [client_id]
        );
    } catch (error) {
        logger.error(`Error fetching locations for client_id ${client_id}:`, error);
    }

    const newPerLocationMap = {};
    try {
        const [newLocRows] = await mysqlConnection.promise().query(
            `SELECT
                u.first_location_id AS location_id,
                COUNT(DISTINCT u.id) AS new_count
            FROM user u
            JOIN client_user cu ON u.id = cu.user_id AND cu.client_id = ?
            JOIN client_location cl ON u.first_location_id = cl.location_id AND cl.client_id = ?
            WHERE u.role_id = 5
              AND CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ?
            GROUP BY u.first_location_id;`,
            [client_id, client_id, from_date_db_start, to_date_db_end]
        );
        newLocRows.forEach(row => { newPerLocationMap[row.location_id] = row.new_count; });
    } catch (error) {
        logger.error(`Error fetching new per location for client_id ${client_id}:`, error);
    }

    const recurringPerLocationMap = {};
    try {
        const recurringPerLocationParams = [
            client_id,
            client_id,
            from_date_db_start,
            to_date_db_end,
            from_date_db_start,
            from_date_db_start
        ];
        const [recLocRows] = await mysqlConnection.promise().query(
            `SELECT
                db_range.location_id,
                COUNT(DISTINCT u.id) AS recurring_count
            FROM user u
            INNER JOIN client_user cu ON u.id = cu.user_id AND cu.client_id = ?
            INNER JOIN delivery_beneficiary db_range ON u.id = db_range.receiving_user_id
            INNER JOIN client_location cl_range ON db_range.location_id = cl_range.location_id AND cl_range.client_id = ?
            WHERE u.role_id = 5 AND u.enabled = 'Y'
              AND CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles') BETWEEN ? AND ?
              AND (
                  EXISTS (
                      SELECT 1
                      FROM delivery_beneficiary db_prev
                      INNER JOIN client_location cl_prev ON db_prev.location_id = cl_prev.location_id AND cl_prev.client_id = cu.client_id
                      WHERE db_prev.receiving_user_id = u.id
                        AND CONVERT_TZ(db_prev.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles')
                  )
                  OR
                  (
                      CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') < ?
                      AND EXISTS (
                          SELECT 1
                          FROM client_location cl_first_check
                          WHERE cl_first_check.location_id = u.first_location_id AND cl_first_check.client_id = cu.client_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM delivery_beneficiary db_no_past
                          WHERE db_no_past.receiving_user_id = u.id
                            AND CONVERT_TZ(db_no_past.creation_date, '+00:00', 'America/Los_Angeles') < ?
                      )
                  )
              )
            GROUP BY db_range.location_id;`,
            recurringPerLocationParams
        );
        recLocRows.forEach(row => { recurringPerLocationMap[row.location_id] = row.recurring_count; });
    } catch (error) {
        logger.error(`Error fetching recurring per location for client_id ${client_id}:`, error);
    }

    const locationTableRows = [];
    locationTableRows.push({ col1: '', col2: '', col3: '', col4: '', col5: '' });

    let locationRowBuilder;
    if (parseInt(client_id) === 1) {
        locationTableRows.push({ col1: 'Id', col2: 'Location', col3: 'New' });
        locationRowBuilder = (loc, newAtLoc) => ({
            col1: loc.id,
            col2: loc.name,
            col3: newAtLoc
        });
    } else {
        locationTableRows.push({ col1: 'Id', col2: 'Location', col3: 'New', col4: 'Recurring', col5: 'Totals' });
        locationRowBuilder = (loc, newAtLoc, recurringAtLoc, totalAtLoc) => ({
            col1: loc.id,
            col2: loc.name,
            col3: newAtLoc,
            col4: recurringAtLoc,
            col5: totalAtLoc
        });
    }

    let totalNewByLocation = 0;
    let totalRecurringByLocation = 0;
    let grandTotalByLocation = 0;

    if (allClientLocations.length > 0) {
        allClientLocations.forEach(loc => {
            const newAtLoc = newPerLocationMap[loc.id] || 0;
            const recurringAtLoc = recurringPerLocationMap[loc.id] || 0;
            const totalAtLoc = newAtLoc + recurringAtLoc;

            if (parseInt(client_id) === 1) {
                locationTableRows.push(locationRowBuilder(loc, newAtLoc));
            } else {
                locationTableRows.push(locationRowBuilder(loc, newAtLoc, recurringAtLoc, totalAtLoc));
            }

            totalNewByLocation += newAtLoc;
            totalRecurringByLocation += recurringAtLoc;
            grandTotalByLocation += totalAtLoc;
        });

        locationTableRows.push({ col1: '', col2: '', col3: '', col4: '', col5: '' });

        if (parseInt(client_id) === 1) {
            locationTableRows.push({
                col1: '',
                col2: 'TOTAL',
                col3: totalNewByLocation
            });
        } else {
            locationTableRows.push({
                col1: '',
                col2: 'TOTAL',
                col3: totalNewByLocation,
                col4: totalRecurringByLocation,
                col5: grandTotalByLocation
            });
        }
    }

    let allCsvRows = summaryPartRows.concat(locationTableRows);

    // let csvFileStringifier;
    // if (parseInt(client_id) === 1) {
    //     csvFileStringifier = createCsvStringifier({
    //         header: [
    //             { id: 'col1', title: 'Column1' }, { id: 'col2', title: 'Column2' },
    //             { id: 'col3', title: 'Column3' }
    //         ],
    //         fieldDelimiter: ';'
    //     });
    // } else {
    //     csvFileStringifier = createCsvStringifier({
    //         header: [
    //             { id: 'col1', title: 'Column1' }, { id: 'col2', title: 'Column2' },
    //             { id: 'col3', title: 'Column3' }, { id: 'col4', title: 'Column4' },
    //             { id: 'col5', title: 'Column5' }
    //         ],
    //         fieldDelimiter: ';'
    //     });
    // }
    // const csvString = csvFileStringifier.stringifyRecords(allCsvRows);

    const emailReportData = {
        clientName: clientName,
        dateRangeDisplay: dateRangeDisplay,
        newCount: newCount,
        recurringCount: recurringCount,
        totalNewRecurring: totalNewRecurring,
        newHealthPlanYes: newHealthPlanYes,
        newHealthPlanNo: newHealthPlanNo,
        totalNewHealthPlan: totalNewHealthPlan,
        locations: allClientLocations,
        newPerLocationMap: newPerLocationMap,
        recurringPerLocationMap: recurringPerLocationMap,
        totalNewByLocation: totalNewByLocation,
        totalRecurringByLocation: totalRecurringByLocation,
        grandTotalByLocation: grandTotalByLocation,
        clientId: client_id
    };

    // Check if there's any meaningful data to report
    // If csvString is just headers (or empty), and emailReportData has all zeros and empty arrays, consider it no data.
    // For simplicity, we'll rely on csvString length. If it's short (just headers), it implies minimal data.
    // The original check was: records.length === 0 && recurringCount === 0 && allClientLocations.length === 0
    // This might be too restrictive. Let's assume if csvString has content beyond headers, we send data.
    // A more robust check could be if (newCount === 0 && recurringCount === 0 && totalNewByLocation === 0 && totalRecurringByLocation === 0)

    // En lugar de crear CSV, crear Excel
    const summaryWorkbook = XLSX.utils.book_new();

    // Crear datos para el summary
    const summaryData = [];

    if (parseInt(client_id) === 1) {
        // Para client_id = 1, solo 3 columnas
        allCsvRows.forEach(row => {
            summaryData.push([row.col1, row.col2, row.col3]);
        });
    } else {
        // Para otros clientes, 5 columnas
        allCsvRows.forEach(row => {
            summaryData.push([row.col1, row.col2, row.col3, row.col4, row.col5]);
        });
    }

    const summaryWorksheet = XLSX.utils.aoa_to_sheet(summaryData);
    XLSX.utils.book_append_sheet(summaryWorkbook, summaryWorksheet, 'Summary');

    const excelBuffer = XLSX.write(summaryWorkbook, { bookType: 'xlsx', type: 'buffer' });

    return { excelBuffer: excelBuffer, emailReportData: emailReportData };
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
adminRule.hour = 22;     // 10:00 PM
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
        // Calculate date range for previous week (Monday through Sunday)
        let today = moment().tz("America/Los_Angeles");
        // This runs Sunday PM, so week is Mon (today-6d) to Sun (today)
        let lastMonday = today.clone().subtract(6, 'days').startOf('day'); // Monday 00:00:00 LA time
        let lastSunday = today.clone().endOf('day'); // Sunday 23:59:59 LA time

        // Format dates for database query (including time)
        let from_date_db = lastMonday.format("YYYY-MM-DD HH:mm:ss");
        let to_date_db = lastSunday.format("YYYY-MM-DD HH:mm:ss");

        // Format dates for display
        let formatted_from_date_display = lastMonday.format("MM-DD-YYYY");
        let formatted_to_date_display = lastSunday.format("MM-DD-YYYY");

        // Send an email for EACH client
        for (const client of adminClients) {
            let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY"); // Use current date for report name consistency

            // Message for email
            const message = `Dear recipient,\n\nAttached you will find the Bienestar Community report for ${date}. The report covers the period from ${formatted_from_date_display} to ${formatted_to_date_display}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;
            const subject = `Bienestar Community report for ${client.client_name} - ${date}`;

            // Generate reports for this specific client using full date-time range
            const excelRawData = await getRawDataExcel(from_date_db, to_date_db, client.client_id);

            if (excelRawData && excelRawData.length > 0) {
                const summaryObject = await getSummaryExcel(
                    lastMonday.format("YYYY-MM-DD"),
                    lastSunday.format("YYYY-MM-DD"),
                    client.client_id,
                    excelRawData);

                if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                    const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));
                    const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));

                    await email.sendEmailWithExcelAttachment(subject, message, excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, [adminEmail]);
                } else {
                    logger.warn(`No summary data generated for client ${client.client_name} (${client.client_id}) for period ${formatted_from_date_display} to ${formatted_to_date_display}. Skipping email.`);
                }
            } else {
                logger.warn(`No raw data found for client ${client.client_name} (${client.client_id}) for period ${formatted_from_date_display} to ${formatted_to_date_display}. Skipping email.`);
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
        // Calculate from_date and to_date for previous complete week
        let today = moment().tz("America/Los_Angeles"); // This is Monday 00:00:00

        // Last Monday (7 days ago)
        let lastMonday = today.clone().subtract(7, 'days').startOf('day'); // Previous Monday 00:00:00 LA time

        // Last Sunday (yesterday)
        let lastSunday = today.clone().subtract(1, 'days').endOf('day'); // Previous Sunday 23:59:59 LA time

        // Format dates for database query (including time)
        let from_date_db = lastMonday.format("YYYY-MM-DD HH:mm:ss");
        let to_date_db = lastSunday.format("YYYY-MM-DD HH:mm:ss");

        // Format dates for display in message
        let formatted_from_date_display = lastMonday.format("MM-DD-YYYY");
        let formatted_to_date_display = lastSunday.format("MM-DD-YYYY");

        const emailsByClient = {};
        rows_emails.forEach(row => {
            if (!emailsByClient[row.client_id]) {
                emailsByClient[row.client_id] = {
                    name: row.client_name,
                    emails: []
                };
            }
            emailsByClient[row.client_id].emails.push(row.email);
        });

        let date = today.format("MM-DD-YYYY");
        const messageBody = `Dear recipient,\n\nAttached you will find the Bienestar Community report for ${date}. The report covers the period from ${formatted_from_date_display} to ${formatted_to_date_display}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;

        for (const clientId in emailsByClient) {
            const clientData = emailsByClient[clientId];
            const subject = `Bienestar Community report for ${clientData.name} - ${date}`;

            const excelRawData = await getRawDataExcel(from_date_db, to_date_db, clientId);

            if (excelRawData && excelRawData.length > 0) {
                const summaryObject = await getSummaryExcel(
                    lastMonday.format("YYYY-MM-DD"),
                    lastSunday.format("YYYY-MM-DD"),
                    clientId,
                    excelRawData);

                if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                    const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));
                    const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));

                    await email.sendEmailWithExcelAttachment(subject, messageBody, excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, clientData.emails);
                } else {
                    logger.warn(`No summary data generated for client ${clientData.name} (${clientId}) for period ${formatted_from_date_display} to ${formatted_to_date_display}. Skipping email.`);
                }
            } else {
                logger.warn(`No raw data found for client ${clientData.name} (${clientId}) for period ${formatted_from_date_display} to ${formatted_to_date_display}. Skipping email.`);
            }
        }
    }
});

// Monthly client reports - Runs every Monday, checks if it's first Monday of month
const monthlyClientRule = new RecurrenceRule();
monthlyClientRule.dayOfWeek = 1;   // Lunes
monthlyClientRule.hour = 0;   // 00:00
monthlyClientRule.minute = 0;
monthlyClientRule.tz = 'America/Los_Angeles';

schedule.scheduleJob(monthlyClientRule, async () => {
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
            let prevMonth = today.clone().subtract(1, 'month');
            let firstDayOfMonth = prevMonth.clone().startOf('month');
            let firstMonday = firstDayOfMonth.clone();
            while (firstMonday.day() !== 1) {
                firstMonday.subtract(1, 'day');
            }
            firstMonday = firstMonday.startOf('day');

            let lastDayOfMonth = prevMonth.clone().endOf('month');
            let lastSunday = lastDayOfMonth.clone();
            while (lastSunday.day() !== 0) {
                lastSunday.add(1, 'day');
            }
            lastSunday = lastSunday.endOf('day');

            let from_date_db = firstMonday.format("YYYY-MM-DD HH:mm:ss");
            let to_date_db = lastSunday.format("YYYY-MM-DD HH:mm:ss");

            let formatted_from_date_display = firstMonday.format("MM-DD-YYYY");
            let formatted_to_date_display = lastSunday.format("MM-DD-YYYY");

            let monthName = prevMonth.format("MMMM");
            let year = prevMonth.format("YYYY");

            const messageBody = `Dear recipient,\n\nAttached you will find the monthly Bienestar Community report for ${monthName} ${year}. The report covers the period from ${formatted_from_date_display} to ${formatted_to_date_display}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;

            const emailsByClient = {};
            rows_emails.forEach(row => {
                if (!emailsByClient[row.client_id]) {
                    emailsByClient[row.client_id] = {
                        name: row.client_name,
                        emails: []
                    };
                }
                emailsByClient[row.client_id].emails.push(row.email);
            });

            for (const clientId in emailsByClient) {
                const clientData = emailsByClient[clientId];
                const subject = `Monthly Bienestar Community report for ${clientData.name} - ${monthName} ${year}`;

                const excelRawData = await getRawDataExcel(from_date_db, to_date_db, clientId);

                if (excelRawData && excelRawData.length > 0) {
                    const summaryObject = await getSummaryExcel(
                        lastMonday.format("YYYY-MM-DD"),
                        lastSunday.format("YYYY-MM-DD"),
                        clientId,
                        excelRawData);

                    if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                        const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));
                        const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));

                        await email.sendEmailWithExcelAttachment(subject, messageBody, excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, clientData.emails);
                    } else {
                        logger.warn(`No summary data generated for client ${clientData.name} (${clientId}) for period ${formatted_from_date_display} to ${formatted_to_date_display} (Monthly). Skipping email.`);
                    }
                } else {
                    logger.warn(`No raw data found for client ${clientData.name} (${clientId}) for period ${formatted_from_date_display} to ${formatted_to_date_display} (Monthly). Skipping email.`);
                }
            }
        }
    }
});

// Monthly admin reports rule
const firstSundayAdminRule = new RecurrenceRule();
firstSundayAdminRule.dayOfWeek = 0; // Domingo
firstSundayAdminRule.hour = 22; // 22:00
firstSundayAdminRule.minute = 0;
firstSundayAdminRule.tz = 'America/Los_Angeles';

schedule.scheduleJob(firstSundayAdminRule, async () => {
    const today = moment().tz("America/Los_Angeles");

    if (today.date() > 7) { return; } // Only run on the first Sunday of the month

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
        // For the monthly admin report, the range is the *current* month up to this first Sunday.
        // This seems to be the logic from the original code for the "Monthly admin reports rule"
        // which calculates based on `today` being the first Sunday.
        // If the intention was for the *previous* full month, the date logic would need to mirror `monthlyClientRule`.
        // Assuming current month up to the first Sunday:
        let firstDayOfCurrentMonth = today.clone().startOf('month');
        let reportFirstMonday = firstDayOfCurrentMonth.clone();
        while (reportFirstMonday.day() !== 1) { // 1 is Monday
            reportFirstMonday.subtract(1, 'day'); // Go to the Monday of or before the 1st of the month
        }
        reportFirstMonday = reportFirstMonday.startOf('day');

        let reportLastSunday = today.clone().endOf('day'); // Report up to "today" (the first Sunday)

        let from_date_db = reportFirstMonday.format("YYYY-MM-DD HH:mm:ss");
        let to_date_db = reportLastSunday.format("YYYY-MM-DD HH:mm:ss");

        let formatted_from_date_display = reportFirstMonday.format("MM-DD-YYYY");
        let formatted_to_date_display = reportLastSunday.format("MM-DD-YYYY");

        let monthName = today.format("MMMM"); // Current month name
        let year = today.format("YYYY");

        for (const client of adminClients) {
            const message = `Dear recipient,\n\nAttached you will find the monthly Bienestar Community report for ${monthName} ${year}. The report covers the period from ${formatted_from_date_display} to ${formatted_to_date_display}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;
            const subject = `Monthly Bienestar Community report for ${client.client_name} - ${monthName} ${year}`;

            const excelRawData = await getRawDataExcel(from_date_db, to_date_db, client.client_id);

            if (excelRawData && excelRawData.length > 0) {
                const summaryObject = await getSummaryExcel(
                    lastMonday.format("YYYY-MM-DD"),
                    lastSunday.format("YYYY-MM-DD"),
                    client.client_id,
                    excelRawData);

                if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                    const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));
                    const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, lastMonday.format("YYYY-MM-DD"), lastSunday.format("YYYY-MM-DD"));

                    await email.sendEmailWithExcelAttachment(subject, message, excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, [adminEmail]);
                } else {
                    logger.warn(`No summary data generated for admin client ${client.client_name} (${client.client_id}) for period ${formatted_from_date_display} to ${formatted_to_date_display} (Monthly Admin). Skipping email.`);
                }
            } else {
                logger.warn(`No raw data found for admin client ${client.client_name} (${client.client_id}) for period ${formatted_from_date_display} to ${formatted_to_date_display} (Monthly Admin). Skipping email.`);
            }
        }
    }
});

module.exports = {
    getRawDataExcel,
    getNewRegistrationsExcel,
    getNewRegistrationsWithoutHealthInsuranceExcel,
    getSummaryExcel
};

server.listen(port, () => logger.info(`Server running on port ${port}`));