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
const {
    isHealthMetricsDriveSyncEnabled,
    syncScheduledDriveCsvsToDrive
} = require('./api/services/healthMetricsDriveSync');
const {
    getRawDataExcel,
    EXCLUDED_REPORT_USER_IDS
} = require('./api/services/rawDataReport');

const XLSX = require('xlsx-js-style');

let isHealthMetricsDriveSyncRunning = false;

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

function getUniqueRawDataRecordsByUserId(records) {
    const uniqueRecordsByUserId = new Map();

    records.forEach(record => {
        const userId = Number(record['User ID']);
        if (Number.isNaN(userId) || uniqueRecordsByUserId.has(userId)) {
            return;
        }

        uniqueRecordsByUserId.set(userId, record);
    });

    return Array.from(uniqueRecordsByUserId.values());
}

function appendExcludedReportUserFilter(query, alias = 'u') {
    if (!Array.isArray(EXCLUDED_REPORT_USER_IDS) || EXCLUDED_REPORT_USER_IDS.length === 0) {
        return query;
    }

    return `${query}\n              AND ${alias}.id NOT IN (${EXCLUDED_REPORT_USER_IDS.map(() => '?').join(',')})`;
}

async function getNewRegistrationsExcel(excelRawData, from_date, to_date) {
    // Leer el Excel buffer
    const workbook = XLSX.read(excelRawData, { type: 'buffer' });
    const worksheet = workbook.Sheets['Raw Data'];
    const jsonData = XLSX.utils.sheet_to_json(worksheet);
    const uniqueJsonData = getUniqueRawDataRecordsByUserId(jsonData);

    if (uniqueJsonData.length === 0) {
        return Buffer.alloc(0);
    }

    // Convertir from_date y to_date a objetos de fecha
    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD").endOf('day');

    // Filtrar las filas según registration_date dentro del rango
    const filteredRecords = uniqueJsonData.filter(record => {
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
    const uniqueJsonData = getUniqueRawDataRecordsByUserId(jsonData);

    if (uniqueJsonData.length === 0) {
        return Buffer.alloc(0);
    }

    // Convertir from_date y to_date a objetos de fecha
    const fromDate = moment(from_date, "YYYY-MM-DD");
    const toDate = moment(to_date, "YYYY-MM-DD").endOf('day');

    // Filtrar las filas sin seguro de salud y con registration_date dentro del rango
    const filteredRecords = uniqueJsonData.filter(record => {
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
    const uniqueRecords = getUniqueRawDataRecordsByUserId(records);

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

    // Overall Summary Calculations aligned with /metrics/participant/register
    let newCount = 0;
    let newHealthPlanYes = 0;
    let newHealthPlanNo = 0;
    let recurringCount = 0; // Initialize recurringCount
    let newUserIds = [];

    // Fetch client locations for filters and table rendering
    let allClientLocations = [];
    try {
        let locationQuery = `SELECT l.id, l.community_city as name FROM location l JOIN client_location cl ON l.id = cl.location_id WHERE cl.client_id = ?`;
        // Exclude Compton (location_id = 32) for Molina (client_id = 2)
        if (parseInt(client_id) === 2) {
            locationQuery += ` AND l.id != 32`;
        }
        locationQuery += ` ORDER BY l.id`;
        [allClientLocations] = await mysqlConnection.promise().query(locationQuery, [client_id]);
    } catch (error) {
        logger.error(`Error fetching locations for client_id ${client_id}:`, error);
    }

    const locationIds = allClientLocations.map(loc => loc.id);
    const locationPlaceholders = locationIds.map(() => '?').join(',');
    const hasLocationFilter = locationIds.length > 0;

    try {
        const newParams = [
            from_date,
            to_date,
            ...EXCLUDED_REPORT_USER_IDS,
            ...(hasLocationFilter ? locationIds : [])
        ];

        const [newRows] = await mysqlConnection.promise().query(
            appendExcludedReportUserFilter(`
            SELECT DISTINCT u.id AS user_id
            FROM user u
            WHERE u.role_id = 5
              AND u.enabled = 'Y'
              AND CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') >= ?
              AND CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
              ${hasLocationFilter ? `AND u.first_location_id IN (${locationPlaceholders})` : ''}
            `),
            newParams
        );

        newUserIds = newRows.map(row => row.user_id);
        newCount = newUserIds.length;
    } catch (error) {
        logger.error(`Error fetching overall new count for client_id ${client_id}:`, error);
    }

    try {
        const recurringParams = [
            from_date,
            to_date,
            ...EXCLUDED_REPORT_USER_IDS,
            ...(hasLocationFilter ? locationIds : []), // db_range.location_id filter
            ...(hasLocationFilter ? locationIds : []), // db_prev_original.location_id filter
            from_date,
            ...(hasLocationFilter ? locationIds : []), // u.first_location_id filter
            from_date
        ];
        const [recurringRows] = await mysqlConnection.promise().query(
            appendExcludedReportUserFilter(`
            SELECT COUNT(DISTINCT u.id) AS recurring
            FROM delivery_beneficiary db_range
            INNER JOIN user u ON db_range.receiving_user_id = u.id
            WHERE 
              CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles') >= ?
              AND CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
              AND u.role_id = 5 AND u.enabled = 'Y'
              ${hasLocationFilter ? `AND db_range.location_id IN (${locationPlaceholders})` : ''}
              AND (
                  EXISTS (
                      SELECT 1
                      FROM delivery_beneficiary db_prev_original
                      WHERE db_prev_original.receiving_user_id = u.id
                        AND CONVERT_TZ(db_prev_original.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles')
                        ${hasLocationFilter ? `AND db_prev_original.location_id IN (${locationPlaceholders})` : ''}
                  )
                  OR
                  (
                      CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
                      ${hasLocationFilter ? `AND u.first_location_id IN (${locationPlaceholders})` : ''}
                      AND NOT EXISTS (
                          SELECT 1
                          FROM delivery_beneficiary db_no_past
                          WHERE db_no_past.receiving_user_id = u.id
                            AND CONVERT_TZ(db_no_past.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
                          )
                      )
                  )
            `),
            recurringParams
        );
        recurringCount = (recurringRows[0] && recurringRows[0].recurring) || 0;
    } catch (error) {
        logger.error(`Error fetching overall recurring count for client_id ${client_id}:`, error);
    }

    // Health plan breakdown scoped to the users counted as "new"
    if (newUserIds.length > 0 && uniqueRecords.length > 0) {
        const newUserIdsSet = new Set(newUserIds);
        uniqueRecords.forEach(record => {
            const recordUserId = Number(record['User ID']);
            if (Number.isNaN(recordUserId)) {
                return;
            }
            if (!newUserIdsSet.has(recordUserId)) {
                return;
            }

            const healthValue = (record['Health Insurance?'] || '').toString().trim().toLowerCase();
            if (healthValue === 'yes') {
                newHealthPlanYes++;
            } else if (healthValue === 'no' || healthValue === '') {
                newHealthPlanNo++;
            }
        });
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

    const newPerLocationMap = {};
    try {
        if (hasLocationFilter) {
            const newPerLocationQuery = `SELECT
                    u.first_location_id AS location_id,
                    COUNT(DISTINCT u.id) AS new_count
                FROM user u
                WHERE u.role_id = 5
                  AND u.enabled = 'Y'
                  AND CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') >= ?
                  AND CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
                  ${EXCLUDED_REPORT_USER_IDS.length > 0 ? `AND u.id NOT IN (${EXCLUDED_REPORT_USER_IDS.map(() => '?').join(',')})` : ''}
                  AND u.first_location_id IN (${locationPlaceholders})
                GROUP BY u.first_location_id;`;

            const [newLocRows] = await mysqlConnection.promise().query(
                newPerLocationQuery,
                [from_date, to_date, ...EXCLUDED_REPORT_USER_IDS, ...locationIds]
            );
            newLocRows.forEach(row => { newPerLocationMap[row.location_id] = row.new_count; });
        }
    } catch (error) {
        logger.error(`Error fetching new per location for client_id ${client_id}:`, error);
    }

    const recurringPerLocationMap = {};
    try {
        if (hasLocationFilter) {
            const recurringPerLocationQuery = `SELECT
                    db_range.location_id,
                    COUNT(DISTINCT u.id) AS recurring_count
                FROM delivery_beneficiary db_range
                INNER JOIN user u ON db_range.receiving_user_id = u.id
                WHERE u.role_id = 5 AND u.enabled = 'Y'
                  AND CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles') >= ?
                  AND CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
                  ${EXCLUDED_REPORT_USER_IDS.length > 0 ? `AND u.id NOT IN (${EXCLUDED_REPORT_USER_IDS.map(() => '?').join(',')})` : ''}
                  AND db_range.location_id IN (${locationPlaceholders})
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM delivery_beneficiary db_prev
                          WHERE db_prev.receiving_user_id = u.id
                            AND CONVERT_TZ(db_prev.creation_date, '+00:00', 'America/Los_Angeles') < CONVERT_TZ(db_range.creation_date, '+00:00', 'America/Los_Angeles')
                            AND db_prev.location_id IN (${locationPlaceholders})
                      )
                      OR
                      (
                          CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
                          AND u.first_location_id IN (${locationPlaceholders})
                          AND NOT EXISTS (
                              SELECT 1
                              FROM delivery_beneficiary db_no_past
                              WHERE db_no_past.receiving_user_id = u.id
                                AND CONVERT_TZ(db_no_past.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)
                          )
                      )
                  )
                GROUP BY db_range.location_id;`;

            const recurringPerLocationParams = [
                from_date,
                to_date,
                ...EXCLUDED_REPORT_USER_IDS,
                ...locationIds, // db_range.location_id
                ...locationIds, // db_prev.location_id
                from_date,
                ...locationIds, // u.first_location_id
                from_date
            ];

            const [recLocRows] = await mysqlConnection.promise().query(
                recurringPerLocationQuery,
                recurringPerLocationParams
            );
            recLocRows.forEach(row => { recurringPerLocationMap[row.location_id] = row.recurring_count; });
        }
    } catch (error) {
        logger.error(`Error fetching recurring per location for client_id ${client_id}:`, error);
    }

    if (!hasLocationFilter) {
        logger.warn(`No locations found for client_id ${client_id}; location breakdown will be empty.`);
    }

    const locationTableRows = [];
    locationTableRows.push({ col1: '', col2: '', col3: '', col4: '', col5: '' });

    let locationRowBuilder;
    if (parseInt(client_id) === 1) {
        locationTableRows.push({ col1: '', col2: 'Location', col3: 'New' });
        locationRowBuilder = (loc, newAtLoc) => ({
            col1: '',
            col2: loc.name,
            col3: newAtLoc
        });
    } else {
        locationTableRows.push({ col1: '', col2: 'Location', col3: 'New', col4: 'Recurring', col5: 'Totals' });
        locationRowBuilder = (loc, newAtLoc, recurringAtLoc, totalAtLoc) => ({
            col1: '',
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
        // Para client_id = 1, 3 columnas (col1 vacia, Location, New)
        allCsvRows.forEach(row => {
            summaryData.push([
                row.col1 !== undefined && row.col1 !== null ? row.col1 : '',
                row.col2 !== undefined && row.col2 !== null ? row.col2 : '',
                row.col3 !== undefined && row.col3 !== null ? row.col3 : 0
            ]);
        });
    } else {
        // Para otros clientes, 5 columnas (col1 vacia, Location, New, Recurring, Totals)
        allCsvRows.forEach(row => {
            summaryData.push([
                row.col1 !== undefined && row.col1 !== null ? row.col1 : '',
                row.col2 !== undefined && row.col2 !== null ? row.col2 : '',
                row.col3 !== undefined && row.col3 !== null ? row.col3 : 0,
                row.col4 !== undefined && row.col4 !== null ? row.col4 : 0,
                row.col5 !== undefined && row.col5 !== null ? row.col5 : 0
            ]);
        });
    }

    const summaryWorksheet = XLSX.utils.aoa_to_sheet(summaryData);

    // Ajustes de estilo y formato para cumplir requisitos visuales
    const setBold = (cellAddress) => {
        if (!summaryWorksheet[cellAddress]) { return; }
        summaryWorksheet[cellAddress].s = {
            font: { bold: true }
        };
    };

    // Bold for Date Range value, first Total (New+Recurring), and YES value (column B)
    setBold('B2'); // Date Range value
    setBold('B4'); // New value
    setBold('B6'); // First Total value
    setBold('B9'); // YES value

    // Calcular filas para la tabla de locations
    const headerRowIndex = summaryPartRows.length + 2; // blank row + header row
    const firstDataRowIndex = headerRowIndex + 1;
    const totalRowIndex = summaryPartRows.length + locationTableRows.length;

    // Encabezados de la tabla de locations en negrita (Location, New, Recurring, Totals)
    if (parseInt(client_id) === 1) {
        setBold(`B${headerRowIndex}`); // Location
        setBold(`C${headerRowIndex}`); // New
    } else {
        setBold(`B${headerRowIndex}`); // Location
        setBold(`C${headerRowIndex}`); // New
        setBold(`D${headerRowIndex}`); // Recurring
        setBold(`E${headerRowIndex}`); // Totals
    }

    // Columna New (column C): valores > 0 en negrita y total de New en negrita
    const newColumnLetter = 'C';
    for (let rowIdx = firstDataRowIndex; rowIdx < totalRowIndex; rowIdx++) {
        const cellAddress = `${newColumnLetter}${rowIdx}`;
        const cell = summaryWorksheet[cellAddress];
        if (cell && Number(cell.v) > 0) {
            setBold(cellAddress);
        }
    }
    setBold(`${newColumnLetter}${totalRowIndex}`);

    // Calcular anchos automáticos de columna basados en el contenido
    const range = XLSX.utils.decode_range(summaryWorksheet['!ref']);
    const colWidths = [];
    
    for (let C = range.s.c; C <= range.e.c; ++C) {
        let maxWidth = 10; // Ancho mínimo
        for (let R = range.s.r; R <= range.e.r; ++R) {
            const cellAddress = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = summaryWorksheet[cellAddress];
            if (cell && cell.v) {
                const cellValue = cell.v.toString();
                const cellWidth = cellValue.length + 2; // Agregar padding
                if (cellWidth > maxWidth) {
                    maxWidth = cellWidth;
                }
            }
        }
        colWidths.push({ wch: maxWidth });
    }
    
    summaryWorksheet['!cols'] = colWidths;

    XLSX.utils.book_append_sheet(summaryWorkbook, summaryWorksheet, 'Summary');

    const excelBuffer = XLSX.write(summaryWorkbook, { bookType: 'xlsx', type: 'buffer', cellStyles: true });

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
            const summaryFromDate = firstMonday.format("YYYY-MM-DD");
            const summaryToDate = lastSunday.format("YYYY-MM-DD");

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
                        summaryFromDate,
                        summaryToDate,
                        clientId,
                        excelRawData);

                    if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                        const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, summaryFromDate, summaryToDate);
                        const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, summaryFromDate, summaryToDate);

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
        const summaryFromDate = reportFirstMonday.format("YYYY-MM-DD");
        const summaryToDate = reportLastSunday.format("YYYY-MM-DD");

        for (const client of adminClients) {
            const message = `Dear recipient,\n\nAttached you will find the monthly Bienestar Community report for ${monthName} ${year}. The report covers the period from ${formatted_from_date_display} to ${formatted_to_date_display}. The file is password protected.\n\nBest regards,\nBienestar Community Team`;
            const subject = `Monthly Bienestar Community report for ${client.client_name} - ${monthName} ${year}`;

            const excelRawData = await getRawDataExcel(from_date_db, to_date_db, client.client_id);

            if (excelRawData && excelRawData.length > 0) {
                const summaryObject = await getSummaryExcel(
                    summaryFromDate,
                    summaryToDate,
                    client.client_id,
                    excelRawData);

                if (summaryObject && summaryObject.excelBuffer && summaryObject.emailReportData) {
                    const excelNewRegistrations = await getNewRegistrationsWithoutHealthInsuranceExcel(excelRawData, summaryFromDate, summaryToDate);
                    const excelAllNewRegistrations = await getNewRegistrationsExcel(excelRawData, summaryFromDate, summaryToDate);

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

const healthMetricsDriveSyncRule = new RecurrenceRule();
healthMetricsDriveSyncRule.hour = [12, 18];
healthMetricsDriveSyncRule.minute = 0;
healthMetricsDriveSyncRule.tz = 'America/Los_Angeles';

schedule.scheduleJob(healthMetricsDriveSyncRule, async () => {
    if (!isHealthMetricsDriveSyncEnabled()) {
        logger.info('Scheduled Drive CSV sync is disabled. Skipping scheduled execution.');
        return;
    }

    if (isHealthMetricsDriveSyncRunning) {
        logger.warn('Scheduled Drive CSV sync is already running. Skipping overlapping execution.');
        return;
    }

    isHealthMetricsDriveSyncRunning = true;
    try {
        const result = await syncScheduledDriveCsvsToDrive();
        if (!result.skipped) {
            logger.info(`Scheduled Drive CSV sync finished. synced=${result.results.length}, errors=${result.errors.length}`);
        }
    } catch (error) {
        logger.error(`Scheduled Drive CSV sync failed: ${error.message}`);
    } finally {
        isHealthMetricsDriveSyncRunning = false;
    }
});

module.exports = {
    getRawDataExcel,
    getNewRegistrationsExcel,
    getNewRegistrationsWithoutHealthInsuranceExcel,
    getSummaryExcel
};

server.listen(port, () => logger.info(`Server running on port ${port}`));
