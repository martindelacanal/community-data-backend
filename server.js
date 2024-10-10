const http = require('http');
const app = require('./app');

const port = process.env.PORT || 3000;

const server = http.createServer(app);
const mysqlConnection = require('./api/connection/connection.js');
const schedule = require('node-schedule');

const logger = require('./api/utils/logger.js');
// const email = require('./api/email/email.js');

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

// schedule.scheduleJob('* * * * *', async () => { // Se ejecuta cada minuto
//     const csvData = [{ test: 'Prueba' }];
//     const password = 'test';
//     await email.sendEmailWithAttachment("Prueba", "Esto es una prueba", csvData, password);
// });

server.listen(port, () => logger.info(`Server running on port ${port}`));