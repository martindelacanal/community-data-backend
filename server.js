const http = require('http');
const app = require('./app');

const port = process.env.PORT || 3000;

const server = http.createServer(app);
const mysqlConnection = require('./api/connection/connection.js');

const logger = require('./api/utils/logger.js');


server.listen(port, () => logger.info(`Server running on port ${port}`));