const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const cors = require('cors');

// const trustProxySetting = process.env.EXPRESS_TRUST_PROXY || 'loopback, linklocal, uniquelocal';

// app.set('trust proxy', trustProxySetting);
app.set('trust proxy', 1);
app.use(bodyParser.urlencoded({extended: false}));
app.use(bodyParser.json());

// const whitelist = ['http://localhost:4200', 'http://smart-lab-frontend.s3-website-sa-east-1.amazonaws.com'];
// app.use(cors({origin: whitelist}));
app.use(cors()); // CORS HABILITADOS PARA TODOS
// app.use(express.static('./api/public/uploads'));
// app.use(express.static('./api/public/imagenes'));

require('dotenv').config({path: './.env'}); // variables de entorno

// ROUTES

const { createRestoreCredentialsRouter } = require('./api/routes/restoreCredentials');
app.use('/api/auth/restore', createRestoreCredentialsRouter({
  pool: require('./api/connection/connection').promise(),
  logger: require('./api/utils/logger'),
}));

const userRoute = require('./api/routes/user');
app.use('/api',userRoute);

const alertsRoute = require('./api/routes/alerts');
app.use('/api',alertsRoute);

const pushNotificationsRoute = require('./api/routes/pushNotifications');
app.use('/api', pushNotificationsRoute);

const healthEventsRoute = require('./api/routes/healthEvents');
app.use('/api', healthEventsRoute);

module.exports = app;
