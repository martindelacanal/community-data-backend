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

const { createClinicalSandboxRouter } = require('./api/routes/clinicalSandbox');
app.use('/api/clinical-sandbox', createClinicalSandboxRouter());

const { createRestoreCredentialsRouter } = require('./api/routes/restoreCredentials');
app.use('/api/auth/restore', createRestoreCredentialsRouter({
  pool: require('./api/connection/connection').promise(),
  logger: require('./api/utils/logger'),
}));

const recoveryPool = require('./api/connection/connection').promise();
const recoveryLogger = require('./api/utils/logger');
const { createStaffPasswordRecovery } = require('./api/services/staffPasswordRecovery');
const staffRecovery = createStaffPasswordRecovery({
  pool: recoveryPool,
  sendEmail: require('./api/email/email').sendStaffPasswordRecoveryEmail,
  secret: process.env.JWT_SECRET,
  logger: recoveryLogger,
});
// Share the mailbox quota between the common popup and existing emailed links.
app.put('/api/beneficiary/reset-password', require('./api/routes/passwordRecovery').createPasswordRecoveryHandler({
  pool: recoveryPool, bcrypt: require('bcryptjs'), logger: recoveryLogger, staffRecovery,
}));
const { createStaffPasswordRecoveryRouter } = require('./api/routes/staffPasswordRecovery');
app.use('/api/auth/password-reset', createStaffPasswordRecoveryRouter({
  service: staffRecovery, logger: recoveryLogger,
}));

const userRoute = require('./api/routes/user');
app.use('/api',userRoute);

const { createTicketAnalysisRouter } = require('./api/routes/ticketAnalysis');
app.use('/api', createTicketAnalysisRouter({
  pool: require('./api/connection/connection').promise(),
  logger: require('./api/utils/logger'),
}));

const alertsRoute = require('./api/routes/alerts');
app.use('/api',alertsRoute);

const pushNotificationsRoute = require('./api/routes/pushNotifications');
app.use('/api', pushNotificationsRoute);

const healthEventsRoute = require('./api/routes/healthEvents');
app.use('/api', healthEventsRoute);

const { createChatbotRouter } = require('./api/routes/chatbot');
app.use('/api/chatbot', createChatbotRouter({
  pool: require('./api/connection/connection').promise(),
  logger: require('./api/utils/logger'),
}));

module.exports = app;
