const { createLogger, format, transports } = require('winston');

const logger = createLogger({
  format: format.combine(
    format.simple(),
    format.timestamp(),
    format.printf(info => `[${info.timestamp}] ${info.level}: ${info.message}`)
  ),
  transports: [
    new transports.File({
      filename: 'logs/info.log',
      level: 'info',
      maxFiles: 5,
      maxsize: 5242880
    }),
    new transports.File({
      filename: 'logs/error.log',
      level: 'error',
      maxFiles: 5,
      maxsize: 5242880
    }),
    new transports.Console({
      level: 'debug'
    })
  ]
});

module.exports = logger;

