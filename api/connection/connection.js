// const mysql = require('mysql');
const mysql = require('mysql2');

function boundedPoolInteger(value, fallback, maximum) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 && parsed <= maximum ? parsed : fallback;
}

// Leave room for administration/background jobs on the current 60-connection
// database. These limits are per Node process, not per deployment.
const connectionLimit = boundedPoolInteger(process.env.DB_POOL_CONNECTION_LIMIT, 15, 40);
const queueLimit = boundedPoolInteger(process.env.DB_POOL_QUEUE_LIMIT, 100, 1000);

// const mysqlConnection = mysql.createConnection({
//   host: process.env.DB_HOST,
//   user: process.env.DB_USER,
//   password: process.env.DB_PASSWORD,
//   database: process.env.DB_DATABASE,
//   port: process.env.DB_PORT,
//   multipleStatements: true
// });

const mysqlConnection = mysql.createPool({
  connectionLimit,
  maxIdle: Math.min(connectionLimit, 5),
  idleTimeout: 60000,
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT,
  multipleStatements: true,
  decimalNumbers: true,
  // Increase timeouts for large content operations
  connectTimeout: 60000, // 60 seconds to establish connection
  waitForConnections: true,
  queueLimit
});

// mysqlConnection.connect( err => {
//   if(err){
//     console.log('Error en db: ', err);
//     return;
//   }else{
//     console.log('Db ok');
//   }
// });


mysqlConnection.on("connection", connection => {
  console.log("Database connected!");

  connection.on("error", err => {
    console.error(new Date(), "MySQL error", err.code);
  });
  
  connection.on("close", () => {
    console.log("Database connection closed");
  });
});

module.exports = mysqlConnection;
