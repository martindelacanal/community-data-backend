// const mysql = require('mysql');
const mysql = require('mysql2');

// const mysqlConnection = mysql.createConnection({
//   host: process.env.DB_HOST,
//   user: process.env.DB_USER,
//   password: process.env.DB_PASSWORD,
//   database: process.env.DB_DATABASE,
//   port: process.env.DB_PORT,
//   multipleStatements: true
// });

const mysqlConnection = mysql.createPool({
  connectionLimit : 1000,
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT,
  multipleStatements: true,
  decimalNumbers: true
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

    connection.on("close", err => {
        console.error(new Date(), "MySQL close", err);
    });
});

module.exports = mysqlConnection;