const express = require('express');
const router = express.Router();
const mysqlConnection = require('../connection/connection');
const jwt = require('jsonwebtoken');
const bcryptjs = require('bcryptjs');
const axios = require('axios');
const logger = require('../utils/logger.js');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;

// S3 INICIO
const S3Client = require("@aws-sdk/client-s3").S3Client;
const PutObjectCommand = require("@aws-sdk/client-s3").PutObjectCommand;
const GetObjectCommand = require("@aws-sdk/client-s3").GetObjectCommand;
const { DeleteObjectCommand } = require("@aws-sdk/client-s3");
const { DeleteObjectsCommand } = require("@aws-sdk/client-s3");
const getSignedUrl = require("@aws-sdk/s3-request-presigner").getSignedUrl;

const bucketName = process.env.BUCKET_NAME;
const bucketRegion = process.env.BUCKET_REGION;
const accessKey = process.env.ACCESS_KEY;
const secretAccessKey = process.env.SECRET_ACCESS_KEY;

const crypto = require("crypto");
const randomImageName = (bytes = 32) =>
  crypto.randomBytes(bytes).toString("hex");

const s3 = new S3Client({
  credentials: {
    accessKeyId: accessKey,
    secretAccessKey: secretAccessKey,
  },
  region: bucketRegion,
});
// S3 FIN

router.get('/ping', (req, res) => {
  res.status(200).send();
});

router.post('/signin', (req, res) => {

  const email = req.body.email || null;
  const password = req.body.password || null;
  const remember = req.body.remember || null;
  console.log(req.body);

  mysqlConnection.query('SELECT user.id, \
                                user.firstname, \
                                user.username, \
                                user.email, \
                                user.password, \
                                user.client_id as client_id, \
                                user.reset_password as reset_password, \
                                role.name AS role, \
                                user.enabled as enabled\
                                FROM user \
                                INNER JOIN role ON role.id = user.role_id \
                                WHERE user.email = ? or user.username = ?',
    [email, email],
    async (err, rows, fields) => {
      if (!err) {
        console.log(rows);
        if (rows.length > 0 && await bcryptjs.compare(password, rows[0].password) && rows[0].enabled == 'Y') {
          const reset_password = rows[0].reset_password;
          delete rows[0].reset_password;
          delete rows[0].password;
          let data = JSON.stringify(rows[0]);
          console.log("los datos del token son: " + data);
          if (remember) {
            jwt.sign({ data }, process.env.JWT_SECRET, { expiresIn: '7d' }, (err, token) => {
              logger.info(`user id: ${rows[0].id} logueado`);
              res.status(200).json({ token: token, reset_password: reset_password });
            });
          } else {
            jwt.sign({ data }, process.env.JWT_SECRET, { expiresIn: '8h' }, (err, token) => {
              logger.info(`user id: ${rows[0].id} logueado`);
              res.status(200).json({ token: token, reset_password: reset_password });
            });
          }
        } else {
          logger.info(`user ${email} no logueado`);
          res.status(401).send();
        }
      } else {
        logger.error(err);
        console.log(err);
        res.status(500).send();
      }
    }
  )
})

router.post('/signup', async (req, res) => {
  // const cabecera = JSON.parse(req.data.data);
  console.log(req.body);

  firstForm = req.body.firstForm;
  secondForm = req.body.secondForm;

  const client_id = 1; // TO-DO obtener client_id de la cabecera

  const role_id = 5;
  const username = firstForm.username || null;
  let passwordHash = await bcryptjs.hash(firstForm.password, 8);
  const firstname = firstForm.firstName || null;
  const lastname = firstForm.lastName || null;
  const dateOfBirth = firstForm.dateOfBirth || null;
  const email = firstForm.email || null;
  const phone = firstForm.phone.toString() || null;
  const zipcode = firstForm.zipcode.toString() || null;
  const householdSize = firstForm.householdSize || null;
  const gender = firstForm.gender || null;
  const ethnicity = firstForm.ethnicity || null;
  const otherEthnicity = firstForm.otherEthnicity || null;

  try {
    const [rows] = await mysqlConnection.promise().query('insert into user(username, \
                                                          password, \
                                                          email, \
                                                          role_id, \
                                                          client_id, \
                                                          firstname, \
                                                          lastname, \
                                                          date_of_birth, \
                                                          phone, \
                                                          zipcode, \
                                                          household_size, \
                                                          gender_id, \
                                                          ethnicity_id, \
                                                          other_ethnicity) \
                                                          values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [username, passwordHash, email, role_id, client_id, firstname, lastname, dateOfBirth, phone, zipcode, householdSize, gender, ethnicity, otherEthnicity]);
    if (rows.affectedRows > 0) {
      // save inserted user id
      const user_id = rows.insertId;
      console.log("user_id: ", user_id);
      console.log("secondForm: ", secondForm);
      // insert user_question, iterate array of questions and insert each question with its answer
      for (let i = 0; i < secondForm.length; i++) {
        const question_id = secondForm[i].question_id;
        const answer_type_id = secondForm[i].answer_type_id;
        const answer = secondForm[i].answer;
        var user_question_id = null;
        if (answer) {
          switch (answer_type_id) {
            case 1: // texto
              const [rows] = await mysqlConnection.promise().query('insert into user_question(user_id, question_id, answer_type_id, answer_text) values(?,?,?,?)',
                [user_id, question_id, answer_type_id, answer]);
              break;
            case 2: // numero
              const [rows2] = await mysqlConnection.promise().query('insert into user_question(user_id, question_id, answer_type_id, answer_number) values(?,?,?,?)',
                [user_id, question_id, answer_type_id, answer]);
              break;
            case 3: // opcion simple
              const [rows3] = await mysqlConnection.promise().query('insert into user_question(user_id, question_id, answer_type_id) values(?,?,?)',
                [user_id, question_id, answer_type_id]);
              user_question_id = rows3.insertId;
              const [rows4] = await mysqlConnection.promise().query('insert into user_question_answer(user_question_id, answer_id) values(?,?)',
                [user_question_id, answer]);
              break;
            case 4: // opcion multiple
              if (answer.length > 0) {
                const [rows5] = await mysqlConnection.promise().query('insert into user_question(user_id, question_id, answer_type_id) values(?,?,?)',
                  [user_id, question_id, answer_type_id]);
                user_question_id = rows5.insertId;
                for (let j = 0; j < answer.length; j++) {
                  const answer_id = answer[j];
                  const [rows6] = await mysqlConnection.promise().query('insert into user_question_answer(user_question_id, answer_id) values(?,?)',
                    [user_question_id, answer_id]);
                }
              }
              break;
            default:
              break;
          }
        }
      }
      res.status(200).json('Data inserted successfully');
    } else {
      res.status(500).json('Could not create user');
    }
  } catch (err) {
    console.log(err);
    res.status(500).json('Internal server error');
  }
});

router.put('/admin/reset-password', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);

  if (cabecera.role === 'admin') {
    const { id, password } = req.body;
    if (id && password) {
      let passwordHash = await bcryptjs.hash(password, 8);
      try {
        const [rows] = await mysqlConnection.promise().query(
          `update client set password = '${passwordHash}' where id = '${id}'`
        );
        if (rows.affectedRows > 0) {
          res.json('Contraseña actualizada correctamente');
        } else {
          res.status(500).json('No se pudo actualizar la contraseña');
        }
      } catch (err) {
        throw err;
      }
    } else {
      res.status(400).json('No se ingreso ningun parametro');
    }
  } else {
    res.status(401).send();
  }
});

const multer = require('multer');
const path = require('path');
const uuid = require('uuid');

const storage = multer.memoryStorage();

// Modificar el middleware upload para aceptar un array de archivos
const upload = multer({ storage: storage }).array('ticket[]');
router.post('/upload/ticket', verifyToken, upload, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'stocker') {
    try {
      if (req.files.length > 0) {
        formulario = JSON.parse(req.body.form);

        const donation_id = formulario.donation_id || null;
        const total_weight = formulario.total_weight || null;
        var provider = formulario.provider || null;
        const destination = formulario.destination || null;
        const delivered_by = formulario.delivered_by || null;
        var products = formulario.products || [];
        var date = null;
        if (formulario.date) {
          fecha = new Date(formulario.date);
          // Formatear la fecha en el formato deseado (YYYY-MM-DD)
          date = fecha.toISOString().slice(0, 10);
        }
        console.log("date: " + date);
        console.log(formulario);
        console.log(req.files);
        if (!Number.isInteger(provider)) {
          const [rows] = await mysqlConnection.promise().query(
            'insert into provider(name) values(?)',
            [provider]
          );
          provider = rows.insertId;
          // insertar en stocker_log la operation 5 (create), el provider insertado y el id del usuario logueado
          const [rows2] = await mysqlConnection.promise().query(
            'insert into stocker_log(user_id, operation_id, provider_id) values(?,?,?)',
            [cabecera.id, 5, provider]
          );
        }
        // iterar el array de objetos products (product,quantity) y si product no es un integer, entonces es un string con el nombre del producto nuevo, debe insertarse en tabla Products y obtener el id para reemplazarlo en el objeto en el campo product en la posicion i
        for (let i = 0; i < products.length; i++) {
          if (!Number.isInteger(products[i].product)) {
            const [rows] = await mysqlConnection.promise().query(
              'insert into product(name) values(?)',
              [products[i].product]
            );
            products[i].product = rows.insertId;
            // insertar en stocker_log la operation 5 (create), el product insertado y el id del usuario logueado
            const [rows2] = await mysqlConnection.promise().query(
              'insert into stocker_log(user_id, operation_id, product_id) values(?,?,?)',
              [cabecera.id, 5, products[i].product]
            );
          }
        }
        const [rows] = await mysqlConnection.promise().query(
          'insert into donation_ticket(client_id, donation_id, total_weight, provider_id, location_id, date, delivered_by) values(?,?,?,?,?,?,?)',
          [cabecera.client_id, donation_id, total_weight, provider, destination, date, delivered_by]
        );

        if (rows.affectedRows > 0) {
          const donation_ticket_id = rows.insertId;
          try {
            for (let i = 0; i < products.length; i++) {
              await mysqlConnection.promise().query(
                'insert into product_donation_ticket(product_id, donation_ticket_id, quantity) values(?,?,?)',
                [products[i].product, donation_ticket_id, products[i].quantity]
              );
            }
          } catch (error) {
            console.log(error);
            logger.error(error);
            res.status(500).json('Could not create product_donation_ticket');
          }
          try {
            for (let i = 0; i < req.files.length; i++) {
              // renombrar cada archivo con un nombre aleatorio
              req.files[i].filename = randomImageName();
              const paramsLogo = {
                Bucket: bucketName,
                Key: req.files[i].filename,
                Body: req.files[i].buffer,
                ContentType: req.files[i].mimetype,
              };
              const commandLogo = new PutObjectCommand(paramsLogo);
              const uploadLogo = await s3.send(commandLogo);
              await mysqlConnection.promise().query(
                'insert into donation_ticket_image(donation_ticket_id, file) values(?,?)',
                [donation_ticket_id, req.files[i].filename]
              );
            }
          } catch (error) {
            console.log(error);
            logger.error(error);
            res.status(500).json('Could not upload image');
          }
          try {
            // insertar en stocker_log la operation 5 (create), el ticket insertado y el id del usuario logueado
            const [rows2] = await mysqlConnection.promise().query(
              'insert into stocker_log(user_id, operation_id, donation_ticket_id) values(?,?,?)',
              [cabecera.id, 5, donation_ticket_id]
            );
          } catch (error) {
            console.log(error);
            logger.error(error);
            res.status(500).json('Could not create stocker_log');
          }
        } else {
          res.status(500).json('Not ticket inserted');
        }
        res.status(200).json('Data inserted successfully');
      } else {
        res.status(400).json('Donation ticket image is required');
      }
    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.put('/beneficiary/reset-password', async (req, res) => {

  try {
    const { email } = req.body;
    const { dateOfBirth } = req.body;

    const [rows] = await mysqlConnection.promise().query('SELECT user.id \
                                                          FROM user \
                                                          INNER JOIN role ON role.id = user.role_id \
                                                          WHERE (user.email = ? or user.username = ?) and user.date_of_birth = ?',
      [email, email, dateOfBirth]);

    if (rows.length > 0) {

      // const newPassword = Math.random().toString(36).slice(-8);
      const newPassword = 'communitydata';
      let passwordHash = await bcryptjs.hash(newPassword, 8);

      const [rows2] = await mysqlConnection.promise().query('update user set password = ?, reset_password = "Y" where id = ?',
        [passwordHash, rows[0].id]);

      if (rows2.affectedRows > 0) {
        res.json({ password: newPassword });
      } else {
        res.status(500).json('Could not update password');
      }

    } else {
      res.status(401).json('Unauthorized');
    }
  } catch (error) {
    console.log(error);
    logger.error(error);
    res.status(500).json('Internal server error');
  }
});

router.put('/change-password/:idUser', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client' || cabecera.role === 'stocker' || cabecera.role === 'delivery' || cabecera.role === 'beneficiary') {
    try {
      const { idUser } = req.params;
      const { password } = req.body;

      let passwordHash = await bcryptjs.hash(password, 8);

      const [rows] = await mysqlConnection.promise().query('update user set password = ?, reset_password = "N" where id = ?',
        [passwordHash, idUser]);

      if (rows.affectedRows > 0) {
        res.json('Password updated successfully');
      } else {
        res.status(500).json('Could not update password');
      }
    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.delete('/user/reset-password/:idUser', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin') {
    try {
      const { idUser } = req.params;

      // const newPassword = Math.random().toString(36).slice(-8);
      const newPassword = 'communitydata';
      let passwordHash = await bcryptjs.hash(newPassword, 8);

      const [rows2] = await mysqlConnection.promise().query('update user set password = ?, reset_password = "Y" where id = ?',
        [passwordHash, idUser]);

      if (rows2.affectedRows > 0) {
        res.json({ password: newPassword });
      } else {
        res.status(500).json('Could not update password');
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.post('/new/user', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin') {
    try {
      console.log("entre a new user")
      formulario = req.body;
      console.log("formulario: ", formulario);
      const username = formulario.username || null;
      const password = formulario.password || null;
      const email = formulario.email || null;
      const firstname = formulario.firstname || null;
      const lastname = formulario.lastname || null;
      const date_of_birth = formulario.date_of_birth || null;
      const gender_id = formulario.gender_id || null;
      const role_id = formulario.role_id || null;
      
      // const newPassword = Math.random().toString(36).slice(-8);
      var newPassword = 'communitydata';
      if (password) {
        newPassword = password;
      }
      let passwordHash = await bcryptjs.hash(newPassword, 8);
      var reset_password = "Y";
      const [rows2] = await mysqlConnection.promise().query(
        'insert into user (username, email, firstname, lastname, date_of_birth, password, reset_password, gender_id, role_id) values(?,?,?,?,?,?,?,?,?)',
        [username, email, firstname, lastname, date_of_birth, passwordHash, reset_password, gender_id, role_id]
      );

      if (rows2.affectedRows > 0) {
        res.json({ password: newPassword });
      } else {
        res.status(500).json('Could not create user');
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.post('/upload/beneficiaryQR/:locationId', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'delivery') {
    if (req.body && req.body.role === 'beneficiary') {
      try {
        const client_id = cabecera.client_id;
        const delivering_user_id = cabecera.id;
        // QR
        const receiving_user_id = req.body.id;
        const approved = req.body.approved;
        const location_id = parseInt(req.params.locationId) || null;
        // buscar en tabla delivery_beneficiary si existe un registro con client_id, delivering_user_id, receiving_user_id y location_id en el dia de hoy y filtrar el más reciente
        const [rows] = await mysqlConnection.promise().query(
          'select id, approved \
          from delivery_beneficiary \
          where client_id = ? and delivering_user_id = ? and receiving_user_id = ? and location_id = ? and date(creation_date) = curdate() \
          order by creation_date desc limit 1',
          [client_id, delivering_user_id, receiving_user_id, location_id]
        );

        if (rows.length > 0 && rows[0].approved === 'N') {
          if (approved === 'Y') {
            // actualizar el campo approved a 'Y'
            const [rows2] = await mysqlConnection.promise().query(
              'update delivery_beneficiary set approved = "Y" where id = ?', [rows[0].id]
            );
            if (rows2.affectedRows > 0) {
              res.json('Beneficiary approved successfully');
            } else {
              res.status(500).json('Could not approve beneficiary');
            }
          } else {
            res.json({ could_approve: 'Y' });
          }
        } else {
          // TO-DO verificar si el beneficiary esta apto para recibir la entrega, sino enviar un 'N'

          // actualizar location_id del user beneficiary
          const [rows2] = await mysqlConnection.promise().query(
            'update user set location_id = ? where id = ?', [location_id, receiving_user_id]
          );
          // insertar en tabla delivery_beneficiary
          const [rows3] = await mysqlConnection.promise().query(
            'insert into delivery_beneficiary(client_id, delivering_user_id, receiving_user_id, location_id) values(?,?,?,?)',
            [client_id, delivering_user_id, receiving_user_id, location_id]
          );
          if (rows3.affectedRows > 0) {
            res.status(200).json({ could_approve: 'Y' });
          } else {
            res.status(500).json('Could not create delivery_beneficiary');
          }
        }
      } catch (error) {
        console.log(error);
        logger.error(error);
        res.status(500).json('Internal server error');
      }
    } else {
      console.log(req.body);
      res.status(401).json('Unauthorized');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/locations', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'stocker' || cabecera.role === 'delivery') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        'select id,organization,community_city,address from location where enabled = "Y" and client_id = ? order by community_city',
        [cabecera.client_id]
      );
      res.json(rows);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/providers', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'stocker') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        'select id,name from provider order by name',
      );
      res.json(rows);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/products', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'stocker') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        'select id,name from product order by name',
      );
      res.json(rows);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/roles', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        'select id,name \
        from role \
        where name != "beneficiary" \
        order by name',
      );
      res.json(rows);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

// get value (true or false) from body and update user_status_id of user. If true update to 3, if false update to 2
router.post('/onBoard', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'delivery') {
    try {
      const user_id = cabecera.id;
      const { value } = req.body;
      const user_status_id = value ? 3 : 4;
      const location_id = value ? req.body.location_id : null;
      const [rows] = await mysqlConnection.promise().query(
        'update user set user_status_id = ?, location_id = ? where id = ?',
        [user_status_id, location_id, user_id]
      );
      // insertar en tabla delivery_log la operation
      const [rows2] = await mysqlConnection.promise().query(
        'insert into delivery_log(user_id, operation_id, location_id) values(?,?,?)',
        [user_id, user_status_id, location_id]
      );

      if (rows.affectedRows > 0) {
        res.json('Status updated successfully');
      } else {
        res.status(500).json('Could not update status');
      }
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

// get status of user, inner join with user_status table and return id and name
router.get('/user/status', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'delivery') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        'select user_status.id, user_status.name \
        from user \
        inner join user_status on user.user_status_id = user_status.id \
        where user.id = ?',
        [cabecera.id]
      );
      if (rows.length > 0) {
        if (rows[0].id === 3) { //verificar si no pasaron 8hs desde que se acualizo el status a 3 usando tabla delivery_log
          const [rows2] = await mysqlConnection.promise().query(
            'select * from delivery_log where user_id = ? and operation_id = 3 order by creation_date desc limit 1',
            [cabecera.id]
          );
          if (rows2.length > 0) {
            const fecha = new Date(rows2[0].creation_date);
            const fechaActual = new Date();
            const diff = fechaActual.getTime() - fecha.getTime();
            const hours = Math.floor(diff / (1000 * 60 * 60));
            // const minutes = Math.floor(diff / (1000 * 60));
            if (hours >= 8) { // si pasaron 8hs, insertar en delivery_log con operation_id 4, actualizar status de user a 4 y su location_id a null
              const [rows3] = await mysqlConnection.promise().query(
                'insert into delivery_log(user_id, operation_id) values(?,?)',
                [cabecera.id, 4]
              );
              const [rows4] = await mysqlConnection.promise().query(
                'update user set user_status_id = 4, location_id = null where id = ?',
                [cabecera.id]
              );
              res.json({ id: 4, name: 'Off boarded' });
            } else {
              res.json(rows[0]);
            }
          } else {
            res.json(rows[0]);
          }
        } else {
          res.json(rows[0]);
        }
      } else {
        res.json({ id: null, name: null });
      }
    
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/user/location', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'delivery') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        'select location.id, location.organization, location.address \
        from user \
        inner join location on user.location_id = location.id \
        where user.id = ?',
        [cabecera.id]
      );
      if (rows.length > 0) {
        res.json(rows[0]);
      } else {
        res.json({ id: null, organization: null });
      }
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/userName/exists/search', async (req, res) => {
  const username = req.query.username || null;
  try {
    if (username) {
      const [rows] = await mysqlConnection.promise().query('select username from user where username = ?', [username]);
      if (rows.length > 0) {
        res.json(true);
      } else {
        res.json(false);
      }
    } else {
      res.json(false);
    }
  } catch (err) {
    console.log(err);
    res.status(500).json('Internal server error');
  }
});

router.get('/phone/exists/search', async (req, res) => {
  const phone = req.query.phone || null;
  try {
    if (phone) {
      const [rows] = await mysqlConnection.promise().query('select phone from user where phone = ?', [phone]);
      if (rows.length > 0) {
        res.json(true);
      } else {
        res.json(false);
      }
    } else {
      res.json(false);
    }
  } catch (err) {
    console.log(err);
    res.status(500).json('Internal server error');
  }
});

router.get('/email/exists/search', async (req, res) => {
  const email = req.query.email || null;
  try {
    if (email) {
      const [rows] = await mysqlConnection.promise().query('select email from user where email = ?', [email]);
      if (rows.length > 0) {
        res.json(true);
      } else {
        res.json(false);
      }
    } else {
      res.json(false);
    }
  } catch (err) {
    console.log(err);
    res.status(500).json('Internal server error');
  }
});

router.get('/donation_id/exists/search', async (req, res) => {
  const donation_id = req.query.donation_id || null;
  try {
    if (donation_id) {
      const [rows] = await mysqlConnection.promise().query('select donation_id from donation_ticket where donation_id = ?', [donation_id]);
      if (rows.length > 0) {
        res.json(true);
      } else {
        res.json(false);
      }
    } else {
      res.json(false);
    }
  } catch (err) {
    console.log(err);
    res.status(500).json('Internal server error');
  }
});

router.get('/register/questions', async (req, res) => {
  const language = req.query.language || 'en';
  try {
    // get all questions and answers from table question and answer, if language === 'es' then get name_es, else get name
    const query = `SELECT q.id as question_id, 
                  ${language === 'en' ? 'q.name' : 'q.name_es'} AS question_name, 
                  q.answer_type_id,
                  q.depends_on_question_id,
                  q.depends_on_answer_id,
                  a.id as answer_id, 
                  ${language === 'en' ? 'a.name' : 'a.name_es'} AS answer_name
                  FROM question q
                  LEFT JOIN answer a ON q.id = a.question_id
                  WHERE q.enabled = 'Y'
                  ORDER BY q.id, a.id ASC`;
    const [rows] = await mysqlConnection.promise().query(query);
    var questions = [];
    var question_id = 0;
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (row.question_id !== question_id) {
        questions.push({
          id: row.question_id,
          name: row.question_name,
          depends_on_question_id: row.depends_on_question_id,
          depends_on_answer_id: row.depends_on_answer_id,
          answer_type_id: row.answer_type_id,
          answers: []
        });
        question_id = row.question_id;
      }
      if (row.answer_id) {
        questions[questions.length - 1].answers.push({
          question_id: row.question_id,
          id: row.answer_id,
          name: row.answer_name
        });
      }
    }

    res.json(questions);

  } catch (error) {
    console.log(error);
    res.status(500).json('Internal server error');
  }
});

router.get('/gender', async (req, res) => {
  const id = req.query.id || null;
  const language = req.query.language || 'en';
  try {
    const query = `SELECT id, ${language === 'en' ? 'name' : 'name_es'} AS name 
                  FROM gender ${id ? ' WHERE id = ?' : ''} ORDER BY name`;
    const params = id ? [id] : [];
    const [rows] = await mysqlConnection.promise().query(query, params);
    res.json(rows);
  } catch (error) {
    console.log(error);
    res.status(500).json('Internal server error');
  }
});

router.get('/ethnicity', async (req, res) => {
  const id = req.query.id || null;
  const language = req.query.language || 'en';
  try {
    const query = `SELECT id, ${language === 'en' ? 'name' : 'name_es'} AS name 
                  FROM ethnicity ${id ? ' WHERE id = ?' : ''} ORDER BY name`;
    const params = id ? [id] : [];
    const [rows] = await mysqlConnection.promise().query(query, params);
    res.json(rows);
  } catch (error) {
    console.log(error);
    res.status(500).json('Internal server error');
  }
});

router.get('/pounds-delivered', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      // sum total_weight from donation_ticket, if cabecera.role === 'client' then sum only donations from client_id (cabecera.client_id)
      const [rows] = await mysqlConnection.promise().query(
        `select sum(total_weight) as pounds_delivered from donation_ticket
        ${cabecera.role === 'client' ? 'where client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      if (rows[0].pounds_delivered === null) {
        rows[0].pounds_delivered = 0;
      }
      res.json(rows[0].pounds_delivered);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-locations', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      // count location from table location, if cabecera.role === 'client' then sum only locations from client_id (cabecera.client_id)
      const [rows] = await mysqlConnection.promise().query(
        `select count(id) as total_locations from location
        ${cabecera.role === 'client' ? 'where client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      res.json(rows[0].total_locations);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-days-operation', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      // count distinct days from field creation_date in table delivery_beneficiary, if cabecera.role === 'client' then sum only days from client_id (cabecera.client_id)
      const [rows] = await mysqlConnection.promise().query(
        `select count(distinct date(creation_date)) as total_days_operation from delivery_beneficiary
        ${cabecera.role === 'client' ? 'where client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      res.json(rows[0].total_days_operation);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-from-role/:role', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const { role } = req.params;
      if (role === 'beneficiary' || role === 'stocker' || role === 'delivery') {
        // count users with role stocker in table user inner join role, if cabecera.role === 'client' then sum only users from client_id (cabecera.client_id)
        const [rows] = await mysqlConnection.promise().query(
          `select count(user.id) as total from user
        inner join role on user.role_id = role.id
        where role.name = ? ${cabecera.role === 'client' ? 'and user.client_id = ?' : ''}`,
          [role, cabecera.client_id]
        );
        res.json(rows[0].total);
      } else {
        res.status(400).json('Bad request');
      }
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-beneficiaries-served', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      // get percentage of users with role 'beneficiary' that are in table 'delivery_beneficiary' compared to the total of beneficiaries in table user, if cabecera.role === 'client' then sum only users from client_id (cabecera.client_id)
      const [rows] = await mysqlConnection.promise().query(
        `SELECT 
          COUNT(DISTINCT user.id) AS total_beneficiaries_served,
          (SELECT COUNT(*) FROM user WHERE role_id = 5 ${cabecera.role === 'client' ? 'AND user.client_id = ?' : ''}) AS total_beneficiaries
        FROM user
        INNER JOIN delivery_beneficiary ON user.id = delivery_beneficiary.receiving_user_id
        WHERE user.role_id = 5 ${cabecera.role === 'client' ? 'AND user.client_id = ?' : ''} AND delivery_beneficiary.approved = 'Y'`,
        [cabecera.client_id, cabecera.client_id]
      );
      const totalBeneficiariesServed = rows[0].total_beneficiaries_served;
      const totalBeneficiaries = rows[0].total_beneficiaries;
      const percentage = (totalBeneficiariesServed / totalBeneficiaries * 100).toFixed(2);
      res.json(percentage);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

/*
para que el beneficiario (user.role = 5) sea considerado como beneficiario calificado debe cumplir con los siguientes requisitos:
1. haber respondido todas las preguntas de la encuesta de su client_id que tengan el campo enabled = 'Y' (cruce de tablas user, user_question, question)
2. si la pregunta en la tabla question tiene el campo recurrent = 'Y' entonces el campo creation_date de la tabla user_question debe ser mayor al campo begin_date de la pregunta en la tabla question
*/
// TO-DO
router.get('/total-beneficiaries-qualified', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      // const [rows] = await mysqlConnection.promise().query(
      //   `SELECT
      //     COUNT(DISTINCT user.id) AS total_beneficiaries_qualified
      //   FROM user
      //   INNER JOIN user_question ON user.id = user_question.user_id
      //   INNER JOIN question ON user_question.question_id = question.id
      //   WHERE user.role_id = 5
      //   AND user_question.enabled = 'Y'
      //   AND question.enabled = 'Y'
      //   AND (question.recurrent = 'N' OR (question.recurrent = 'Y' AND user_question.creation_date > question.begin_date))
      //   ${cabecera.role === 'client' ? 'AND question.client_id = ? AND user.client_id = ?' : ''}`,
      //   [cabecera.client_id, cabecera.client_id]
      // );
      const [rows] = await mysqlConnection.promise().query(
        `SELECT
          COUNT(DISTINCT user.id) AS total_beneficiaries_qualified
        FROM user
        WHERE user.role_id = 5
        ${cabecera.role === 'client' ? 'AND user.client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      res.json(rows[0].total_beneficiaries_qualified);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-enabled-users', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        `SELECT
          COUNT(DISTINCT user.id) AS total_enabled_users
        FROM user
        WHERE user.enabled = 'Y'
        ${cabecera.role === 'client' ? 'AND user.client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      res.json(rows[0].total_enabled_users);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-tickets-uploaded', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        `SELECT
          COUNT(DISTINCT donation_ticket.id) AS total_tickets_uploaded
        FROM donation_ticket
        ${cabecera.role === 'client' ? 'WHERE donation_ticket.client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      res.json(rows[0].total_tickets_uploaded);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-locations-enabled', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        `SELECT
          COUNT(DISTINCT location.id) AS total_locations_enabled
          FROM location
          WHERE location.enabled = 'Y' ${cabecera.role === 'client' ? 'and location.client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      res.json(rows[0].total_locations_enabled);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-products-uploaded', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        `SELECT
            COUNT(DISTINCT product.id) AS total_products_uploaded
          FROM product`
      );
      res.json(rows[0].total_products_uploaded);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/total-delivered', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        `SELECT
            COUNT(db.id) AS total_delivered
          FROM delivery_beneficiary as db
          WHERE db.approved = 'Y' ${cabecera.role === 'client' ? 'and db.client_id = ?' : ''}`,
      );
      res.json(rows[0].total_delivered);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/map/locations', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const [rows] = await mysqlConnection.promise().query(
        `SELECT
        ST_X(coordinates) as lng, ST_Y(coordinates) as lat, organization as label
        FROM location
        WHERE location.enabled = 'Y'
        ${cabecera.role === 'client' ? 'AND location.client_id = ?' : ''}`,
        [cabecera.client_id]
      );
      const locations = rows.map(row => ({
        position: { lat: row.lat, lng: row.lng },
        label: row.label
      }));
      const center = locations.reduce((acc, curr) => ({
        lat: acc.lat + curr.position.lat,
        lng: acc.lng + curr.position.lng
      }), { lat: 0, lng: 0 });
      center.lat /= locations.length;
      center.lng /= locations.length;
      res.json({ center, locations });
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.get('/dashboard/graphic-line/:tabSelected', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const language = req.query.language || 'en';
      const { tabSelected } = req.params;
      let name = '';
      var rows = [];
      var series = [];
      var isTabSelectedCorrect = false;
      switch (tabSelected) {
        case 'pounds':
          name = 'Pounds';
          if (language === 'es') {
            name = 'Libras';
          }
          [rows] = await mysqlConnection.promise().query(
            `SELECT
              SUM(total_weight) AS value,
              DATE_FORMAT(creation_date, '%Y-%m-%dT%TZ') AS name
            FROM donation_ticket
            ${cabecera.role === 'client' ? 'WHERE client_id = ?' : ''}
            GROUP BY YEAR(creation_date), MONTH(creation_date)
            ORDER BY creation_date`,
            [cabecera.client_id]
          );
          isTabSelectedCorrect = true;
          break;
        case 'beneficiaries':
          name = 'Beneficiaries';
          if (language === 'es') {
            name = 'Beneficiarios';
          }
          [rows] = await mysqlConnection.promise().query(
            `SELECT
              COUNT(DISTINCT user.id) AS value,
              DATE_FORMAT(creation_date, '%Y-%m-%dT%TZ') AS name
            FROM user
            WHERE user.role_id = 5 ${cabecera.role === 'client' ? 'AND user.client_id = ?' : ''}
            GROUP BY YEAR(creation_date), MONTH(creation_date)
            ORDER BY creation_date`,
            [cabecera.client_id]
          );
          isTabSelectedCorrect = true;
          break;
        case 'deliveryPeople':
          name = 'Delivery people';
          if (language === 'es') {
            name = 'Repartidores';
          }
          [rows] = await mysqlConnection.promise().query(
            `SELECT
              COUNT(DISTINCT user.id) AS value,
              DATE_FORMAT(creation_date, '%Y-%m-%dT%TZ') AS name
            FROM user
            WHERE user.role_id = 4 ${cabecera.role === 'client' ? 'AND user.client_id = ?' : ''}
            GROUP BY YEAR(creation_date), MONTH(creation_date)
            ORDER BY creation_date`,
            [cabecera.client_id]
          );
          isTabSelectedCorrect = true;
          break;
        case 'operations':
          name = 'Operations';
          if (language === 'es') {
            name = 'Operaciones';
          }
          [rows] = await mysqlConnection.promise().query(
            `SELECT
              COUNT(DISTINCT delivery_beneficiary.location_id) AS value,
              DATE_FORMAT(creation_date, '%Y-%m-%dT%TZ') AS name
            FROM delivery_beneficiary
            ${cabecera.role === 'client' ? 'WHERE client_id = ?' : ''}
            GROUP BY YEAR(creation_date), MONTH(creation_date)
            ORDER BY creation_date`,
            [cabecera.client_id]
          );
          isTabSelectedCorrect = true;
          break;
        default:
          res.status(400).json('Bad request');
          break;
      }
      if (isTabSelectedCorrect && rows.length > 0) {
        series = rows.map(row => ({
          value: row.value,
          name: row.name
        }));
      }
      res.json({ name, series });
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.post('/message', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client' || cabecera.role === 'delivery' || cabecera.role === 'stocker' || cabecera.role === 'beneficiary') {
    try {
      const user_id = cabecera.id;
      const message = req.body.message || null;

      if (message) {
        const [rows] = await mysqlConnection.promise().query(
          'insert into message(user_id,name) values(? , ?)',
          [user_id, message]
        );
        if (rows.affectedRows > 0) {
          res.json('Message sent successfully');
        } else {
          res.status(500).json('Could not send message');
        }
      } else {
        res.status(400).json('Bad request');
      }
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  } else {
    res.status(401).json('Unauthorized');
  }
});

router.put('/settings/password', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);

  if (cabecera.role === 'admin' || cabecera.role === 'client' || cabecera.role === 'delivery' || cabecera.role === 'stocker' || cabecera.role === 'beneficiary') {
    try {
      const user_id = cabecera.id;
      const { actual_password, new_password } = req.body;

      if (actual_password && new_password) {
        const [rows] = await mysqlConnection.promise().query(
          'select password from user where id = ?', [user_id]
        );

        if (rows.length > 0) {
          const passwordCorrect = await bcryptjs.compare(actual_password, rows[0].password);
          if (passwordCorrect) {
            let passwordHash = await bcryptjs.hash(new_password, 8);
            const [rows2] = await mysqlConnection.promise().query(
              'update user set password = ? where id = ?', [passwordHash, user_id]
            );
            if (rows2.affectedRows > 0) {
              res.json('Password updated successfully');
            } else {
              res.status(500).json('Could not update password');
            }
          } else {
            res.status(401).json('Unauthorized');
          }
        } else {
          res.status(500).json('Could not update password');
        }
      } else {
        res.status(400).json('Bad request');
      }
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  }
});

router.get('/metrics/download-csv', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const from_date = req.query.from_date || '1970-01-01';
      const to_date = req.query.to_date || '2100-01-01';
      console.log("download CSV ticket from_date: " + from_date + " to_date: " + to_date);

      const [rows] = await mysqlConnection.promise().query(
        `SELECT u.username,
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
                loc.community_city AS location,
                DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', '-07:00'), '%m/%d/%Y') AS registration_date,
                DATE_FORMAT(CONVERT_TZ(u.creation_date, '+00:00', '-07:00'), '%T') AS registration_time,
                q.id AS question_id,
                at.id AS answer_type_id,
                q.name AS question,
                a.name AS answer,
                uq.answer_text AS answer_text,
                uq.answer_number AS answer_number
        FROM user u
        INNER JOIN gender AS g ON u.gender_id = g.id
        INNER JOIN ethnicity AS eth ON u.ethnicity_id = eth.id
        LEFT JOIN location AS loc ON u.location_id = loc.id
        CROSS JOIN question AS q
        LEFT JOIN answer_type as at ON q.answer_type_id = at.id
        LEFT JOIN user_question AS uq ON u.id = uq.user_id AND uq.question_id = q.id
        LEFT JOIN user_question_answer AS uqa ON uq.id = uqa.user_question_id
        left join answer as a ON a.id = uqa.answer_id and a.question_id = q.id
        WHERE u.role_id = 5 AND q.enabled = 'Y' AND CONVERT_TZ(u.creation_date, '+00:00', '-07:00') >= ? AND CONVERT_TZ(u.creation_date, '+00:00', '-07:00') < DATE_ADD(?, INTERVAL 1 DAY)
        ${cabecera.role === 'client' ? 'and u.client_id = ?' : ''}
        order by u.id, q.id, a.id`,
        [from_date, to_date, cabecera.client_id]
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
          row_filtered["location"] = rows[i].location;
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
        { id: 'location', title: 'Location' },
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

      res.setHeader('Content-disposition', 'attachment; filename=results-beneficiary-form.csv');
      res.setHeader('Content-type', 'text/csv; charset=utf-8');
      res.send(csvData);

    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  }
}
);

router.get('/table/delivered/download-csv', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin') {
    try {
      const from_date = req.query.from_date || '1970-01-01';
      const to_date = req.query.to_date || '2100-01-01';
      console.log("download CSV delivered from_date: " + from_date + " to_date: " + to_date);

      const [rows] = await mysqlConnection.promise().query(
        `SELECT db.id, 
        db.delivering_user_id, 
        u1.username as delivery_username, 
        db.receiving_user_id, 
        u2.username as beneficiary_username, 
        u2.firstname as beneficiary_firstname, 
        u2.lastname as beneficiary_lastname, 
        db.location_id, 
        l.community_city, 
        db.approved, 
        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', '-07:00'), '%m/%d/%Y') AS creation_date,
                        DATE_FORMAT(CONVERT_TZ(db.creation_date, '+00:00', '-07:00'), '%T') AS creation_time
        FROM delivery_beneficiary as db
        INNER JOIN user as u1 ON db.delivering_user_id = u1.id
        INNER JOIN user as u2 ON db.receiving_user_id = u2.id
        INNER JOIN location as l ON db.location_id = l.id
        WHERE CONVERT_TZ(db.creation_date, '+00:00', '-07:00') >= ? AND CONVERT_TZ(db.creation_date, '+00:00', '-07:00') < DATE_ADD(?, INTERVAL 1 DAY)
        ORDER BY db.id`,
        [from_date, to_date]
      );

      var headers_array = [
        { id: 'id', title: 'ID' },
        { id: 'delivering_user_id', title: 'Delivering user ID' },
        { id: 'delivery_username', title: 'Delivery username' },
        { id: 'receiving_user_id', title: 'Receiving user ID' },
        { id: 'beneficiary_username', title: 'Beneficiary username' },
        { id: 'beneficiary_firstname', title: 'Beneficiary firstname' },
        { id: 'beneficiary_lastname', title: 'Beneficiary lastname' },
        { id: 'location_id', title: 'Location ID' },
        { id: 'community_city', title: 'Community city' },
        { id: 'approved', title: 'Approved' },
        { id: 'creation_date', title: 'Creation date' },
        { id: 'creation_time', title: 'Creation time' }
      ];

      const csvStringifier = createCsvStringifier({
        header: headers_array,
        fieldDelimiter: ';'
      });

      let csvData = csvStringifier.getHeaderString();
      csvData += csvStringifier.stringifyRecords(rows);

      res.setHeader('Content-disposition', 'attachment; filename=results-beneficiary-form.csv');
      res.setHeader('Content-type', 'text/csv; charset=utf-8');
      res.send(csvData);

    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  }
}
);

router.get('/table/ticket/download-csv', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin') {
    try {
      const from_date = req.query.from_date || '1970-01-01';
      const to_date = req.query.to_date || '2100-01-01';
      console.log("download CSV ticket from_date: " + from_date + " to_date: " + to_date);

      const [rows] = await mysqlConnection.promise().query(
        `SELECT dt.id,
                dt.donation_id,
                dt.total_weight,
                p.name as provider,
                loc.community_city as location,
                DATE_FORMAT(dt.date, '%m/%d/%Y') as date,
                dt.delivered_by,
                u.id as created_by_id,
                u.username as created_by_username,
                DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', '-07:00'), '%m/%d/%Y') AS creation_date,
                DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', '-07:00'), '%T') AS creation_time,
                product.id as product_id,
                product.name as product,
                pdt.quantity as quantity
        FROM donation_ticket as dt
        INNER JOIN provider as p ON dt.provider_id = p.id
        INNER JOIN location as loc ON dt.location_id = loc.id
        INNER JOIN stocker_log as sl ON dt.id = sl.donation_ticket_id
        INNER JOIN user as u ON sl.user_id = u.id
        INNER JOIN product_donation_ticket as pdt ON dt.id = pdt.donation_ticket_id
        INNER JOIN product as product ON pdt.product_id = product.id
        WHERE dt.date >= ? AND dt.date < DATE_ADD(?, INTERVAL 1 DAY)
        ORDER BY dt.date, dt.id`,
        [from_date, to_date]
        );
        // WHERE CONVERT_TZ(dt.creation_date, '+00:00', '-07:00') >= ? AND CONVERT_TZ(dt.creation_date, '+00:00', '-07:00') < DATE_ADD(?, INTERVAL 1 DAY)

      var headers_array = [
        { id: 'id', title: 'ID' },
        { id: 'donation_id', title: 'Donation ID' },
        { id: 'total_weight', title: 'Total weight' },
        { id: 'location', title: 'Location' },
        { id: 'date', title: 'Date' },
        { id: 'delivered_by', title: 'Delivered by' },
        { id: 'created_by_id', title: 'Created by ID' },
        { id: 'created_by_username', title: 'Created by username' },
        { id: 'creation_date', title: 'Creation date' },
        { id: 'creation_time', title: 'Creation time' },
        { id: 'product_id', title: 'Product ID' },
        { id: 'product', title: 'Product' },
        { id: 'quantity', title: 'Quantity' }
      ];

      const csvStringifier = createCsvStringifier({
        header: headers_array,
        fieldDelimiter: ';'
      });

      let csvData = csvStringifier.getHeaderString();
      csvData += csvStringifier.stringifyRecords(rows);

      res.setHeader('Content-disposition', 'attachment; filename=results-beneficiary-form.csv');
      res.setHeader('Content-type', 'text/csv; charset=utf-8');
      res.send(csvData);

    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  }
}
);

router.get('/metrics/questions/:locationId', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin' || cabecera.role === 'client') {
    try {
      const language = req.query.language || 'en';
      const location_id = parseInt(req.params.locationId) || null;
      const [rows] = await mysqlConnection.promise().query(
        `SELECT u.id AS user_id,
                q.id AS question_id,
                at.id AS answer_type_id,
                ${language === 'en' ? 'q.name' : 'q.name_es'} AS question,
                a.id AS answer_id,
                ${language === 'en' ? 'a.name' : 'a.name_es'} AS answer,
                uq.answer_text AS answer_text,
                uq.answer_number AS answer_number
        FROM user u
        LEFT JOIN location AS loc ON u.location_id = loc.id
        CROSS JOIN question AS q
        LEFT JOIN answer_type as at ON q.answer_type_id = at.id
        LEFT JOIN user_question AS uq ON u.id = uq.user_id AND uq.question_id = q.id
        LEFT JOIN user_question_answer AS uqa ON uq.id = uqa.user_question_id
        left join answer as a ON a.id = uqa.answer_id and a.question_id = q.id
        WHERE u.role_id = 5 AND q.enabled = 'Y' and (q.answer_type_id = 3 or q.answer_type_id = 4)
        ${cabecera.role === 'client' ? 'and u.client_id = ?' : ''}
        ${location_id ? 'and u.location_id = ?' : ''}
        order by q.id, a.id`,
        [cabecera.client_id]
      );

      // crear array de objetos pregunta, cada pregunta tiene un array de objetos respuesta, donde cada respuesta tiene un nombre y la suma de usuarios que la eligieron
      // iterar el array rows y agregar el campo question, ir sumando sus respuestas sobre esa question hasta que cambie de question_id, luego se pushea el objeto question al array questions
      // estructura:
      // questions = [
      //   {
      //     question_id: 1,
      //     question: '¿Cuál es su género?',
      //     answers: [
      //       {
      //         answer_id: 1,
      //         answer: 'Masculino',
      //         total: 5
      //       }
      //     ]
      //   }
      // ]
      const questions = [];
      let currentQuestion = null;

      for (const row of rows) {
        if (row.question_id !== (currentQuestion && currentQuestion.question_id)) {
          const question = {
            question_id: row.question_id,
            question: row.question,
            answers: []
          };
          questions.push(question);
          currentQuestion = question;
        }

        if (row.answer_id) {
          const answer = {
            answer_id: row.answer_id,
            answer: row.answer,
            total: 1
          };

          const existingAnswer = currentQuestion.answers.find(a => a.answer_id === answer.answer_id);
          if (existingAnswer) {
            existingAnswer.total++;
            answer.answer = null;
          } else {
            currentQuestion.answers.push(answer);
          }
        }
      }


      // Obtener todas las posibles respuestas para cada pregunta
      const [answerRows] = await mysqlConnection.promise().query(`
        SELECT q.id AS question_id, 
              a.id AS answer_id, 
              ${language === 'en' ? 'a.name' : 'a.name_es'} AS answer
              FROM question q
              JOIN answer_type at ON q.answer_type_id = at.id
              JOIN answer a ON q.id = a.question_id
              WHERE q.enabled = 'Y' AND at.id = 3 OR at.id = 4
              ${cabecera.role === 'client' ? 'AND q.client_id = ?' : ''}
              ORDER BY q.id, a.id
      `);

      const possibleAnswers = {};
      for (const row of answerRows) {
        if (!possibleAnswers[row.question_id]) {
          possibleAnswers[row.question_id] = [];
        }
        possibleAnswers[row.question_id].push({
          answer_id: row.answer_id,
          answer: row.answer,
          total: 0
        });
      }

      // Contar cuántas veces se ha elegido cada respuesta
      const answerCounts = {};
      for (const row of rows) {
        if (row.answer_id) {
          if (!answerCounts[row.question_id]) {
            answerCounts[row.question_id] = {};
          }
          if (!answerCounts[row.question_id][row.answer_id]) {
            answerCounts[row.question_id][row.answer_id] = 0;
          }
          answerCounts[row.question_id][row.answer_id]++;
        }
      }

      // Agregar las respuestas que no fueron elegidas
      for (const question of questions) {
        if (possibleAnswers[question.question_id]) {
          for (const answer of possibleAnswers[question.question_id]) {
            if (!answerCounts[question.question_id][answer.answer_id]) {
              answerCounts[question.question_id][answer.answer_id] = 0;
              answer.total = answerCounts[question.question_id][answer.answer_id];
              question.answers.push(answer);
            }
          }
        }
      }

      res.json(questions);
    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  }
}
);

router.get('/table/notification', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  let buscar = req.query.search;
  let queryBuscar = '';

  var page = req.query.page ? Number(req.query.page) : 1;

  if (page < 1) {
    page = 1;
  }
  var resultsPerPage = 10;
  var start = (page - 1) * resultsPerPage;

  var orderBy = req.query.orderBy ? req.query.orderBy : 'id';
  var orderType = ['asc', 'desc'].includes(req.query.orderType) ? req.query.orderType : 'desc';
  var queryOrderBy = `${orderBy} ${orderType}`;

  if (buscar) {
    buscar = '%' + buscar + '%';
    if (cabecera.role === 'admin') {
      queryBuscar = `WHERE (message.id like '${buscar}' or message.user_id like '${buscar}' or user.username like '${buscar}' or message.name like '${buscar}' or DATE_FORMAT(CONVERT_TZ(message.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') like '${buscar}')`;
    }
  }

  if (cabecera.role === 'admin') {
    try {
      const query = `SELECT
      message.id,
      message.user_id,
      user.username as user_name,
      message.name as message,
      DATE_FORMAT(CONVERT_TZ(message.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') as creation_date
      FROM message
      INNER JOIN user ON message.user_id = user.id
      ${queryBuscar}
      ORDER BY ${queryOrderBy}
      LIMIT ?, ?`

      const [rows] = await mysqlConnection.promise().query(
        query
        , [start, resultsPerPage]);
      if (rows.length > 0) {
        const [countRows] = await mysqlConnection.promise().query(`
        SELECT COUNT(*) as count
        FROM message
        INNER JOIN user ON message.user_id = user.id
        ${queryBuscar}
      `);

        const numOfResults = countRows[0].count;
        const numOfPages = Math.ceil(numOfResults / resultsPerPage);

        res.json({ results: rows, numOfPages: numOfPages, totalItems: numOfResults, page: page - 1, orderBy: orderBy, orderType: orderType });
      } else {
        res.json({ results: rows, numOfPages: 0, totalItems: 0, page: page - 1, orderBy: orderBy, orderType: orderType });
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Error interno');
    }
  } else {
    res.status(401).json('No autorizado');
  }
});

router.get('/table/user', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  let buscar = req.query.search;
  let queryBuscar = '';

  var page = req.query.page ? Number(req.query.page) : 1;

  if (page < 1) {
    page = 1;
  }
  var resultsPerPage = 10;
  var start = (page - 1) * resultsPerPage;

  var orderBy = req.query.orderBy ? req.query.orderBy : 'id';
  var orderType = ['asc', 'desc'].includes(req.query.orderType) ? req.query.orderType : 'desc';
  var queryOrderBy = `${orderBy} ${orderType}`;

  if (buscar) {
    buscar = '%' + buscar + '%';
    if (cabecera.role === 'admin') {
      queryBuscar = `and (user.id like '${buscar}' or user.username like '${buscar}' or user.email like '${buscar}' or user.firstname like '${buscar}' or user.lastname like '${buscar}' or role.name like '${buscar}' or DATE_FORMAT(CONVERT_TZ(user.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') like '${buscar}')`;
    }
  }

  if (cabecera.role === 'admin') {
    try {
      const query = `SELECT
      user.id,
      user.username,
      user.email,
      user.firstname,
      user.lastname,
      role.name as role,
      DATE_FORMAT(CONVERT_TZ(user.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') as creation_date
      FROM user
      INNER JOIN role ON user.role_id = role.id
      WHERE user.enabled = "Y" ${queryBuscar}
      ORDER BY ${queryOrderBy}
      LIMIT ?, ?`

      const [rows] = await mysqlConnection.promise().query(
        query
        , [start, resultsPerPage]);
      if (rows.length > 0) {
        const [countRows] = await mysqlConnection.promise().query(`
        SELECT COUNT(*) as count
        FROM user
        INNER JOIN role ON user.role_id = role.id
        WHERE user.enabled = "Y" ${queryBuscar}
      `);

        const numOfResults = countRows[0].count;
        const numOfPages = Math.ceil(numOfResults / resultsPerPage);

        res.json({ results: rows, numOfPages: numOfPages, totalItems: numOfResults, page: page - 1, orderBy: orderBy, orderType: orderType });
      } else {
        res.json({ results: rows, numOfPages: 0, totalItems: 0, page: page - 1, orderBy: orderBy, orderType: orderType });
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Error interno');
    }
  } else {
    res.status(401).json('No autorizado');
  }
});

router.get('/table/delivered', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  let buscar = req.query.search;
  let queryBuscar = '';

  var page = req.query.page ? Number(req.query.page) : 1;

  if (page < 1) {
    page = 1;
  }
  var resultsPerPage = 10;
  var start = (page - 1) * resultsPerPage;

  var orderBy = req.query.orderBy ? req.query.orderBy : 'id';
  var orderType = ['asc', 'desc'].includes(req.query.orderType) ? req.query.orderType : 'desc';
  var queryOrderBy = `${orderBy} ${orderType}`;

  if (buscar) {
    buscar = '%' + buscar + '%';
    if (cabecera.role === 'admin') {
      queryBuscar = `WHERE (delivery_beneficiary.id like '${buscar}' or delivery_beneficiary.delivering_user_id like '${buscar}' or user_delivery.username like '${buscar}' or delivery_beneficiary.receiving_user_id like '${buscar}' or user_beneficiary.username like '${buscar}' or delivery_beneficiary.location_id like '${buscar}' or location.community_city like '${buscar}' or delivery_beneficiary.approved like '${buscar}' or DATE_FORMAT(CONVERT_TZ(delivery_beneficiary.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') like '${buscar}')`;
    }
  }

  if (cabecera.role === 'admin') {
    try {
      const query = `SELECT
      delivery_beneficiary.id,
      delivery_beneficiary.delivering_user_id,
      user_delivery.username as delivery_username,
      delivery_beneficiary.receiving_user_id,
      user_beneficiary.username as beneficiary_username,
      delivery_beneficiary.location_id,
      location.community_city,
      delivery_beneficiary.approved,
      DATE_FORMAT(CONVERT_TZ(delivery_beneficiary.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') as creation_date
      FROM delivery_beneficiary
      INNER JOIN user as user_delivery ON delivery_beneficiary.delivering_user_id = user_delivery.id
      INNER JOIN user as user_beneficiary ON delivery_beneficiary.receiving_user_id = user_beneficiary.id
      INNER JOIN location ON delivery_beneficiary.location_id = location.id
      ${queryBuscar}
      ORDER BY ${queryOrderBy}
      LIMIT ?, ?`

      const [rows] = await mysqlConnection.promise().query(
        query
        , [start, resultsPerPage]);
      if (rows.length > 0) {
        const [countRows] = await mysqlConnection.promise().query(`
        SELECT COUNT(DISTINCT delivery_beneficiary.id) as count
        FROM delivery_beneficiary
        INNER JOIN user as user_delivery ON delivery_beneficiary.delivering_user_id = user_delivery.id
        INNER JOIN user as user_beneficiary ON delivery_beneficiary.receiving_user_id = user_beneficiary.id
        INNER JOIN location ON delivery_beneficiary.location_id = location.id
        ${queryBuscar}
      `);

        const numOfResults = countRows[0].count;
        const numOfPages = Math.ceil(numOfResults / resultsPerPage);

        res.json({ results: rows, numOfPages: numOfPages, totalItems: numOfResults, page: page - 1, orderBy: orderBy, orderType: orderType });
      } else {
        res.json({ results: rows, numOfPages: 0, totalItems: 0, page: page - 1, orderBy: orderBy, orderType: orderType });
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Error interno');
    }
  } else {
    res.status(401).json('No autorizado');
  }
});

router.get('/table/ticket', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  let buscar = req.query.search;
  let queryBuscar = '';

  var page = req.query.page ? Number(req.query.page) : 1;

  if (page < 1) {
    page = 1;
  }
  var resultsPerPage = 10;
  var start = (page - 1) * resultsPerPage;

  var orderBy = req.query.orderBy ? req.query.orderBy : 'id';
  var orderType = ['asc', 'desc'].includes(req.query.orderType) ? req.query.orderType : 'desc';
  var queryOrderBy = `${orderBy} ${orderType}`;

  if (buscar) {
    buscar = '%' + buscar + '%';
    if (cabecera.role === 'admin') {
      queryBuscar = `WHERE (donation_ticket.id like '${buscar}' or donation_ticket.donation_id like '${buscar}' or donation_ticket.total_weight like '${buscar}' or provider.name like '${buscar}' or location.community_city like '${buscar}' or DATE_FORMAT(donation_ticket.date, '%m/%d/%Y') like '${buscar}' or donation_ticket.delivered_by like '${buscar}' or DATE_FORMAT(CONVERT_TZ(donation_ticket.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') like '${buscar}')`;
    }
  }

  if (cabecera.role === 'admin') {
    try {
      const query = `SELECT
      donation_ticket.id,
      donation_ticket.donation_id,
      donation_ticket.total_weight,
      provider.name as provider,
      location.community_city as location,
      DATE_FORMAT(donation_ticket.date, '%m/%d/%Y') as date,
      donation_ticket.delivered_by,
      COUNT(DISTINCT product_donation_ticket.product_id) AS products,
      DATE_FORMAT(CONVERT_TZ(donation_ticket.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') as creation_date
      FROM donation_ticket
      INNER JOIN provider ON donation_ticket.provider_id = provider.id
      INNER JOIN location ON donation_ticket.location_id = location.id
      INNER JOIN product_donation_ticket ON donation_ticket.id = product_donation_ticket.donation_ticket_id
      ${queryBuscar}
      GROUP BY donation_ticket.id
      ORDER BY ${queryOrderBy}
      LIMIT ?, ?`

      const [rows] = await mysqlConnection.promise().query(
        query
        , [start, resultsPerPage]);
      if (rows.length > 0) {
        const [countRows] = await mysqlConnection.promise().query(`
        SELECT COUNT(DISTINCT donation_ticket.id) as count
        FROM donation_ticket
        INNER JOIN provider ON donation_ticket.provider_id = provider.id
        INNER JOIN location ON donation_ticket.location_id = location.id
        INNER JOIN product_donation_ticket ON donation_ticket.id = product_donation_ticket.donation_ticket_id
        ${queryBuscar}
      `);

        const numOfResults = countRows[0].count;
        const numOfPages = Math.ceil(numOfResults / resultsPerPage);

        res.json({ results: rows, numOfPages: numOfPages, totalItems: numOfResults, page: page - 1, orderBy: orderBy, orderType: orderType });
      } else {
        res.json({ results: rows, numOfPages: 0, totalItems: 0, page: page - 1, orderBy: orderBy, orderType: orderType });
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Error interno');
    }
  } else {
    res.status(401).json('No autorizado');
  }
});

router.get('/table/product', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  let buscar = req.query.search;
  let queryBuscar = '';

  var page = req.query.page ? Number(req.query.page) : 1;

  if (page < 1) {
    page = 1;
  }
  var resultsPerPage = 10;
  var start = (page - 1) * resultsPerPage;

  var orderBy = req.query.orderBy ? req.query.orderBy : 'id';
  var orderType = ['asc', 'desc'].includes(req.query.orderType) ? req.query.orderType : 'desc';
  var queryOrderBy = `${orderBy} ${orderType}`;

  if (buscar) {
    buscar = '%' + buscar + '%';
    if (cabecera.role === 'admin') {
      queryBuscar = `WHERE (product.id like '${buscar}' or product.name like '${buscar}' or product.value_usd like '${buscar}' or DATE_FORMAT(CONVERT_TZ(product.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') like '${buscar}')`;
    }
  }

  if (cabecera.role === 'admin') {
    try {
      const query = `SELECT
      product.id,
      product.name,
      product.value_usd,
      DATE_FORMAT(CONVERT_TZ(product.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') as creation_date
      FROM product
      ${queryBuscar}
      ORDER BY ${queryOrderBy}
      LIMIT ?, ?`

      const [rows] = await mysqlConnection.promise().query(
        query
        , [start, resultsPerPage]);
      if (rows.length > 0) {
        const [countRows] = await mysqlConnection.promise().query(`
        SELECT COUNT(*) as count
        FROM product
        ${queryBuscar}
      `);

        const numOfResults = countRows[0].count;
        const numOfPages = Math.ceil(numOfResults / resultsPerPage);

        res.json({ results: rows, numOfPages: numOfPages, totalItems: numOfResults, page: page - 1, orderBy: orderBy, orderType: orderType });
      } else {
        res.json({ results: rows, numOfPages: 0, totalItems: 0, page: page - 1, orderBy: orderBy, orderType: orderType });
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Error interno');
    }
  } else {
    res.status(401).json('No autorizado');
  }
});

router.get('/table/location', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  let buscar = req.query.search;
  let queryBuscar = '';

  var page = req.query.page ? Number(req.query.page) : 1;

  if (page < 1) {
    page = 1;
  }
  var resultsPerPage = 10;
  var start = (page - 1) * resultsPerPage;

  var orderBy = req.query.orderBy ? req.query.orderBy : 'id';
  var orderType = ['asc', 'desc'].includes(req.query.orderType) ? req.query.orderType : 'desc';
  var queryOrderBy = `${orderBy} ${orderType}`;

  if (buscar) {
    buscar = '%' + buscar + '%';
    if (cabecera.role === 'admin') {
      queryBuscar = `WHERE (location.id like '${buscar}' or location.organization like '${buscar}' or location.community_city like '${buscar}' or location.partner like '${buscar}' or location.address like '${buscar}' or location.enabled like '${buscar}' or DATE_FORMAT(CONVERT_TZ(location.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') like '${buscar}')`;
    }
  }

  if (cabecera.role === 'admin') {
    try {
      const query = `SELECT
      location.id,
      location.organization,
      location.community_city,
      location.partner,
      location.address,
      location.enabled,
      DATE_FORMAT(CONVERT_TZ(location.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') as creation_date
      FROM location
      ${queryBuscar}
      ORDER BY ${queryOrderBy}
      LIMIT ?, ?`

      const [rows] = await mysqlConnection.promise().query(
        query
        , [start, resultsPerPage]);
      if (rows.length > 0) {
        const [countRows] = await mysqlConnection.promise().query(`
        SELECT COUNT(*) as count
        FROM location
        ${queryBuscar}
      `);

        const numOfResults = countRows[0].count;
        const numOfPages = Math.ceil(numOfResults / resultsPerPage);

        res.json({ results: rows, numOfPages: numOfPages, totalItems: numOfResults, page: page - 1, orderBy: orderBy, orderType: orderType });
      } else {
        res.json({ results: rows, numOfPages: 0, totalItems: 0, page: page - 1, orderBy: orderBy, orderType: orderType });
      }

    } catch (error) {
      console.log(error);
      logger.error(error);
      res.status(500).json('Error interno');
    }
  } else {
    res.status(401).json('No autorizado');
  }
});

router.get('/view/ticket/:idTicket', verifyToken, async (req, res) => {
  const cabecera = JSON.parse(req.data.data);
  if (cabecera.role === 'admin') {
    try {
      const { idTicket } = req.params;

      const [rows] = await mysqlConnection.promise().query(
        `SELECT dt.id,
                dt.donation_id,
                dt.total_weight,
                p.name as provider,
                loc.community_city as location,
                DATE_FORMAT(dt.date, '%m/%d/%Y') as date,
                dt.delivered_by,
                u.id as created_by_id,
                u.username as created_by_username,
                DATE_FORMAT(CONVERT_TZ(dt.creation_date, '+00:00', '-07:00'), '%m/%d/%Y %T') AS creation_date,
                product.id as product_id,
                product.name as product,
                pdt.quantity as quantity
        FROM donation_ticket as dt
        INNER JOIN provider as p ON dt.provider_id = p.id
        INNER JOIN location as loc ON dt.location_id = loc.id
        INNER JOIN stocker_log as sl ON dt.id = sl.donation_ticket_id
        INNER JOIN user as u ON sl.user_id = u.id
        INNER JOIN product_donation_ticket as pdt ON dt.id = pdt.donation_ticket_id
        INNER JOIN product as product ON pdt.product_id = product.id
        WHERE dt.id = ?`,
        [idTicket]
      );

      // create object with ticket data and field 'products' with array of products
      var ticket = {};
      var products = [];
      ticket["id"] = rows[0].id;
      ticket["donation_id"] = rows[0].donation_id;
      ticket["total_weight"] = rows[0].total_weight;
      ticket["provider"] = rows[0].provider;
      ticket["location"] = rows[0].location;
      ticket["date"] = rows[0].date;
      ticket["delivered_by"] = rows[0].delivered_by;
      ticket["created_by_id"] = rows[0].created_by_id;
      ticket["created_by_username"] = rows[0].created_by_username;
      ticket["creation_date"] = rows[0].creation_date;

      for (let i = 0; i < rows.length; i++) {
        products.push({ product_id: rows[i].product_id, product: rows[i].product, quantity: rows[i].quantity });
      }
      ticket["products"] = products;

      res.json(ticket);

    } catch (err) {
      console.log(err);
      res.status(500).json('Internal server error');
    }
  }
}
);


function verifyToken(req, res, next) {

  if (!req.headers.authorization) return res.status(401).json('No autorizado');

  const token = req.headers.authorization.substr(7);
  if (token !== '') {
    jwt.verify(token, process.env.JWT_SECRET, (error, authData) => {
      if (error) {
        res.status(403).json('Error en el token');
      } else {
        req.data = authData;
        next();
      }
    });
  } else {
    res.status(401).json('Token vacio');
  }

}

module.exports = router;