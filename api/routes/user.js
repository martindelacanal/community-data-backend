const express = require('express');
const router = express.Router();
const mysqlConnection = require('../connection/connection');
const jwt = require('jsonwebtoken');
const bcryptjs = require('bcryptjs');
const axios = require('axios');
const logger = require('../utils/logger.js');

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
        const date = formulario.date || null;
        const delivered_by = formulario.delivered_by || null;
        var products = formulario.products || [];

        console.log(formulario);
        console.log(req.files);
        if (!Number.isInteger(provider)) {
          const [rows] = await mysqlConnection.promise().query(
            'insert into provider(name) values(?)',
            [provider]
          );
          provider = rows.insertId;
        }
        // iterar el array de objetos products (product,quantity) y si product no es un integer, entonces es un string con el nombre del producto nuevo, debe insertarse en tabla Products y obtener el id para reemplazarlo en el objeto en el campo product en la posicion i
        for (let i = 0; i < products.length; i++) {
          if (!Number.isInteger(products[i].product)) {
            const [rows] = await mysqlConnection.promise().query(
              'insert into product(name) values(?)',
              [products[i].product]
            );
            products[i].product = rows.insertId;
          }
        }

        const [rows] = await mysqlConnection.promise().query(
          'insert into donation_ticket(client_id, donation_id, total_weight, provider_id, location_id, date, delivered_by) values(?,?,?,?,?,?,?)',
          [cabecera.client_id, donation_id, total_weight, provider, destination, date, delivered_by]
        );

        if (rows.affectedRows > 0) {
          const donation_ticket_id = rows.insertId;
          for (let i = 0; i < products.length; i++) {
            await mysqlConnection.promise().query(
              'insert into product_donation_ticket(product_id, donation_ticket_id, quantity) values(?,?,?)',
              [products[i].product, donation_ticket_id, products[i].quantity]
            );
          }
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
        } else {
          res.status(500).json('Could not create ticket');
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
  if (cabecera.role === 'beneficiary') {
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
        'select id,organization,address from location where enabled = "Y" and client_id = ? order by organization',
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
      res.json(rows[0]);
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