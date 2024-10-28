const nodemailer = require("nodemailer");
const archiver = require('archiver');

const archiverZipEncrypted = require("archiver-zip-encrypted");
const moment = require('moment-timezone');

// Registrar el formato zip-encrypted
archiver.registerFormat('zip-encrypted', archiverZipEncrypted);

// Configuración del transporte de nodemailer
let transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: 'bienestarcommunity@gmail.com',
    pass: 'auag ynko amyv rsuj' // Asegúrate de usar una contraseña de aplicación válida
  }
});

async function createPasswordProtectedZip(csvRawData, csvNewRegistrations, csvSummary, password) {
  return new Promise((resolve, reject) => {
    const archive = archiver.create('zip-encrypted', {
      zlib: { level: 9 },
      encryptionMethod: 'aes256',
      password: password
    });

    const buffers = [];
    archive.on('data', data => buffers.push(data));
    archive.on('end', () => resolve(Buffer.concat(buffers)));
    archive.on('error', err => reject(err));

    archive.append(csvRawData, { name: 'raw-data.csv' });
    archive.append(csvNewRegistrations, { name: 'new-registrations-without-health-insurance.csv' });
    archive.append(csvSummary, { name: 'summary.csv' });
    archive.finalize();
  });
}

async function sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, password, emails) {
  return new Promise(async (resolve) => {
    try {

      // Crear el archivo ZIP protegido con contraseña en memoria
      const zipContent = await createPasswordProtectedZip(csvRawData, csvNewRegistrations, csvSummary, password);
      // obtener fecha actual en formato mm/dd/yyyy y convertido de UTC a California
      let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");

      // Opciones del correo
      let mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: emails.join(', '),
        subject: subject,
        text: message,
        attachments: [
          {
            filename: `community-data-${date}.zip`,
            content: zipContent
          }
        ]
      };

      // Enviar el correo
      transporter.sendMail(mailOptions, async (err, info) => {
        if (err) {
          console.log(`error sendEmail to ${emails.join(', ')}: `, err);
          resolve({ error: err, status: 500 });
        } else {
          console.log(`Email enviado to ${emails.join(', ')}: ` + info.response);
          resolve({ error: null, status: 200 });
        }
      });

    } catch (error) {
      console.log(`error catch email to ${emails.join(', ')}: `, error);
      resolve({ error: error, status: 500 });
    }
  });
}

module.exports.sendEmailWithAttachment = sendEmailWithAttachment;
