const nodemailer = require("nodemailer");
const archiver = require('archiver');
const { createObjectCsvStringifier } = require('csv-writer');
const archiverZipEncrypted = require("archiver-zip-encrypted");

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

async function createPasswordProtectedZip(csvContent, password) {
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

    archive.append(csvContent, { name: 'file.csv' });
    archive.finalize();
  });
}

async function sendEmailWithAttachment(subject, message, csvData, password) {
  return new Promise(async (resolve) => {
    try {
      // Crear el archivo CSV en memoria
      const csvStringifier = createObjectCsvStringifier({
        header: [{ id: 'test', title: 'Test' }]
      });
      const csvContent = csvStringifier.stringifyRecords(csvData);

      // Crear el archivo ZIP protegido con contraseña en memoria
      const zipContent = await createPasswordProtectedZip(csvContent, password);

      // Opciones del correo
      let mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: 'martin.delacanalerbetta@gmail.com, mazzottadamian@gmail.com',
        subject: subject,
        text: message,
        attachments: [
          {
            filename: 'file.zip',
            content: zipContent
          }
        ]
      };

      // Enviar el correo
      transporter.sendMail(mailOptions, async (err, info) => {
        if (err) {
          console.log("error sendEmail: ", err);
          resolve({ error: err, status: 500 });
        } else {
          console.log('Email enviado: ' + info.response);
          resolve({ error: null, status: 200 });
        }
      });

    } catch (error) {
      console.log("error catch email: ", error);
      resolve({ error: error, status: 500 });
    }
  });
}

module.exports.sendEmailWithAttachment = sendEmailWithAttachment;
