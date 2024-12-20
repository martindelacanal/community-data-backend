const nodemailer = require("nodemailer");
const archiver = require('archiver');

const archiverZipEncrypted = require("archiver-zip-encrypted");
const moment = require('moment-timezone');

// Registrar el formato zip-encrypted
archiver.registerFormat('zip-encrypted', archiverZipEncrypted);
const { parse } = require('csv-parse/sync');

// Configuración del transporte de nodemailer
let transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: 'bienestarcommunity@gmail.com',
    pass: 'auag ynko amyv rsuj' // Asegúrate de usar una contraseña de aplicación válida
  }
});

async function createPasswordProtectedZip(csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password) {
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
    archive.append(csvAllNewRegistrations, { name: 'new-registrations.csv' });
    archive.append(csvSummary, { name: 'summary.csv' });
    archive.finalize();
  });
}

async function sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password, emails) {
  return new Promise(async (resolve) => {
    try {

      // Crear el archivo ZIP protegido con contraseña en memoria
      const zipContent = await createPasswordProtectedZip(csvRawData, csvNewRegistrations, csvSummary, csvAllNewRegistrations, password);
      // obtener fecha actual en formato mm/dd/yyyy y convertido de UTC a California
      let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");

      // Append formatted summary to the message
      message += '\n\nSummary:\n';

      // Opciones del correo
      let mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: emails.join(', '),
        subject: subject,
        text: message,
        // If you want to send HTML content
        html: message.replace(/\n/g, '<br>') + '<br>' + generateHtmlTable(csvSummary),
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

// Optional: Function to generate an HTML table from csvSummary
function generateHtmlTable(csvSummary) {
  const records = parse(csvSummary, {
    columns: true,
    skip_empty_lines: true,
    delimiter: ';'
  });

  if (records.length === 0) {
    return '';
  }

  let html = '<table border="1" cellspacing="0" cellpadding="5">';
  html += '<tr>';
  for (let key in records[0]) {
    html += `<th>${key}</th>`;
  }
  html += '</tr><tr>';
  for (let key in records[0]) {
    html += `<td>${records[0][key]}</td>`;
  }
  html += '</tr></table>';

  return html;
}

async function sendTicketEmail(formData, products, images, emails) {
  try {
    // Construct the email message with form data
    let message = '';
    for (let key in formData) {
      message += `${key}: ${formData[key]}\n`;
    }

    // Generate HTML table for products
    let productTable = '<table border="1"><tr><th>Product</th><th>Product Type</th><th>Quantity</th></tr>';
    products.forEach(product => {
      productTable += `<tr><td>${product.productName}</td><td>${product.productType}</td><td>${product.quantity}</td></tr>`;
    });
    productTable += '</table>';

    // Prepare image attachments
    let attachments = images.map(image => ({
      filename: image.originalname,
      content: image.buffer,
    }));

    // Mail options
    let mailOptions = {
      from: 'bienestarcommunity@gmail.com',
      to: emails.join(', '),
      subject: 'New Donation Ticket Uploaded',
      text: message,
      html: message.replace(/\n/g, '<br>') + '<br>' + productTable,
      attachments: attachments,
    };

    // Send the email
    transporter.sendMail(mailOptions, (err, info) => {
      if (err) {
        console.log(`Error sending email: `, err);
      } else {
        console.log(`Email sent: ` + info.response);
      }
    });
  } catch (error) {
    console.log(`Error in sendTicketEmail: `, error);
  }
}

module.exports.sendTicketEmail = sendTicketEmail;

module.exports.sendEmailWithAttachment = sendEmailWithAttachment;
