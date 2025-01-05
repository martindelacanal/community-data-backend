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

async function sendVolunteerConfirmation(volunteerEmail, locationCity) {
  try {
    const htmlMessage = `<b>Location chosen:</b> ${locationCity}<br><br>
                        <b>2025 Volunteer Liability Waiver, Terms and conditions:</b><br>
                        I have agreed to volunteer my services ("Activity") for Bienestar is Well-being ("Organization"). 
                        I further understand that Bienestar is Well-being provides no compensation for my services and 
                        that I am not entitled to any benefits from the Organization, including but not limited to 
                        workers' compensation benefits.<br>
                        <b>Assumption of Risk:</b><br>
                        I understand that there are risks of injury, death and damage to property from performing the Volunteer Activity for the Organization. 
                        I attest and verify that I possess the physical fitness and ability to perform the Activity and that I have no physical limitations 
                        that would affect my performance of the Activity. If I do not feel that I am capable of performing the Activity, I assume the responsibility 
                        of informing whomever is designated as the on-site Supervisor or Team Lead. In consideration for being allowed to participate in the Activity, 
                        I hereby assume the risk of, and responsibility for, any such injury, death or damage which I may sustain arising out of or in any way 
                        connected with performance of the Activity, including injury, death or damage resulting from any acts or omissions, whether negligent or not, 
                        or any property or equipment owned or supplied by or on behalf of the Organization, its officials, officers, employees, agents, volunteers, 
                        and any other promoters, operators or co-sponsors of the Activity.<br>
                        <b>Release and Indemnification:</b><br>
                        In consideration for being allowed to participate in the Activity, I hereby release, waive and discharge the Organization, its officials, 
                        officers, employees, agents, volunteers, and any other promoters, operators or co-sponsors of the Activity, from any and all liability, claims, 
                        or causes of action arising out of or in any way connected with my performance of the Activity, or upon its acts or omissions, whether 
                        negligent or not (“Waiver”). I agree to this Waiver on behalf of myself, my heirs, executors, administrators and assigns.<br>
                        As further consideration for being allowed to participate in the Activity, I hereby agree, on behalf of myself, my heirs, executors, 
                        administrators and assigns, to indemnify and hold harmless the Organization, its officials, officers, employees, agents, volunteers, 
                        and any other promoters, operators or co-sponsors of the Activity, from any and all claims for compensation, personal injury, property 
                        damage or wrongful death caused by my negligence or willful misconduct, in the performance of the Activity.<br>
                        <b>Knowing and Voluntary Execution:</b><br>
                        I have carefully read this Waiver and Release Form and fully understand its contents. I understand that I am giving up valuable legal rights. 
                        I knowingly and voluntarily give up these rights of my own free will.<br>
                        `;

    const textMessage = `Location chosen: ${locationCity}\n\n
                        2025 Volunteer Liability Waiver, Terms and conditions:\n
                        I have agreed to volunteer my services ("Activity") for Bienestar is Well-being ("Organization"). 
                        I further understand that Bienestar is Well-being provides no compensation for my services and 
                        that I am not entitled to any benefits from the Organization, including but not limited to 
                        workers' compensation benefits.\n
                        Assumption of Risk:\n
                        I understand that there are risks of injury, death and damage to property from performing the Volunteer Activity for the Organization.
                        I attest and verify that I possess the physical fitness and ability to perform the Activity and that I have no physical limitations
                        that would affect my performance of the Activity. If I do not feel that I am capable of performing the Activity, I assume the responsibility
                        of informing whomever is designated as the on-site Supervisor or Team Lead. In consideration for being allowed to participate in the Activity,
                        I hereby assume the risk of, and responsibility for, any such injury, death or damage which I may sustain arising out of or in any way
                        connected with performance of the Activity, including injury, death or damage resulting from any acts or omissions, whether negligent or not,
                        or any property or equipment owned or supplied by or on behalf of the Organization, its officials, officers, employees, agents, volunteers,
                        and any other promoters, operators or co-sponsors of the Activity.\n
                        Release and Indemnification:\n
                        In consideration for being allowed to participate in the Activity, I hereby release, waive and discharge the Organization, its officials,
                        officers, employees, agents, volunteers, and any other promoters, operators or co-sponsors of the Activity, from any and all liability, claims,
                        or causes of action arising out of or in any way connected with my performance of the Activity, or upon its acts or omissions, whether
                        negligent or not (“Waiver”). I agree to this Waiver on behalf of myself, my heirs, executors, administrators and assigns.\n
                        As further consideration for being allowed to participate in the Activity, I hereby agree, on behalf of myself, my heirs, executors,
                        administrators and assigns, to indemnify and hold harmless the Organization, its officials, officers, employees, agents, volunteers,
                        and any other promoters, operators or co-sponsors of the Activity, from any and all claims for compensation, personal injury, property
                        damage or wrongful death caused by my negligence or willful misconduct, in the performance of the Activity.\n
                        Knowing and Voluntary Execution:\n
                        I have carefully read this Waiver and Release Form and fully understand its contents. I understand that I am giving up valuable legal rights.
                        I knowingly and voluntarily give up these rights of my own free will.\n
                        `;

    const mailOptions = {
      from: 'bienestarcommunity@gmail.com',
      to: volunteerEmail,
      subject: 'Terms and conditions signed',
      text: textMessage,
      html: htmlMessage
    };

    await transporter.sendMail(mailOptions);
  } catch (error) {
    console.log('Error sending volunteer confirmation email:', error);
  }
}

module.exports.sendVolunteerConfirmation = sendVolunteerConfirmation;

module.exports.sendTicketEmail = sendTicketEmail;

module.exports.sendEmailWithAttachment = sendEmailWithAttachment;
