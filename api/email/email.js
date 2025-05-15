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

async function createPasswordProtectedZip(csvRawData, csvNewRegistrations, csvSummaryString, csvAllNewRegistrations, password) {
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
    archive.append(csvSummaryString, { name: 'summary.csv' }); // Use the CSV string here
    archive.finalize();
  });
}

async function sendEmailWithAttachment(subject, message, csvRawData, csvNewRegistrations, summaryObject, csvAllNewRegistrations, password, emails) {
  return new Promise(async (resolve) => {
    try {

      // Crear el archivo ZIP protegido con contraseña en memoria
      // Pass summaryObject.csvString for the zip file content
      const zipContent = await createPasswordProtectedZip(csvRawData, csvNewRegistrations, summaryObject.csvString, csvAllNewRegistrations, password);
      // obtener fecha actual en formato mm/dd/yyyy y convertido de UTC a California
      let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");

      // Append formatted summary to the message
      // Pass summaryObject.emailTableData to generateHtmlTable
      const summaryHtmlTable = generateHtmlTable(summaryObject.emailTableData);
      let fullHtmlMessage = message.replace(/\n/g, '<br>');
      if (summaryHtmlTable) {
        fullHtmlMessage += '<br><br><b>Summary:</b><br>' + summaryHtmlTable;
      }


      // Opciones del correo
      let mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: emails.join(', '),
        subject: subject,
        text: message, // Text part remains simple
        html: fullHtmlMessage, // HTML part includes the table
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
function generateHtmlTable(records) {
  // records is now expected to be summaryObject.emailTableData
  // which is an array like:
  // [
  //   {
  //     'New': newCount,
  //     'Recurring': recurringCount,
  //     // ... other summary fields
  //   }
  // ]

  if (!records || records.length === 0) {
    return '';
  }

  let html = '<table border="1" cellspacing="0" cellpadding="5" style="border-collapse: collapse; width: auto;">';
  
  // Header row
  html += '<thead><tr>';
  for (let key in records[0]) {
    html += `<th style="background-color: #f2f2f2; text-align: left; padding: 8px;">${key}</th>`;
  }
  html += '</tr></thead>';
  
  // Data row(s)
  html += '<tbody>';
  records.forEach(record => {
    html += '<tr>';
    for (let key in record) {
      html += `<td style="text-align: left; padding: 8px; border: 1px solid #ddd;">${record[key]}</td>`;
    }
    html += '</tr>';
  });
  html += '</tbody></table>';

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

async function sendVolunteerConfirmation(volunteerEmail, locationCity, language) {
  try {
    let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");
    let htmlMessage = '';
    let textMessage = '';
    let subjectMessage = '';
    if (language === 'en') {
      subjectMessage = 'Terms and conditions signed';
      htmlMessage = `<b>Location chosen:</b> ${locationCity}<br>
                        <b>Date:</b> ${date}<br><br>
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

      textMessage = `Location chosen: ${locationCity}\n
                        Date: ${date}\n\n
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
    } else {
      subjectMessage = 'Términos y condiciones firmados';
      htmlMessage = `<b>Locación elegida:</b> ${locationCity}<br>
                        <b>Fecha:</b> ${date}<br><br>
                        <b>2025 Exención de responsabilidad voluntaria, términos y condiciones:</b><br>
                        Acepto ofrecer mis servicios como voluntario (“Actividad”) para Bienestar is Well-being (“Organización”). 
                        Además, entiendo que Bienestar is Well-being no proporciona compensación por mis servicios y que no tengo derecho 
                        a ningún beneficio de la Organización, incluidos, entre otros, los beneficios de compensación laboral.<br>
                        <b>Asunción de Riesgo:</b><br>
                        Entiendo que existen riesgos de lesiones, muerte y daños a la propiedad al realizar la actividad de voluntariado para la Organización. 
                        Doy fe y verifico que poseo la aptitud física y la capacidad para realizar la Actividad y que no tengo limitaciones físicas que puedan 
                        afectar mi desempeño de la Actividad. Si no me siento capaz de realizar la Actividad, asumo la responsabilidad de informar a quien esté 
                        designado como Supervisor en el sitio o Líder del equipo. En consideración a que se me permita participar en la Actividad, por la presente 
                        asumo el riesgo y la responsabilidad por cualquier lesión, muerte o daño que pueda sufrir como resultado de o de alguna manera relacionado 
                        con la realización de la Actividad, incluidas lesiones, muerte o daño resultante de cualquier acto u omisión, ya sea negligente o no, o 
                        cualquier propiedad o equipo de propiedad o suministrado por o en nombre de la Organización, sus funcionarios, funcionarios, empleados, 
                        agentes, voluntarios y cualquier otro promotor, operador o co -patrocinadores de la Actividad.<br>
                        <b>Liberación e Indemnización:</b><br>
                        En consideración por permitirme participar en la Actividad, por la presente libero, renuncio y descargo a la Organización, sus funcionarios, 
                        funcionarios, empleados, agentes, voluntarios y cualquier otro promotor, operador o copatrocinador de la Actividad. de toda responsabilidad, 
                        reclamo o causa de acción que surja de o esté relacionado de alguna manera con mi desempeño de la Actividad, o por sus actos u omisiones, 
                        ya sean negligentes o no (“Renuncia”). Acepto esta Renuncia en mi nombre, el de mis herederos, albaceas, administradores y cesionarios.<br>
                        Como consideración adicional para poder participar en la Actividad, por la presente acepto, en mi nombre y el de mis herederos, ejecutores, 
                        administradores y cesionarios, indemnizar y eximir de responsabilidad a la Organización, sus funcionarios, funcionarios, empleados, agentes, 
                        voluntarios y cualquier otro promotor, operador o copatrocinador de la Actividad, de todos y cada uno de los reclamos de compensación, 
                        lesiones personales, daños a la propiedad o muerte por negligencia causados por mi negligencia o mala conducta intencional, en el desempeño 
                        de la Actividad.<br>
                        <b>Conocimiento y ejecución voluntaria:</b><br>
                        He leído atentamente este Formulario de exención y liberación y comprendo plenamente su contenido. Entiendo que estoy renunciando a 
                        valiosos derechos legales. Renuncio consciente y voluntariamente a estos derechos por mi propia voluntad.<br>
                        `;

      textMessage = `Locación elegida: ${locationCity}\n
                        Fecha: ${date}\n\n
                        2025 Exención de responsabilidad voluntaria, términos y condiciones:\n
                        Acepto ofrecer mis servicios como voluntario (“Actividad”) para Bienestar is Well-being (“Organización”). 
                        Además, entiendo que Bienestar is Well-being no proporciona compensación por mis servicios y que no tengo derecho 
                        a ningún beneficio de la Organización, incluidos, entre otros, los beneficios de compensación laboral.\n
                        Asunción de Riesgo:\n
                        Entiendo que existen riesgos de lesiones, muerte y daños a la propiedad al realizar la actividad de voluntariado para la Organización. 
                        Doy fe y verifico que poseo la aptitud física y la capacidad para realizar la Actividad y que no tengo limitaciones físicas que puedan 
                        afectar mi desempeño de la Actividad. Si no me siento capaz de realizar la Actividad, asumo la responsabilidad de informar a quien esté 
                        designado como Supervisor en el sitio o Líder del equipo. En consideración a que se me permita participar en la Actividad, por la presente 
                        asumo el riesgo y la responsabilidad por cualquier lesión, muerte o daño que pueda sufrir como resultado de o de alguna manera relacionado 
                        con la realización de la Actividad, incluidas lesiones, muerte o daño resultante de cualquier acto u omisión, ya sea negligente o no, o 
                        cualquier propiedad o equipo de propiedad o suministrado por o en nombre de la Organización, sus funcionarios, funcionarios, empleados, 
                        agentes, voluntarios y cualquier otro promotor, operador o co -patrocinadores de la Actividad.\n
                        Liberación e Indemnización:\n
                        En consideración por permitirme participar en la Actividad, por la presente libero, renuncio y descargo a la Organización, sus funcionarios, 
                        funcionarios, empleados, agentes, voluntarios y cualquier otro promotor, operador o copatrocinador de la Actividad. de toda responsabilidad, 
                        reclamo o causa de acción que surja de o esté relacionado de alguna manera con mi desempeño de la Actividad, o por sus actos u omisiones, 
                        ya sean negligentes o no (“Renuncia”). Acepto esta Renuncia en mi nombre, el de mis herederos, albaceas, administradores y cesionarios.\n
                        Como consideración adicional para poder participar en la Actividad, por la presente acepto, en mi nombre y el de mis herederos, ejecutores, 
                        administradores y cesionarios, indemnizar y eximir de responsabilidad a la Organización, sus funcionarios, funcionarios, empleados, agentes, 
                        voluntarios y cualquier otro promotor, operador o copatrocinador de la Actividad, de todos y cada uno de los reclamos de compensación, 
                        lesiones personales, daños a la propiedad o muerte por negligencia causados por mi negligencia o mala conducta intencional, en el desempeño 
                        de la Actividad.\n
                        Conocimiento y ejecución voluntaria:\n
                        He leído atentamente este Formulario de exención y liberación y comprendo plenamente su contenido. Entiendo que estoy renunciando a 
                        valiosos derechos legales. Renuncio consciente y voluntariamente a estos derechos por mi propia voluntad.\n
                        `;
    }

    const mailOptions = {
      from: 'bienestarcommunity@gmail.com',
      to: volunteerEmail,
      subject: subjectMessage,
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
