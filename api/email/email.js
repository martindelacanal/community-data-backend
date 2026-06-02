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

async function createPasswordProtectedZipExcel(excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password) {
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

    archive.append(excelRawData, { name: 'raw-data.xlsx' });
    archive.append(excelNewRegistrations, { name: 'new-registrations-without-health-insurance.xlsx' });
    archive.append(excelAllNewRegistrations, { name: 'new-registrations.xlsx' });
    archive.append(summaryObject.excelBuffer, { name: 'summary.xlsx' });
    archive.finalize();
  });
}

async function sendEmailWithExcelAttachment(subject, message, excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, emails) {
  return new Promise(async (resolve) => {
    try {
      const zipContent = await createPasswordProtectedZipExcel(excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password);
      let date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");

      const summaryHtmlReport = generateSummaryHtmlReport(summaryObject.emailReportData);
      let fullHtmlMessage = message.replace(/\n/g, '<br>');
      if (summaryHtmlReport) {
        fullHtmlMessage += '<br><br><b>Summary Report:</b><br>' + summaryHtmlReport;
      }

      let mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: emails.join(', '),
        subject: subject,
        text: message, 
        html: fullHtmlMessage,
        attachments: [
          {
            filename: `community-data-${date}.zip`,
            content: zipContent
          }
        ]
      };

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
function generateSummaryHtmlReport(reportData) {
    if (!reportData) {
        return '<p>No summary data available for this period.</p>';
    }

    const {
        clientName, dateRangeDisplay,
        newCount, recurringCount, totalNewRecurring,
        newHealthPlanYes, newHealthPlanNo, newHealthPlanUnanswered = 0, totalNewHealthPlan,
        locations, newPerLocationMap, recurringPerLocationMap,
        totalNewByLocation, totalRecurringByLocation, grandTotalByLocation,
        clientId
    } = reportData;

    let html = '';
    const tableStyle = 'border="1" cellspacing="0" cellpadding="5" style="border-collapse: collapse; width: auto; margin-bottom: 15px;"';
    const thStyle = 'style="background-color: #f2f2f2; text-align: left; padding: 8px; border: 1px solid #ddd;"';
    const tdStyle = 'style="padding: 8px; border: 1px solid #ddd;"';
    const tdRightStyle = 'style="padding: 8px; border: 1px solid #ddd; text-align: right;"';

    // Client and Date Info
    html += `<p><b>Client Name:</b> ${clientName}<br>`;
    html += `<b>Date Range:</b> ${dateRangeDisplay}</p>`;

    // Table 1: Overall Summary (New, Recurring, Total)
    html += `<table ${tableStyle}>`;
    html += '<tbody>';
    html += `<tr><td ${tdStyle}>New</td><td ${tdRightStyle}>${newCount}</td></tr>`;
    html += `<tr><td ${tdStyle}>Recurring</td><td ${tdRightStyle}>${recurringCount}</td></tr>`;
    html += `<tr><td ${tdStyle}><b>Total</b></td><td ${tdRightStyle}><b>${totalNewRecurring}</b></td></tr>`;
    html += '</tbody></table>';

    // Table 2: (New) Health Plan Summary
    html += `<table ${tableStyle}>`;
    html += `<thead><tr><th ${thStyle} colspan="2">(New) Health Plan</th></tr></thead>`;
    html += '<tbody>';
    html += `<tr><td ${tdStyle}>&nbsp;&nbsp;YES</td><td ${tdRightStyle}>${newHealthPlanYes}</td></tr>`;
    html += `<tr><td ${tdStyle}>&nbsp;&nbsp;NO</td><td ${tdRightStyle}>${newHealthPlanNo}</td></tr>`;
    html += `<tr><td ${tdStyle}>&nbsp;&nbsp;Unanswered</td><td ${tdRightStyle}>${newHealthPlanUnanswered}</td></tr>`;
    html += `<tr><td ${tdStyle}>&nbsp;&nbsp;<b>Total</b></td><td ${tdRightStyle}><b>${totalNewHealthPlan}</b></td></tr>`;
    html += '</tbody></table>';

    // Table 3: Location Breakdown
    if (locations && locations.length > 0) {
        html += `<table ${tableStyle}>`;
        html += '<thead><tr>';
        html += `<th ${thStyle}>Id</th>`;
        html += `<th ${thStyle}>Location</th>`;
        html += `<th ${thStyle.replace('text-align: left;', 'text-align: right;')}>New</th>`; // Align right for numbers
        if (parseInt(clientId) !== 1) {
            html += `<th ${thStyle.replace('text-align: left;', 'text-align: right;')}>Recurring</th>`;
            html += `<th ${thStyle.replace('text-align: left;', 'text-align: right;')}>Totals</th>`;
        }
        html += '</tr></thead>';
        html += '<tbody>';

        locations.forEach(loc => {
            const newAtLoc = newPerLocationMap[loc.id] || 0;
            const recurringAtLoc = recurringPerLocationMap[loc.id] || 0;
            const totalAtLoc = newAtLoc + recurringAtLoc;
            html += '<tr>';
            html += `<td ${tdStyle}>${loc.id}</td>`;
            html += `<td ${tdStyle}>${loc.name}</td>`;
            html += `<td ${tdRightStyle}>${newAtLoc}</td>`;
            if (parseInt(clientId) !== 1) {
                html += `<td ${tdRightStyle}>${recurringAtLoc}</td>`;
                html += `<td ${tdRightStyle}>${totalAtLoc}</td>`;
            }
            html += '</tr>';
        });

        // Total row for locations
        html += '<tr>';
        html += `<td ${tdStyle}></td>`;
        html += `<td ${tdStyle}><b>TOTAL</b></td>`;
        html += `<td ${tdRightStyle}><b>${totalNewByLocation}</b></td>`;
        if (parseInt(clientId) !== 1) {
            html += `<td ${tdRightStyle}><b>${totalRecurringByLocation}</b></td>`;
            html += `<td ${tdRightStyle}><b>${grandTotalByLocation}</b></td>`;
        }
        html += '</tr>';
        html += '</tbody></table>';
    } else {
        html += '<p>No location-specific data available for this period.</p>';
    }

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
                        <b>2026 Volunteer Liability Waiver, Terms and conditions:</b><br>
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
                        2026 Volunteer Liability Waiver, Terms and conditions:\n
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
                        <b>2026 Exención de responsabilidad voluntaria, términos y condiciones:</b><br>
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
                        2026 Exención de responsabilidad voluntaria, términos y condiciones:\n
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

// ---------------------------------------------------------------------------
// New volunteer registration notification (sent to admin-configured recipients)
// ---------------------------------------------------------------------------

const VOLUNTEER_NOTIFICATION_BRAND = {
  rose: '#df3d7a',
  roseDark: '#c72f69',
  sky: '#11b3d1',
  textDark: '#434543',
  border: '#c5e1e1',
  lightCyan: '#d1f8f8',
  pageBg: '#f4fbfb'
};

const VOLUNTEER_NOTIFICATION_I18N = {
  en: {
    subject: (name) => `New volunteer registration: ${name}`,
    preheader: 'A new volunteer just completed the registration form.',
    brand: 'Bienestar Community',
    title: 'New Volunteer Registration',
    intro: 'A new volunteer has just completed the registration form. These are all the details they submitted:',
    sectionPersonal: 'Personal information',
    sectionDemographics: 'Demographics',
    sectionConsent: 'Consent & submission',
    labels: {
      firstname: 'First name',
      lastname: 'Last name',
      dateOfBirth: 'Date of birth',
      email: 'Email',
      phone: 'Phone',
      zipcode: 'ZIP code',
      location: 'Volunteer location',
      gender: 'Gender',
      ethnicity: 'Ethnicity',
      otherEthnicity: 'Other ethnicity',
      registeredLanguage: 'Registration language',
      consent: 'Legal consent',
      submittedOn: 'Submitted on'
    },
    consentAccepted: 'Accepted',
    consentVersion: (v) => `version ${v}`,
    signatureHeading: 'Signature',
    signatureUnavailable: 'Signature image not available.',
    notProvided: 'Not provided',
    footer: 'You are receiving this email because you are configured as a recipient of new volunteer registrations in Bienestar Community.',
    languageName: { en: 'English', es: 'Spanish' }
  },
  es: {
    subject: (name) => `Nuevo registro de voluntario: ${name}`,
    preheader: 'Una nueva persona voluntaria acaba de completar el formulario de registro.',
    brand: 'Bienestar Community',
    title: 'Nuevo registro de voluntario',
    intro: 'Una nueva persona voluntaria acaba de completar el formulario de registro. Estos son todos los datos que envió:',
    sectionPersonal: 'Información personal',
    sectionDemographics: 'Datos demográficos',
    sectionConsent: 'Consentimiento y envío',
    labels: {
      firstname: 'Nombre',
      lastname: 'Apellido',
      dateOfBirth: 'Fecha de nacimiento',
      email: 'Correo electrónico',
      phone: 'Teléfono',
      zipcode: 'Código postal',
      location: 'Locación de voluntariado',
      gender: 'Género',
      ethnicity: 'Etnia',
      otherEthnicity: 'Otra etnia',
      registeredLanguage: 'Idioma de registro',
      consent: 'Consentimiento legal',
      submittedOn: 'Enviado el'
    },
    consentAccepted: 'Aceptado',
    consentVersion: (v) => `versión ${v}`,
    signatureHeading: 'Firma',
    signatureUnavailable: 'Imagen de la firma no disponible.',
    notProvided: 'No proporcionado',
    footer: 'Estás recibiendo este correo porque estás configurado como destinatario de los nuevos registros de voluntarios en Bienestar Community.',
    languageName: { en: 'Inglés', es: 'Español' }
  }
};

function escapeHtmlValue(value) {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function normalizeVolunteerLanguage(language) {
  return language === 'es' ? 'es' : 'en';
}

function pickLocalizedName(field, language) {
  if (field === null || field === undefined) {
    return '';
  }
  if (typeof field === 'object') {
    return (language === 'es' ? field.es : field.en) || field.en || field.es || '';
  }
  return field;
}

function buildVolunteerNotificationContent(volunteerData, language, signatureCid) {
  const t = VOLUNTEER_NOTIFICATION_I18N[language] || VOLUNTEER_NOTIFICATION_I18N.en;
  const B = VOLUNTEER_NOTIFICATION_BRAND;

  const fullName = `${volunteerData.firstname || ''} ${volunteerData.lastname || ''}`.trim() || volunteerData.email || '-';
  const genderName = pickLocalizedName(volunteerData.gender, language);
  const ethnicityName = pickLocalizedName(volunteerData.ethnicity, language);
  const registeredLanguageName = t.languageName[normalizeVolunteerLanguage(volunteerData.registeredLanguage)];

  let consentValue = t.consentAccepted;
  if (volunteerData.legalConsentVersion) {
    consentValue += ` (${t.consentVersion(volunteerData.legalConsentVersion)})`;
  }
  if (volunteerData.legalConsentAcceptedAt) {
    consentValue += ` · ${volunteerData.legalConsentAcceptedAt}`;
  }

  const rows = [
    { section: t.sectionPersonal },
    { label: t.labels.firstname, value: volunteerData.firstname },
    { label: t.labels.lastname, value: volunteerData.lastname },
    { label: t.labels.dateOfBirth, value: volunteerData.dateOfBirth },
    { label: t.labels.email, value: volunteerData.email },
    { label: t.labels.phone, value: volunteerData.phone },
    { label: t.labels.zipcode, value: volunteerData.zipcode },
    { label: t.labels.location, value: volunteerData.locationCity },
    { section: t.sectionDemographics },
    { label: t.labels.gender, value: genderName },
    { label: t.labels.ethnicity, value: ethnicityName }
  ];

  if (volunteerData.otherEthnicity) {
    rows.push({ label: t.labels.otherEthnicity, value: volunteerData.otherEthnicity });
  }

  rows.push({ label: t.labels.registeredLanguage, value: registeredLanguageName });
  rows.push({ section: t.sectionConsent });
  rows.push({ label: t.labels.consent, value: consentValue });
  rows.push({ label: t.labels.submittedOn, value: volunteerData.submittedOn });

  // Plain-text version
  let text = `${t.title}\n\n${t.intro}\n\n`;
  rows.forEach((row) => {
    if (row.section) {
      text += `\n${row.section.toUpperCase()}\n`;
    } else {
      const value = (row.value === null || row.value === undefined || row.value === '') ? t.notProvided : row.value;
      text += `${row.label}: ${value}\n`;
    }
  });
  text += `\n${t.footer}\n`;

  // HTML rows
  const rowsHtml = rows.map((row) => {
    if (row.section) {
      return `<tr><td colspan="2" style="padding:22px 28px 8px 28px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${B.sky};">${escapeHtmlValue(row.section)}</td></tr>`;
    }
    const displayValue = (row.value === null || row.value === undefined || String(row.value).trim() === '')
      ? `<span style="color:#9aa6a6;font-style:italic;">${escapeHtmlValue(t.notProvided)}</span>`
      : escapeHtmlValue(row.value);
    return `<tr>
      <td style="padding:10px 28px;border-top:1px solid ${B.border};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:#7c8a8a;width:42%;vertical-align:top;">${escapeHtmlValue(row.label)}</td>
      <td style="padding:10px 28px;border-top:1px solid ${B.border};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;font-weight:600;color:${B.textDark};vertical-align:top;">${displayValue}</td>
    </tr>`;
  }).join('');

  const signatureBlock = signatureCid
    ? `<tr><td colspan="2" style="padding:22px 28px 8px 28px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${B.sky};">${escapeHtmlValue(t.signatureHeading)}</td></tr>
       <tr><td colspan="2" style="padding:6px 28px 24px 28px;border-top:1px solid ${B.border};">
         <img src="cid:${signatureCid}" alt="${escapeHtmlValue(t.signatureHeading)}" style="display:block;max-width:320px;width:100%;height:auto;border:1px solid ${B.border};border-radius:8px;background:#ffffff;">
       </td></tr>`
    : `<tr><td colspan="2" style="padding:22px 28px 8px 28px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${B.sky};">${escapeHtmlValue(t.signatureHeading)}</td></tr>
       <tr><td colspan="2" style="padding:6px 28px 24px 28px;border-top:1px solid ${B.border};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:#9aa6a6;font-style:italic;">${escapeHtmlValue(t.signatureUnavailable)}</td></tr>`;

  const html = `<!DOCTYPE html>
<html lang="${language}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${escapeHtmlValue(t.title)}</title>
</head>
<body style="margin:0;padding:0;background:${B.pageBg};">
  <span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;">${escapeHtmlValue(t.preheader)}</span>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${B.pageBg};padding:24px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="width:600px;max-width:100%;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 6px 24px rgba(67,69,67,0.08);">
          <!-- Header -->
          <tr>
            <td style="background:linear-gradient(135deg,${B.rose} 0%,${B.roseDark} 100%);padding:32px 28px;">
              <p style="margin:0 0 6px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:rgba(255,255,255,0.85);">${escapeHtmlValue(t.brand)}</p>
              <h1 style="margin:0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:24px;font-weight:700;color:#ffffff;">${escapeHtmlValue(t.title)}</h1>
              <p style="margin:10px 0 0 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;color:#ffffff;font-weight:600;">${escapeHtmlValue(fullName)}</p>
            </td>
          </tr>
          <!-- Intro -->
          <tr>
            <td style="padding:24px 28px 4px 28px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.6;color:${B.textDark};">${escapeHtmlValue(t.intro)}</td>
          </tr>
          <!-- Details -->
          <tr>
            <td style="padding:8px 0 0 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                ${rowsHtml}
                ${signatureBlock}
              </table>
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="padding:20px 28px 28px 28px;">
              <div style="border-top:2px solid ${B.lightCyan};padding-top:16px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:12px;line-height:1.6;color:#9aa6a6;">${escapeHtmlValue(t.footer)}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;

  // Sanitize the name used in the subject (an SMTP header) to prevent header
  // injection via CR/LF and to keep the subject a sensible length.
  const safeSubjectName = fullName.replace(/[\r\n\t]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 120);

  return { subject: t.subject(safeSubjectName), html, text };
}

/**
 * Sends the "new volunteer registration" notification to the admin-configured
 * recipients. Recipients are grouped by their preferred language so each one
 * receives the form rendered in their language. Never throws.
 *
 * @param {Object} volunteerData    Resolved volunteer info (names, not ids).
 * @param {Array}  recipients       [{ email, language }]
 * @param {Array}  signatureAttachments [{ filename, content(Buffer), contentType }]
 */
async function sendVolunteerRegistrationNotification(volunteerData, recipients, signatureAttachments = []) {
  try {
    const validRecipients = (recipients || []).filter(
      (r) => r && typeof r.email === 'string' && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(r.email.trim())
    );

    if (validRecipients.length === 0) {
      return { sent: 0, status: 200 };
    }

    // Group recipient emails by language (en | es), de-duplicating globally and
    // case-insensitively so an address never gets more than one notification.
    const groups = {};
    const seenEmails = new Set();
    validRecipients.forEach((r) => {
      const normalizedEmail = r.email.trim();
      const key = normalizedEmail.toLowerCase();
      if (seenEmails.has(key)) {
        return;
      }
      seenEmails.add(key);
      const lang = normalizeVolunteerLanguage(r.language);
      if (!groups[lang]) {
        groups[lang] = [];
      }
      groups[lang].push(normalizedEmail);
    });

    const hasSignature = Array.isArray(signatureAttachments) && signatureAttachments.length > 0;
    const signatureCid = hasSignature ? 'volunteer-signature' : null;
    const attachments = hasSignature
      ? [{
          filename: signatureAttachments[0].filename || 'signature.jpg',
          content: signatureAttachments[0].content,
          contentType: signatureAttachments[0].contentType || 'image/jpeg',
          cid: signatureCid
        }]
      : [];

    let sent = 0;
    for (const lang of Object.keys(groups)) {
      const content = buildVolunteerNotificationContent(volunteerData, lang, signatureCid);
      const mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        bcc: groups[lang].join(', '),
        subject: content.subject,
        text: content.text,
        html: content.html,
        attachments
      };

      try {
        const info = await transporter.sendMail(mailOptions);
        sent += groups[lang].length;
        console.log(`Volunteer registration notification (${lang}) sent to ${groups[lang].join(', ')}: ` + info.response);
      } catch (err) {
        console.log(`error sending volunteer registration notification (${lang}) to ${groups[lang].join(', ')}: `, err);
      }
    }

    return { sent, status: 200 };
  } catch (error) {
    console.log('Error in sendVolunteerRegistrationNotification: ', error);
    return { sent: 0, status: 500, error };
  }
}

async function sendAlertEmail(subject, body, emails) {
  const mailOptions = {
    from: 'bienestarcommunity@gmail.com',
    to: Array.isArray(emails) ? emails.join(', ') : emails,
    subject,
    text: body,
    html: `<pre style="font-family: monospace; white-space: pre-wrap;">${body
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')}</pre>`
  };

  await transporter.sendMail(mailOptions);
}

module.exports.sendVolunteerConfirmation = sendVolunteerConfirmation;

module.exports.sendVolunteerRegistrationNotification = sendVolunteerRegistrationNotification;

module.exports.sendTicketEmail = sendTicketEmail;

module.exports.sendEmailWithExcelAttachment = sendEmailWithExcelAttachment;

module.exports.sendAlertEmail = sendAlertEmail;
