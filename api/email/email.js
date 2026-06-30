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

async function createPasswordProtectedZipExcel(excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, extraZipFiles = []) {
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

    if (Array.isArray(extraZipFiles)) {
      extraZipFiles.forEach(file => {
        if (file && file.name && file.content !== undefined && file.content !== null) {
          archive.append(file.content, { name: file.name });
        }
      });
    }

    archive.finalize();
  });
}

async function sendEmailWithExcelAttachment(subject, message, excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, emails, extraZipFiles = []) {
  return new Promise(async (resolve) => {
    try {
      const zipContent = await createPasswordProtectedZipExcel(excelRawData, excelNewRegistrations, summaryObject, excelAllNewRegistrations, password, extraZipFiles);
      const date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");
      const zipFilename = `community-data-${date}.zip`;
      const reportData = summaryObject ? summaryObject.emailReportData : null;

      const fullHtmlMessage = buildReportEmailHtml(subject, message, reportData, zipFilename);
      const fullTextMessage = buildReportEmailText(message, reportData);

      let mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: emails.join(', '),
        subject: subject,
        text: fullTextMessage,
        html: fullHtmlMessage,
        attachments: [
          {
            filename: zipFilename,
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

// ---------------------------------------------------------------------------
// Scheduled report emails (weekly & monthly, for both clients and the admin).
// Branded, friendly layout that mirrors the volunteer notification emails: the
// same rose/sky palette, Quicksand typography and email-safe table shell.
// Entry point: buildReportEmailHtml(subject, message, reportData, zipFilename).
// ---------------------------------------------------------------------------

// Small uppercase, sky-coloured section label (matches the volunteer email).
function reportSectionLabel(text) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  return `<p style="margin:26px 0 12px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${B.sky};">${escapeHtmlValue(text)}</p>`;
}

// A row of headline "stat" cards (e.g. New / Recurring / Total).
// Each card: { label, value, bg, numberColor, labelColor }.
function buildReportStatCards(cards) {
  const width = Math.floor(100 / cards.length);
  const cells = cards.map((c) => `
        <td width="${width}%" valign="top" style="padding:6px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-radius:14px;background:${c.bg};">
            <tr><td align="center" style="padding:18px 8px;">
              <div style="font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:28px;line-height:1;font-weight:700;color:${c.numberColor};">${escapeHtmlValue(c.value)}</div>
              <div style="margin-top:7px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:12px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${c.labelColor};">${escapeHtmlValue(c.label)}</div>
            </td></tr>
          </table>
        </td>`).join('');
  return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 2px 0;"><tr>${cells}</tr></table>`;
}

// A branded data table. `headers`/`totalRow` are arrays of { text, align };
// `rows` is an array of cell-arrays of { text, align, muted }.
function buildReportTable(headers, rows, totalRow) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const headCells = headers.map((h) =>
    `<th style="padding:11px 14px;background:${B.sky};color:#ffffff;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:12px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;text-align:${h.align || 'left'};">${escapeHtmlValue(h.text)}</th>`
  ).join('');

  const bodyRows = rows.map((cells, i) => {
    const bg = (i % 2 === 1) ? B.pageBg : '#ffffff';
    const tds = cells.map((c) =>
      `<td style="padding:10px 14px;border-top:1px solid ${B.border};background:${bg};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;font-weight:600;text-align:${c.align || 'left'};color:${c.muted ? '#9aa6a6' : B.textDark};">${escapeHtmlValue(c.text)}</td>`
    ).join('');
    return `<tr>${tds}</tr>`;
  }).join('');

  let totalHtml = '';
  if (totalRow) {
    const tds = totalRow.map((c) =>
      `<td style="padding:12px 14px;border-top:2px solid ${B.sky};background:${B.lightCyan};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;font-weight:700;text-align:${c.align || 'left'};color:${B.textDark};">${escapeHtmlValue(c.text)}</td>`
    ).join('');
    totalHtml = `<tr>${tds}</tr>`;
  }

  return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:separate;border-spacing:0;border:1px solid ${B.border};border-radius:12px;overflow:hidden;margin:0 0 6px 0;">
    <thead><tr>${headCells}</tr></thead>
    <tbody>${bodyRows}${totalHtml}</tbody>
  </table>`;
}

// Highlighted note about the password-protected attachment.
function buildAttachmentCallout(zipFilename) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${B.lightCyan};border:1px solid ${B.border};border-radius:12px;margin:4px 0 6px 0;">
    <tr><td style="padding:16px 20px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.7;color:${B.textDark};">
      <strong style="color:${B.sky};">&#128206; Attached file</strong><br>
      <span style="font-weight:700;">${escapeHtmlValue(zipFilename)}</span>&nbsp;&middot;&nbsp;<span style="color:#7c8a8a;">password-protected</span><br>
      <span style="font-size:13px;color:#7c8a8a;">Includes the raw data, the new registrations (with and without a health plan) and the summary workbook.</span>
    </td></tr>
  </table>`;
}

// Inner report block: headline stat cards + health-plan + per-location tables.
function generateSummaryHtmlReport(reportData) {
  if (!reportData) {
    return `<p style="margin:8px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:#9aa6a6;font-style:italic;">No summary data is available for this period.</p>`;
  }

  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const {
    newCount = 0, recurringCount = 0, totalNewRecurring = 0,
    newHealthPlanYes = 0, newHealthPlanNo = 0, newHealthPlanUnanswered = 0, totalNewHealthPlan = 0,
    locations = [], newPerLocationMap = {}, recurringPerLocationMap = {},
    totalNewByLocation = 0, totalRecurringByLocation = 0, grandTotalByLocation = 0,
    clientId
  } = reportData;

  const isClientOne = parseInt(clientId, 10) === 1;

  // 1) Headline numbers
  const cards = isClientOne
    ? [{ label: 'New participants', value: newCount, bg: B.rose, numberColor: '#ffffff', labelColor: 'rgba(255,255,255,0.85)' }]
    : [
        { label: 'New', value: newCount, bg: B.lightCyan, numberColor: B.rose, labelColor: '#7c8a8a' },
        { label: 'Recurring', value: recurringCount, bg: B.lightCyan, numberColor: B.sky, labelColor: '#7c8a8a' },
        { label: 'Total', value: totalNewRecurring, bg: B.rose, numberColor: '#ffffff', labelColor: 'rgba(255,255,255,0.85)' }
      ];

  let html = reportSectionLabel(isClientOne ? 'Participants' : 'Participants overview');
  html += buildReportStatCards(cards);

  // 2) Health plan (new participants)
  html += reportSectionLabel('Health plan (new participants)');
  html += buildReportTable(
    [{ text: 'Coverage' }, { text: 'Participants', align: 'right' }],
    [
      [{ text: 'Has a health plan' }, { text: newHealthPlanYes, align: 'right' }],
      [{ text: 'No health plan' }, { text: newHealthPlanNo, align: 'right' }],
      [{ text: 'Unanswered', muted: true }, { text: newHealthPlanUnanswered, align: 'right' }]
    ],
    [{ text: 'Total' }, { text: totalNewHealthPlan, align: 'right' }]
  );

  // 3) Per-location breakdown
  html += reportSectionLabel('By location');
  if (locations && locations.length > 0) {
    const headers = isClientOne
      ? [{ text: 'ID' }, { text: 'Location' }, { text: 'New', align: 'right' }]
      : [{ text: 'ID' }, { text: 'Location' }, { text: 'New', align: 'right' }, { text: 'Recurring', align: 'right' }, { text: 'Total', align: 'right' }];

    const rows = locations.map((loc) => {
      const newAtLoc = Number(newPerLocationMap[loc.id] || 0);
      const recurringAtLoc = Number(recurringPerLocationMap[loc.id] || 0);
      const totalAtLoc = newAtLoc + recurringAtLoc;
      return isClientOne
        ? [{ text: loc.id, muted: true }, { text: loc.name }, { text: newAtLoc, align: 'right' }]
        : [{ text: loc.id, muted: true }, { text: loc.name }, { text: newAtLoc, align: 'right' }, { text: recurringAtLoc, align: 'right' }, { text: totalAtLoc, align: 'right' }];
    });

    const totalRow = isClientOne
      ? [{ text: '' }, { text: 'TOTAL' }, { text: totalNewByLocation, align: 'right' }]
      : [{ text: '' }, { text: 'TOTAL' }, { text: totalNewByLocation, align: 'right' }, { text: totalRecurringByLocation, align: 'right' }, { text: grandTotalByLocation, align: 'right' }];

    html += buildReportTable(headers, rows, totalRow);
  } else {
    html += `<p style="margin:8px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:#9aa6a6;font-style:italic;">No location-specific data is available for this period.</p>`;
  }

  return html;
}

// Full branded HTML email for a scheduled report (wraps the summary in the
// shared rose/sky shell with header, intro, attachment note and footer).
function buildReportEmailHtml(subject, message, reportData, zipFilename) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const isMonthly = /monthly/i.test(subject || '');
  const periodWord = isMonthly ? 'monthly' : 'weekly';
  const clientName = (reportData && reportData.clientName) ? reportData.clientName : 'Bienestar Community';
  const dateRangeDisplay = (reportData && reportData.dateRangeDisplay) ? reportData.dateRangeDisplay : '';

  const title = isMonthly ? 'Monthly Activity Report' : 'Weekly Activity Report';
  const subtitle = dateRangeDisplay ? `${clientName} · ${dateRangeDisplay}` : clientName;

  const intro = `Here is the ${periodWord} Bienestar Community activity summary for ${clientName}${dateRangeDisplay ? `, covering ${dateRangeDisplay}` : ''}. A quick overview is below, and the complete data set is attached as a password-protected file.`;

  const bodyHtml = `
    <p style="margin:0 0 18px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${B.textDark};">${escapeHtmlValue(intro)}</p>
    ${buildAttachmentCallout(zipFilename)}
    ${generateSummaryHtmlReport(reportData)}
  `;

  const footerHtml = 'You are receiving this automated report because your address is configured as a recipient in Bienestar Community. If you have any questions, just reply to this email &mdash; we are happy to help.<br><br>With gratitude, the Bienestar Community team.';

  return wrapBrandedEmail({
    lang: 'en',
    eyebrow: 'Bienestar Community',
    title,
    subtitle,
    preheader: dateRangeDisplay ? `${title} for ${clientName} (${dateRangeDisplay})` : `${title} for ${clientName}`,
    bodyHtml,
    footerHtml
  });
}

// Plain-text counterpart: keep the original message and append a text summary.
function buildReportEmailText(message, reportData) {
  let text = String(message || '');
  const summaryText = generateSummaryTextReport(reportData);
  if (summaryText) {
    text += `\n\n----------------------------------------\nSUMMARY\n----------------------------------------\n${summaryText}`;
  }
  return text;
}

function generateSummaryTextReport(reportData) {
  if (!reportData) { return 'No summary data is available for this period.'; }
  const {
    newCount = 0, recurringCount = 0, totalNewRecurring = 0,
    newHealthPlanYes = 0, newHealthPlanNo = 0, newHealthPlanUnanswered = 0, totalNewHealthPlan = 0,
    locations = [], newPerLocationMap = {}, recurringPerLocationMap = {},
    totalNewByLocation = 0, totalRecurringByLocation = 0, grandTotalByLocation = 0,
    clientId
  } = reportData;
  const isClientOne = parseInt(clientId, 10) === 1;

  let t = '';
  if (isClientOne) {
    t += `Participants\n  New participants: ${newCount}\n`;
  } else {
    t += `Participants overview\n  New: ${newCount}\n  Recurring: ${recurringCount}\n  Total: ${totalNewRecurring}\n`;
  }

  t += `\nHealth plan (new participants)\n  Has a health plan: ${newHealthPlanYes}\n  No health plan: ${newHealthPlanNo}\n  Unanswered: ${newHealthPlanUnanswered}\n  Total: ${totalNewHealthPlan}\n`;

  t += `\nBy location\n`;
  if (locations && locations.length > 0) {
    locations.forEach((loc) => {
      const newAtLoc = Number(newPerLocationMap[loc.id] || 0);
      const recurringAtLoc = Number(recurringPerLocationMap[loc.id] || 0);
      if (isClientOne) {
        t += `  [${loc.id}] ${loc.name}: ${newAtLoc}\n`;
      } else {
        t += `  [${loc.id}] ${loc.name}: New ${newAtLoc}, Recurring ${recurringAtLoc}, Total ${newAtLoc + recurringAtLoc}\n`;
      }
    });
    if (isClientOne) {
      t += `  TOTAL: ${totalNewByLocation}\n`;
    } else {
      t += `  TOTAL: New ${totalNewByLocation}, Recurring ${totalRecurringByLocation}, Total ${grandTotalByLocation}\n`;
    }
  } else {
    t += `  No location-specific data is available for this period.\n`;
  }

  return t;
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

// Shared branded e-mail shell (rose/sky brand, Quicksand, email-safe table layout).
function wrapBrandedEmail({ lang = 'en', eyebrow = 'Bienestar Community', title = '', subtitle = '', preheader = '', bodyHtml = '', footerHtml = '' }) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const subtitleHtml = subtitle
    ? `<p style="margin:10px 0 0 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;color:#ffffff;font-weight:600;">${escapeHtmlValue(subtitle)}</p>`
    : '';

  return `<!DOCTYPE html>
<html lang="${lang}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${escapeHtmlValue(title)}</title>
</head>
<body style="margin:0;padding:0;background:${B.pageBg};">
  <span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;">${escapeHtmlValue(preheader)}</span>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${B.pageBg};padding:24px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="width:600px;max-width:100%;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 6px 24px rgba(67,69,67,0.08);">
          <tr>
            <td style="background:${B.rose};background:linear-gradient(135deg,${B.rose} 0%,${B.roseDark} 100%);padding:32px 28px;">
              <p style="margin:0 0 6px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:rgba(255,255,255,0.85);">${escapeHtmlValue(eyebrow)}</p>
              <h1 style="margin:0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:24px;font-weight:700;color:#ffffff;">${escapeHtmlValue(title)}</h1>
              ${subtitleHtml}
            </td>
          </tr>
          <tr>
            <td style="padding:24px 28px 8px 28px;">${bodyHtml}</td>
          </tr>
          <tr>
            <td style="padding:8px 28px 28px 28px;">
              <div style="border-top:2px solid ${B.lightCyan};padding-top:16px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;line-height:1.7;color:#7c8a8a;">${footerHtml}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;
}

// Bilingual content for the volunteer confirmation (Terms & Conditions) email.
// The legal wording is preserved verbatim from the original implementation.
const VOLUNTEER_CONFIRMATION_I18N = {
  en: {
    subject: 'Terms and conditions signed',
    eyebrow: 'Bienestar Community',
    title: 'Thank you for volunteering!',
    subtitle: 'Your registration is confirmed',
    preheader: 'A copy of the Volunteer Liability Waiver, Terms and Conditions you signed.',
    intro: 'Hi, and welcome! We are so glad you are joining the Bienestar is Well-being volunteer team. For your records, here is a copy of the Volunteer Liability Waiver, Terms and Conditions you reviewed and signed during your registration.',
    locationLabel: 'Location chosen',
    dateLabel: 'Date',
    waiverTitle: '2026 Volunteer Liability Waiver, Terms and Conditions',
    introClause: 'I have agreed to volunteer my services ("Activity") for Bienestar is Well-being ("Organization"). I further understand that Bienestar is Well-being provides no compensation for my services and that I am not entitled to any benefits from the Organization, including but not limited to workers\' compensation benefits.',
    sections: [
      {
        heading: 'Assumption of Risk',
        paragraphs: [
          'I understand that there are risks of injury, death and damage to property from performing the Volunteer Activity for the Organization. I attest and verify that I possess the physical fitness and ability to perform the Activity and that I have no physical limitations that would affect my performance of the Activity. If I do not feel that I am capable of performing the Activity, I assume the responsibility of informing whomever is designated as the on-site Supervisor or Team Lead. In consideration for being allowed to participate in the Activity, I hereby assume the risk of, and responsibility for, any such injury, death or damage which I may sustain arising out of or in any way connected with performance of the Activity, including injury, death or damage resulting from any acts or omissions, whether negligent or not, or any property or equipment owned or supplied by or on behalf of the Organization, its officials, officers, employees, agents, volunteers, and any other promoters, operators or co-sponsors of the Activity.'
        ]
      },
      {
        heading: 'Release and Indemnification',
        paragraphs: [
          'In consideration for being allowed to participate in the Activity, I hereby release, waive and discharge the Organization, its officials, officers, employees, agents, volunteers, and any other promoters, operators or co-sponsors of the Activity, from any and all liability, claims, or causes of action arising out of or in any way connected with my performance of the Activity, or upon its acts or omissions, whether negligent or not (“Waiver”). I agree to this Waiver on behalf of myself, my heirs, executors, administrators and assigns.',
          'As further consideration for being allowed to participate in the Activity, I hereby agree, on behalf of myself, my heirs, executors, administrators and assigns, to indemnify and hold harmless the Organization, its officials, officers, employees, agents, volunteers, and any other promoters, operators or co-sponsors of the Activity, from any and all claims for compensation, personal injury, property damage or wrongful death caused by my negligence or willful misconduct, in the performance of the Activity.'
        ]
      },
      {
        heading: 'Knowing and Voluntary Execution',
        paragraphs: [
          'I have carefully read this Waiver and Release Form and fully understand its contents. I understand that I am giving up valuable legal rights. I knowingly and voluntarily give up these rights of my own free will.'
        ]
      }
    ],
    footer: 'Thank you for giving your time and energy to your community. If you have any questions, just reply to this email — we are here to help. With gratitude, the Bienestar Community team.'
  },
  es: {
    subject: 'Términos y condiciones firmados',
    eyebrow: 'Bienestar Community',
    title: '¡Gracias por tu voluntariado!',
    subtitle: 'Tu registro está confirmado',
    preheader: 'Una copia de la Exención de responsabilidad voluntaria, Términos y condiciones que firmaste.',
    intro: '¡Hola y bienvenido/a! Nos alegra muchísimo que te sumes al equipo de voluntariado de Bienestar is Well-being. Para tu registro, aquí tienes una copia de la Exención de responsabilidad voluntaria, Términos y condiciones que revisaste y firmaste durante tu inscripción.',
    locationLabel: 'Locación elegida',
    dateLabel: 'Fecha',
    waiverTitle: '2026 Exención de responsabilidad voluntaria, términos y condiciones',
    introClause: 'Acepto ofrecer mis servicios como voluntario (“Actividad”) para Bienestar is Well-being (“Organización”). Además, entiendo que Bienestar is Well-being no proporciona compensación por mis servicios y que no tengo derecho a ningún beneficio de la Organización, incluidos, entre otros, los beneficios de compensación laboral.',
    sections: [
      {
        heading: 'Asunción de Riesgo',
        paragraphs: [
          'Entiendo que existen riesgos de lesiones, muerte y daños a la propiedad al realizar la actividad de voluntariado para la Organización. Doy fe y verifico que poseo la aptitud física y la capacidad para realizar la Actividad y que no tengo limitaciones físicas que puedan afectar mi desempeño de la Actividad. Si no me siento capaz de realizar la Actividad, asumo la responsabilidad de informar a quien esté designado como Supervisor en el sitio o Líder del equipo. En consideración a que se me permita participar en la Actividad, por la presente asumo el riesgo y la responsabilidad por cualquier lesión, muerte o daño que pueda sufrir como resultado de o de alguna manera relacionado con la realización de la Actividad, incluidas lesiones, muerte o daño resultante de cualquier acto u omisión, ya sea negligente o no, o cualquier propiedad o equipo de propiedad o suministrado por o en nombre de la Organización, sus funcionarios, funcionarios, empleados, agentes, voluntarios y cualquier otro promotor, operador o co -patrocinadores de la Actividad.'
        ]
      },
      {
        heading: 'Liberación e Indemnización',
        paragraphs: [
          'En consideración por permitirme participar en la Actividad, por la presente libero, renuncio y descargo a la Organización, sus funcionarios, funcionarios, empleados, agentes, voluntarios y cualquier otro promotor, operador o copatrocinador de la Actividad. de toda responsabilidad, reclamo o causa de acción que surja de o esté relacionado de alguna manera con mi desempeño de la Actividad, o por sus actos u omisiones, ya sean negligentes o no (“Renuncia”). Acepto esta Renuncia en mi nombre, el de mis herederos, albaceas, administradores y cesionarios.',
          'Como consideración adicional para poder participar en la Actividad, por la presente acepto, en mi nombre y el de mis herederos, ejecutores, administradores y cesionarios, indemnizar y eximir de responsabilidad a la Organización, sus funcionarios, funcionarios, empleados, agentes, voluntarios y cualquier otro promotor, operador o copatrocinador de la Actividad, de todos y cada uno de los reclamos de compensación, lesiones personales, daños a la propiedad o muerte por negligencia causados por mi negligencia o mala conducta intencional, en el desempeño de la Actividad.'
        ]
      },
      {
        heading: 'Conocimiento y ejecución voluntaria',
        paragraphs: [
          'He leído atentamente este Formulario de exención y liberación y comprendo plenamente su contenido. Entiendo que estoy renunciando a valiosos derechos legales. Renuncio consciente y voluntariamente a estos derechos por mi propia voluntad.'
        ]
      }
    ],
    footer: 'Gracias por dedicar tu tiempo y energía a tu comunidad. Si tienes alguna pregunta, simplemente responde a este correo: estamos para ayudarte. Con gratitud, el equipo de Bienestar Community.'
  }
};

function buildVolunteerConfirmationContent(locationCity, date, language) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const t = VOLUNTEER_CONFIRMATION_I18N[language === 'es' ? 'es' : 'en'];

  const sectionsHtml = t.sections.map((section) => {
    const paragraphsHtml = section.paragraphs.map((paragraph) =>
      `<p style="margin:0 0 12px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.7;color:#5c6a6a;">${escapeHtmlValue(paragraph)}</p>`
    ).join('');
    return `<h3 style="margin:22px 0 8px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;font-weight:700;color:${B.rose};">${escapeHtmlValue(section.heading)}</h3>${paragraphsHtml}`;
  }).join('');

  const bodyHtml = `
    <p style="margin:0 0 18px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${B.textDark};">${escapeHtmlValue(t.intro)}</p>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${B.lightCyan};border-radius:12px;margin:0 0 22px 0;">
      <tr>
        <td style="padding:16px 20px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.8;color:${B.textDark};">
          <strong style="color:${B.sky};">${escapeHtmlValue(t.locationLabel)}:</strong> ${escapeHtmlValue(locationCity)}<br>
          <strong style="color:${B.sky};">${escapeHtmlValue(t.dateLabel)}:</strong> ${escapeHtmlValue(date)}
        </td>
      </tr>
    </table>
    <h2 style="margin:0 0 10px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:17px;font-weight:700;color:${B.textDark};">${escapeHtmlValue(t.waiverTitle)}</h2>
    <p style="margin:0 0 12px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.7;color:#5c6a6a;">${escapeHtmlValue(t.introClause)}</p>
    ${sectionsHtml}
  `;

  const html = wrapBrandedEmail({
    lang: language === 'es' ? 'es' : 'en',
    eyebrow: t.eyebrow,
    title: t.title,
    subtitle: t.subtitle,
    preheader: t.preheader,
    bodyHtml,
    footerHtml: escapeHtmlValue(t.footer)
  });

  let text = `${t.title}\n\n${t.intro}\n\n${t.locationLabel}: ${locationCity}\n${t.dateLabel}: ${date}\n\n${t.waiverTitle}\n${t.introClause}\n`;
  t.sections.forEach((section) => {
    text += `\n${section.heading}\n${section.paragraphs.join('\n')}\n`;
  });
  text += `\n${t.footer}\n`;

  return { subject: t.subject, html, text };
}

async function sendVolunteerConfirmation(volunteerEmail, locationCity, language) {
  try {
    const date = moment().tz("America/Los_Angeles").format("MM-DD-YYYY");
    const { subject, html, text } = buildVolunteerConfirmationContent(locationCity, date, language);

    const mailOptions = {
      from: 'bienestarcommunity@gmail.com',
      to: volunteerEmail,
      subject: subject,
      text: text,
      html: html
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
    const deliveryResults = [];
    for (const lang of Object.keys(groups)) {
      const content = buildVolunteerNotificationContent(volunteerData, lang, signatureCid);
      const mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: groups[lang].join(', '),
        subject: content.subject,
        text: content.text,
        html: content.html,
        attachments
      };

      try {
        const info = await transporter.sendMail(mailOptions);
        const acceptedCount = Array.isArray(info.accepted) ? info.accepted.length : groups[lang].length;
        sent += acceptedCount;
        deliveryResults.push({
          language: lang,
          recipients: groups[lang].length,
          accepted: info.accepted || [],
          rejected: info.rejected || []
        });
        console.log(`Volunteer registration notification (${lang}) sent to ${groups[lang].join(', ')}: ` + info.response);
      } catch (err) {
        deliveryResults.push({
          language: lang,
          recipients: groups[lang].length,
          accepted: [],
          rejected: groups[lang],
          error: err && err.message ? err.message : err
        });
        console.log(`error sending volunteer registration notification (${lang}) to ${groups[lang].join(', ')}: `, err);
      }
    }

    return { sent, status: 200, results: deliveryResults };
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

// Exposed for previewing/testing the report email rendering without sending.
module.exports.buildReportEmailHtml = buildReportEmailHtml;
module.exports.buildReportEmailText = buildReportEmailText;

module.exports.sendAlertEmail = sendAlertEmail;
