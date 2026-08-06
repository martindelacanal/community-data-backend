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

// Send a scheduled report email with each workbook attached as an individual,
// non password-protected file (used for the admin reports). `attachments` is an
// array of { filename, content(Buffer) }. The branded HTML/summary body is the
// same as the zipped variant; only the attachment presentation differs.
async function sendReportEmailWithSeparateAttachments(subject, message, attachments, summaryObject, emails) {
  return new Promise((resolve) => {
    try {
      const validAttachments = (Array.isArray(attachments) ? attachments : []).filter(
        (file) => file && file.filename && file.content !== undefined && file.content !== null
      );

      const reportData = summaryObject ? summaryObject.emailReportData : null;
      const fullHtmlMessage = buildReportEmailHtmlSeparate(
        subject,
        message,
        reportData,
        validAttachments.map((file) => file.filename)
      );
      const fullTextMessage = buildReportEmailText(message, reportData);

      let mailOptions = {
        from: 'bienestarcommunity@gmail.com',
        to: emails.join(', '),
        subject: subject,
        text: fullTextMessage,
        html: fullHtmlMessage,
        attachments: validAttachments
      };

      transporter.sendMail(mailOptions, (err, info) => {
        if (err) {
          console.log(`error sendReportEmail to ${emails.join(', ')}: `, err);
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

// Highlighted note listing the individual (non password-protected) attachments.
function buildSeparateAttachmentsCallout(filenames) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const list = (Array.isArray(filenames) ? filenames : [])
    .map((name) => `<span style="font-weight:700;">${escapeHtmlValue(name)}</span>`)
    .join('<br>');
  return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${B.lightCyan};border:1px solid ${B.border};border-radius:12px;margin:4px 0 6px 0;">
    <tr><td style="padding:16px 20px;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.7;color:${B.textDark};">
      <strong style="color:${B.sky};">&#128206; Attached files</strong>&nbsp;&middot;&nbsp;<span style="color:#7c8a8a;">no password required</span><br>
      ${list}
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

// Variant of the branded report email for when the workbooks are attached as
// individual, non password-protected files (used for the admin reports). Same
// shell as buildReportEmailHtml; only the intro line and the attachment callout
// change so the recipient knows the files open directly, without a password.
function buildReportEmailHtmlSeparate(subject, message, reportData, filenames) {
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const isMonthly = /monthly/i.test(subject || '');
  const periodWord = isMonthly ? 'monthly' : 'weekly';
  const clientName = (reportData && reportData.clientName) ? reportData.clientName : 'Bienestar Community';
  const dateRangeDisplay = (reportData && reportData.dateRangeDisplay) ? reportData.dateRangeDisplay : '';

  const title = isMonthly ? 'Monthly Activity Report' : 'Weekly Activity Report';
  const subtitle = dateRangeDisplay ? `${clientName} · ${dateRangeDisplay}` : clientName;

  const intro = `Here is the ${periodWord} Bienestar Community activity summary for ${clientName}${dateRangeDisplay ? `, covering ${dateRangeDisplay}` : ''}. A quick overview is below, and the complete data set is attached as separate files that open directly, without a password.`;

  const bodyHtml = `
    <p style="margin:0 0 18px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${B.textDark};">${escapeHtmlValue(intro)}</p>
    ${buildSeparateAttachmentsCallout(filenames)}
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
        <!--[if mso]><table role="presentation" width="600" cellpadding="0" cellspacing="0"><tr><td><![endif]-->
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 6px 24px rgba(67,69,67,0.08);">
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
        <!--[if mso]></td></tr></table><![endif]-->
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
        <!--[if mso]><table role="presentation" width="600" cellpadding="0" cellspacing="0"><tr><td><![endif]-->
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 6px 24px rgba(67,69,67,0.08);">
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
        <!--[if mso]></td></tr></table><![endif]-->
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

// ---------------------------------------------------------------------------
// Health event volunteer credentials (account created from the event form)
// ---------------------------------------------------------------------------

const HEALTH_VOLUNTEER_CREDENTIALS_I18N = {
  en: {
    subject: (eventName) => `Your volunteer account — ${eventName}`,
    title: 'Welcome, volunteer!',
    intro: (eventName) => `Thank you for registering as a volunteer for <strong>${eventName}</strong>. An account was created for you in the Bienestar Community system — you will use it on the day of the event to scan participants at your assigned service point.`,
    usernameLabel: 'Username',
    passwordLabel: 'Password',
    loginHint: 'Sign in at',
    keepSafe: 'Please keep these credentials safe. You can change your password from My Account after signing in.',
    pendingApproval: 'Your request is pending review: you will be able to sign in with these credentials once our team approves your volunteer application. We will be in touch soon!',
    footer: 'Bienestar is Wellbeing — Community Health Events'
  },
  es: {
    subject: (eventName) => `Tu cuenta de voluntario — ${eventName}`,
    title: '¡Bienvenido/a, voluntario/a!',
    intro: (eventName) => `Gracias por registrarte como voluntario/a para <strong>${eventName}</strong>. Se creó una cuenta para ti en el sistema de Bienestar Community: la usarás el día del evento para escanear participantes en tu punto de servicio asignado.`,
    usernameLabel: 'Usuario',
    passwordLabel: 'Contraseña',
    loginHint: 'Inicia sesión en',
    keepSafe: 'Guarda estas credenciales en un lugar seguro. Puedes cambiar tu contraseña desde Mi Cuenta después de iniciar sesión.',
    pendingApproval: 'Tu solicitud está pendiente de revisión: podrás iniciar sesión con estas credenciales cuando nuestro equipo apruebe tu solicitud de voluntariado. ¡Te contactaremos pronto!',
    footer: 'Bienestar is Wellbeing — Eventos Comunitarios de Salud'
  }
};

async function sendHealthEventVolunteerCredentials({ to, language, eventNameEn, eventNameEs, username, password, pendingApproval = false }) {
  try {
    const lang = language === 'es' ? 'es' : 'en';
    const t = HEALTH_VOLUNTEER_CREDENTIALS_I18N[lang];
    const B = VOLUNTEER_NOTIFICATION_BRAND;
    const eventName = lang === 'es' ? (eventNameEs || eventNameEn) : (eventNameEn || eventNameEs);
    const loginUrl = 'https://bienestarcommunity.org/home';

    const html = `
      <div style="background:${B.pageBg};padding:24px 12px;font-family:'Segoe UI',Arial,sans-serif;color:${B.textDark};">
        <div style="width:100%;max-width:560px;margin:0 auto;background:#ffffff;border:2px solid ${B.border};border-radius:12px;overflow:hidden;box-sizing:border-box;">
          <div style="background:${B.lightCyan};padding:20px 24px;">
            <h1 style="margin:0;color:${B.rose};font-size:24px;">${t.title}</h1>
          </div>
          <div style="padding:24px;">
            <p style="margin:0 0 16px;font-size:15px;line-height:1.5;">${t.intro(eventName)}</p>
            <div style="background:${B.pageBg};border:2px solid ${B.border};border-radius:10px;padding:16px 20px;margin:0 0 16px;">
              <p style="margin:0 0 8px;font-size:14px;"><strong>${t.usernameLabel}:</strong>
                <span style="font-family:monospace;font-size:16px;color:${B.rose};">${username}</span></p>
              <p style="margin:0;font-size:14px;"><strong>${t.passwordLabel}:</strong>
                <span style="font-family:monospace;font-size:16px;color:${B.rose};">${password}</span></p>
            </div>
            ${pendingApproval ? `<div style="background:#fff7e6;border:2px solid #f0c36d;border-radius:10px;padding:12px 16px;margin:0 0 16px;">
              <p style="margin:0;font-size:14px;color:#8a5a00;">${t.pendingApproval}</p>
            </div>` : ''}
            <p style="margin:0 0 8px;font-size:14px;">${t.loginHint}: <a href="${loginUrl}" style="color:${B.sky};">${loginUrl}</a></p>
            <p style="margin:0;font-size:13px;color:#6b7280;">${t.keepSafe}</p>
            ${buildAppDownloadSectionHtml(lang)}
          </div>
          <div style="border-top:2px solid ${B.border};padding:14px 24px;font-size:12px;color:#6b7280;">${t.footer}</div>
        </div>
      </div>`;
    const text = `${t.title}\n\n${t.intro(eventName).replace(/<[^>]+>/g, '')}\n\n` +
      `${t.usernameLabel}: ${username}\n${t.passwordLabel}: ${password}\n\n` +
      (pendingApproval ? `${t.pendingApproval}\n\n` : '') +
      `${t.loginHint}: ${loginUrl}\n\n${t.keepSafe}\n` +
      buildAppDownloadSectionText(lang);

    await transporter.sendMail({
      from: 'bienestarcommunity@gmail.com',
      to,
      subject: t.subject(eventName),
      text,
      html
    });
  } catch (error) {
    console.log('Error sending health event volunteer credentials email:', error);
  }
}

// ---------------------------------------------------------------------------
// Mobile app download invitation (shared block for registrant-facing emails)
// ---------------------------------------------------------------------------

const MOBILE_APP_LINKS = {
  android: 'https://play.google.com/store/apps/details?id=com.bienestarcommunity.app',
  ios: 'https://apps.apple.com/ar/app/bienestar-community/id6761124586'
};

const MOBILE_APP_I18N = {
  en: {
    title: 'Take Bienestar Community with you!',
    text: 'Carry your event QR code, appointments and services in your pocket. Everything is faster and easier from our free mobile app.',
    playTag: 'Get it on',
    appTag: 'Download on the',
    playName: 'Google Play',
    appName: 'App Store',
    hint: 'Free for Android and iPhone.',
    textIntro: 'Download our free mobile app:'
  },
  es: {
    title: '¡Lleva Bienestar Community contigo!',
    text: 'Ten tu código QR del evento, tus citas y servicios en el bolsillo. Todo es más rápido y fácil desde nuestra app móvil gratuita.',
    playTag: 'Disponible en',
    appTag: 'Descárgala en',
    playName: 'Google Play',
    appName: 'App Store',
    hint: 'Gratis para Android y iPhone.',
    textIntro: 'Descarga nuestra app móvil gratuita:'
  }
};

/**
 * Email-safe "download our app" card: brand cyan card, two store badges built
 * with pure HTML/CSS (no remote images, so nothing gets blocked by clients).
 */
function buildAppDownloadSectionHtml(language) {
  const t = MOBILE_APP_I18N[language === 'es' ? 'es' : 'en'];
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  // Elastic badge: no fixed min-width, so narrow clients (Outlook mobile) can
  // shrink it instead of overflowing the viewport sideways.
  const badge = (href, tag, name) => `
    <a href="${href}" target="_blank" rel="noopener"
       style="display:inline-block;box-sizing:border-box;width:100%;max-width:220px;background:#1c1e21;border:1px solid #3a3d40;border-radius:12px;padding:10px 22px;text-decoration:none;text-align:left;">
      <span style="display:block;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:10px;font-weight:600;letter-spacing:0.09em;text-transform:uppercase;color:#9fe8f5;">${escapeHtmlValue(tag)}</span>
      <span style="display:block;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:17px;font-weight:700;color:#ffffff;line-height:1.3;">${escapeHtmlValue(name)}</span>
    </a>`;

  // Badges stacked one per row: two side-by-side cells have an irreducible
  // combined width (~420px) that horizontally overflowed Outlook mobile.
  return `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0 8px 0;">
    <tr>
      <td style="background:${B.lightCyan};background:linear-gradient(160deg,${B.lightCyan} 0%,#e9fbfb 100%);border:1px solid ${B.border};border-radius:14px;padding:26px 22px;text-align:center;">
        <div style="font-size:34px;line-height:1;margin-bottom:10px;">📱</div>
        <h3 style="margin:0 0 6px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:19px;font-weight:700;color:${B.textDark};">${escapeHtmlValue(t.title)}</h3>
        <p style="margin:0 0 18px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.6;color:#5c6a6a;">${escapeHtmlValue(t.text)}</p>
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0;">
          <tr><td align="center" style="padding:0 0 10px 0;">${badge(MOBILE_APP_LINKS.android, t.playTag, t.playName)}</td></tr>
          <tr><td align="center" style="padding:0;">${badge(MOBILE_APP_LINKS.ios, t.appTag, t.appName)}</td></tr>
        </table>
        <p style="margin:8px 0 0 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:12px;color:#7c8a8a;">${escapeHtmlValue(t.hint)}</p>
      </td>
    </tr>
  </table>`;
}

function buildAppDownloadSectionText(language) {
  const t = MOBILE_APP_I18N[language === 'es' ? 'es' : 'en'];
  return `\n${t.title}\n${t.textIntro}\n- Android (${t.playName}): ${MOBILE_APP_LINKS.android}\n- iPhone (${t.appName}): ${MOBILE_APP_LINKS.ios}\n`;
}

// ---------------------------------------------------------------------------
// Health event — beneficiary registration confirmation (sent to the registrant)
// ---------------------------------------------------------------------------

const HEALTH_BENEFICIARY_CONFIRMATION_I18N = {
  en: {
    subject: (eventName) => `You're registered! — ${eventName}`,
    eyebrow: 'Bienestar Community',
    title: 'Registration confirmed!',
    subtitle: (eventName) => eventName,
    preheader: 'Your spot is saved. Here are your event details.',
    intro: (firstname, eventName) => `Hi${firstname ? ' ' + firstname : ''}! Thank you for registering for <strong>${eventName}</strong>. Your spot is saved — we can't wait to see you there.`,
    whereLabel: 'Where',
    datesLabel: 'Your date(s)',
    timeLabel: 'Hours',
    appointmentsLabel: 'Your appointments',
    qrHint: 'On the day of the event, sign in with your account from our app or website and show your QR code at the entrance — that\'s your ticket to every service.',
    credentialsTitle: 'Your sign-in details',
    credentialsHint: 'Use them to sign in from our app or website. You will be asked to choose your own password the first time you sign in.',
    usernameLabel: 'Username',
    passwordLabel: 'Password',
    footer: 'Questions? Just reply to this email — we are happy to help. With love, the Bienestar Community team.'
  },
  es: {
    subject: (eventName) => `¡Registro confirmado! — ${eventName}`,
    eyebrow: 'Bienestar Community',
    title: '¡Registro confirmado!',
    subtitle: (eventName) => eventName,
    preheader: 'Tu lugar está reservado. Aquí tienes los detalles del evento.',
    intro: (firstname, eventName) => `¡Hola${firstname ? ' ' + firstname : ''}! Gracias por registrarte en <strong>${eventName}</strong>. Tu lugar está reservado, ¡te esperamos!`,
    whereLabel: 'Dónde',
    datesLabel: 'Tu(s) fecha(s)',
    timeLabel: 'Horario',
    appointmentsLabel: 'Tus citas',
    qrHint: 'El día del evento, inicia sesión con tu cuenta desde nuestra app o el sitio web y muestra tu código QR en la entrada: es tu pase para todos los servicios.',
    credentialsTitle: 'Tus datos para iniciar sesión',
    credentialsHint: 'Úsalos para iniciar sesión desde nuestra app o el sitio web. La primera vez que inicies sesión te pediremos que elijas tu propia contraseña.',
    usernameLabel: 'Usuario',
    passwordLabel: 'Contraseña',
    footer: '¿Preguntas? Simplemente responde a este correo, estamos para ayudarte. Con cariño, el equipo de Bienestar Community.'
  }
};

function formatHealthEventDate(dateStr, language) {
  if (!dateStr) return '';
  const m = moment(String(dateStr).slice(0, 10), 'YYYY-MM-DD', true);
  if (!m.isValid()) return String(dateStr);
  return language === 'es'
    ? m.locale('es').format('dddd D [de] MMMM [de] YYYY')
    : m.locale('en').format('dddd, MMMM D, YYYY');
}

function buildHealthBeneficiaryConfirmationContent({
  language, eventNameEn, eventNameEs, firstname,
  locationName, address, startTime, endTime,
  eventStartDate, eventEndDate, dates = [], appointments = [], credentials = null
}) {
  {
    const lang = language === 'es' ? 'es' : 'en';
    const t = HEALTH_BENEFICIARY_CONFIRMATION_I18N[lang];
    const B = VOLUNTEER_NOTIFICATION_BRAND;
    const eventName = lang === 'es' ? (eventNameEs || eventNameEn) : (eventNameEn || eventNameEs);

    const chosenDates = (dates || []).map(d => {
      const label = formatHealthEventDate(d.event_date || d, lang);
      const priority = d && d.priority_service ? ` · ${d.priority_service}` : '';
      return `${label}${priority}`;
    });
    const dateLines = chosenDates.length
      ? chosenDates
      : [ [formatHealthEventDate(eventStartDate, lang), formatHealthEventDate(eventEndDate, lang)]
          .filter((v, i, arr) => v && arr.indexOf(v) === i).join(' — ') ].filter(Boolean);
    const timeLine = startTime && endTime ? `${startTime} — ${endTime}` : (startTime || endTime || null);
    // service_key is a machine key ('dental', 'vision'); capitalize for the registrant-facing email.
    const appointmentLines = (appointments || []).map(a => {
      const rawKey = String(a.service_key || '');
      const service = rawKey ? rawKey.charAt(0).toUpperCase() + rawKey.slice(1) : '';
      return `${service} · ${formatHealthEventDate(a.slot_date, lang)} · ${a.start_time}`;
    });

    const detailRow = (label, valueHtml) => `
      <tr>
        <td style="padding:7px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;color:${B.sky};vertical-align:top;width:34%;">${escapeHtmlValue(label)}</td>
        <td style="padding:7px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.6;color:${B.textDark};font-weight:600;">${valueHtml}</td>
      </tr>`;

    let detailRows = '';
    detailRows += detailRow(t.whereLabel,
      `${escapeHtmlValue(locationName || '')}${address ? `<br><span style="font-weight:500;color:#5c6a6a;">${escapeHtmlValue(address)}</span>` : ''}`);
    if (dateLines.length) {
      detailRows += detailRow(t.datesLabel, dateLines.map(escapeHtmlValue).join('<br>'));
    }
    if (timeLine) {
      detailRows += detailRow(t.timeLabel, escapeHtmlValue(timeLine));
    }
    if (appointmentLines.length) {
      detailRows += detailRow(t.appointmentsLabel, appointmentLines.map(escapeHtmlValue).join('<br>'));
    }

    const bodyHtml = `
      <p style="margin:0 0 18px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${B.textDark};">${t.intro(escapeHtmlValue(firstname || ''), escapeHtmlValue(eventName))}</p>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:${B.lightCyan};border-radius:12px;margin:0 0 20px 0;">
        <tr>
          <td style="padding:18px 22px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0">${detailRows}</table>
          </td>
        </tr>
      </table>
      <p style="margin:0 0 4px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;line-height:1.7;color:#5c6a6a;">${escapeHtmlValue(t.qrHint)}</p>
      ${credentials && credentials.username && credentials.password ? `
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:16px 0 0 0;">
        <tr>
          <td style="background:${B.pageBg};border:2px solid ${B.border};border-radius:12px;padding:16px 20px;">
            <p style="margin:0 0 4px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;color:${B.sky};">${escapeHtmlValue(t.credentialsTitle)}</p>
            <p style="margin:0 0 6px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:${B.textDark};"><strong>${escapeHtmlValue(t.usernameLabel)}:</strong>
              <span style="font-family:monospace;font-size:16px;color:${B.rose};">${escapeHtmlValue(credentials.username)}</span></p>
            <p style="margin:0 0 8px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:${B.textDark};"><strong>${escapeHtmlValue(t.passwordLabel)}:</strong>
              <span style="font-family:monospace;font-size:16px;color:${B.rose};">${escapeHtmlValue(credentials.password)}</span></p>
            <p style="margin:0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:12px;line-height:1.6;color:#7c8a8a;">${escapeHtmlValue(t.credentialsHint)}</p>
          </td>
        </tr>
      </table>` : ''}
      ${buildAppDownloadSectionHtml(lang)}
    `;

    const html = wrapBrandedEmail({
      lang,
      eyebrow: t.eyebrow,
      title: t.title,
      subtitle: t.subtitle(eventName),
      preheader: t.preheader,
      bodyHtml,
      footerHtml: escapeHtmlValue(t.footer)
    });

    let text = `${t.title} — ${eventName}\n\n`;
    text += `${t.intro(firstname || '', eventName).replace(/<[^>]+>/g, '')}\n\n`;
    text += `${t.whereLabel}: ${locationName || ''}${address ? ' — ' + address : ''}\n`;
    if (dateLines.length) text += `${t.datesLabel}: ${dateLines.join(' | ')}\n`;
    if (timeLine) text += `${t.timeLabel}: ${timeLine}\n`;
    if (appointmentLines.length) text += `${t.appointmentsLabel}: ${appointmentLines.join(' | ')}\n`;
    text += `\n${t.qrHint}\n`;
    if (credentials && credentials.username && credentials.password) {
      text += `\n${t.credentialsTitle}\n${t.usernameLabel}: ${credentials.username}\n${t.passwordLabel}: ${credentials.password}\n${t.credentialsHint}\n`;
    }
    text += buildAppDownloadSectionText(lang);
    text += `\n${t.footer}\n`;

    return { subject: t.subject(eventName), html, text };
  }
}

/**
 * Confirmation sent to the beneficiary right after registering to a health
 * event (web form, logged-in flow or self-register). Includes the event
 * details, the chosen dates/appointments and the app download invitation.
 */
async function sendHealthEventBeneficiaryConfirmation(params) {
  try {
    if (!params || !params.to) return;
    const { subject, html, text } = buildHealthBeneficiaryConfirmationContent(params);
    await transporter.sendMail({
      from: 'bienestarcommunity@gmail.com',
      to: params.to,
      subject,
      text,
      html
    });
  } catch (error) {
    console.log('Error sending health event beneficiary confirmation email:', error);
  }
}

// ---------------------------------------------------------------------------
// Health event — new registration notification (sent to the per-event,
// per-audience recipient lists configured by the admin). Independent from the
// global food-distribution volunteer list (sendVolunteerRegistrationNotification).
// ---------------------------------------------------------------------------

const HEALTH_REGISTRATION_NOTIFICATION_I18N = {
  en: {
    subject: (roleWord, name, eventName) => `New ${roleWord} registration: ${name} — ${eventName}`,
    roleWord: { beneficiary: 'beneficiary', volunteer: 'volunteer' },
    title: { beneficiary: 'New Beneficiary Registration', volunteer: 'New Volunteer Registration' },
    preheader: 'A new registration form was just submitted.',
    intro: (eventName) => `A new registration form was just submitted for <strong>${eventName}</strong>. These are the details:`,
    sectionEvent: 'Event',
    sectionPerson: 'Registrant',
    sectionAttendance: 'Attendance',
    sectionAnswers: 'Form answers',
    labels: {
      event: 'Event', location: 'Location', source: 'Source', submittedOn: 'Submitted on',
      firstname: 'First name', lastname: 'Last name', email: 'Email', phone: 'Phone',
      dateOfBirth: 'Date of birth', zipcode: 'ZIP code', username: 'Username',
      dates: 'Chosen date(s)', appointments: 'Appointments'
    },
    consentAccepted: 'Accepted',
    notProvided: 'Not provided',
    noAnswers: 'This registration has no form answers.',
    footer: (eventName) => `You are receiving this email because you are configured as a recipient of new registrations for "${eventName}" in Bienestar Community.`
  },
  es: {
    subject: (roleWord, name, eventName) => `Nuevo registro de ${roleWord}: ${name} — ${eventName}`,
    roleWord: { beneficiary: 'beneficiario', volunteer: 'voluntario' },
    title: { beneficiary: 'Nuevo registro de beneficiario', volunteer: 'Nuevo registro de voluntario' },
    preheader: 'Se acaba de enviar un nuevo formulario de registro.',
    intro: (eventName) => `Se acaba de enviar un nuevo formulario de registro para <strong>${eventName}</strong>. Estos son los datos:`,
    sectionEvent: 'Evento',
    sectionPerson: 'Persona inscripta',
    sectionAttendance: 'Asistencia',
    sectionAnswers: 'Respuestas del formulario',
    labels: {
      event: 'Evento', location: 'Locación', source: 'Origen', submittedOn: 'Enviado el',
      firstname: 'Nombre', lastname: 'Apellido', email: 'Correo electrónico', phone: 'Teléfono',
      dateOfBirth: 'Fecha de nacimiento', zipcode: 'Código postal', username: 'Usuario',
      dates: 'Fecha(s) elegida(s)', appointments: 'Citas'
    },
    consentAccepted: 'Aceptado',
    notProvided: 'No proporcionado',
    noAnswers: 'Este registro no tiene respuestas de formulario.',
    footer: (eventName) => `Recibes este correo porque estás configurado como destinatario de los nuevos registros de "${eventName}" en Bienestar Community.`
  }
};

function formatHealthAnswerValue(answer, language, t) {
  if (answer.question_type === 'consent') return t.consentAccepted;
  if (answer.answer_text != null && String(answer.answer_text).trim() !== '') return String(answer.answer_text);
  if (answer.answer_number != null) return String(answer.answer_number);
  if (answer.answer_date != null && String(answer.answer_date) !== '') {
    // Defensive: mysql2 DATE columns arrive as JS Date objects unless normalized upstream.
    const value = answer.answer_date instanceof Date
      ? moment(answer.answer_date).format('YYYY-MM-DD')
      : String(answer.answer_date).slice(0, 10);
    return value;
  }
  const options = language === 'es' ? (answer.options_es || answer.options_en) : (answer.options_en || answer.options_es);
  if (options) return answer.other_text ? `${options} (${answer.other_text})` : String(options);
  return answer.other_text || null;
}

function buildHealthRegistrationNotificationContent(data, language) {
  const lang = language === 'es' ? 'es' : 'en';
  const t = HEALTH_REGISTRATION_NOTIFICATION_I18N[lang];
  const B = VOLUNTEER_NOTIFICATION_BRAND;
  const audience = data.audience === 'volunteer' ? 'volunteer' : 'beneficiary';
  const eventName = lang === 'es' ? (data.eventNameEs || data.eventNameEn) : (data.eventNameEn || data.eventNameEs);
  const user = data.user || {};
  const fullName = `${user.firstname || ''} ${user.lastname || ''}`.trim() || data.contactEmail || '-';

  const dateLines = (data.dates || []).map(d =>
    `${formatHealthEventDate(d.event_date, lang)}${d.priority_service ? ` · ${d.priority_service}` : ''}`);
  const appointmentLines = (data.appointments || []).map(a =>
    `${a.service_key} · ${formatHealthEventDate(a.slot_date, lang)} · ${a.start_time}`);

  const rows = [
    { section: t.sectionEvent },
    { label: t.labels.event, value: eventName },
    { label: t.labels.location, value: data.locationName },
    { label: t.labels.source, value: data.source },
    { label: t.labels.submittedOn, value: data.submittedOn },
    { section: t.sectionPerson },
    { label: t.labels.firstname, value: user.firstname },
    { label: t.labels.lastname, value: user.lastname },
    { label: t.labels.email, value: user.email || data.contactEmail },
    { label: t.labels.phone, value: user.phone },
    { label: t.labels.dateOfBirth, value: user.date_of_birth },
    { label: t.labels.zipcode, value: user.zipcode }
  ];
  if (audience === 'volunteer' && user.username) {
    rows.push({ label: t.labels.username, value: user.username });
  }
  if (dateLines.length || appointmentLines.length) {
    rows.push({ section: t.sectionAttendance });
    if (dateLines.length) rows.push({ label: t.labels.dates, value: dateLines.join('\n') });
    if (appointmentLines.length) rows.push({ label: t.labels.appointments, value: appointmentLines.join('\n') });
  }

  const rowHtml = (row) => {
    if (row.section) {
      return `<tr><td colspan="2" style="padding:20px 0 6px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${B.sky};">${escapeHtmlValue(row.section)}</td></tr>`;
    }
    const empty = row.value === null || row.value === undefined || String(row.value).trim() === '';
    const displayValue = empty
      ? `<span style="color:#9aa6a6;font-style:italic;">${escapeHtmlValue(t.notProvided)}</span>`
      : escapeHtmlValue(String(row.value)).replace(/\n/g, '<br>');
    return `<tr>
      <td style="padding:9px 0;border-top:1px solid ${B.border};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:#7c8a8a;width:40%;vertical-align:top;">${escapeHtmlValue(row.label)}</td>
      <td style="padding:9px 0;border-top:1px solid ${B.border};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;font-weight:600;color:${B.textDark};vertical-align:top;">${displayValue}</td>
    </tr>`;
  };

  const answers = data.answers || [];
  const answersHtml = answers.length
    ? answers.map(answer => {
        const question = lang === 'es' ? (answer.question_es || answer.question_en) : (answer.question_en || answer.question_es);
        const value = formatHealthAnswerValue(answer, lang, t);
        const displayValue = value == null || String(value).trim() === ''
          ? `<span style="color:#9aa6a6;font-style:italic;">${escapeHtmlValue(t.notProvided)}</span>`
          : escapeHtmlValue(String(value));
        return `<tr>
          <td style="padding:9px 0;border-top:1px solid ${B.border};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;color:#7c8a8a;width:55%;vertical-align:top;line-height:1.5;">${escapeHtmlValue(question || '')}</td>
          <td style="padding:9px 0;border-top:1px solid ${B.border};font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;font-weight:600;color:${B.textDark};vertical-align:top;">${displayValue}</td>
        </tr>`;
      }).join('')
    : `<tr><td style="padding:9px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:14px;color:#9aa6a6;font-style:italic;">${escapeHtmlValue(t.noAnswers)}</td></tr>`;

  const bodyHtml = `
    <p style="margin:0 0 12px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${B.textDark};">${t.intro(escapeHtmlValue(eventName))}</p>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
      ${rows.map(rowHtml).join('')}
      <tr><td colspan="2" style="padding:20px 0 6px 0;font-family:'Quicksand',Helvetica,Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${B.sky};">${escapeHtmlValue(t.sectionAnswers)}</td></tr>
      ${answersHtml}
    </table>
  `;

  const html = wrapBrandedEmail({
    lang,
    eyebrow: 'Bienestar Community',
    title: t.title[audience],
    subtitle: fullName,
    preheader: t.preheader,
    bodyHtml,
    footerHtml: escapeHtmlValue(t.footer(eventName))
  });

  let text = `${t.title[audience]} — ${eventName}\n\n`;
  for (const row of rows) {
    if (row.section) {
      text += `\n${String(row.section).toUpperCase()}\n`;
    } else {
      const value = (row.value === null || row.value === undefined || String(row.value).trim() === '') ? t.notProvided : row.value;
      text += `${row.label}: ${value}\n`;
    }
  }
  text += `\n${t.sectionAnswers.toUpperCase()}\n`;
  if (answers.length) {
    for (const answer of answers) {
      const question = lang === 'es' ? (answer.question_es || answer.question_en) : (answer.question_en || answer.question_es);
      const value = formatHealthAnswerValue(answer, lang, t);
      text += `${question}: ${value == null || String(value).trim() === '' ? t.notProvided : value}\n`;
    }
  } else {
    text += `${t.noAnswers}\n`;
  }
  text += `\n${t.footer(eventName)}\n`;

  const safeSubjectName = fullName.replace(/[\r\n\t]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 120);
  const safeSubjectEvent = String(eventName || '').replace(/[\r\n\t]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 120);
  return { subject: t.subject(t.roleWord[audience], safeSubjectName, safeSubjectEvent), html, text };
}

/**
 * Sends the "new health event registration" notification to the per-event
 * recipient list configured by the admin (health_event_notification_recipient).
 * Recipients are grouped by preferred language. Never throws.
 *
 * @param {Object} data       { audience, eventNameEn, eventNameEs, locationName, source,
 *                              submittedOn, contactEmail, user, dates, appointments, answers }
 * @param {Array}  recipients [{ email, language }]
 */
async function sendHealthEventRegistrationNotification(data, recipients) {
  try {
    const validRecipients = (recipients || []).filter(
      (r) => r && typeof r.email === 'string' && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(r.email.trim())
    );
    if (validRecipients.length === 0) {
      return { sent: 0 };
    }

    const groups = {};
    const seenEmails = new Set();
    validRecipients.forEach((r) => {
      const normalizedEmail = r.email.trim();
      const key = normalizedEmail.toLowerCase();
      if (seenEmails.has(key)) return;
      seenEmails.add(key);
      const lang = r.language === 'es' ? 'es' : 'en';
      if (!groups[lang]) groups[lang] = [];
      groups[lang].push(normalizedEmail);
    });

    let sent = 0;
    for (const lang of Object.keys(groups)) {
      const content = buildHealthRegistrationNotificationContent(data, lang);
      try {
        const info = await transporter.sendMail({
          from: 'bienestarcommunity@gmail.com',
          to: groups[lang].join(', '),
          subject: content.subject,
          text: content.text,
          html: content.html
        });
        sent += Array.isArray(info.accepted) ? info.accepted.length : groups[lang].length;
        console.log(`Health event registration notification (${lang}) sent to ${groups[lang].join(', ')}: ` + info.response);
      } catch (err) {
        console.log(`error sending health event registration notification (${lang}) to ${groups[lang].join(', ')}: `, err);
      }
    }
    return { sent };
  } catch (error) {
    console.log('Error in sendHealthEventRegistrationNotification: ', error);
    return { sent: 0, error };
  }
}

module.exports.sendHealthEventBeneficiaryConfirmation = sendHealthEventBeneficiaryConfirmation;

module.exports.sendHealthEventRegistrationNotification = sendHealthEventRegistrationNotification;

module.exports.sendHealthEventVolunteerCredentials = sendHealthEventVolunteerCredentials;

module.exports.sendVolunteerConfirmation = sendVolunteerConfirmation;

module.exports.sendVolunteerRegistrationNotification = sendVolunteerRegistrationNotification;

module.exports.sendTicketEmail = sendTicketEmail;

module.exports.sendEmailWithExcelAttachment = sendEmailWithExcelAttachment;

module.exports.sendReportEmailWithSeparateAttachments = sendReportEmailWithSeparateAttachments;

// Exposed for previewing/testing the report email rendering without sending.
module.exports.buildReportEmailHtml = buildReportEmailHtml;
module.exports.buildReportEmailText = buildReportEmailText;
module.exports.buildHealthBeneficiaryConfirmationContent = buildHealthBeneficiaryConfirmationContent;
module.exports.buildHealthRegistrationNotificationContent = buildHealthRegistrationNotificationContent;
module.exports.buildAppDownloadSectionHtml = buildAppDownloadSectionHtml;
module.exports.buildVolunteerConfirmationContent = buildVolunteerConfirmationContent;

module.exports.sendAlertEmail = sendAlertEmail;
