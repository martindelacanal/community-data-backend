const path = require('path');

require('dotenv').config({
  path: path.resolve(__dirname, '../.env')
});

const { google } = require('googleapis');

const GOOGLE_DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive'];

const FILES_TO_CHECK = [
  { label: 'Health metrics', envVar: 'HEALTH_METRICS_DRIVE_FILE_ID' },
  { label: 'Volunteers', envVar: 'VOLUNTEERS_DRIVE_FILE_ID' },
  { label: 'Workers', envVar: 'WORKERS_DRIVE_FILE_ID' },
  { label: 'Tickets', envVar: 'TICKETS_DRIVE_FILE_ID' },
  { label: 'Tickets with food', envVar: 'TICKETS_WITH_FOOD_DRIVE_FILE_ID' },
  { label: 'Beneficiary summary', envVar: 'BENEFICIARY_SUMMARY_DRIVE_FILE_ID' },
  { label: 'Delivery summary', envVar: 'DELIVERY_SUMMARY_DRIVE_FILE_ID' }
];

function buildCredentials() {
  return {
    type: process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT_TYPE,
    project_id: process.env.GOOGLE_DRIVE_PROJECT_ID,
    private_key_id: process.env.GOOGLE_DRIVE_PRIVATE_KEY_ID,
    private_key: process.env.GOOGLE_DRIVE_PRIVATE_KEY
      ? process.env.GOOGLE_DRIVE_PRIVATE_KEY.replace(/\\n/g, '\n')
      : undefined,
    client_email: process.env.GOOGLE_DRIVE_CLIENT_EMAIL,
    client_id: process.env.GOOGLE_DRIVE_CLIENT_ID,
    auth_uri: process.env.GOOGLE_DRIVE_AUTH_URI,
    token_uri: process.env.GOOGLE_DRIVE_TOKEN_URI,
    auth_provider_x509_cert_url: process.env.GOOGLE_DRIVE_AUTH_PROVIDER_X509_CERT_URL,
    client_x509_cert_url: process.env.GOOGLE_DRIVE_CLIENT_X509_CERT_URL,
    universe_domain: process.env.GOOGLE_DRIVE_UNIVERSE_DOMAIN
  };
}

function maskFileId(value) {
  if (!value) {
    return '(empty)';
  }
  const trimmed = String(value).trim();
  if (trimmed.length <= 8) {
    return trimmed;
  }
  return `${trimmed.slice(0, 4)}...${trimmed.slice(-4)}`;
}

async function main() {
  const summary = {
    envCheck: { ok: true, missing: [] },
    auth: { ok: false, error: null, clientEmail: null },
    folder: { ok: false, error: null, id: null, name: null },
    files: [],
    about: { ok: false, error: null, storage: null }
  };

  console.log('--- 1) Verificando variables de entorno ---');
  const requiredEnv = [
    'GOOGLE_DRIVE_SERVICE_ACCOUNT_TYPE',
    'GOOGLE_DRIVE_PROJECT_ID',
    'GOOGLE_DRIVE_PRIVATE_KEY_ID',
    'GOOGLE_DRIVE_PRIVATE_KEY',
    'GOOGLE_DRIVE_CLIENT_EMAIL',
    'GOOGLE_DRIVE_CLIENT_ID',
    'HEALTH_METRICS_DRIVE_FOLDER_ID'
  ];
  for (const name of requiredEnv) {
    if (!process.env[name]) {
      summary.envCheck.missing.push(name);
    }
  }
  if (summary.envCheck.missing.length > 0) {
    summary.envCheck.ok = false;
    console.log(`FAIL: faltan variables: ${summary.envCheck.missing.join(', ')}`);
  } else {
    console.log('OK: variables base presentes.');
  }
  console.log(`  HEALTH_METRICS_DRIVE_SYNC_ENABLED = ${process.env.HEALTH_METRICS_DRIVE_SYNC_ENABLED}`);
  console.log(`  HEALTH_METRICS_DRIVE_FOLDER_ID    = ${process.env.HEALTH_METRICS_DRIVE_FOLDER_ID}`);
  console.log(`  GOOGLE_DRIVE_CLIENT_EMAIL         = ${process.env.GOOGLE_DRIVE_CLIENT_EMAIL}`);

  console.log('\n--- 2) Probando autenticacion con la cuenta de servicio ---');
  let drive;
  try {
    const auth = new google.auth.GoogleAuth({
      credentials: buildCredentials(),
      scopes: GOOGLE_DRIVE_SCOPES
    });
    const authClient = await auth.getClient();
    drive = google.drive({ version: 'v3', auth: authClient });
    summary.auth.ok = true;
    summary.auth.clientEmail = process.env.GOOGLE_DRIVE_CLIENT_EMAIL;
    console.log(`OK: token obtenido para ${summary.auth.clientEmail}`);
  } catch (error) {
    summary.auth.error = error.message;
    console.log(`FAIL: ${error.message}`);
    console.log('\nResumen final:');
    console.log(JSON.stringify(summary, null, 2));
    process.exit(1);
  }

  console.log('\n--- 3) Probando about() para ver cuota/identidad ---');
  try {
    const about = await drive.about.get({ fields: 'user, storageQuota' });
    summary.about.ok = true;
    summary.about.storage = about.data.storageQuota;
    console.log(`OK: identidad = ${about.data.user && about.data.user.emailAddress}`);
    console.log(`  storageQuota = ${JSON.stringify(about.data.storageQuota)}`);
  } catch (error) {
    summary.about.error = error.message;
    console.log(`FAIL: ${error.message}`);
  }

  console.log('\n--- 4) Verificando carpeta destino ---');
  const folderId = (process.env.HEALTH_METRICS_DRIVE_FOLDER_ID || '').trim();
  if (!folderId) {
    summary.folder.error = 'HEALTH_METRICS_DRIVE_FOLDER_ID vacia';
    console.log('FAIL: HEALTH_METRICS_DRIVE_FOLDER_ID vacia');
  } else {
    try {
      const folder = await drive.files.get({
        fileId: folderId,
        fields: 'id, name, mimeType, driveId, capabilities, webViewLink',
        supportsAllDrives: true
      });
      summary.folder.ok = true;
      summary.folder.id = folder.data.id;
      summary.folder.name = folder.data.name;
      summary.folder.driveId = folder.data.driveId || null;
      summary.folder.canEdit = folder.data.capabilities && folder.data.capabilities.canEdit;
      console.log(`OK: carpeta "${folder.data.name}" (${folder.data.id})`);
      console.log(`  mimeType = ${folder.data.mimeType}`);
      console.log(`  driveId  = ${folder.data.driveId || '(My Drive personal)'}`);
      console.log(`  canEdit  = ${folder.data.capabilities && folder.data.capabilities.canEdit}`);
      console.log(`  link     = ${folder.data.webViewLink}`);
    } catch (error) {
      summary.folder.error = error.message;
      console.log(`FAIL: ${error.message}`);
    }
  }

  console.log('\n--- 5) Verificando metadata de cada CSV ---');
  for (const file of FILES_TO_CHECK) {
    const fileId = (process.env[file.envVar] || '').trim();
    const fileSummary = {
      label: file.label,
      envVar: file.envVar,
      fileId: maskFileId(fileId),
      ok: false,
      error: null
    };

    if (!fileId) {
      fileSummary.error = 'env var vacia';
      summary.files.push(fileSummary);
      console.log(`SKIP ${file.label}: ${file.envVar} vacia`);
      continue;
    }

    try {
      const meta = await drive.files.get({
        fileId,
        fields: 'id, name, mimeType, parents, modifiedTime, size, capabilities, trashed, owners',
        supportsAllDrives: true
      });
      fileSummary.ok = true;
      fileSummary.name = meta.data.name;
      fileSummary.parents = meta.data.parents;
      fileSummary.modifiedTime = meta.data.modifiedTime;
      fileSummary.size = meta.data.size;
      fileSummary.canEdit = meta.data.capabilities && meta.data.capabilities.canEdit;
      fileSummary.trashed = meta.data.trashed;
      fileSummary.owners = (meta.data.owners || []).map(o => o.emailAddress);
      const inFolder = Array.isArray(meta.data.parents) && meta.data.parents.includes(folderId);
      fileSummary.inFolder = inFolder;

      console.log(`OK ${file.label}:`);
      console.log(`    name         = ${meta.data.name}`);
      console.log(`    parents      = ${(meta.data.parents || []).join(',')}`);
      console.log(`    inFolder     = ${inFolder}`);
      console.log(`    modifiedTime = ${meta.data.modifiedTime}`);
      console.log(`    size         = ${meta.data.size}`);
      console.log(`    canEdit      = ${meta.data.capabilities && meta.data.capabilities.canEdit}`);
      console.log(`    trashed      = ${meta.data.trashed}`);
      console.log(`    owners       = ${(meta.data.owners || []).map(o => o.emailAddress).join(',')}`);
    } catch (error) {
      fileSummary.error = error.message;
      console.log(`FAIL ${file.label} (${file.envVar}=${maskFileId(fileId)}): ${error.message}`);
    }

    summary.files.push(fileSummary);
  }

  console.log('\n--- Resumen JSON ---');
  console.log(JSON.stringify(summary, null, 2));
}

main().catch(error => {
  console.error('Diagnostic crashed:', error);
  process.exit(1);
});
