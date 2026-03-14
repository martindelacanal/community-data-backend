const { Readable } = require('stream');
const { google } = require('googleapis');

const GOOGLE_DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive'];

function getGoogleDriveCredentialsFromEnv() {
  const credentials = {
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

  const missing = [
    'GOOGLE_DRIVE_SERVICE_ACCOUNT_TYPE',
    'GOOGLE_DRIVE_PROJECT_ID',
    'GOOGLE_DRIVE_PRIVATE_KEY_ID',
    'GOOGLE_DRIVE_PRIVATE_KEY',
    'GOOGLE_DRIVE_CLIENT_EMAIL',
    'GOOGLE_DRIVE_CLIENT_ID',
    'GOOGLE_DRIVE_AUTH_URI',
    'GOOGLE_DRIVE_TOKEN_URI',
    'GOOGLE_DRIVE_AUTH_PROVIDER_X509_CERT_URL',
    'GOOGLE_DRIVE_CLIENT_X509_CERT_URL',
    'GOOGLE_DRIVE_UNIVERSE_DOMAIN'
  ].filter(envName => !process.env[envName]);

  if (missing.length > 0) {
    throw new Error(`Missing Google Drive credentials in env: ${missing.join(', ')}`);
  }

  return credentials;
}

async function getGoogleDriveClient() {
  const auth = new google.auth.GoogleAuth({
    credentials: getGoogleDriveCredentialsFromEnv(),
    scopes: GOOGLE_DRIVE_SCOPES
  });

  const authClient = await auth.getClient();
  return google.drive({
    version: 'v3',
    auth: authClient
  });
}

async function getGoogleDriveFileMetadata(fileId) {
  if (!fileId) {
    throw new Error('Google Drive fileId is required');
  }

  const drive = await getGoogleDriveClient();
  const response = await drive.files.get({
    fileId,
    fields: 'id, name, parents, mimeType, webViewLink',
    supportsAllDrives: true
  });

  return response.data;
}

async function updateGoogleDriveFile({ fileId, fileName, content, mimeType = 'text/csv; charset=utf-8' }) {
  if (!fileId) {
    throw new Error('Google Drive fileId is required');
  }

  const drive = await getGoogleDriveClient();
  const bodyContent = Buffer.isBuffer(content)
    ? content
    : Buffer.from(String(content ?? ''), 'utf8');

  const response = await drive.files.update({
    fileId,
    requestBody: fileName ? { name: fileName } : undefined,
    media: {
      mimeType,
      body: Readable.from([bodyContent])
    },
    fields: 'id, name, parents, webViewLink, webContentLink',
    supportsAllDrives: true
  });

  return response.data;
}

module.exports = {
  getGoogleDriveFileMetadata,
  updateGoogleDriveFile
};
