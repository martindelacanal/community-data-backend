const DEFAULT_RP_ID = 'bienestarcommunity.org';

// Verified in Play Console's App integrity page: Play App Signing, followed by
// the owner's upload/release certificate. Never include the local debug key.
const RELEASE_SIGNING_SHA256 = [
  '15:1B:53:2B:25:90:82:EB:1F:CC:8E:5B:3E:88:AF:50:24:86:44:3A:E4:43:A9:E5:C6:8C:0F:F4:4B:44:74:42',
  '6A:69:87:34:92:75:CE:ED:DA:13:47:DB:CC:76:2B:29:25:EF:1B:4F:24:74:EF:1F:91:9D:6F:65:75:66:74:31',
];

function fingerprintToOrigin(fingerprint) {
  const hex = String(fingerprint).replace(/:/g, '');
  if (!/^[a-fA-F0-9]{64}$/.test(hex)) {
    throw new Error('Invalid Android Restore signing fingerprint');
  }
  return `android:apk-key-hash:${Buffer.from(hex, 'hex').toString('base64url')}`;
}

function getRestoreCredentialsConfig(env = process.env) {
  const rpID = env.RESTORE_CREDENTIALS_RP_ID || DEFAULT_RP_ID;
  if (!/^[a-z0-9]+(?:[.-][a-z0-9]+)+$/.test(rpID)) {
    throw new Error('Invalid Android Restore RP ID');
  }
  const origins = RELEASE_SIGNING_SHA256.map(fingerprintToOrigin);
  // A development key must be explicitly opted into and cannot be used by a
  // production process, including one whose NODE_ENV has not been configured.
  if (env.RESTORE_CREDENTIALS_DEV_CERT_SHA256) {
    if (!['development', 'test'].includes(env.NODE_ENV)
        || env.RESTORE_CREDENTIALS_ALLOW_DEV_CERT !== 'true') {
      throw new Error('Android Restore development certificates are disabled');
    }
    origins.push(fingerprintToOrigin(env.RESTORE_CREDENTIALS_DEV_CERT_SHA256));
  }
  return {
    enabled: env.RESTORE_CREDENTIALS_ENABLED !== 'false',
    rpID,
    rpName: 'Bienestar Community',
    origins,
    jwtSecret: env.JWT_SECRET,
    challengeTtlMs: 5 * 60 * 1000,
  };
}

module.exports = { getRestoreCredentialsConfig, fingerprintToOrigin, RELEASE_SIGNING_SHA256 };
