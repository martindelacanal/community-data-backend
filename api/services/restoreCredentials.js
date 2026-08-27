const crypto = require('node:crypto');
const jwt = require('jsonwebtoken');
const webauthn = require('@simplewebauthn/server');
const { buildRestoreAuthBinding } = require('../utils/restoreAuthBinding');

class RestoreCredentialError extends Error {
  constructor(status = 401, code = 'restore_verification_failed') {
    super(code);
    this.status = status;
    this.code = code;
  }
}

function digest(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function equalDigest(left, right) {
  return typeof left === 'string' && typeof right === 'string'
    && left.length === right.length
    && crypto.timingSafeEqual(Buffer.from(left), Buffer.from(right));
}

function requireBase64Url(value, maxLength, exactLength) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]+$/.test(value)
      || value.length > maxLength || (exactLength && value.length !== exactLength)
      || Buffer.from(value, 'base64url').toString('base64url') !== value) {
    throw new RestoreCredentialError(400, 'invalid_restore_request');
  }
  return value;
}

function requireCredential(value, purpose) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || value.type !== 'public-key' || !value.response
      || typeof value.response !== 'object' || Array.isArray(value.response)) {
    throw new RestoreCredentialError(400, 'invalid_restore_request');
  }
  requireBase64Url(value.id, 2048);
  if (value.id !== value.rawId) throw new RestoreCredentialError(400, 'invalid_restore_request');
  requireBase64Url(value.response.clientDataJSON, 4096);
  if (purpose === 'registration') {
    requireBase64Url(value.response.attestationObject, 22000);
  } else {
    requireBase64Url(value.response.authenticatorData, 4096);
    requireBase64Url(value.response.signature, 4096);
    if (value.response.userHandle != null) requireBase64Url(value.response.userHandle, 86);
  }
  return value;
}

function createRestoreCredentialsService({ repository, config, clock = Date.now }) {
  const supportedAlgorithmIDs = [-7, -257];

  function assertAvailable() {
    if (!config.enabled || !config.jwtSecret || !config.origins?.length) {
      throw new RestoreCredentialError(503, 'restore_unavailable');
    }
  }

  function isActive(user) {
    return !!user && user.enabled === 'Y' && user.deleted === 'N'
      && typeof user.password === 'string' && user.password.length > 0
      && typeof user.role === 'string' && user.role.length > 0;
  }

  function hmac(domain, value) {
    return crypto.createHmac('sha256', config.jwtSecret)
      .update(domain + '\0' + value).digest();
  }

  function accountBinding(user) {
    return buildRestoreAuthBinding(user, config.jwtSecret);
  }

  function userHandle(user) {
    return hmac('android-restore-user-v1', String(user.id)).toString('base64url');
  }

  function revocationToken(credentialId, authBinding) {
    return hmac('android-restore-revoke-v1', credentialId + '\0' + authBinding).toString('base64url');
  }

  function cancellationToken(requestId) {
    return hmac('android-restore-cancel-v1', requestId).toString('base64url');
  }

  async function issueOptions(publicKey, purpose, user = null) {
    const requestId = crypto.randomBytes(32).toString('base64url');
    const ttlMs = config.challengeTtlMs;
    await repository.issueChallenge({
      requestId, challenge: publicKey.challenge, purpose, ttlMs,
      userId: user ? Number(user.id) : null,
      userHandle: user ? userHandle(user) : null,
      authBinding: user ? accountBinding(user) : null,
    });
    return { requestId, publicKey, expiresAt: new Date(clock() + ttlMs).toISOString() };
  }

  async function consume(requestId, purpose, userId) {
    requireBase64Url(requestId, 43, 43);
    const challenge = await repository.consumeChallenge(requestId, purpose, userId);
    if (!challenge) throw new RestoreCredentialError();
    return challenge;
  }

  async function verifyWebAuthn(operation) {
    try {
      const verification = await operation();
      if (!verification.verified) throw new RestoreCredentialError();
      return verification;
    } catch {
      // WebAuthn parser messages may contain client data and challenge values.
      // Do not expose or log those messages.
      throw new RestoreCredentialError();
    }
  }

  return {
    async registrationOptions(userId, sessionBinding) {
      assertAvailable();
      const user = await repository.findAccount(userId);
      if (!isActive(user) || user.reset_password === 'Y'
          || !equalDigest(accountBinding(user), sessionBinding)) throw new RestoreCredentialError();
      const handle = userHandle(user);
      const publicKey = await webauthn.generateRegistrationOptions({
        rpName: config.rpName, rpID: config.rpID,
        userID: new Uint8Array(Buffer.from(handle, 'base64url')),
        userName: handle, userDisplayName: config.rpName,
        attestationType: 'none', supportedAlgorithmIDs,
        authenticatorSelection: { residentKey: 'required', userVerification: 'discouraged' },
        timeout: 60000,
      });
      const options = await issueOptions(publicKey, 'registration', user);
      return { ...options, cancellationToken: cancellationToken(options.requestId) };
    },
    async registrationVerify(userId, { requestId, credential: value }, sessionBinding) {
      assertAvailable();
      const challenge = await consume(requestId, 'registration', userId);
      if (!equalDigest(challenge.authBinding, sessionBinding)) throw new RestoreCredentialError();
      const credential = requireCredential(value, 'registration');
      const verification = await verifyWebAuthn(() => webauthn.verifyRegistrationResponse({
        response: credential, expectedChallenge: challenge.challenge,
        expectedOrigin: config.origins, expectedRPID: config.rpID,
        expectedType: 'webauthn.create', supportedAlgorithmIDs,
        // Restore is intentionally silent. This exception applies only to
        // these Android-only endpoints; origin/RP/challenge still must match.
        requireUserPresence: false, requireUserVerification: false,
      }));
      const registered = verification.registrationInfo?.credential;
      if (!registered || registered.id !== credential.id) throw new RestoreCredentialError();
      const token = revocationToken(registered.id, challenge.authBinding);
      let saved;
      try {
        saved = await repository.registerCredential({
          credentialId: registered.id, userId, userHandle: challenge.userHandle,
          publicKey: registered.publicKey, counter: registered.counter,
          authBinding: challenge.authBinding, revocationHash: digest(token),
          registrationRequestId: requestId,
        }, (user) => isActive(user) && user.reset_password !== 'Y'
          && equalDigest(accountBinding(user), challenge.authBinding));
      } catch (error) {
        if (error.code === 'ER_DUP_ENTRY') throw new RestoreCredentialError();
        throw error;
      }
      if (!saved) throw new RestoreCredentialError();
      return { credentialId: registered.id, revocationToken: token };
    },
    async authenticationOptions() {
      assertAvailable();
      const publicKey = await webauthn.generateAuthenticationOptions({
        rpID: config.rpID, userVerification: 'discouraged', timeout: 60000,
      });
      // An anonymous restore never discloses account or credential IDs.
      delete publicKey.allowCredentials;
      return issueOptions(publicKey, 'authentication');
    },
    async authenticationVerify({ requestId, credential: value }) {
      assertAvailable();
      const challenge = await consume(requestId, 'authentication', null);
      const credential = requireCredential(value, 'authentication');
      const session = await repository.authenticateCredential(credential.id, async ({ user, credential: saved }) => {
        if (!isActive(user) || !equalDigest(accountBinding(user), saved.authBinding)
            || (credential.response.userHandle != null && credential.response.userHandle !== saved.userHandle)) {
          throw new RestoreCredentialError();
        }
        const verification = await verifyWebAuthn(() => webauthn.verifyAuthenticationResponse({
          response: credential, expectedChallenge: challenge.challenge,
          expectedOrigin: config.origins, expectedRPID: config.rpID,
          expectedType: 'webauthn.get',
          credential: { id: saved.credentialId, publicKey: saved.publicKey, counter: saved.counter },
          requireUserVerification: false,
          // requireUserVerification:false alone still requires UP. Android
          // Restore also lacks UP; SimpleWebAuthn's FIDO option handles both
          // while retaining the real signature, RP, origin and counter checks.
          advancedFIDOConfig: { userVerification: 'discouraged' },
        }));
        const data = JSON.stringify({
          id: Number(user.id), firstname: user.firstname, username: user.username,
          email: user.email, client_id: user.client_id, role: user.role,
          language: user.language, enabled: user.enabled,
        });
        const token = jwt.sign({ data, restore_auth_binding: accountBinding(user) },
          config.jwtSecret, { algorithm: 'HS256', expiresIn: '6h' });
        return {
          counter: verification.authenticationInfo.newCounter,
          session: {
            token,
            expiresAt: new Date(jwt.decode(token).exp * 1000).toISOString(),
            reset_password: user.reset_password,
            language: user.language,
            credentialId: saved.credentialId,
            revocationToken: revocationToken(saved.credentialId, saved.authBinding),
          },
        };
      });
      if (!session) throw new RestoreCredentialError();
      return session;
    },
    async revoke({ credentialId, revocationToken: token }) {
      requireBase64Url(credentialId, 2048);
      requireBase64Url(token, 43, 43);
      await repository.revokeCredential(credentialId, digest(token));
      // Same response for a missing, revoked or unowned key. This capability
      // grants no login, enumeration or access to other credentials.
      return { revoked: true };
    },
    async cancelRegistration({ requestId, cancellationToken: token }) {
      requireBase64Url(requestId, 43, 43);
      requireBase64Url(token, 43, 43);
      if (!config.jwtSecret) throw new RestoreCredentialError(503, 'restore_unavailable');
      if (equalDigest(cancellationToken(requestId), token)) {
        await repository.cancelRegistration(requestId);
      }
      return { cancelled: true };
    },
  };
}

module.exports = { createRestoreCredentialsService, RestoreCredentialError };
