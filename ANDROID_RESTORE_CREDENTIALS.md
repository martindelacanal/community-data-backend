# Android Restore Credentials

This feature supplements the existing sign-in flow for the Android app
`com.bienestarcommunity.app`. It does not replace password login, change the
`data` JSON inside existing JWTs, or add a web/iOS credential flow.

## HTTP contract

All routes use POST, JSON, HTTPS and the prefix `/api/auth/restore`.
Responses use `Cache-Control: no-store`. WebAuthn credential bodies are the
parsed native `responseJson` object, not an additional JSON string.

| Route | Authorization and body | Response |
| --- | --- | --- |
| `/registration/options` | Bearer JWT; `{}` | `{requestId, publicKey, expiresAt, cancellationToken}` |
| `/registration/verify` | Same account's Bearer JWT; `{requestId, credential}` | `{credentialId, revocationToken}` |
| `/registration/cancel` | No JWT; `{requestId, cancellationToken}` | `{cancelled: true}` |
| `/authentication/options` | No JWT; `{}` | `{requestId, publicKey, expiresAt}` |
| `/authentication/verify` | No JWT; `{requestId, credential}` | `{token, expiresAt, reset_password, language, credentialId, revocationToken}` |
| `/revoke` | No JWT; `{credentialId, revocationToken}` | `{revoked: true}` |

`expiresAt` in options is the challenge expiry, five minutes. In authentication
verification it is the new JWT's expiry, six hours. The backend enforces expiry
regardless of a client clock or a cached value.

Persist the cancellation capability **before** native creation/verification.
Keep it until the verified credential's revocation capability has been saved.
On logout or an interrupted enrollment, queue cancellation for retry if offline.
Cancellation works before registration, during verification, and after a lost
post-commit response. It affects only that registration operation. Even after
the challenge is cleaned up, the credential retains its registration request ID
so an offline cancellation can still remove it.

The revocation capability is returned both at enrollment and after a valid
restore signature. It remains usable after JWT expiry, but only deletes that
credential; it cannot sign in, enumerate keys, or revoke other devices' keys.
An invalid/missing-key capability returns the same success-shaped response.

## Validation and account state

- `@simplewebauthn/server@13.3.3` verifies the challenge, ceremony type,
  Android signing origin, RP ID, public key and real assertion signature.
- Restore has no user-presence or user-verification interaction. Registration
  uses `requireUserPresence: false` and `requireUserVerification: false`.
  Assertion verification uses `advancedFIDOConfig.userVerification:
  'discouraged'`; setting only `requireUserVerification: false` is insufficient.
  These settings apply only to these Android Restore endpoints.
- Anonymous options contain no account selectors or `allowCredentials`.
  The registered key identifies the account. A supplied user handle must match
  its saved opaque handle. Client-supplied roles/user IDs never authorize login.
- Each verification consumes its challenge atomically, even when cryptographic
  verification fails. Authentication deletes the challenge. Registration retains
  a consumed/cancelled operation until expiry and locks it again before insert.
- Credential use locks the user and credential, and updates its signature
  counter before returning a session. A simultaneous logout is serialized:
  revocation that commits first prevents restoration. A session issued before
  logout is not retroactively revoked; existing JWT semantics are unchanged.
- JWT claims use current database user/role/client/language values. Disabled or
  deleted users are rejected. A keyed binding of password hash, reset flag,
  enabled/deleted state and account creation time invalidates existing keys
  after a password reset.
- Newly authenticated JWTs carry an opaque `restore_auth_binding` claim.
  Enrollment requires it to match current account state. Refresh preserves it
  without recomputing it, so a JWT issued before a password reset cannot enroll
  another persistent key. Existing JWTs without this claim keep their normal
  login behavior but need a fresh sign-in before first enrollment.
- Enrollment is disabled while `reset_password = 'Y'`. Password-change UI must
  complete normally. Signup/status JWTs without the claim can wait for sign-in.
- Disabling a user through the existing administration route deletes its keys
  and pending registrations in the same transaction. Account deletion is also
  rejected by current `enabled/deleted` checks.

## Configuration and rollout

No new secret is required. The existing `JWT_SECRET` signs JWTs and derives
separate HMAC domains for account binding, user handle, cancellation and
revocation. Rotating it invalidates old restore credentials/capabilities as
well as JWTs; retain the normal manual sign-in fallback.

Defaults:

- RP ID: `bienestarcommunity.org`; optional override:
  `RESTORE_CREDENTIALS_RP_ID`.
- Enabled by default. `RESTORE_CREDENTIALS_ENABLED=false` disables enrollment
  and restore, leaving cancellation/revocation available.
- Allowed signing certificates, confirmed in Play Console on 2026-08-27:
  Play App Signing `15:1B:53:2B:25:90:82:EB:1F:CC:8E:5B:3E:88:AF:50:24:86:44:3A:E4:43:A9:E5:C6:8C:0F:F4:4B:44:74:42`;
  owner's upload/release certificate `6A:69:87:34:92:75:CE:ED:DA:13:47:DB:CC:76:2B:29:25:EF:1B:4F:24:74:EF:1F:91:9D:6F:65:75:66:74:31`.
  The exact origins are derived from certificate bytes as unpadded base64url.
- No debug origin is accepted in production or when `NODE_ENV` is unset.
  A separate development/test process can opt in with both
  `RESTORE_CREDENTIALS_ALLOW_DEV_CERT=true` and
  `RESTORE_CREDENTIALS_DEV_CERT_SHA256`, and `NODE_ENV=development` or `test`.
- Node 20+ is required by the pinned library; this change was tested on Node 22.
  Its dependency tree adds 22 packages and updates shared `tslib` 2.6.2 to 2.8.1.

The first request lazily creates two additive InnoDB tables:
`android_restore_credentials` and `android_restore_challenges`. The application
DB user needs CREATE plus normal SELECT/INSERT/UPDATE/DELETE privileges.
No existing table is altered and no real DB is contacted by the unit tests.
DDL is always executed outside account-change transactions.

Limits are 32 KiB per JSON body, 60 requests/IP/minute, 1,200 requests/process/
minute, ten enrollment options/account/ten minutes and ten stored keys/account.
The oldest key is evicted when an eleventh device is enrolled. Rate counters are
bounded, process-local and need no background timer. Proxy/IP configuration
continues to use the existing Express deployment settings.

Deploy the backend and public Digital Asset Links before releasing the signed
Android build. The production `/.well-known/assetlinks.json` must include
`delegate_permission/common.get_login_creds` for this package and its authorized
signing certificates. Never add a debug key to production to make emulator
tests pass.

The Android backup agent may obtain a new signed-in session during device
restoration and store it encrypted in no-backup storage. A cached session is
usable only until its JWT expiry. FCM is associated with the new installation
when Angular starts; background FCM re-registration is not part of this change.
Actual cloud/device-transfer behavior still requires a signed supported device
test, a participating Google account and Google Play services.

## Isolated verification

Run only these explicit files; do not run the repository's `server.test.js`,
which initializes live application services:

```powershell
node --test api/services/restoreCredentials.test.js api/services/restoreCredentialsRepository.test.js api/routes/restoreCredentials.test.js
node --check app.js
node --check api/routes/user.js
```

Tests cover real EC P-256/CBOR/signatures with UP/UV unset, invalid signatures/
origins/RP IDs/challenges, replay, TTL, password reset/deletion, old JWT
reenrollment, counter updates, logout races, lost registration responses,
cancellation after cleanup, SQL lock order and HTTP limits. SQL is mocked;
no `.env`, production DB, mail service or app/server import is used.

After deployment, an anonymous POST with `{}` to
`/api/auth/restore/authentication/options` should return a random request ID,
the expected RP ID, `userVerification: 'discouraged'` and an expiry, with
`Cache-Control: no-store`. This creates only an expiring challenge. An invalid
signature must never produce a JWT, and normal `/api/ping` must remain healthy.

## Primary references

- [Android Restore Credentials](https://developer.android.com/identity/sign-in/restore-credentials)
- [Android signing origins](https://developer.android.com/identity/passkeys/create-passkeys)
- [SimpleWebAuthn v13.3.3 assertion validation](https://github.com/MasterKale/SimpleWebAuthn/blob/v13.3.3/packages/server/src/authentication/verifyAuthenticationResponse.ts)
- [SimpleWebAuthn v13.3.3 registration validation](https://github.com/MasterKale/SimpleWebAuthn/blob/v13.3.3/packages/server/src/registration/verifyRegistrationResponse.ts)
