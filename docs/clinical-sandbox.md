# Synthetic clinical pilot

This module accepts training records for newly generated `CPTEST-` fixtures only. It has no API to import beneficiaries, associate a real beneficiary, or enable real clinical data. It is not a declaration of HIPAA compliance. Public health-event registrations, analytics, communication workflows and existing patient tables are not used for clinical storage.

## Deployment

Run `node scripts/migrateClinicalSandbox.js --target=development` first. Production uses the explicit `--target=production` target only after review. The additive migration creates eight private tables; its rerun also adds the finalization context column if the first pilot schema was already installed. It does not grant access or create a sandbox event.

Configure these values through the deployment's secret/configuration mechanism; never commit their values:

| Variable | Purpose |
| --- | --- |
| `CLINICAL_SANDBOX_ENABLED=true` | Enables the private synthetic routes; absent/false disables them. |
| `CLINICAL_OPENEMR_BRIDGE_URL` | HTTPS URL ending in `/interface/modules/custom_modules/oe-module-clinical-platform/public/bridge.php`. |
| `CLINICAL_OPENEMR_BRIDGE_SECRET` | Same high-entropy secret as the OpenEMR module's `CP_BRIDGE_SECRET`. |
| `CLINICAL_OPENEMR_CA_FILE` | Optional path to the trusted private certificate authority PEM; TLS verification remains enabled. |

The current adapter uses the dedicated HMAC bridge with a native OpenEMR technical account. It does not use the earlier OAuth proposal. The exact request body, logical `/v1/...` route, nonce, timestamp and separate human actor context are signed. Connection failures return a fixed `503` response; they never mark a record synced. OpenEMR should be reachable only on the private network. Allow up to 90 seconds for an initial finalization containing patient, encounter and record calls; each individual call has a 30-second limit and redirects are disabled.

All API paths below start with `/api/clinical-sandbox` and use the existing Bearer session. Current active user and role are loaded from the database for every request. Admitted roles are admin, opsmanager and eventvolunteer. Managers can read/manage private events; writes and finalization require explicit per-event specialty grants, including for managers. A manager can explicitly grant themself while creating an event.

## API contract

| Method/path | Result or input |
| --- | --- |
| `GET /status` | `enabled`, synthetic mode, `real_data_enabled:false`, OpenEMR configured/available, scanner availability and attachment types. |
| `GET /events` | `{events}` accessible to the current actor. |
| `POST /events` | `{name,start_date,end_date,grant_self}`; creates one private event with three fictitious patients, returns `{event}`. |
| `GET /staff?search=...` | Manager-only matching active staff (minimum two characters), `{staff}`. |
| `GET /events/:eventId/grants` | Manager-only `{grants}`, including revocations. |
| `POST /events/:eventId/grants` | `{user_id,specialties,can_write,can_finalize}`; finalize requires write. |
| `DELETE /events/:eventId/grants/:userId` | Revokes access for the next request, even with an existing JWT. |
| `GET /patients` or `/events/:eventId/patients` | `{patients}` accessible fictitious fixtures. |
| `GET /patients/:patientId/records` | `{records}` filtered by current event/specialty access; usable after event dates. |
| `GET /patients/:patientId/export` | Audited JSON attachment with schema version, unit map and accessible history. `/records/export` is an alias. |
| `GET /patients/:patientId/export/fhir` | FHIR R4 `Bundle` collection, `application/fhir+json`, including protected attachment bytes. Same event/specialty permissions. |
| `POST /events/:eventId/patients/:patientId/records` | `{specialty,data,idempotency_key,synthetic_confirmed:true}` creates a draft. |
| `GET /events/:eventId/records/:recordId` | `{record}` with attachment metadata and finalization state. |
| `PATCH /events/:eventId/records/:recordId` | `{revision,data,synthetic_confirmed:true}` replaces draft fields with optimistic concurrency. |
| `GET /events/:eventId/records/:recordId/revisions` | `{revisions}` append-only snapshots with actor/action/time. |
| `POST /events/:eventId/records/:recordId/finalize` | `{revision,idempotency_key,synthetic_confirmed:true}`; assessment and plan required, real native save required. |
| `POST /events/:eventId/records/:recordId/amendments` | `{reason,data,idempotency_key,synthetic_confirmed:true}` creates a new draft referencing an immutable final. |
| `POST /events/:eventId/records/:recordId/attachments` | Multipart `file` and `synthetic_confirmed=true`; final native record required. |
| `GET /events/:eventId/records/:recordId/attachments/:attachmentId/download` | Authorized, audited native-document download with integrity check. |
| `POST /events/:eventId/feedback` | `{category,message,rating?}`; categories usability, clinical_form, technical, other. |
| `GET /events/:eventId/feedback` | Manager-only `{feedback}`. |

History and export also have `/events/:eventId/patients/:patientId/...` forms. Record IDs are UUID strings; event/patient/user IDs are integers. No patient names or record contents belong in URLs, telemetry or logs. All clinical responses set `Cache-Control: no-store, private`.

Structured schema version 1 is implemented in `api/services/clinicalValidation.js`: common notes, dated follow-up, medications/allergies, explicit not-assessed fields, general/clearance vitals with named units, dental tooth notation and entries, optometry eyes, and clearance decision/restrictions. Missing values are preserved as absent/null; the module produces no clinical recommendation. Unknown fields, invalid measurement ranges and ambiguous tooth numbering are rejected.

## Integrity and recovery

Draft changes require the current revision and return `409` when stale. Every successful draft/update/final/amendment writes a separate snapshot. Final records cannot be edited; an amendment preserves its parent and reuses the same native encounter. Amend the latest final revision when one already exists.

Beginning finalization persists the original recorder/finalizer context and freezes the draft before calling OpenEMR. `finalization_pending:true` and `finalization_actor_id` explain this state to the UI. If the bridge fails, the same authorized finalizer can retry with the same revision and a new client idempotency key; the stable native key and actor context prevent duplicates after an ambiguous native commit. A different actor cannot take over that pending finalization. A committed final accepts exact client-key replay and otherwise remains immutable. Avoid manually clearing a pending finalization without reconciling native state first.

Attachments are capped at 5 MiB with bounded upload concurrency and per-user rate limits. Bytes remain in memory quarantine, images are decoded/re-encoded to remove metadata, and active PDF constructs are rejected. The conservative static-PDF policy also rejects compressed object streams, interactive forms, external links and embedded files; use a flattened static PDF or PNG/JPEG when rejected. Every file also requires the native ClamAV scanner to report healthy and scan before native encrypted document storage. A healthy scanner is a prerequisite for all three types, including images. No local/public copy is written. Native attachment keys include the record UUID as well as the content hash.

## Verification

Run unit tests with `node --test api/services/clinicalValidation.test.js api/services/clinicalOpenEmr.test.js`.

After development migration, set `RUN_CLINICAL_SANDBOX_INTEGRATION=development` and run `node --test api/routes/clinicalSandbox.integration.test.js`. This test uses the actual development database and HTTP router with isolated synthetic staff/events and cleanup. The OpenEMR adapter is an explicit test double. It exercises role spoofing, event/patient isolation, revocation, real-data markers, parallel idempotency, optimistic editing, immutable finals, ambiguous native-commit recovery, actor preservation, amendment encounters, attachment protection, history export and feedback permissions. Passing it does not prove the deployed OpenEMR/TLS/scanner installation; verify those separately against the actual bridge.

Real-data readiness, legal agreements, licensed-clinician identity verification, MFA/session policy, durable distributed rate limiting, production retention/recovery and operational monitoring remain separate requirements before any real clinical release.
