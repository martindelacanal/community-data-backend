# Health Events wristbands

This module is confined to `/api/health-events`. It adds an identity input to the
existing beneficiary scan flow; QR, PIN, phone, required-question confirmations,
daily visit rules and checkout forms retain their behavior. Food distribution
routes and tables are not involved.

## Hardware identity

Seven-byte factory UID from an NTAG213/215/216 tag. The API accepts 14 hex
characters, optionally separated by spaces, colons or hyphens. It canonicalizes
to uppercase, preserves leading zeroes and byte order, and rejects truncated
UIDs, decimal conversions, URLs and other-sized RFID IDs. Readers must output
the entire UID as hex (not a four-byte suffix). Sources are `usb`, `bluetooth`,
`nfc_native`, `nfc_web`. A UID identifies a wristband and does not authenticate a
participant or grant application permissions.

## Bindings, return and reuse

`health_event_wristband` is the binding history. Nullable unique active UID and
active registration keys enforce one global active participant per wristband,
and one active wristband per registration, including concurrent requests across
different events. Assignment is transactional, idempotent when the same binding
is submitted again, and requires an active registration and enabled/non-deleted
user. Replacement must be explicitly requested; it releases the previous
binding and records `replaced`. Physical return records `returned`. Releasing a
wristband frees the active keys while retaining its previous association and
operator timestamps. It can then be assigned to a new registration or event.

Beneficiary visits use `health_event_scan`. New visits scanned with a wristband
have transport and binding metadata in `health_event_wristband_scan`. This does
not modify registration source (`web`, `import_jotform`, `walkin`, etc.).
Volunteer event presence is stored exclusively in
`health_event_staff_attendance`; it does not increase beneficiary visits or
service analytics. Volunteer attendance is scanned at entry stands, alternates
checkin/checkout, pairs a checkout to its checkin, and has both durable and
sliding duplicate guards. Dates follow the event timezone.

## Permissions

List/assign/resolve/return and staff history require admin/opsmanager, or an
enabled, approved eventvolunteer registered for this same event and currently
assigned to an enabled entry stand of this same event. Wristband scans at other
stands require the same event approval and an active assignment to the specific
stand. Volunteer wristbands cannot be scanned as beneficiary visits at service
stands. Returns remain available after the event to recover hardware;
assignment and attendance do not continue after its end.

## API

- `GET /:id/wristbands?role=beneficiary|volunteer&search=&page=&pageSize=`:
  `{rows,total,page,pageSize}`; each row includes registration/profile status,
  `credential` (active binding or null), and `staff_attendance` (latest staff
  presence record or null).
- `POST /:id/wristbands/assign`:
  `{registration_id,uid,source,replace?:true}` → `{credential,already_assigned}`.
- `POST /:id/wristbands/:credentialId/return`:
  `{}` → `{returned:true,already_returned}`.
- `POST /:id/wristbands/resolve`: `{uid}` →
  `{credential,participant_role,person,registration}`. No visit is inserted.
- `GET /:id/wristbands/staff-attendance?page=&pageSize=`:
  `{rows,total,page,pageSize}`. Rows include `id`, `registration_id`, `scan_type`,
  `scanned_at`, `scanned_at_local`, `source`, `paired_scan_id`, names and UID.
- `POST /scan`: existing fields plus `wristband_uid,wristband_source`. An identity
  must use only one method. Wristband responses add `participant_role` and
  `credential`; volunteer results also include `attendance_kind:'staff'` and
  `checkout_form:null`.

Conflict/error codes: `INVALID_WRISTBAND_UID`, `INVALID_DATA`,
`WRISTBAND_IN_USE`, `PARTICIPANT_HAS_WRISTBAND`, `WRISTBAND_NOT_ASSIGNED`,
`WRISTBAND_NOT_FOUND`, `PARTICIPANT_NOT_ACTIVE`, `NOT_REGISTERED`,
`VOLUNTEER_ENTRY_ONLY`, `FORBIDDEN`, `EVENT_ENDED`.

## Migration and verification

From the repository root:

```powershell
node BACKEND/scripts/migrateHealthWristbands.js --target=development
node BACKEND/scripts/migrateHealthWristbands.js --target=production
node --test BACKEND/api/utils/healthWristband.test.js BACKEND/api/services/healthEventWristbands.test.js BACKEND/api/utils/healthScanGuard.test.js
$env:RUN_HEALTH_WRISTBAND_INTEGRATION='development'
node --test BACKEND/api/routes/healthEvents.wristbands.integration.test.js
```

The migration reads the explicit database blocks in `BACKEND/.env`, creates
three additive tables, is repeatable and prints no credentials. The integration
suite refuses a development target matching the production host; it creates
synthetic private events, users and registrations, does not email anyone, and
removes fixtures in `finally`. It verifies ACL, concurrent global UID conflicts,
replacement history, return/reuse, beneficiary scan idempotency and checkout
answers, separate volunteer attendance and pairing, and entry access revocation.
Hardware testing in the frontend laboratory is local and produces no API scans
or bindings, even before an event has been created or made public.
