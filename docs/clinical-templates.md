# Clinical documentation templates, version 2

The clinical sandbox uses versioned documentation templates for fictional patients only. It has no real-patient import or real-data activation route. These are documented adaptations of OpenEMR and public federal forms, not a claim of clinical validation, government endorsement, physician credential verification, consent, HIPAA compliance or ONC certification.

## Catalog and provenance

`api/data/clinical-templates.v2.json` is the executable catalog. `GET /api/clinical-sandbox/templates` returns it to authenticated, currently enabled staff admitted to the clinical sandbox. The endpoint uses the existing no-store response policy and feature flag. A catalog contains no patient data; per-event grants still govern reading, writing and finalizing records.

| Template ID | Specialty | Fields | Documentation sources | Narrative required to finalize |
| --- | --- | ---: | --- | --- |
| `openemr-general-v2` | `general` | 209 | OpenEMR 8.4.1 SOAP, vitals, review of systems, physical examination and history fields | `soap_assessment`, `soap_plan` |
| `federal-dental-v2` | `dental` | 153 | IHS 42-1 medical history, GSA SF603 examination concepts and OpenEMR SOAP | `soap_assessment`, `soap_plan` |
| `openemr-eye-v2` | `optometry` | 142 | OpenEMR 8.4.1 `eye_mag` encounter documentation | `title`, `plan` |
| `federal-consult-v2` | `clearance` | 20 | GSA SF513 consultation request and report | `consult_report` |

The four templates contain 524 source fields. Catalog `sources` and per-field `source` entries retain attribution. OpenEMR source is pinned to tag `v8_4_1`, commit `a43edad9ea6d969fcbcc1df7d8ccc50ad34fd872`; the source snapshot and its license are retained under `deliverables/clinical-templates-2026-10-06`. English source labels are accompanied by independent Spanish assistance labels. These translations are not represented as officially authorized or clinically validated translations.

Dental history follows [IHS 42-1, revision 4/2021](https://www.ihs.gov/DOH/documents/forms/IHS%2042-1%20%28Health%20History%29%204-2021.pdf). All history groups, the two wellbeing questions, personal safety, substances, allergy choices and six medication rows are available as optional pilot fields. The wellbeing questions retain their frequency choices without calculating a score or diagnosis. The [IHS reuse policy](https://www.ihs.gov/disclaimers/) allows document reproduction unless otherwise stated; third-party content and images have separate restrictions. The adaptation includes no agency logos or copied artwork.

[SF603](https://www.gsa.gov/cdnstatic/SF_603.pdf), revision 10/1975, supplies dental examination and treatment-record concepts. Its military administrative fields, readiness classes and historical periodontal categories are not reproduced as modern clinical classifications. Periodontal findings remain professional narrative. Dental tooth entries use the application's explicit Universal or FDI numbering and are stored outside the flat template answers.

[SF513](https://www.gsa.gov/system/files/SF_513.pdf), revision 4/1998, explicitly permits local reproduction. The adaptation documents a consultation request, review and report. It omits military sponsor/SSN/rank fields. It does not calculate or grant medical clearance. Version-1 clearance decisions remain in their original records rather than being reinterpreted as version-2 consultation answers.

Source signature lines and blanket consent text are not implemented as simulated legal attestations. Authenticated recorder, editor, finalizer and timestamps belong to the record's audit/provenance outside questionnaire answers. A typed professional title is descriptive and does not verify a license. ADA/CDA member forms, commercial CDT catalogs, logos and restricted artwork are not bundled.

## Stored data and validation

```json
{
  "template_version": 2,
  "template_id": "openemr-general-v2",
  "template_fields": {
    "soap_assessment": "Fictional assessment",
    "soap_plan": "Fictional follow-up",
    "temperature": 98.6,
    "temp_method": "Oral"
  },
  "not_assessed": ["ros_gu"]
}
```

The outer request still carries `specialty`, `synthetic_confirmed:true` and an idempotency key. JSON numbers stay numbers; strings such as `"120"`, booleans, objects and non-finite numeric values are rejected for numeric fields. Selects accept only the catalog's exact option values. Unknown field names, mixed v1/v2 fields, unrecognized template IDs, version mismatches and templates from a different specialty fail validation.

Dates use real calendar dates in `YYYY-MM-DD`. Datetimes require UTC ISO format, with seconds and optional three-digit milliseconds, and are stored with milliseconds. Text obeys its catalog byte limit (2,000 bytes when no field limit is specified). The complete normalized record is limited to 64 KiB of UTF-8 JSON; one byte more returns `CLINICAL_RECORD_TOO_LARGE` with HTTP 413. This is a storage/input safeguard, not a physiological range or clinical decision rule.

Absent answers are not inserted as negative or normal findings. Null and omitted answers carry no asserted value. `unknown` is available where defined; it is an explicit response rather than a default. `not_assessed` contains unique section IDs from the selected template and cannot mark a section that also has answered fields. Source fields remain optional for the pilot except the catalog's required finalization narratives.

Dental records additionally allow `tooth_notation` (`universal` or `fdi`) and up to 52 `teeth` entries containing `number`, `condition` and `treatment`. Numbering must be explicit, and both adult and primary tooth identifiers are supported. Unselected teeth are not assumed normal.

## Lifecycle and native integration

`clinicalTemplates.sameTemplate` prevents changing an existing record's template ID/version through PATCH or amendment. Legacy records continue using their original version-1 schema. Changing a template means creating a separate new consultation, not silently upgrading an existing clinical document.

Draft edits use the existing revision check and append a snapshot. Final records remain immutable; amendments preserve the original record and native encounter. Finalization checks the selected template's required narratives before native patient, encounter or record calls. Missing narratives return `CLINICAL_TEMPLATE_REVIEW_REQUIRED` for v2. Existing retry/idempotency and finalizer-ownership controls continue to apply.

The backend sends native `schema_version:2` with the validated `data` envelope intact. The bridge stores it in the native `clinical_platform` form and returns the same structured content on record readback. This does not claim that every answer has also populated a separate built-in OpenEMR table/form or OpenEMR's public FHIR API. Deploy the matching backend catalog and native bridge catalog together. Native readiness advertises `supported_schema_versions: [1,2]` and keeps `real_phi_enabled:false`.

## FHIR export

See [clinical-fhir.md](clinical-fhir.md) for authorization, attachment integrity, export budgets and v1 semantics. Version-2 exports add a versioned Questionnaire per template and typed QuestionnaireResponse answers for every recorded field. The questionnaire canonical includes `|2.0.0`; the Bundle embeds the matching definition and local terminology. A patient's export can include version-1 and version-2 records simultaneously, each bound to its own definition. Tooth documentation stays a repeating group.

Where the source explicitly identifies a measurement and unit, a derived Observation preserves the source value and UCUM unit. OpenEMR general measurements retain Fahrenheit (`[degF]`), pounds (`[lb_av]`) and inches (`[in_i]`); they are not relabeled as Celsius, kilograms or centimeters. An explicitly entered measurement datetime is used when available. Missing measurement time is not invented from document timestamps.

Eye lens powers retain zero and negative values with `[diop]`, axis uses `deg`, distances use `mm`, and numeric applanation/Tono-Pen results use `mm[Hg]` with their method. `ODIOPFTN` and `OSIOPFTN` are palpation text, not measured pressure, so they remain questionnaire text without a pressure unit. Missing blood-pressure components remain unknown, not zero. No narrative allergy, medication, assessment or plan is automatically converted into a coded diagnosis, prescription or performed procedure.

This provides a self-contained FHIR R4 export, not a generic device interface, public FHIR server, universal import contract or US Core conformance claim. Receiving applications must agree on the questionnaire and local codes.

## Verification

Run the template unit suite:

```powershell
node --test api/services/clinicalTemplates.test.js
```

The opt-in development integration suite uses the actual DEV database, a local HTTP router and an explicitly mocked native adapter. It asserts that DEV and production hosts differ, creates isolated fictional staff/events, and removes its fixtures in `finally`:

```powershell
$env:RUN_CLINICAL_SANDBOX_INTEGRATION='development'
node --test api/routes/clinicalTemplates.integration.test.js
```

Coverage includes all 524 fields through validation/JSON/DB/native payload/FHIR, numeric coercion rejection, invalid options/dates/unknown fields, the exact 64 KiB boundary, readiness before native calls, template immutability in PATCH/amendments, authorization/revocation, source units, zero/negative optics and mixed-version questionnaire references. These suites supplement the existing clinical sandbox, bridge and FHIR tests.

The separate live smoke at `deliverables/clinical-templates-2026-10-06/live-smoke.cjs` requires `--run` after native v2 deployment and a TLS-verified localhost tunnel on port 18443. It reads an existing active DEV admin without modifying the account, uses short-lived local JWTs only in memory, creates one private `CLINICAL TEMPLATE QA` event with fictitious CPTEST patients and an admin event grant, finalizes all four specialties through the actual bridge, reads native schema/data back, reopens backend records and exports FHIR. It retains only fictional evidence. The result report contains checks and synthetic identifiers; no credentials are written to reports. `qa-bridge.json`, environment files and secrets must never be committed or printed.

Release verification on 2026-10-06: all 11 template unit tests and the opt-in DEV integration test passed. The separate live smoke against the deployed OpenEMR bridge also passed for all four specialties, verifying native `schema_version:2`, exact submitted-data readback, backend reopening and idempotent finalization replay. Its synthetic FHIR export contains 35 resources, four Questionnaires, four QuestionnaireResponses and 14 Observations. Evidence is retained as `live-smoke-result.json` and `live-development-bundle.json` beside the smoke script.

Passing the mocked integration suite alone does not establish that the deployed PHP bridge works; the live result is separate evidence. FHIR validation similarly requires a separate official validator run. Real-data use remains disabled pending the organizational, legal, clinical and operational readiness work outside this template release.
