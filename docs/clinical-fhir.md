# Clinical pilot FHIR R4 export

The web history screen offers **Export FHIR R4** beside the original JSON download. The API is `GET /api/clinical-sandbox/patients/:patientId/export/fhir` (also available under `/events/:eventId/patients/:patientId/export/fhir`). It uses the existing Bearer session, checks current database identity and event/specialty grants, and logs `history.fhir.export`. Responses are private, uncached attachments with media type `application/fhir+json`.

This is a FHIR **4.0.1 collection Bundle**, generated from the current synthetic clinical records on demand. Existing records do not need a migration, duplicate storage or new infrastructure. Final consultations remain in OpenEMR; the adapter exports their structured local representation and retrieves verified attachment bytes through the private OpenEMR bridge. Drafts remain identified as drafts. No real patient capability is enabled.

## Mapping version 1.0.0

| Source | FHIR representation |
| --- | --- |
| Fictitious patient | `Patient`, CPTEST identifier, synthetic tag |
| Consultation/specialty | `Encounter`, shared by amendments to the original consultation |
| Every supported field in general, dental, optometry and clearance forms | Versioned `Questionnaire` and typed `QuestionnaireResponse` answers |
| Numerical vital measurements | `Observation` with verified LOINC codes and UCUM units |
| Glucose, optical measurements and pain score | `Observation` with explicit local codes; UCUM units where captured |
| Teeth, notation and tooth notes | Repeating questionnaire group; pediatric numbering remains unchanged |
| Eye measurements | Separate right/left questionnaire groups and Observation body sites |
| Original recorder, latest content editor and finalizer | Local `Practitioner` identifiers and `Provenance` with distinct enterer/author/verifier roles |
| Amendments | Separate responses, original retained, `Provenance.entity.role=revision` links to parent |
| Attachments | `DocumentReference` with inline base64 bytes, media type, title, size and SHA-1 base64 hash; separate upload Provenance |

The Bundle embeds the questionnaires and local `CodeSystem`. Resource references use stable `urn:uuid:` identifiers and resolve within the collection. Questionnaire canonical URLs identify the versioned definitions; they are identifiers, not a public FHIR API. Local operator IDs do not assert a medical license, NPI or digital signature. Per-patient resource UUIDs are deterministic; the export Bundle identifier and export Provenance reflect the download time.

LOINC mapping: heart rate 8867-4, respiratory rate 9279-1, temperature 8310-5, generic oxygen saturation 2708-6, weight 29463-7, height 8302-2, blood pressure panel 85354-9 with systolic 8480-6 and diastolic 8462-4. Glucose remains locally coded because specimen/method are not captured. UCUM optical lens power uses `[diop]` (not prism diopters), axis `deg`, distance `mm`, pressure `mm[Hg]`.

No absent value becomes zero or a negative clinical finding. Explicit null/empty answers have no FHIR answer value; empty groups/lists carry no clinical assertion. `not_assessed` is preserved literally. An incomplete blood pressure panel includes the available component and an unknown missing component. Free-text allergies, medication, assessment and treatment notes remain answers, rather than inferred `AllergyIntolerance`, medication orders, coded diagnoses or performed procedures. A clearance response is the recorded clinician decision, not a software-generated recommendation or certificate.

The source schema does not capture measurement timestamps, encounter completion status, observer identity, method/specimen or coded diagnoses. Therefore `Encounter.status=unknown`; Observation `issued` is documentation time and no measurement `effectiveDateTime` value is invented. Known vital-sign observations carry the vital-sign category and an `effectivePeriod` containing only the standard data-absent-reason extension (`unknown`), without invented start/end values. Draft responses are `in-progress`; finalized responses are `completed` or `amended`. Observation statuses are preliminary/final/amended respectively. Current source records and their amendment chain are included; intermediate draft-edit snapshots remain available in the authenticated application revision history rather than being presented as separate clinical findings. Original finalized values remain alongside amendments and must not be counted as independent visits.

Response authorship is taken from the latest content revision, excluding finalization actions. Documentation timestamps are converted through the database session timezone, avoiding shifts between local development and production. Attachment Provenance identifies the uploader and upload time; neither the original file author nor the file creation date is inferred from upload metadata.

## Operational limits

Maximum 200 authorized records, 40 attachments, and 20 MiB total attachment bytes per export; 10 requests/minute/operator and two concurrent exports per backend process. Oversize exports return `FHIR_EXPORT_TOO_LARGE` (413), with no silent truncation. Unavailable or corrupted native attachments fail the entire export. SHA-256 from stored metadata is checked before release; FHIR R4 `Attachment.hash` uses SHA-1 solely to comply with that data type. After downloads, current account and grants are checked again before sending the Bundle. No bridge credentials, native storage URLs or database connection details are exported.

## Validation and interoperability scope

Run `node --test api/services/clinicalFhir.test.js` and the existing clinical integration suite documented in `clinical-sandbox.md`. The integration suite uses the actual development database and a named adapter test double, checking permissions, revocation, attachment bytes/hash, content type and export audit. Official HL7 Validator CLI validation should use R4 4.0.1, the embedded questionnaires as definitions, `-questionnaire required`, and offline terminology mode. Do not upload patient bundles to public validators.

Release verification (2026-10-05): the 70-resource synthetic fixture covering all four specialties, drafts, an amendment, partial blood pressure and an attachment passed HL7 Validator 6.10.4 for FHIR 4.0.1 with zero errors. The 141 warnings remain documented: 55 offline terminology checks, 17 non-resolved identifier namespaces, 41 unknown measurement performers, 19 unknown measurement times and 9 Provenance terminology warnings. The run used `-tx n/a`, `-no-http-access` and `-questionnaire required`; it is not a full terminology-service validation. The mapper, existing bridge/validation tests and development integration suite passed 22 tests, including corrupt attachments, download limits and grant revocation during an export.

This release provides an export representation, not a public REST FHIR server, SMART launch, device interface, external-system import or US Core conformance declaration. It does not automatically populate native OpenEMR FHIR endpoints from the custom form. Receiving systems need agreement on the local questionnaire/terminology mappings. No universal interoperability, clinical template validation, HIPAA compliance or ONC certification follows merely from FHIR validation.

Primary references: [HL7 R4 Bundle](https://hl7.org/fhir/R4/bundle.html), [QuestionnaireResponse](https://hl7.org/fhir/R4/questionnaireresponse.html), [vital signs](https://hl7.org/fhir/R4/observation-vitalsigns.html), [validation](https://hl7.org/fhir/R4/validation.html), [Attachment data type](https://hl7.org/fhir/R4/datatypes-definitions.html#Attachment.hash), [UCUM](https://ucum.org/ucum).
