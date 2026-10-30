# Health event duplication

`POST /api/health-events/:id/duplicate` is available to Admin and Ops Manager.
It creates a disabled draft with independent image objects and independent rows for:

- Landing and promotion content, event details and all image variants.
- Stands, their services and checkout forms.
- Participant and volunteer forms, questions, options, profile catalogue mappings and conditional dependencies.
- Appointment slots, with their service, hours, capacity and enabled state.

Question/option dependencies (including `config_json.option_dates`) are remapped
to the new IDs. Global profile catalogue IDs stay unchanged. No registrations,
answers, appointments, scans, clinical records, wristbands, volunteer assignments
or notification recipients are copied. All database inserts share the duplication
transaction; a failure rolls back the copy and cleans up new S3 objects.

Structured form/option/slot dates move by the difference between event start
dates, using calendar days without daylight-saving drift. Recognizable English
and Spanish date labels also move, including the setup day before the event.
Custom prose and links should still be reviewed. If the destination is shorter,
out-of-range slots, dated options/questions and their dependent questions are
disabled. Longer events retain the source schedule; administrators can add days.

## Completing an older presentation-only copy

The maintenance script refuses a destination with any configuration or existing
registrations, and verifies both IDs and slugs. It never writes event details,
landing content, images or publication flags. A repeat run refuses to duplicate
existing configuration rather than overwrite client edits.

From the backend directory, with its normal database environment:

```sh
node scripts/backfillHealthEventConfiguration.js 1 banning 3 san-jacinto
node scripts/backfillHealthEventConfiguration.js 1 banning 3 san-jacinto --apply
```

The default is a read-only dry run. Apply locks source/destination rows, saves a
restricted local JSON backup before insertion, verifies counts and relationships,
and commits atomically. The output includes the backup path and a receipt is saved
alongside it. If interrupted, inspect the target and receipt before rerunning.
