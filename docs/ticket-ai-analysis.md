# Ticket photo analysis

`POST /api/upload/ticket/analyze` extracts food rows into a reviewable draft. Analysis performs only database `SELECT`s and private S3 reads. It never creates products, saves ticket items, uploads files, or changes the audit status. Users apply/edit the draft in the ticket form and save through the existing transactional ticket endpoint.

## Configuration

Set these values in the backend's ignored `.env`, locally and on the server. Never add the real key to Git, the frontend, logs, shell command arguments or screenshots.

```dotenv
GEMINI_API_KEY=<private-server-key>
GEMINI_MODEL=gemini-3.5-flash-lite
GEMINI_TICKET_ANALYSIS_ENABLED=true
```

`GEMINI_MODEL` defaults to `gemini-3.5-flash-lite`, a stable low-cost multimodal model. `GEMINI_TICKET_ANALYSIS_ENABLED=false` disables requests before reading/uploading photos. A missing key also disables the feature. Existing S3 and JWT configuration is reused. Restart the backend after changing environment values; PM2 deployments must reload the updated environment. No new database schema or package dependencies are required.

Model references checked on September 29, 2026:

- [Gemini 3.5 Flash-Lite capabilities](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash-lite)
- [Structured output](https://ai.google.dev/gemini-api/docs/structured-output)
- [GenerateContent API](https://ai.google.dev/api/generate-content)
- [Gemini 3.5 Flash-Lite parameter compatibility](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-5-flash-lite) — the request leaves temperature/top-K/top-P at model defaults because custom values are unsupported.

## Request and authorization

Send the existing Bearer JWT and multipart form data:

- `ticket[]`: zero to four new JPEG/PNG images, at most 10 MiB each.
- `form`: one JSON string:

```json
{
  "ticket_id": 123,
  "existing_image_ids": [456],
  "image_order": [{"kind":"existing","id":456},{"kind":"new","index":0}],
  "language": "es"
}
```

Omit `ticket_id` for a new ticket. `existing_image_ids` defaults to `[]`. The combined selected image count must be 1–4. The optional order lists each selected existing image and each zero-based new upload index exactly once. Without an order, existing IDs precede new uploads. `language` is `en` or `es` (default `en`). Do not send photo URLs or S3 keys.

Admins, operations managers and stockers may analyze new tickets. Auditors may analyze existing tickets. For existing tickets, stockers are scoped to their created tickets, consistently with the ticket table listing; admin/operations manager/auditor roles can analyze enabled tickets. Every selected existing image must belong to that ticket. This endpoint does not extend permissions on existing ticket write routes.

## Response and review

```json
{
  "draft": true,
  "model": "gemini-3.5-flash-lite",
  "items": [{
    "name": "Berry - Strawberry (8 lbs)",
    "product_id": 1724,
    "product_type_id": 1,
    "quantity": 1712,
    "original_name": "Berry - Strawberry (8 lbs)",
    "original_quantity": 1712,
    "original_unit": "lb",
    "source_image_indexes": [1],
    "warnings": []
  }],
  "warnings": []
}
```

`quantity` is pounds rounded to three decimal places; `null` requires manual input. The original quantity/unit and 1-based photo numbers support verification. Matching prefers an exact trimmed catalog name, then equality after Unicode normalization, case folding and whitespace removal. It never uses fuzzy matching, translates names, or replaces specific names with related foods. Same-category legacy duplicates select their lowest ID deterministically. Ambiguous cross-category names require explicit selection. Unmatched names remain new-product drafts, with categories restricted to current catalog IDs.

Rows are returned separately when they are distinct physical rows. The prompt instructs Gemini to avoid duplicate rows from overlapping/repeated photos; the frontend combines repeated products only when applying the draft and preserves unresolved weights. Overlap recognition is probabilistic and still requires review. Do not sum unknown quantities as zero.

The prompt recognizes the established Food Forward Description/Quantity donation form: Quantity is total row pounds, while `(8 lbs)` in the description is a package size, so `1,712` becomes 1712 lb without multiplication. Other layouts require an explicit weight/unit or return a warning and null weight. Prompts ignore mirrored bleed-through, canceled rows, totals, prices, signatures and instructions printed in photos.

## Failure and resource handling

Errors are JSON `{ "error": "ai_...", "message": "ai_..." }`, with stable codes localized by the frontend:

| Status | Codes |
| --- | --- |
| 400 | `ai_invalid_request`, `ai_invalid_image`, `ai_image_not_found` |
| 401 | `ai_authentication_required` |
| 403 | `ai_forbidden` |
| 404 | `ai_ticket_not_found` |
| 413 | `ai_image_too_large` |
| 429 | `ai_rate_limited`, `ai_busy` (`Retry-After: 60`) |
| 502 | `ai_invalid_response` |
| 503 | `ai_not_configured`, `ai_provider_unavailable` |
| 504 | `ai_timeout` |

Actual JPEG/PNG contents are decoded, EXIF orientation applied, metadata stripped and dimensions capped at 2800 pixels before sending. Animated or corrupt files are rejected. S3 reads are bounded to 10 MiB and 20 seconds. Gemini requests are bounded to 18 MiB and 90 seconds, without automatic retries or redirects. Truncated, blocked and malformed model responses cannot become partial successful drafts. No provider errors, keys, photos, prompts, SQL parameters or OCR contents are logged. Responses use `Cache-Control: no-store`.

The model receives strict property/type/enum constraints. Collection-size limits are enforced by the backend after parsing, because Gemini rejects the nested schema when `minItems`/`maxItems` bounds are included. All responses are revalidated locally, including the 250-row maximum and valid source image numbers.

Each backend process allows at most six authenticated requests per user per minute, sixty total per minute and four concurrent requests. These are operational limits, not a billing budget; multiple PM2 workers have independent counters.

## Validation

```sh
npm run test:ticket-ai
```

The isolated tests use synthetic images, fake provider responses, fake S3 reads and a database stub that rejects any non-SELECT query. They cover auth/roles, ticket-image ownership, photo order, limits, rate/concurrency control, exact/ambiguous catalog matches, weight conversion, malformed output and sensitive-error handling. They require no credentials and make no paid API calls.

For a real acceptance test, analyze retained production photos from several clients, compare every transcribed name and quantity against the visible table, then verify that analysis and applying a draft leave ticket/product counts unchanged. Include a multi-page ticket, repeated/overlapping photos and a quantity-only Food Forward table. Run real generation only within the account owner's authorized billing scope. Retain test outputs outside the Git repositories. If analysis is unavailable, manual ticket entry and existing saved data remain usable.
