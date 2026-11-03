# Article CSV import

Administrators and content managers can import articles from the article table. One UTF-8 CSV row creates one article. One numbered image is required for each data row: `1.jpg` belongs to the first article below the CSV header. The same uploaded image is used for both languages, stored under independent keys so subsequent language-specific edits remain safe.

## API

- `GET /api/article/bulk/schema`: column descriptions in English and Spanish, current catalogs and upload limits.
- `POST /api/article/bulk/validate`: JSON `{ "csv": "...", "images": [{ "name": "1.png", "size": 1234, "type": "image/png", "hash": "<SHA-256>" }] }`. Read-only preflight. Returns `valid`, `errors`, `rowCount` and `summary` with `draft`, `published` and `archived` counts.
- `POST /api/article/bulk`: multipart with one `csv` file and all image files under repeated `images` fields. Revalidates the complete CSV, database conflicts and image bytes. Returns `{ "createdCount": 1, "articleIds": [123] }` only after commit.

Errors contain `code`, `messageEn`, `messageEs`, optional `row` (data-row/article number starting at 1) and `column`. Invalid preflight data returns HTTP 200 with `valid: false`; invalid final imports return 400, concurrent import conflicts 409/429, and infrastructure failures 500.

## CSV

The schema endpoint is authoritative. The eight required columns and values are `titleEnglish`, `titleSpanish`, `contentEnglish`, `contentSpanish`, `author`, `date`, `categoryIds` and `article_status_id`.

Optional columns are `subtitleEnglish`, `subtitleSpanish`, `author_gender`, `filter`, `priority_category_id`, `priority_filter_id`, `priority`, `tags`, `genderIds`, `ethnicityIds`, `locationIds`, `ageRanges`, `imageCaptionEnglish`, `imageCaptionSpanish`, `showImageEnglish` and `showImageSpanish`.

- Titles: 500 characters maximum; subtitles: 1,000; author: 255; each language body: 100,000. Duplicate titles or generated URLs are rejected, including existing articles.
- Status: `draft`/`borrador`, `published`/`publicado`, `archived`/`archivado`, or an existing status ID. A status is mandatory so importing never publishes by accident.
- Date: ISO date/time with explicit timezone, such as `2026-11-03T12:00:00-03:00`; stored as UTC like the ordinary editor.
- Catalog lists: existing numeric IDs separated by `|`; `tags` also uses existing tag IDs. Primary category/filter must belong to the corresponding selected list.
- Priority: optional unused number from 1 through 8. Importing does not silently reorder existing featured articles.
- Age ranges: `18-35|65-100`, within 0–120 years; overlapping ranges are rejected.
- Image visibility: `true` or `false`; empty means `true`. Author avatar: `girl`, `boy` or empty.
- CSV accepts comma or semicolon separators, UTF-8 BOM, quoted multiline fields and doubled quote escaping. Embedded data/blob images in article bodies are rejected; use numbered preview images.

## Upload and transaction behavior

Limits: 100 articles, CSV up to 2 MiB, each static JPG/PNG/WebP up to 5 MiB and 40 megapixels, images totaling at most 100 MiB. At most two uploads run concurrently, with one per editor. Temporary files are streamed to an isolated OS temporary directory and removed after completion or failure.

The browser computes hashes for preflight. The server checks file names, counts, types, dimensions, actual decoding and SHA-256 duplicates independently before writing any S3 objects or articles. After uploading image variants, one database transaction inserts every article and all relationships. A database advisory lock serializes final bulk-import conflict checks. Unique article URL indexes also reject conflicting ordinary-editor writes.

Failed batches roll back their articles and clean up uploaded S3 objects, including anticipated partial variants. A lost commit response is reconciled against persisted image records before cleanup. If that reconciliation is unavailable, images are retained rather than risking damage to a committed article. Retrying a committed batch is rejected by existing-title/URL checks; refresh the article table after an uncertain network result.

## Reverse proxy

The production API virtual host has a dedicated exact-match location for this endpoint, retaining its existing proxy headers:

```nginx
location = /api/article/bulk {
    client_max_body_size 128M;
    proxy_read_timeout 300s;
    proxy_send_timeout 300s;
    proxy_pass http://localhost:3000;
    # Same proxy HTTP version and headers as the existing API location.
}
```

The ordinary API limits are unchanged. The application uses a dedicated 5 MiB JSON parser for preflight to accommodate JSON escaping around the bounded CSV. The frontend shows byte-upload progress and a separate processing state because image variant uploads and the final transaction occur after browser upload completes.

Run regression checks with `node --test api/services/articleBulkImport.test.js api/routes/articleBulkImport.test.js`.
