# Community AI assistant

The Angular web and Capacitor applications share the floating assistant. Administration lives at `/admin/chatbot` (also linked from the administrator's My Account). The backend mounts `/api/chatbot`.

## Initial rollout

The additive migration enables every existing role except `beneficiary`, `delivery`, and `stocker`. Public mode is `hidden`. Administrators can independently change the role allowlist and public mode: `hidden`, `login_required`, or `enabled`. Migration reruns never overwrite saved settings. The role is read from the current database record on every request; history belongs to a user, or to an opaque anonymous browser session when public chat is enabled.

Run from BACKEND:

```text
node scripts/migrateChatbot.js --target=development
node scripts/migrateChatbot.js --target=production
node scripts/seedChatbotSources.js --target=development
node scripts/seedChatbotSources.js --target=production
npm run test:chatbot
```

The optional idempotent seed imports three exact pages from WHO, MedlinePlus/NIH and CDC. It does not crawl linked pages. Existing references, including disabled references, are not overwritten.

## Model and retrieval decision (2026-10-09)

The existing Google key was tested against both models. `gemini-3.8-flash` generates grounded answers with low thinking. `gemini-3.5-flash-lite` performs a small structured scope classification with minimal thinking. No additional decision API, external agent framework, open web search or model tools are required. Source ingestion uses `gemini-embedding-001`, 768 dimensions, for multilingual text retrieval. Vectors are created once per document import and retained in MySQL; each question needs only its query embedding.

Official references: [Google model catalog](https://ai.google.dev/gemini-api/docs/models), [pricing](https://ai.google.dev/gemini-api/docs/pricing), [embeddings](https://ai.google.dev/gemini-api/docs/embeddings), [managed File Search alternative](https://ai.google.dev/gemini-api/docs/file-search). The local index was selected to keep source enable/delete controls and audit provenance in the existing database rather than a second managed document store. Model names are pinned, not `latest` aliases.

Server configuration: `CHATBOT_GEMINI_API_KEY` optionally overrides existing `GEMINI_API_KEY`; `CHATBOT_MODEL`, `CHATBOT_SCOPE_MODEL`, and `CHATBOT_SESSION_SECRET` optionally override defaults. Otherwise session ownership is HMAC-derived with `JWT_SECRET`. Changing the session secret changes ownership hashes, so preserve it across deployments. Keys and prompts are never returned by public endpoints or bundled in Angular.

## Limits and operational behavior

- Input: 2,000 characters. Generated answer: 800 output tokens, normally under 180 words. Scope/language/location classification: 180 tokens. Recent history: six messages, 1,200 characters each. Generative context: up to eight passages of 1,400 characters. The general distribution-calendar overview is rendered directly from current database rows and can exceed the normal answer length.
- Eight messages/minute per actor, 80/minute globally, four concurrent generations per process. Persistent daily limit: 60 per signed-in user, 20 per anonymous IP hash. A conversation holds up to 200 messages; older conversations remain in history.
- Stable client request UUIDs make accepted message retries idempotent. Database processing locks prevent overlapping responses within a conversation. Monotonic message sequences preserve order even when timestamps tie.
- PDF: 10 MB, 1,000 pages and one million extracted characters maximum; selectable text is required. Encrypted, corrupt and scanned/no-text PDFs return explicit errors. Extraction runs in an isolated subprocess with a memory limit and 30-second timeout. The original binary is not kept; text fragments, embeddings, filename, hash and metadata are stored.
- Up to 2,000 supplemental sources and 200,000 indexed fragments, with at most 1,000 fragments per source. Limits are shared by ingestion and retrieval in `api/services/chatbotLimits.js`; the admin source response reports `maxSources` and `maxChunks`. Disabled sources count toward capacity; deleted sources do not. The source-count and indexed-content limits return distinct errors. Retrieval scans all eligible vectors up to the corpus limit in batches of 500 to bound memory, retaining up to 64 candidate passages and sending at most eight to the model. Scan latency grows with the number of eligible fragments, so monitor retrieval latency as the library grows. Pages require public HTTPS, DNS/IP validation on every redirect, and pinned DNS sockets. No internal, loopback, cloud metadata or alternate-port URLs. HTML scripts/navigation are excluded. Updating a page is an explicit admin action.
- Published articles, active resources and enabled upcoming distributions are fetched from the current application database. No patient records, beneficiary profiles, private health answers or private operational tables enter model context.

## Safety and audit

Fixed server policy takes precedence over the admin's tone prompt. Direct override attempts and common emergency phrases have deterministic handling. A scope classifier, bounded untrusted source envelopes, educational-health restrictions, JSON validation, exact citation registry validation and escaped plain text provide layered protection. No prompt-only defense can guarantee immunity to all injection techniques; the model has no browsing or mutation tools and cannot access private application records. Unknown or unsupported answers decline rather than invent evidence.

Conversation messages retain model, tokens, latency, request IDs, settings revision, result status and source references. Audit events record configuration/source changes, retained cited fragment IDs and administrator transcript views. The welcome disclaimer explains AI limitations, physician consultation and avoiding sensitive medical information. History is deliberately retained as requested; define a retention/deletion policy before a wider public rollout.

Source disabling/deletion affects new retrieval immediately and is checked again before an in-flight answer is returned. Deletion retains source metadata and historic citations but removes the searchable fragments. Previously delivered answers remain in the audit trail. Admin prompt changes store a digest and revision in the audit trail; the current prompt itself remains in the admin-only settings table.

## Verification and release

Backend tests cover live-role authorization, owner isolation, hidden public defaults, prompt secrecy, injection/refusal handling, citations, rate limits, SSRF/DNS rebinding, bounded retrieval and actual searchable PDF extraction. Capacity regression tests verify ingestion beyond the former 100-source limit and retrieval beyond 500 sources and the former 20,000-fragment cutoff, while enforcing the new shared limits. Angular widget tests cover sessions, cross-tab changes, stale responses, drag snapping, input bounds and retry IDs. Production deployment uses the existing `deploy-backend.ps1` and frontend `tools/deploy-frontend.ps1` scripts after commits are pushed. The capacity increase requires no database migration or reindexing of existing sources.

Native applications bundle their web assets. A web deployment does not update already-installed App Store/Play Store applications; synchronize Capacitor and distribute a new native release through the existing store workflow.

### Retrieval corrections (2026-10-30)

- Published articles and active resources are read in pages of 200 until the eligible catalog is exhausted. The former limits of 400 articles and 600 resources no longer hide newer entries. Retrieval includes article subtitles, categories, filters and tags, plus resource cities, categories, services and price classifications. Resource details are searched beyond their initial 1,400 characters. Existing metadata is used without a migration or reindex; this release does not add tagging fields for uploaded URLs/PDFs.
- Resource and article discovery use matching internal pages. General educational answers reserve space for relevant internal content before adding external evidence. Internal citations use application-relative URLs so the existing widget navigates inside Community, including installed native clients. Uploaded approved references remain available for general educational questions.
- The default upcoming-distributions question lists every published future calendar date, with distribution counts and separate no-distribution notices. The current calendar is evaluated using California's date, without the former 120-day horizon. For unusually large calendars, the first 120 distinct dates are displayed with an explicit remainder notice and a link to the complete calendar. Detailed/location-specific calendar questions still use bounded passages and must not claim that subset is the full calendar. All rows contributing to an overview are revalidated before the reply is saved.
- The chat receives no GPS or IP/VPN geolocation. Nearby requests ask for city/ZIP when it has not been supplied. The scope classifier extracts a city/ZIP, which must be present in recent **user** text before retrieval accepts it. Resources and calendar entries then match the catalog's city/address text. This is not geocoding, radius matching, live opening-hours verification or a guarantee of geographic coverage; unmatched locations return an honest directory/calendar fallback. New topics no longer automatically inherit every short previous question.

The assistant is generative for contextual/educational answers, using retrieved approved content (RAG). It stores each conversation and supplies the last six messages as context; it does not learn from previous answers, train itself from chats, or reuse other conversations as knowledge. A repeated request with the **same client request UUID** replays its saved answer to prevent duplicate billing/messages. A newly submitted question retrieves current content and produces a new answer; a new conversation does not inherit another conversation's history. Calendar overviews, safety responses and location clarification are deterministic.

Production verification for this release found 1,075 active resources (475 were beyond the old cutoff), and 73 upcoming calendar entries across 43 dates. Tests cover both default prompts, internal citations, metadata-only matches, long resource details, topic changes, missing/explicit locations, calendar pagination, cancellations and the California/UTC date boundary. The change is entirely server-side, so no frontend or app-store release is required.

### Admin source batches

The admin Sources tab accepts UTF-8 CSV website lists (`url` required, `title` optional; comma or semicolon separators) and multiple PDFs with a separate optional title for each file. The browser previews and processes these batches sequentially through the existing authenticated single-source API, preserving per-row results and allowing stop-after-current and retry of unfinished rows. Keep the page open while processing. Each URL still uses the existing HTTPS/DNS checks and every source is independently audited. Duplicate website URLs are skipped. CSV export fetches the complete current source library, including disabled sources and PDFs, independently of search and page selection; it exports metadata, not PDF contents. Source text is escaped and spreadsheet formulas are neutralized. Search applies to the full library before paginating in groups of 20.
