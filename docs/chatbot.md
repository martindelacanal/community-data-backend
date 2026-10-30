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

- Input: 2,000 characters. Generated answer: 800 output tokens, normally under 180 words. Scope/language/location classification plus bilingual search keywords: 300 tokens. Recent history: six messages, 1,200 characters each. Generative context: up to eight passages of 1,400 characters. The general distribution-calendar overview is rendered directly from current database rows and can exceed the normal answer length.
- Eight messages/minute per actor, 160/minute globally, eight concurrent generations per process. Persistent daily limit: 60 per signed-in user, 20 per anonymous IP hash. A conversation holds up to 200 messages; older conversations remain in history.
- Stable client request UUIDs make accepted message retries idempotent. Database processing locks prevent overlapping responses within a conversation. Monotonic message sequences preserve order even when timestamps tie.
- PDF: 10 MB, 1,000 pages and one million extracted characters maximum; selectable text is required. Encrypted, corrupt and scanned/no-text PDFs return explicit errors. Extraction runs in an isolated subprocess with a memory limit and 30-second timeout. The original binary is not kept; text fragments, embeddings, filename, hash and metadata are stored.
- Up to 10,000 supplemental sources and 200,000 indexed fragments, with at most 1,000 fragments per source. Disabled sources count toward capacity; deleted sources do not. Source-count and indexed-content limits return distinct errors. The admin API reports `maxSources`, `maxChunks` and `maxConcurrentImports`. Imports/reindexing share two slots per process, reserved before PDF buffering through transaction completion. The web batch uploader uses two workers, stops starting new rows on cancellation/throttling, and drains active requests. PDF extraction remains serialized in an isolated 192 MB worker; network downloads and embedding calls can overlap.
- Retrieval searches all eligible documents using `chatbot_search` FULLTEXT and English/Spanish keywords from the existing classifier call. It selects up to 2,048 lightweight matches, diversifies by source, and loads at most 512 embeddings in batches of 128. Vector/lexical reranking retains 64 candidates and sends at most eight passages to the model. A healthy index with no matches does not scan embeddings; the bounded compatibility fallback runs only if the index table is missing (one scan at a time, 10-second budget including queueing). Text matching is an approximation: bilingual keywords improve recall, but cannot guarantee every semantically related passage will be selected. The 10,000-source cap is not a promise of constant latency at every document size or request rate.
- Pages require public HTTPS, DNS/IP validation on every redirect, and pinned DNS sockets. No internal, loopback, cloud metadata or alternate-port URLs. HTML scripts/navigation are excluded. Updating a page is an explicit admin action.
- Published articles, active resources and enabled upcoming distributions are fetched from the current application database. No patient records, beneficiary profiles, private health answers or private operational tables enter model context.

## Safety and audit

Fixed server policy takes precedence over the admin's tone prompt. Direct override attempts and common emergency phrases have deterministic handling. A scope classifier, bounded untrusted source envelopes, educational-health restrictions, JSON validation, exact citation registry validation and escaped plain text provide layered protection. No prompt-only defense can guarantee immunity to all injection techniques; the model has no browsing or mutation tools and cannot access private application records. Unknown or unsupported answers decline rather than invent evidence.

Conversation messages retain model, tokens, latency, request IDs, settings revision, result status and source references. Audit events record configuration/source changes, retained cited fragment IDs and administrator transcript views. The welcome disclaimer explains AI limitations, physician consultation and avoiding sensitive medical information. History is deliberately retained as requested; define a retention/deletion policy before a wider public rollout.

Source disabling/deletion affects new retrieval immediately and is checked again before an in-flight answer is returned. Deletion retains source metadata and historic citations but removes the searchable fragments. Previously delivered answers remain in the audit trail. Admin prompt changes store a digest and revision in the audit trail; the current prompt itself remains in the admin-only settings table.

## Verification and release

Backend tests cover live-role authorization, owner isolation, hidden public defaults, prompt secrecy, injection/refusal handling, citations, rate limits, SSRF/DNS rebinding, bounded retrieval and actual searchable PDF extraction. Capacity regression tests verify ingestion beyond the former 100-source limit and retrieval beyond 500 sources and the former 20,000-fragment cutoff, while enforcing the new shared limits. Angular widget tests cover sessions, cross-tab changes, stale responses, drag snapping, input bounds and retry IDs. Production deployment uses the existing `deploy-backend.ps1` and frontend `tools/deploy-frontend.ps1` scripts after commits are pushed. Before deploying the indexed-search release, run `node scripts/migrateChatbotSearch.js --server-env` from the backend directory (or use `--target=production` with the local migration configuration). This additive, idempotent migration backfills the lightweight search table without re-embedding PDFs/URLs or altering the large chunk table. Run it again after rollout to catch imports made by the prior release. New imports/reindexing write search rows in the same transaction; chunk deletion cascades to the search index. Fresh `migrateChatbot.js` installs include this step. Rollback may leave the additive search table in place; rerun the backfill before re-deploying this release.

Native applications bundle their web assets. A web deployment does not update already-installed App Store/Play Store applications; synchronize Capacitor and distribute a new native release through the existing store workflow.

### Retrieval corrections (2026-10-30)

- Published articles and active resources are read in pages of 200 until the eligible catalog is exhausted. The former limits of 400 articles and 600 resources no longer hide newer entries. Retrieval includes article subtitles, categories, filters and tags, plus resource cities, categories, services and price classifications. Resource details are searched beyond their initial 1,400 characters. Existing metadata is used without a migration or reindex; this release does not add tagging fields for uploaded URLs/PDFs.
- Resource and article discovery use matching internal pages. General educational answers reserve space for relevant internal content before adding external evidence. Internal citations use application-relative URLs so the existing widget navigates inside Community, including installed native clients. Uploaded approved references remain available for general educational questions.
- The default upcoming-distributions question lists every published future calendar date, with distribution counts and separate no-distribution notices. The current calendar is evaluated using California's date, without the former 120-day horizon. For unusually large calendars, the first 120 distinct dates are displayed with an explicit remainder notice and a link to the complete calendar. Detailed/location-specific calendar questions still use bounded passages and must not claim that subset is the full calendar. All rows contributing to an overview are revalidated before the reply is saved.
- The chat receives no GPS or IP/VPN geolocation. Nearby requests ask for city/ZIP when it has not been supplied. The scope classifier extracts a city/ZIP, which must be present in recent **user** text before retrieval accepts it. Resources and calendar entries then match the catalog's city/address text. This is not geocoding, radius matching, live opening-hours verification or a guarantee of geographic coverage; unmatched locations return an honest directory/calendar fallback. New topics no longer automatically inherit every short previous question.

The assistant is generative for contextual/educational answers, using retrieved approved content (RAG). It stores each conversation and supplies the last six messages as context; it does not learn from previous answers, train itself from chats, or reuse other conversations as knowledge. A repeated request with the **same client request UUID** replays its saved answer to prevent duplicate billing/messages. A newly submitted question retrieves current content and produces a new answer; a new conversation does not inherit another conversation's history. Calendar overviews, safety responses and location clarification are deterministic.

Production verification for this release found 1,075 active resources (475 were beyond the old cutoff), and 73 upcoming calendar entries across 43 dates. Tests cover both default prompts, internal citations, metadata-only matches, long resource details, topic changes, missing/explicit locations, calendar pagination, cancellations and the California/UTC date boundary. The change is entirely server-side, so no frontend or app-store release is required.

### Follow-up review of production conversations (2026-10-30)

The read-only review covered 18 stored conversations / 44 assistant replies, including implementation QA. There were 11 `no_context` replies, seven scope refusals, two safety responses and no stored provider errors. Historical outcomes are not proof that the current release still fails: ten synthetic, non-identifying regression scenarios were replayed against the deployed service first. Four remained unsuccessful: cooking with strawberries received at a distribution, pineapple recipes using the regional term “ananá”, preparing/storing eggplant after a calendar question, and educational-center discovery incorrectly refused as out of scope.

The follow-up separates practical cooking/storage intent from calendar logistics, adds bilingual ingredient aliases (including ananá/piña/pineapple), ranks exact ingredient evidence more strongly, and excludes pharmacy prescription listings from cooking retrieval. Parks, educational centers, childcare and senior centers use the internal resource directory and are explicitly within scope. A prior city/ZIP question no longer drags unrelated follow-ups into the old topic; a genuine location answer still preserves it. General no-evidence replies no longer append unrelated medical advice, and the generator receives California's current time to avoid recommending a start time that already passed today.

PDFs and imported web pages are searchable knowledge, independent of optional taxonomy fields. Production has 16 enabled ready PDFs (114 indexed fragments) and 1,888 enabled ready URL sources (11,240 fragments). The history already contains a citation to the PHFE WIC produce-storage PDF, and a live replay against the pre-change service cited it again. Adding administrator-defined categories/tags to those supplemental sources is separate future work; it is not required for their ingestion or retrieval.

### Admin source batches

The admin Sources tab accepts UTF-8 CSV website lists (`url` required, `title` optional; comma or semicolon separators) and multiple PDFs with a separate optional title for each file. The browser previews and processes these batches sequentially through the existing authenticated single-source API, preserving per-row results and allowing stop-after-current and retry of unfinished rows. Keep the page open while processing. Each URL still uses the existing HTTPS/DNS checks and every source is independently audited. Duplicate website URLs are skipped. CSV export fetches the complete current source library, including disabled sources and PDFs, independently of search and page selection; it exports metadata, not PDF contents. Source text is escaped and spreadsheet formulas are neutralized. Search applies to the full library before paginating in groups of 20.


## Capacity verification (2026-10-30)

- Existing production library: 1,904 ready sources, 11,354 chunks. Three read-only comparisons selected the same source titles; transferred vector/content data fell from about 128.8 MB to 4.4–5.8 MB per query. Baseline retrieval was 9.3–9.9 seconds. The final small-batch implementation measured 1.2–2.2 seconds in a later run; these timings exclude answer generation and vary with cache/server load.
- Disposable MySQL tables exercised 10,000 synthetic sources and 60,000 full-size text/vector chunks. Eight concurrent retrievals finished in 5.8 seconds, fetched 512 vectors each in batches no larger than 128, and left at least 310 MB available on the approximately 1 GB / 1 CPU host. Idempotent backfill and delete cascade were verified. The disposable tables were removed.
- Eight real-library retrievals plus two small real searchable-PDF extraction/embedding workflows completed in 5.0 seconds. All sampled public config probes returned 200 (11–585 ms); minimum available memory was 315 MB. This is a short burst test, not a sustained-throughput guarantee or a test of two maximum-size PDFs. PDF worker serialization and existing file/page/text caps remain necessary.
- The full backend suite passed 457 tests with 9 existing skips; 33 Angular chatbot tests passed. Ten synthetic conversation regressions derived from earlier production findings passed, including the produce-storage PDF. No customer conversations or knowledge sources were added by these checks.
- The 200,000-fragment hard boundary is covered by unit tests; the real MySQL load test covered 60,000 fragments. Before approaching 200,000, measure sustained traffic and storage growth again. AI-provider quotas/costs are independent of these application limits.
