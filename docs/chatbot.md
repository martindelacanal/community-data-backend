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

- Input: 2,000 characters. Generated answer: 800 output tokens, normally under 180 words. Scope classification: 100 tokens. Recent history: six messages, 1,200 characters each. Context: up to eight passages of 1,400 characters.
- Eight messages/minute per actor, 80/minute globally, four concurrent generations per process. Persistent daily limit: 60 per signed-in user, 20 per anonymous IP hash. A conversation holds up to 200 messages; older conversations remain in history.
- Stable client request UUIDs make accepted message retries idempotent. Database processing locks prevent overlapping responses within a conversation. Monotonic message sequences preserve order even when timestamps tie.
- PDF: 10 MB, 1,000 pages and one million extracted characters maximum; selectable text is required. Encrypted, corrupt and scanned/no-text PDFs return explicit errors. Extraction runs in an isolated subprocess with a memory limit and 30-second timeout. The original binary is not kept; text fragments, embeddings, filename, hash and metadata are stored.
- Up to 100 supplemental sources and 20,000 indexed fragments. Retrieval scans vectors in batches of 500 to bound memory. Pages require public HTTPS, DNS/IP validation on every redirect, and pinned DNS sockets. No internal, loopback, cloud metadata or alternate-port URLs. HTML scripts/navigation are excluded. Updating a page is an explicit admin action.
- Published articles, active resources and enabled upcoming distributions are fetched from the current application database. No patient records, beneficiary profiles, private health answers or private operational tables enter model context.

## Safety and audit

Fixed server policy takes precedence over the admin's tone prompt. Direct override attempts and common emergency phrases have deterministic handling. A scope classifier, bounded untrusted source envelopes, educational-health restrictions, JSON validation, exact citation registry validation and escaped plain text provide layered protection. No prompt-only defense can guarantee immunity to all injection techniques; the model has no browsing or mutation tools and cannot access private application records. Unknown or unsupported answers decline rather than invent evidence.

Conversation messages retain model, tokens, latency, request IDs, settings revision, result status and source references. Audit events record configuration/source changes, retained cited fragment IDs and administrator transcript views. The welcome disclaimer explains AI limitations, physician consultation and avoiding sensitive medical information. History is deliberately retained as requested; define a retention/deletion policy before a wider public rollout.

Source disabling/deletion affects new retrieval immediately and is checked again before an in-flight answer is returned. Deletion retains source metadata and historic citations but removes the searchable fragments. Previously delivered answers remain in the audit trail. Admin prompt changes store a digest and revision in the audit trail; the current prompt itself remains in the admin-only settings table.

## Verification and release

Backend tests cover live-role authorization, owner isolation, hidden public defaults, prompt secrecy, injection/refusal handling, citations, rate limits, SSRF/DNS rebinding, bounded retrieval and actual searchable PDF extraction. Angular widget tests cover sessions, cross-tab changes, stale responses, drag snapping, input bounds and retry IDs. Production deployment uses the existing `deploy-backend.ps1` and frontend `tools/deploy-frontend.ps1` scripts after commits are pushed.

Native applications bundle their web assets. A web deployment does not update already-installed App Store/Play Store applications; synchronize Capacitor and distribute a new native release through the existing store workflow.
