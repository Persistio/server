# Persistio Server

Self-hosted, API-first memory service for AI agents. Stores raw conversation chunks, extracts durable facts asynchronously, and exposes semantic recall over both layers with vault-scoped API key auth.

## Prerequisites

- Docker & Docker Compose
- A PostgreSQL instance with the [pgvector](https://github.com/pgvector/pgvector) extension enabled
- Private Ollama for Qwen embeddings, plus hosted provider keys for extraction/curation/escalation

## Quick Start

```bash
git clone https://github.com/chriscoveyduck/persistio.git
cd persistio
cp .env.example .env
# fill in DATABASE_URL, ADMIN_API_KEY, and the Gemini/Anthropic keys in .env
docker compose up
```

The API listens on `http://localhost:4827`.

## Deployment Modes

Persistio uses a single Docker image that can run in three modes, controlled by the `PERSISTIO_MODE` environment variable:

| Value | Role | Notes |
|---|---|---|
| `combined` | Runs the HTTP server and extraction worker in a single process | **Default.** Suitable for local dev and single-host deployments. |
| `api` | HTTP server only — ingest, recall, memories, admin, health | Run this on your internet-facing instance. |
| `worker` | Extraction pipeline only — no HTTP server | Run alongside an `api` instance, sharing the same database. No external ingress required. |

The `api` + `worker` split is useful when you want to scale the HTTP layer and the extraction pipeline independently, or keep extraction off your public-facing host.

## Prompt Customization

The published Docker image includes basic public prompts in `/prompts` for extraction and curation. They are intentionally minimal starter prompts: enough to exercise the pipeline and provide general behavior, but not enough to encode every product policy, domain-specific memory rule, model quirk, or safety requirement.

Before using Persistio for a real deployment, refine the extractor and curator prompts for your use case. Decide what durable memory means for your users, what should be rejected as noise, how secrets and sensitive details should be handled, how curation should merge or archive memories, and how strict the output format needs to be for your selected models. Mount your refined prompt files at runtime and set `EXTRACTOR_PROMPT_FILE`, `CURATOR_PROMPT_FILE`, and `PROMPTS_DIR`; the repository includes `docker-compose.override.example.yml` as a minimal example.

Vaults can also set a prompt `type` through the admin API. Unset or `general` vaults use the mounted/default prompts; `custom` vaults use owner-supplied extraction and curation prompts after validation. Custom prompts require an `unlimited` plan and are stored through the same vault encryption path as customer memory data when encryption is enabled.

## API Overview

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service health probe |
| POST | `/v1/ingest` | Append raw conversation chunks for extraction |
| POST | `/v1/recall` | Semantic search across memories and raw chunks |
| GET | `/v1/memories` | List memories for the authenticated vault |
| POST | `/v1/memories` | Manually add a memory |
| GET | `/v1/memories/:id` | Fetch a single memory |
| PATCH | `/v1/memories/:id` | Update a memory |
| DELETE | `/v1/memories/:id` | Archive a memory |
| POST | `/v1/extract` | Trigger an extraction run |
| GET | `/v1/jobs/:id` | Check extraction job status |
| GET | `/stats` | Vault plan, quota, and usage stats |
| POST | `/admin/vaults` | Create a vault and return its API key |
| GET | `/admin/vaults` | List vaults |
| DELETE | `/admin/vaults/:id` | Delete a vault |
| POST | `/admin/vaults/:id/rotate-key` | Rotate a vault API key |

The full OpenAPI description is in [`openapi.yaml`](https://github.com/chriscoveyduck/persistio/blob/main/openapi.yaml).

`POST /v1/ingest` requires every chunk to include a `timestamp` ISO 8601 datetime with UTC `Z` or an explicit offset representing the original conversation turn time. The OpenClaw plugin supplies this automatically; custom clients must set it. Memory and recall responses expose `source_timestamp` as the source event time, while `created_at` remains the memory row creation/extraction time.

## Configuration

| Variable | Description | Default |
|---|---|---|
| `PERSISTIO_MODE` | Deployment mode: `combined`, `api`, or `worker` | `combined` |
| `DATABASE_URL` | PostgreSQL connection string | — |
| `PORT` | HTTP listen port | `4827` |
| `ADMIN_API_KEY` | API key for `/admin/*` routes | — |
| `HEALTH_API_KEY` | Optional secret for `/health` endpoint. If set, requests must include `X-Health-Key: <value>`. Leave empty to allow unauthenticated health checks. | `""` |
| `EMBEDDER_PROVIDER` | Embedding provider: `openai`, `ollama`, `tei`, or `vertex`. Compose self-host sets `ollama`. | `openai` |
| `STORAGE_EMBEDDING_DIMENSIONS` | pgvector storage dimensions. Compose self-host sets `1024` for `qwen3-embedding:0.6b`. | `1536` |
| `OPENAI_API_KEY` | OpenAI API key (required when `EMBEDDER_PROVIDER=openai`) | — |
| `OPENAI_EMBEDDING_MODEL` | OpenAI embedding model | `text-embedding-3-small` |
| `OLLAMA_BASE_URL` | Ollama base URL | `http://ollama:11434` |
| `OLLAMA_EMBEDDING_MODEL` | Ollama embedding model. Compose self-host sets `qwen3-embedding:0.6b`. | `nomic-embed-text` |
| `TEI_BASE_URL` | Hugging Face Text Embeddings Inference base URL. Used when `EMBEDDER_PROVIDER=tei`. | `http://tei:80` |
| `TEI_EMBEDDING_MODEL` | TEI OpenAI-compatible embedding model name. TEI examples use `text-embeddings-inference`. | `text-embeddings-inference` |
| `VERTEX_PROJECT_ID` | Google Cloud project ID. Required when `EMBEDDER_PROVIDER=vertex`. | `""` |
| `VERTEX_LOCATION` | Vertex AI region for embeddings. GCP UK deployments should use `europe-west2`. | `europe-west2` |
| `VERTEX_EMBEDDING_MODEL` | Vertex AI embedding model. | `gemini-embedding-001` |
| `VERTEX_EMBEDDING_DIMENSIONS` | Optional Vertex output dimensionality. `0` uses `STORAGE_EMBEDDING_DIMENSIONS`. | `0` |
| `EXTRACTOR_BASE_URL` | Legacy OpenAI-compatible base URL used by extraction and escalation when role-specific URLs are unset. Compose self-host sets the Google OpenAI-compatible endpoint. | `https://api.openai.com/v1` |
| `EXTRACTOR_API_KEY` | Legacy API key used by extraction and escalation when role-specific keys are unset | — |
| `EXTRACTOR_MODEL` | Legacy chat model used by extraction and escalation when role-specific models are unset. Compose self-host sets `gemini-2.5-flash`. | `gpt-4o-mini` |
| `EXTRACTOR_PROMPT_FILE` | Extractor prompt file. Public image default is a basic starter prompt; mount and configure a refined prompt for production use. | Docker image: `/prompts/extractor.txt`; compose sample: `prompts/extractor.txt` |
| `EXTRACTION_BASE_URL` | Optional OpenAI-compatible base URL for routine extraction/session-context calls | `EXTRACTOR_BASE_URL` |
| `EXTRACTION_API_KEY` | Optional API key for routine extraction/session-context calls | `EXTRACTOR_API_KEY` |
| `EXTRACTION_MODEL` | Optional model for routine extraction/session-context calls, for example Gemini Flash | `EXTRACTOR_MODEL` |
| `ESCALATION_BASE_URL` | Optional OpenAI-compatible base URL for conflict and contradiction escalation calls | `EXTRACTOR_BASE_URL` |
| `ESCALATION_API_KEY` | Optional API key for conflict and contradiction escalation calls | `EXTRACTOR_API_KEY` |
| `ESCALATION_MODEL` | Optional model for conflict and contradiction escalation calls, for example Claude Sonnet | `EXTRACTOR_MODEL` |
| `CURATOR_BASE_URL` | OpenAI-compatible base URL for curator calls | — |
| `CURATOR_MODEL` | Curator model. Compose self-host sets `gemini-2.5-flash`. | `claude-sonnet-4-5` |
| `CURATOR_PROMPT_FILE` | Curator prompt file. Public image default is a basic starter prompt; mount and configure a refined prompt for production use. | Docker image: `/prompts/curator.txt`; compose sample: `prompts/curator.txt` |
| `PROMPTS_DIR` | Directory used to constrain prompt file loading. | Docker image: `/prompts`; local image: `/app/prompts` |
| `RAW_CHUNK_STORAGE_PROVIDER` | Raw chunk payload storage backend: `local`, `azure_blob`, or `gcs`. | `local` |
| `RAW_CHUNK_LOCAL_DIR` | Local filesystem root for raw chunks when `RAW_CHUNK_STORAGE_PROVIDER=local`. Relative paths resolve from the server repository root so runtime and operator scripts agree. | `./data/raw-chunks` |
| `RAW_CHUNK_BLOB_CONTAINER` | Azure Blob container for raw chunks when `RAW_CHUNK_STORAGE_PROVIDER=azure_blob`. | `raw-chunks` |
| `AZURE_STORAGE_ACCOUNT_NAME` | Storage account name for Azure Blob raw chunk storage when using managed identity. | — |
| `AZURE_STORAGE_CONNECTION_STRING` | Optional Azure Storage connection string for local/admin migration use. If set, it takes precedence over managed identity. | — |
| `RAW_CHUNK_GCS_BUCKET` | Google Cloud Storage bucket for raw chunks when `RAW_CHUNK_STORAGE_PROVIDER=gcs`. | `""` |
| `TELEMETRY_PROVIDER` | OpenTelemetry export provider: `none`, `azure_monitor`, or `gcp_otlp`. If unset, legacy deployments with `APPLICATIONINSIGHTS_CONNECTION_STRING` automatically use `azure_monitor`. | `none` |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | Azure Monitor/Application Insights connection string. Required when `TELEMETRY_PROVIDER=azure_monitor`. | `""` |
| `OTEL_SERVICE_NAME` | OpenTelemetry service name for traces and metrics. Cloud Run deployments set this to the role service name. | `persistio-server` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP HTTP collector base endpoint for `TELEMETRY_PROVIDER=gcp_otlp`. The runtime appends `/v1/traces` and `/v1/metrics`. | `http://localhost:4318` |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Optional exact OTLP HTTP trace endpoint override. | `""` |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Optional exact OTLP HTTP metric endpoint override. | `""` |
| `EVENT_PUBLISHER` | Platform event publisher adapter: `noop`, `webhook`, `azure_service_bus`, or `gcp_pubsub`. | `noop` |
| `GCP_PUBSUB_PROJECT_ID` | Optional Google Cloud project id for Pub/Sub event publishing. Empty uses application default credentials/project detection. | `""` |
| `GCP_PUBSUB_TOPIC` | Pub/Sub topic name for platform event delivery when `EVENT_PUBLISHER=gcp_pubsub`. | `""` |
| `SUBJECT_INJECTION_TOP_N` | Number of most-used vault subjects injected into extraction prompts as canonicalisation hints | `10` |
| `SUBJECT_INJECTION_RECENT_N` | Number of recently active vault subjects injected into extraction prompts as canonicalisation hints | `10` |
| `SUBJECT_TEXT_MATCH_DISTANCE` | Maximum Levenshtein distance for free text subject matching before embedding fallback | `2` |
| `SUBJECT_EMBED_HIGH_THRESHOLD` | Subject embedding similarity at or above which an extracted subject is automatically aliased to the known canonical subject | `0.92` |
| `SUBJECT_EMBED_LOW_THRESHOLD` | Subject embedding similarity at or above which the worker asks the extraction model to arbitrate the subject | `0.80` |
| `EXTRACTION_INTERVAL_MS` | Worker polling interval (ms) | `30000` |
| `EXTRACTION_BATCH_SIZE` | Max chunks processed per extraction cycle | `20` |
| `MEMORY_ARCHIVE_TTL_DAYS` | Days before stale memories are archived | `90` |
| `DEFAULT_TOKEN_BUDGET` | Client-side recall token budget hint | `2000` |
| `DEFAULT_RECALL_TOP_K` | Default result count for recall | `10` |
| `MIN_RECALL_SIMILARITY` | Default semantic recall quality floor; callers can override per request with `min_similarity` | `0.30` |
| `ENCRYPTION_ENABLED` | Enable envelope encryption for memory subjects | `false` |
| `KEY_PROVIDER` | Key encryption provider: `azure_key_vault` or `gcp_kms`. | `azure_key_vault` |
| `KEY_VAULT_URI` | Azure Key Vault URI. Required when `ENCRYPTION_ENABLED=true` and `KEY_PROVIDER=azure_key_vault`. | `""` |
| `KEK_KEY_NAME` | Azure Key Vault key encryption key name. | `""` |
| `GCP_KMS_KEY_NAME` | Full Cloud KMS crypto key resource name. Required when `ENCRYPTION_ENABLED=true` and `KEY_PROVIDER=gcp_kms`. | `""` |

`npm run migrate:raw-chunks` writes migrated payloads through the configured raw chunk storage provider, including `local`, `azure_blob`, and `gcs`. When using `local`, set `RAW_CHUNK_LOCAL_DIR` explicitly to the same path or mounted volume the server uses.

The compose and `.env.example` self-host profile uses private Ollama `qwen3-embedding:0.6b` on the recall hot path. TEI can be used as a drop-in embedding provider by setting `EMBEDDER_PROVIDER=tei`, `TEI_BASE_URL`, `TEI_EMBEDDING_MODEL=text-embeddings-inference`, and the same storage dimension. GCP deployments can use managed Vertex embeddings with `EMBEDDER_PROVIDER=vertex`, `VERTEX_PROJECT_ID`, `VERTEX_LOCATION=europe-west2`, and `VERTEX_EMBEDDING_MODEL=gemini-embedding-001`. Fresh databases use 1024-dimensional pgvector columns when `STORAGE_EMBEDDING_DIMENSIONS=1024`; existing databases with stored vectors must be re-embedded before changing dimensions. The re-embedding script reads raw chunks from `local`, `azure_blob`, or `gcs`, and decrypts encrypted vault data through the configured `KEY_PROVIDER`. The runtime code default remains 1536 for upgrade safety when operators have not opted into the Qwen or Vertex profile. See [`../../docs/reembedding-migration.md`](../../docs/reembedding-migration.md).

When switching a role to a different provider, set that role's `*_BASE_URL`, `*_API_KEY`, and `*_MODEL` together. Incomplete role-specific overrides are rejected so provider keys and endpoints cannot be mixed accidentally.

Vault AI throughput is provider-neutral. Plans define `ai_requests_per_minute` and `ai_tokens_per_minute`, with per-role weights for extraction, escalation, and curation; each role uses its own per-vault in-process bucket. Background jobs that exhaust local AI budget are deferred in the queue rather than failed. Extractor circuit-breaker logs use role-specific service names: `extractor.extraction` and `extractor.escalation`.

## TLS / HTTPS

Persistio does not implement TLS directly. The server speaks plain HTTP on port 4827 and is designed to run behind a reverse proxy that handles TLS termination. **Do not expose port 4827 directly to the internet.**

Recommended setup: place Persistio behind [Traefik](https://traefik.io), [nginx](https://nginx.org), or an equivalent ingress controller that terminates HTTPS and proxies to the container internally.

## OpenClaw Plugin

If you're using [OpenClaw](https://openclaw.ai), install the official plugin to connect your agents to Persistio automatically:

```bash
npm install -g @persistio/openclaw-plugin
```

See the [plugin repo](https://github.com/persistio/openclaw-persistio) for setup instructions.

## License

Business Source License 1.1 (BUSL-1.1).
