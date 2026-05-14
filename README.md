# Persistio Server

Self-hosted, API-first memory service for AI agents. Stores raw conversation chunks, extracts durable facts asynchronously, and exposes semantic recall over both layers with vault-scoped API key auth.

## Prerequisites

- Docker & Docker Compose
- A PostgreSQL instance with the [pgvector](https://github.com/pgvector/pgvector) extension enabled
- An OpenAI API key (or a compatible Ollama instance) for embeddings and extraction

## Quick Start

```bash
git clone https://github.com/chriscoveyduck/persistio.git
cd persistio
cp .env.example .env
# fill in your DATABASE_URL, ADMIN_API_KEY, and OPENAI_API_KEY
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

## Configuration

| Variable | Description | Default |
|---|---|---|
| `PERSISTIO_MODE` | Deployment mode: `combined`, `api`, or `worker` | `combined` |
| `DATABASE_URL` | PostgreSQL connection string | — |
| `PORT` | HTTP listen port | `4827` |
| `ADMIN_API_KEY` | API key for `/admin/*` routes | — |
| `HEALTH_API_KEY` | Optional secret for `/health` endpoint. If set, requests must include `X-Health-Key: <value>`. Leave empty to allow unauthenticated health checks. | `""` |
| `EMBEDDER_PROVIDER` | Embedding provider: `openai` or `ollama` | `openai` |
| `OPENAI_API_KEY` | OpenAI API key (required when `EMBEDDER_PROVIDER=openai`) | — |
| `OPENAI_EMBEDDING_MODEL` | OpenAI embedding model | `text-embedding-3-small` |
| `OLLAMA_BASE_URL` | Ollama base URL | `http://ollama:11434` |
| `OLLAMA_EMBEDDING_MODEL` | Ollama embedding model | `nomic-embed-text` |
| `EXTRACTOR_BASE_URL` | Legacy OpenAI-compatible base URL used by extraction and escalation when role-specific URLs are unset | `https://api.openai.com/v1` |
| `EXTRACTOR_API_KEY` | Legacy API key used by extraction and escalation when role-specific keys are unset | — |
| `EXTRACTOR_MODEL` | Legacy chat model used by extraction and escalation when role-specific models are unset | `gpt-4o-mini` |
| `EXTRACTION_BASE_URL` | Optional OpenAI-compatible base URL for routine extraction/session-context calls | `EXTRACTOR_BASE_URL` |
| `EXTRACTION_API_KEY` | Optional API key for routine extraction/session-context calls | `EXTRACTOR_API_KEY` |
| `EXTRACTION_MODEL` | Optional model for routine extraction/session-context calls, for example Gemini Flash | `EXTRACTOR_MODEL` |
| `ESCALATION_BASE_URL` | Optional OpenAI-compatible base URL for arbitration/entity-resolution escalation calls | `EXTRACTOR_BASE_URL` |
| `ESCALATION_API_KEY` | Optional API key for arbitration/entity-resolution escalation calls | `EXTRACTOR_API_KEY` |
| `ESCALATION_MODEL` | Optional model for arbitration/entity-resolution escalation calls, for example Claude Sonnet | `EXTRACTOR_MODEL` |
| `EXTRACTION_INTERVAL_MS` | Worker polling interval (ms) | `30000` |
| `EXTRACTION_BATCH_SIZE` | Max chunks processed per extraction cycle | `20` |
| `MEMORY_ARCHIVE_TTL_DAYS` | Days before stale memories are archived | `90` |
| `DEFAULT_TOKEN_BUDGET` | Client-side recall token budget hint | `2000` |
| `DEFAULT_RECALL_TOP_K` | Default result count for recall | `10` |
| `ENCRYPTION_ENABLED` | Enable envelope encryption for memory subjects | `false` |
| `KEY_VAULT_URI` | Key vault URI (required when `ENCRYPTION_ENABLED=true`) | `""` |
| `KEK_KEY_NAME` | Key encryption key name in the vault | `""` |

When switching a role to a different provider, set that role's `*_BASE_URL`, `*_API_KEY`, and `*_MODEL` together. Per-field fallback is supported for compatibility, but mixed provider settings are easy to misconfigure.

Vault `gemini_rpm` / `gemini_tpm` limits apply only to extractor roles resolved to Gemini models or Google AI endpoints. Mixed setups such as Gemini extraction plus non-Gemini escalation debit Gemini quota for extraction only. Extractor circuit-breaker logs now use role-specific service names: `extractor.extraction` and `extractor.escalation`.

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
