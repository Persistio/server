# Persistio Server

Self-hosted, API-first memory service for AI agents. Persistio stores raw conversation chunks, extracts durable memories asynchronously, and exposes semantic recall with vault-scoped API key auth.

## Public prompts and release acceptance

`prompts/extractor.txt` and `prompts/curator.txt` are canonical public
defaults, with SHA-256 identities in `prompts/manifest.json`. The image
uses `/prompts` plus legacy `/app/prompts`. The source release builds
this Dockerfile and accepts its compiled loaders and facts-object /
curation-plan.v2 contracts before export. The manifest-only
`build-context/` retains the reviewed locked server dependency graph.
Explicit mounts and custom-vault prompts remain supported, subject to
mandatory contracts and PROMPTS_DIR realpath confinement. Source-only
runtimes must set PROMPTS_DIR to the checkout's absolute prompts path.
Image rollback does not roll back vault overrides. Never commit private
prompts or credentials.

## Prerequisites

- Docker and Docker Compose
- PostgreSQL with the `pgvector` extension, or the included Compose database
- An embedding provider and an OpenAI-compatible chat provider for extraction/curation

## Quick Start

```bash
git clone https://github.com/Persistio/server.git persistio-server
cd persistio-server
cp .env.example .env
# set ADMIN_API_KEY and provider keys in .env
docker compose up
```

The API listens on `http://localhost:4827`.

## Self-Host Modes

Persistio uses one image that can run in three modes through `PERSISTIO_MODE`:

| Value | Role | Notes |
|---|---|---|
| `combined` | HTTP server and workers in one process | Default for local and single-host use |
| `api` | HTTP server only | Use when splitting the API from workers |
| `worker` | Extraction and curation workers only | Run against the same database as the API |

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

The full OpenAPI description is in [`openapi.yaml`](./openapi.yaml).

## Core Configuration

| Variable | Description | Default |
|---|---|---|
| `PERSISTIO_MODE` | `combined`, `api`, or `worker` | `combined` |
| `DATABASE_URL` | PostgreSQL connection string | required |
| `PORT` | HTTP listen port | `4827` |
| `ADMIN_API_KEY` | API key for `/admin/*` routes | required |
| `HEALTH_API_KEY` | Optional `/health` key sent as `X-Health-Key` | unset |
| `EMBEDDER_PROVIDER` | `openai`, `ollama`, or `tei` | `ollama` in Compose |
| `STORAGE_EMBEDDING_DIMENSIONS` | pgvector storage dimensions | `1024` in Compose |
| `OPENAI_API_KEY` | Key for OpenAI embeddings when `EMBEDDER_PROVIDER=openai` | unset |
| `OPENAI_EMBEDDING_MODEL` | OpenAI embedding model | `text-embedding-3-small` |
| `OLLAMA_BASE_URL` | Ollama base URL | `http://ollama:11434` |
| `OLLAMA_EMBEDDING_MODEL` | Ollama embedding model | `qwen3-embedding:0.6b` |
| `TEI_BASE_URL` | Text Embeddings Inference base URL | `http://tei:80` |
| `TEI_EMBEDDING_MODEL` | TEI embedding model name | `text-embeddings-inference` |
| `EXTRACTOR_BASE_URL` | OpenAI-compatible chat endpoint | required |
| `EXTRACTOR_API_KEY` | Chat provider API key | required |
| `EXTRACTOR_MODEL` | Chat model for extraction | required |
| `CURATOR_AUTO_RUN` | Enable background curation | `false` |
| `CURATOR_BASE_URL` | Optional OpenAI-compatible curator endpoint | unset |
| `CURATOR_API_KEY` | Optional curator API key | unset |
| `CURATOR_MODEL` | Optional curator model | unset |
| `CURATOR_REASONING_EFFORT` | Optional curator-only reasoning effort | unset |
| `CURATOR_REASONING_TOKEN_RESERVE` | Tokens reserved for hidden curator reasoning | `0` |
| `CURATOR_MAX_GENERATION_TOKENS` | Curator generation hard ceiling | `8192` |
| `RAW_CHUNK_STORAGE_PROVIDER` | Raw chunk storage backend | `local` |
| `RAW_CHUNK_LOCAL_DIR` | Local raw chunk storage root | `./data/raw-chunks` |

## OpenClaw Plugin

Use the [official Persistio OpenClaw plugin](https://github.com/Persistio/openclaw-persistio/blob/main/README.md) to connect OpenClaw agents to your self-host Persistio server.

## TLS

Persistio speaks plain HTTP on port `4827`. Put it behind a local or self-managed reverse proxy if you need HTTPS.
