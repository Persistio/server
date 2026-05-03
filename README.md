# Persistio Server

**Self-hosted semantic memory service for AI agents.**

Persistio Server stores raw conversation chunks, asynchronously extracts durable facts and memories using an LLM, and exposes semantic recall via a REST API. Designed to be embedded in agent workflows — give your agents persistent, queryable memory without relying on a hosted service.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
- An OpenAI-compatible API key (for embeddings and extraction), **or** [Ollama](https://ollama.com/) running locally

---

## Quick Start

```bash
git clone https://github.com/Persistio/server.git
cd server
cp .env.example .env
# Edit .env with your API keys
docker compose up
```

The API will be available at **http://localhost:4827**.

---

## Using Ollama Instead of OpenAI

Set `EMBEDDER_PROVIDER=ollama` in your `.env` and point `OLLAMA_BASE_URL` at your Ollama instance. To include the bundled Ollama service (useful for local dev with no existing Ollama install):

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```

This starts Ollama alongside Persistio and Postgres in the same Compose project.

---

## API Overview

All tenant endpoints require a `Bearer` token obtained when you create a tenant. Admin endpoints require the `X-Admin-Key` header.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/ingest` | Append raw conversation chunks |
| `POST` | `/v1/recall` | Semantic recall across memories and raw chunks |
| `GET` | `/v1/memories` | List memories (supports filtering and pagination) |
| `POST` | `/v1/memories` | Create a memory manually |
| `GET` | `/v1/memories/:id` | Fetch a single memory |
| `PATCH` | `/v1/memories/:id` | Update a memory |
| `DELETE` | `/v1/memories/:id` | Archive a memory |
| `POST` | `/v1/extract` | Trigger extraction manually |
| `GET` | `/v1/jobs/:id` | Check extraction job status |
| `POST` | `/admin/tenants` | Create a tenant |
| `GET` | `/admin/tenants` | List all tenants |
| `DELETE` | `/admin/tenants/:id` | Delete a tenant |

Full API spec: [`openapi.yaml`](./openapi.yaml)

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` and edit as needed.

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://mnemex:mnemex@postgres:5432/mnemex` |
| `PORT` | HTTP port the server listens on | `4827` |
| `ADMIN_API_KEY` | Secret key for admin endpoints | *(required — change before deploying)* |
| `EMBEDDER_PROVIDER` | Embedding provider: `openai` or `ollama` | `openai` |
| `OPENAI_API_KEY` | OpenAI (or compatible) API key | *(required for openai provider)* |
| `OPENAI_EMBEDDING_MODEL` | Embedding model to use | `text-embedding-3-small` |
| `OLLAMA_BASE_URL` | Ollama base URL | `http://ollama:11434` |
| `OLLAMA_EMBEDDING_MODEL` | Ollama embedding model | `nomic-embed-text` |
| `EXTRACTOR_BASE_URL` | LLM extractor API base URL | `https://api.openai.com/v1` |
| `EXTRACTOR_API_KEY` | LLM extractor API key | *(required)* |
| `EXTRACTOR_MODEL` | LLM model for extraction | `gpt-4o-mini` |
| `EXTRACTION_INTERVAL_MS` | How often the extraction daemon runs (ms) | `30000` |
| `EXTRACTION_BATCH_SIZE` | Max chunks processed per extraction run | `20` |
| `MEMORY_ARCHIVE_TTL_DAYS` | Days before archived memories are purged | `90` |
| `DEFAULT_TOKEN_BUDGET` | Hint to clients: max tokens in recall response | `2000` |
| `DEFAULT_RECALL_TOP_K` | Hint to clients: default number of recall results | `10` |

---

## Tenant Management

Persistio is multi-tenant. Use the admin API to create and manage tenants:

```bash
curl -X POST http://localhost:4827/admin/tenants \
  -H "X-Admin-Key: your-admin-api-key" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-agent"}'
```

The response includes an API key scoped to that tenant. Each tenant is fully isolated — memories, sessions, and extraction jobs are never shared across tenants.

---

## OpenClaw Plugin

Use Persistio as the memory backend for your [OpenClaw](https://openclaw.ai) agents with the official plugin:

👉 **[https://github.com/Persistio/openclaw-persistio](https://github.com/Persistio/openclaw-persistio)**

---

## License

[Business Source License 1.1](https://mariadb.com/bsl11/)
