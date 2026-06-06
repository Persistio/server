#!/usr/bin/env bash
set -euo pipefail

print_only=false
if [[ "${1:-}" == "--print-only" ]]; then
  print_only=true
  shift
fi

if [[ $# -gt 0 ]]; then
  echo "Usage: $0 [--print-only]" >&2
  exit 2
fi

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud CLI is required" >&2
  exit 1
fi

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "$PROJECT_ID" ]]; then
  echo "PROJECT_ID is required or must be configured in gcloud" >&2
  exit 1
fi

REGION="${REGION:-europe-west2}"
IMAGE="${IMAGE:-chriscoveyduck/persistio:latest}"
API_SERVICE="${API_SERVICE:-persistio-api}"
WORKER_SERVICE="${WORKER_SERVICE:-persistio-worker}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-persistio-platform@${PROJECT_ID}.iam.gserviceaccount.com}"
CLOUD_SQL_INSTANCE="${CLOUD_SQL_INSTANCE:-persistio-db}"
CLOUD_SQL_CONNECTION_NAME="${CLOUD_SQL_CONNECTION_NAME:-${PROJECT_ID}:${REGION}:${CLOUD_SQL_INSTANCE}}"
RAW_CHUNK_GCS_BUCKET="${RAW_CHUNK_GCS_BUCKET:-persistio-raw-chunks-${PROJECT_ID}}"
GCP_KMS_KEY_NAME="${GCP_KMS_KEY_NAME:-projects/${PROJECT_ID}/locations/${REGION}/keyRings/persistio/cryptoKeys/vault-dek}"
GCP_PUBSUB_TOPIC="${GCP_PUBSUB_TOPIC:-platform-events}"

DATABASE_URL_SECRET="${DATABASE_URL_SECRET:-persistio-database-url}"
ADMIN_API_KEY_SECRET="${ADMIN_API_KEY_SECRET:-persistio-admin-api-key}"
HEALTH_API_KEY_SECRET="${HEALTH_API_KEY_SECRET:-persistio-health-api-key}"
GOOGLE_AI_API_KEY_SECRET="${GOOGLE_AI_API_KEY_SECRET:-persistio-google-ai-api-key}"
SECRET_VERSION="${SECRET_VERSION:-latest}"

API_MIN_INSTANCES="${API_MIN_INSTANCES:-1}"
API_MAX_INSTANCES="${API_MAX_INSTANCES:-3}"
API_CPU="${API_CPU:-1}"
API_MEMORY="${API_MEMORY:-1Gi}"
API_CONCURRENCY="${API_CONCURRENCY:-40}"

WORKER_MIN_INSTANCES="${WORKER_MIN_INSTANCES:-1}"
WORKER_MAX_INSTANCES="${WORKER_MAX_INSTANCES:-1}"
WORKER_CPU="${WORKER_CPU:-1}"
WORKER_MEMORY="${WORKER_MEMORY:-1Gi}"

TELEMETRY_PROVIDER="${TELEMETRY_PROVIDER:-none}"
OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4318}"

join_csv() {
  local IFS=,
  echo "$*"
}

run_cmd() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  if [[ "$print_only" == false ]]; then
    "$@"
  fi
}

common_env=(
  "NODE_ENV=production"
  "LOG_LEVEL=${LOG_LEVEL:-info}"
  "DB_POOL_MAX=${DB_POOL_MAX:-10}"
  "EMBEDDER_PROVIDER=vertex"
  "STORAGE_EMBEDDING_DIMENSIONS=${STORAGE_EMBEDDING_DIMENSIONS:-1024}"
  "VERTEX_PROJECT_ID=${VERTEX_PROJECT_ID:-$PROJECT_ID}"
  "VERTEX_LOCATION=${VERTEX_LOCATION:-$REGION}"
  "VERTEX_EMBEDDING_MODEL=${VERTEX_EMBEDDING_MODEL:-gemini-embedding-001}"
  "VERTEX_EMBEDDING_DIMENSIONS=${VERTEX_EMBEDDING_DIMENSIONS:-1024}"
  "RAW_CHUNK_STORAGE_PROVIDER=gcs"
  "RAW_CHUNK_GCS_BUCKET=${RAW_CHUNK_GCS_BUCKET}"
  "ENCRYPTION_ENABLED=${ENCRYPTION_ENABLED:-true}"
  "KEY_PROVIDER=gcp_kms"
  "GCP_KMS_KEY_NAME=${GCP_KMS_KEY_NAME}"
  "EVENT_PUBLISHER=gcp_pubsub"
  "GCP_PUBSUB_PROJECT_ID=${GCP_PUBSUB_PROJECT_ID:-$PROJECT_ID}"
  "GCP_PUBSUB_TOPIC=${GCP_PUBSUB_TOPIC}"
  "TELEMETRY_PROVIDER=${TELEMETRY_PROVIDER}"
  "OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT}"
)

api_env=(
  "PERSISTIO_MODE=api"
  "OTEL_SERVICE_NAME=${API_SERVICE}"
  "INGEST_EMBEDDING_CONCURRENCY=${INGEST_EMBEDDING_CONCURRENCY:-4}"
  "INGEST_CHUNK_MAX_CHARS=${INGEST_CHUNK_MAX_CHARS:-8000}"
  "BULK_INGEST_MAX_CHUNKS=${BULK_INGEST_MAX_CHUNKS:-2048}"
  "BULK_INGEST_BODY_LIMIT_BYTES=${BULK_INGEST_BODY_LIMIT_BYTES:-26214400}"
  "DEFAULT_TOKEN_BUDGET=${DEFAULT_TOKEN_BUDGET:-2000}"
  "DEFAULT_RECALL_TOP_K=${DEFAULT_RECALL_TOP_K:-10}"
  "MIN_RECALL_SIMILARITY=${MIN_RECALL_SIMILARITY:-0.30}"
)

worker_env=(
  "PERSISTIO_MODE=worker"
  "OTEL_SERVICE_NAME=${WORKER_SERVICE}"
  "EXTRACTOR_BASE_URL=${EXTRACTOR_BASE_URL:-https://generativelanguage.googleapis.com/v1beta/openai/}"
  "EXTRACTOR_MODEL=${EXTRACTOR_MODEL:-gemini-2.5-flash}"
  "EXTRACTION_BASE_URL=${EXTRACTION_BASE_URL:-https://generativelanguage.googleapis.com/v1beta/openai/}"
  "EXTRACTION_MODEL=${EXTRACTION_MODEL:-gemini-2.5-flash}"
  "ESCALATION_BASE_URL=${ESCALATION_BASE_URL:-https://generativelanguage.googleapis.com/v1beta/openai/}"
  "ESCALATION_MODEL=${ESCALATION_MODEL:-gemini-2.5-flash}"
  "CURATOR_AUTO_RUN=${CURATOR_AUTO_RUN:-true}"
  "CURATOR_BASE_URL=${CURATOR_BASE_URL:-https://generativelanguage.googleapis.com/v1beta/openai/}"
  "CURATOR_MODEL=${CURATOR_MODEL:-gemini-2.5-flash}"
  "PROMPTS_DIR=/prompts"
  "EXTRACTOR_PROMPT_FILE=/prompts/extractor.txt"
  "CURATOR_PROMPT_FILE=/prompts/curator.txt"
  "EXTRACTION_INTERVAL_MS=${EXTRACTION_INTERVAL_MS:-30000}"
  "EXTRACTION_BATCH_SIZE=${EXTRACTION_BATCH_SIZE:-20}"
  "EXTRACTION_WORKER_CONCURRENCY=${EXTRACTION_WORKER_CONCURRENCY:-5}"
  "CURATION_INTERVAL_MS=${CURATION_INTERVAL_MS:-2000}"
  "CURATION_BATCH_SIZE=${CURATION_BATCH_SIZE:-5}"
  "EVENT_OUTBOX_DISPATCH_INTERVAL_MS=${EVENT_OUTBOX_DISPATCH_INTERVAL_MS:-10000}"
  "EVENT_OUTBOX_BATCH_SIZE=${EVENT_OUTBOX_BATCH_SIZE:-20}"
)

common_secrets=(
  "DATABASE_URL=${DATABASE_URL_SECRET}:${SECRET_VERSION}"
  "ADMIN_API_KEY=${ADMIN_API_KEY_SECRET}:${SECRET_VERSION}"
  "HEALTH_API_KEY=${HEALTH_API_KEY_SECRET}:${SECRET_VERSION}"
)

worker_secrets=(
  "EXTRACTOR_API_KEY=${GOOGLE_AI_API_KEY_SECRET}:${SECRET_VERSION}"
  "EXTRACTION_API_KEY=${GOOGLE_AI_API_KEY_SECRET}:${SECRET_VERSION}"
  "ESCALATION_API_KEY=${GOOGLE_AI_API_KEY_SECRET}:${SECRET_VERSION}"
  "CURATOR_API_KEY=${GOOGLE_AI_API_KEY_SECRET}:${SECRET_VERSION}"
)

echo "Project:          ${PROJECT_ID}"
echo "Region:           ${REGION}"
echo "Image:            ${IMAGE}"
echo "Service account:  ${SERVICE_ACCOUNT}"
echo "Cloud SQL:        ${CLOUD_SQL_CONNECTION_NAME}"
echo "GCS bucket:       ${RAW_CHUNK_GCS_BUCKET}"
echo "KMS key:          ${GCP_KMS_KEY_NAME}"
echo "Pub/Sub topic:    ${GCP_PUBSUB_TOPIC}"
echo "Telemetry:        ${TELEMETRY_PROVIDER}"
echo "Secret version:   ${SECRET_VERSION}"
echo

run_cmd gcloud run deploy "$API_SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --image "$IMAGE" \
  --service-account "$SERVICE_ACCOUNT" \
  --platform managed \
  --allow-unauthenticated \
  --ingress all \
  --port 4827 \
  --cpu "$API_CPU" \
  --memory "$API_MEMORY" \
  --concurrency "$API_CONCURRENCY" \
  --min-instances "$API_MIN_INSTANCES" \
  --max-instances "$API_MAX_INSTANCES" \
  --add-cloudsql-instances "$CLOUD_SQL_CONNECTION_NAME" \
  --set-env-vars "$(join_csv "${common_env[@]}" "${api_env[@]}")" \
  --set-secrets "$(join_csv "${common_secrets[@]}")"

run_cmd gcloud run deploy "$WORKER_SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --image "$IMAGE" \
  --service-account "$SERVICE_ACCOUNT" \
  --platform managed \
  --no-allow-unauthenticated \
  --ingress internal \
  --port 4827 \
  --cpu "$WORKER_CPU" \
  --memory "$WORKER_MEMORY" \
  --concurrency 1 \
  --min-instances "$WORKER_MIN_INSTANCES" \
  --max-instances "$WORKER_MAX_INSTANCES" \
  --no-cpu-throttling \
  --add-cloudsql-instances "$CLOUD_SQL_CONNECTION_NAME" \
  --set-env-vars "$(join_csv "${common_env[@]}" "${worker_env[@]}")" \
  --set-secrets "$(join_csv "${common_secrets[@]}" "${worker_secrets[@]}")"
