import { z } from 'zod';

import { RAW_CHUNK_LOCAL_DIR_DEFAULT } from './services/raw-chunk-storage-config';

const booleanFlag = z.preprocess((value) => {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    return value === 'true';
  }

  return false;
}, z.boolean());

const configSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  DATABASE_URL: z.string().min(1),
  PORT: z.coerce.number().int().positive().default(4827),
  ADMIN_API_KEY: z.string().min(1),
  HEALTH_API_KEY: z.string().default(''),
  PERSISTIO_MODE: z.enum(['api', 'worker', 'combined']).default('combined'),
  EMBEDDER_PROVIDER: z.enum(['openai', 'ollama', 'tei', 'vertex']).default('openai'),
  STORAGE_EMBEDDING_DIMENSIONS: z.coerce.number().int().positive().max(2000).default(1536),
  OPENAI_API_KEY: z.string().default(''),
  OPENAI_EMBEDDING_MODEL: z.string().default('text-embedding-3-small'),
  OLLAMA_BASE_URL: z.string().url().default('http://ollama:11434'),
  OLLAMA_EMBEDDING_MODEL: z.string().default('nomic-embed-text'),
  TEI_BASE_URL: z.string().url().default('http://tei:80'),
  TEI_EMBEDDING_MODEL: z.string().default('text-embeddings-inference'),
  VERTEX_PROJECT_ID: z.string().default(''),
  VERTEX_LOCATION: z.string().default('europe-west2'),
  VERTEX_EMBEDDING_MODEL: z.string().default('gemini-embedding-001'),
  VERTEX_EMBEDDING_DIMENSIONS: z.coerce.number().int().min(0).max(2000).default(0),
  EXTRACTOR_BASE_URL: z.string().url().default('https://api.openai.com/v1'),
  EXTRACTOR_API_KEY: z.string().default(''),
  EXTRACTOR_MODEL: z.string().default('gpt-4o-mini'),
  EXTRACTOR_PROMPT_FILE: z.string().default('prompts/extractor.txt'),
  EXTRACTION_BASE_URL: z.string().default(''),
  EXTRACTION_API_KEY: z.string().default(''),
  EXTRACTION_MODEL: z.string().default(''),
  ESCALATION_BASE_URL: z.string().default(''),
  ESCALATION_API_KEY: z.string().default(''),
  ESCALATION_MODEL: z.string().default(''),
  LLM_SYSTEM_PROMPT_PREFIX: z.string().default(''),
  LLM_REASONING_EFFORT: z.string().default(''),
  CURATOR_AUTO_RUN: booleanFlag,
  CURATOR_BASE_URL: z.string().default(''),
  CURATOR_API_KEY: z.string().default(''),
  CURATOR_MODEL: z.string().default('claude-sonnet-4-5'),
  CURATOR_PROMPT_FILE: z.string().default('prompts/curator.txt'),
  PROMPTS_DIR: z.string().default('/prompts'),
  RAW_CHUNK_STORAGE_PROVIDER: z.enum(['local', 'azure_blob', 'gcs']).default('local'),
  RAW_CHUNK_LOCAL_DIR: z.string().default(RAW_CHUNK_LOCAL_DIR_DEFAULT),
  RAW_CHUNK_BLOB_CONTAINER: z.string().default('raw-chunks'),
  AZURE_STORAGE_ACCOUNT_NAME: z.string().default(''),
  AZURE_STORAGE_CONNECTION_STRING: z.string().default(''),
  RAW_CHUNK_GCS_BUCKET: z.string().default(''),
  TELEMETRY_PROVIDER: z.enum(['none', 'azure_monitor', 'gcp_otlp']).default('none'),
  APPLICATIONINSIGHTS_CONNECTION_STRING: z.string().default(''),
  OTEL_SERVICE_NAME: z.string().default('persistio-server'),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().default('http://localhost:4318'),
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: z.string().default(''),
  OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: z.string().default(''),
  EVENT_PUBLISHER: z.enum(['noop', 'webhook', 'azure_service_bus', 'gcp_pubsub']).default('noop'),
  PLATFORM_EVENTS_WEBHOOK_URL: z.string().default(''),
  PLATFORM_EVENTS_WEBHOOK_SECRET: z.string().default(''),
  PLATFORM_EVENTS_WEBHOOK_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
  AZURE_SERVICE_BUS_CONNECTION_STRING: z.string().default(''),
  AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE: z.string().default(''),
  AZURE_SERVICE_BUS_TOPIC_OR_QUEUE: z.string().default(''),
  GCP_PUBSUB_PROJECT_ID: z.string().default(''),
  GCP_PUBSUB_TOPIC: z.string().default(''),
  EXTRACTION_INTERVAL_MS: z.coerce.number().int().positive().default(30000),
  EXTRACTION_BATCH_SIZE: z.coerce.number().int().positive().default(20),
  MAX_EXTRACTION_RETRIES: z.coerce.number().int().positive().default(5),
  CURATION_INTERVAL_MS: z.coerce.number().int().positive().default(2000),
  CURATION_BATCH_SIZE: z.coerce.number().int().positive().default(5),
  EVENT_OUTBOX_DISPATCH_INTERVAL_MS: z.coerce.number().int().positive().default(10000),
  EVENT_OUTBOX_BATCH_SIZE: z.coerce.number().int().positive().default(20),
  EVENT_OUTBOX_MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  EVENT_OUTBOX_RETRY_BASE_DELAY_MS: z.coerce.number().int().positive().default(1000),
  EVENT_OUTBOX_RETRY_MAX_DELAY_MS: z.coerce.number().int().positive().default(300000),
  EXTRACTION_WORKER_CONCURRENCY: z.coerce.number().int().positive().max(20).default(5),
  ARBITRATION_BATCH_SIZE: z.coerce.number().int().positive().default(15),
  DB_POOL_MAX: z.coerce.number().int().positive().default(20),
  MAX_INGEST_CHUNKS: z.coerce.number().int().positive().default(100),
  INGEST_EMBEDDING_CONCURRENCY: z.coerce.number().int().positive().default(4),
  INGEST_RATE_LIMIT_RPM: z.coerce.number().int().positive().default(60),
  INGEST_CHUNK_MAX_CHARS: z.coerce.number().int().positive().default(8000),
  BULK_INGEST_MAX_CHUNKS: z.coerce.number().int().positive().default(2048),
  BULK_INGEST_BODY_LIMIT_BYTES: z.coerce.number().int().positive().default(25 * 1024 * 1024),
  EXTRACTION_SCORE_THRESHOLD: z.coerce.number().int().min(1).max(10).default(5),
  SEGMENTATION_THRESHOLD: z.coerce.number().min(0).max(1).default(0.75),
  DEFAULT_TOKEN_BUDGET: z.coerce.number().int().positive().default(2000),
  DEFAULT_RECALL_TOP_K: z.coerce.number().int().positive().default(10),
  MIN_RECALL_SIMILARITY: z.coerce.number().min(0).max(1).default(0.30),
  CIRCUIT_BREAKER_THRESHOLD: z.coerce.number().int().positive().default(3),
  CIRCUIT_BREAKER_PROBE_INTERVAL_MS: z.coerce.number().int().positive().default(300000),
  CIRCUIT_BREAKER_MAX_PROBE_INTERVAL_MS: z.coerce.number().int().positive().default(600000),
  MEMORY_ARCHIVE_TTL_DAYS: z.coerce.number().int().positive().default(90),
  CONTRADICTION_SCAN_ENABLED: booleanFlag,
  CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH: z.coerce.number().int().positive().default(20),
  CONTRADICTION_SCAN_MIN_SIMILARITY: z.coerce.number().min(0).max(1).default(0.70),
  CONFIDENCE_DECAY_INTERVAL_DAYS: z.coerce.number().int().positive().default(30),
  CONFIDENCE_DECAY_AUTO_ARCHIVE_SALIENCE_THRESHOLD: z.coerce.number().min(0).max(1).default(0.3),
  SUBJECT_INJECTION_TOP_N: z.coerce.number().int().positive().default(10),
  SUBJECT_INJECTION_RECENT_N: z.coerce.number().int().positive().default(10),
  SUBJECT_TEXT_MATCH_DISTANCE: z.coerce.number().int().min(0).default(2),
  SUBJECT_EMBED_HIGH_THRESHOLD: z.coerce.number().min(0).max(1).default(0.92),
  SUBJECT_EMBED_LOW_THRESHOLD: z.coerce.number().min(0).max(1).default(0.80),
  ENCRYPTION_ENABLED: booleanFlag,
  KEY_PROVIDER: z.enum(['azure_key_vault', 'gcp_kms']).default('azure_key_vault'),
  KEY_VAULT_URI: z.string().default(''),
  KEK_KEY_NAME: z.string().default(''),
  GCP_KMS_KEY_NAME: z.string().default('')
}).superRefine((value, ctx) => {
  if (value.EMBEDDER_PROVIDER === 'openai' && !value.OPENAI_API_KEY) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'OPENAI_API_KEY is required when EMBEDDER_PROVIDER=openai',
      path: ['OPENAI_API_KEY']
    });
  }

  if (value.EMBEDDER_PROVIDER === 'vertex' && !value.VERTEX_PROJECT_ID) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'VERTEX_PROJECT_ID is required when EMBEDDER_PROVIDER=vertex',
      path: ['VERTEX_PROJECT_ID']
    });
  }

  validateRoleOverrideTriplet(ctx, value, 'EXTRACTION');
  validateRoleOverrideTriplet(ctx, value, 'ESCALATION');

  if (value.ENCRYPTION_ENABLED && value.KEY_PROVIDER === 'azure_key_vault' && !value.KEY_VAULT_URI) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'KEY_VAULT_URI is required when ENCRYPTION_ENABLED=true and KEY_PROVIDER=azure_key_vault',
      path: ['KEY_VAULT_URI']
    });
  }

  if (value.ENCRYPTION_ENABLED && value.KEY_PROVIDER === 'azure_key_vault' && !value.KEK_KEY_NAME) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'KEK_KEY_NAME is required when ENCRYPTION_ENABLED=true and KEY_PROVIDER=azure_key_vault',
      path: ['KEK_KEY_NAME']
    });
  }

  if (value.ENCRYPTION_ENABLED && value.KEY_PROVIDER === 'gcp_kms' && !value.GCP_KMS_KEY_NAME) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'GCP_KMS_KEY_NAME is required when ENCRYPTION_ENABLED=true and KEY_PROVIDER=gcp_kms',
      path: ['GCP_KMS_KEY_NAME']
    });
  }

  if (value.CURATOR_AUTO_RUN && !value.CURATOR_BASE_URL) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'CURATOR_BASE_URL is required when CURATOR_AUTO_RUN=true',
      path: ['CURATOR_BASE_URL']
    });
  }

  if (value.CURATOR_AUTO_RUN && !value.CURATOR_API_KEY) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'CURATOR_API_KEY is required when CURATOR_AUTO_RUN=true',
      path: ['CURATOR_API_KEY']
    });
  }

  if (
    value.RAW_CHUNK_STORAGE_PROVIDER === 'azure_blob' &&
    !value.AZURE_STORAGE_CONNECTION_STRING &&
    !value.AZURE_STORAGE_ACCOUNT_NAME
  ) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING is required when RAW_CHUNK_STORAGE_PROVIDER=azure_blob',
      path: ['AZURE_STORAGE_ACCOUNT_NAME']
    });
  }

  if (value.RAW_CHUNK_STORAGE_PROVIDER === 'gcs' && !value.RAW_CHUNK_GCS_BUCKET) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'RAW_CHUNK_GCS_BUCKET is required when RAW_CHUNK_STORAGE_PROVIDER=gcs',
      path: ['RAW_CHUNK_GCS_BUCKET']
    });
  }

  if (value.TELEMETRY_PROVIDER === 'azure_monitor' && !value.APPLICATIONINSIGHTS_CONNECTION_STRING) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'APPLICATIONINSIGHTS_CONNECTION_STRING is required when TELEMETRY_PROVIDER=azure_monitor',
      path: ['APPLICATIONINSIGHTS_CONNECTION_STRING']
    });
  }

  validateOptionalHttpUrl(ctx, value.OTEL_EXPORTER_OTLP_ENDPOINT, 'OTEL_EXPORTER_OTLP_ENDPOINT');
  validateOptionalHttpUrl(ctx, value.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT');
  validateOptionalHttpUrl(ctx, value.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, 'OTEL_EXPORTER_OTLP_METRICS_ENDPOINT');

  const shouldValidateEventPublisher = value.PERSISTIO_MODE !== 'api';

  if (shouldValidateEventPublisher && value.EVENT_PUBLISHER === 'webhook') {
    if (!value.PLATFORM_EVENTS_WEBHOOK_URL) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'PLATFORM_EVENTS_WEBHOOK_URL is required when EVENT_PUBLISHER=webhook',
        path: ['PLATFORM_EVENTS_WEBHOOK_URL']
      });
    } else if (!isHttpUrl(value.PLATFORM_EVENTS_WEBHOOK_URL)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'PLATFORM_EVENTS_WEBHOOK_URL must be a valid http(s) URL when EVENT_PUBLISHER=webhook',
        path: ['PLATFORM_EVENTS_WEBHOOK_URL']
      });
    }
    if (!value.PLATFORM_EVENTS_WEBHOOK_SECRET) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'PLATFORM_EVENTS_WEBHOOK_SECRET is required when EVENT_PUBLISHER=webhook',
        path: ['PLATFORM_EVENTS_WEBHOOK_SECRET']
      });
    }
  }

  if (shouldValidateEventPublisher && value.EVENT_PUBLISHER === 'azure_service_bus') {
    if (!value.AZURE_SERVICE_BUS_TOPIC_OR_QUEUE) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'AZURE_SERVICE_BUS_TOPIC_OR_QUEUE is required when EVENT_PUBLISHER=azure_service_bus',
        path: ['AZURE_SERVICE_BUS_TOPIC_OR_QUEUE']
      });
    }
    if (!value.AZURE_SERVICE_BUS_CONNECTION_STRING && !value.AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'AZURE_SERVICE_BUS_CONNECTION_STRING or AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE is required when EVENT_PUBLISHER=azure_service_bus',
        path: ['AZURE_SERVICE_BUS_CONNECTION_STRING']
      });
    } else if (
      !value.AZURE_SERVICE_BUS_CONNECTION_STRING &&
      !isServiceBusFullyQualifiedNamespace(value.AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE)
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE must be a valid Service Bus namespace FQDN when EVENT_PUBLISHER=azure_service_bus',
        path: ['AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE']
      });
    }
  }

  if (shouldValidateEventPublisher && value.EVENT_PUBLISHER === 'gcp_pubsub' && !value.GCP_PUBSUB_TOPIC) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'GCP_PUBSUB_TOPIC is required when EVENT_PUBLISHER=gcp_pubsub',
      path: ['GCP_PUBSUB_TOPIC']
    });
  }
});

export type AppConfig = z.infer<typeof configSchema>;

let cachedConfig: AppConfig | undefined;

export function getConfig(): AppConfig {
  cachedConfig ??= configSchema.parse(normalizeConfigEnv(process.env));
  return cachedConfig;
}

function normalizeConfigEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const telemetryProvider = env.TELEMETRY_PROVIDER?.trim();

  return {
    ...env,
    TELEMETRY_PROVIDER: telemetryProvider || (env.APPLICATIONINSIGHTS_CONNECTION_STRING ? 'azure_monitor' : undefined),
    EXTRACTION_WORKER_CONCURRENCY: env.EXTRACTION_WORKER_CONCURRENCY ?? env.WORKER_CONCURRENCY,
    DB_POOL_MAX: env.DB_POOL_MAX ?? env.PG_POOL_MAX
  };
}

function validateRoleOverrideTriplet(
  ctx: z.RefinementCtx,
  value: z.infer<typeof configSchema>,
  prefix: 'EXTRACTION' | 'ESCALATION'
) {
  const fields = [
    `${prefix}_BASE_URL`,
    `${prefix}_API_KEY`,
    `${prefix}_MODEL`
  ] as const;
  const setFields = fields.filter((field) => Boolean(value[field]));
  if (setFields.length === 0 || setFields.length === fields.length) {
    return;
  }

  for (const field of fields) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `${prefix}_BASE_URL, ${prefix}_API_KEY, and ${prefix}_MODEL must be set together or all left empty`,
      path: [field]
    });
  }
}

function isHttpUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

function validateOptionalHttpUrl(ctx: z.RefinementCtx, value: string, path: string) {
  if (!value || isHttpUrl(value)) {
    return;
  }

  ctx.addIssue({
    code: z.ZodIssueCode.custom,
    message: `${path} must be a valid http(s) URL when set`,
    path: [path]
  });
}

function isServiceBusFullyQualifiedNamespace(value: string): boolean {
  return /^[a-z0-9][a-z0-9-]*[a-z0-9]\.servicebus\.windows\.net$/i.test(value);
}

export function getConfiguredEmbeddingDimensions(config = getConfig()): number {
  return config.STORAGE_EMBEDDING_DIMENSIONS;
}

export function getStorageEmbeddingDimensions(config = getConfig()): number {
  return config.STORAGE_EMBEDDING_DIMENSIONS;
}
