import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const originalEnv = process.env;

function baseEnv(): NodeJS.ProcessEnv {
  return {
    ADMIN_API_KEY: 'test-admin-key',
    DATABASE_URL: 'postgres://example.com/test',
    OPENAI_API_KEY: 'test-openai-key'
  };
}

async function parseConfig(overrides: NodeJS.ProcessEnv = {}) {
  vi.resetModules();
  process.env = {
    ...baseEnv(),
    ...overrides
  };

  const { getConfig } = await import('./config');
  return getConfig();
}

describe('config environment normalization', () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = { ...baseEnv() };
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.resetModules();
  });

  it('uses PG_POOL_MAX as the legacy alias for DB_POOL_MAX', async () => {
    process.env.PG_POOL_MAX = '50';

    const { getConfig } = await import('./config');

    expect(getConfig().DB_POOL_MAX).toBe(50);
  });

  it('lets DB_POOL_MAX take precedence over PG_POOL_MAX', async () => {
    process.env.DB_POOL_MAX = '30';
    process.env.PG_POOL_MAX = '50';

    const { getConfig } = await import('./config');

    expect(getConfig().DB_POOL_MAX).toBe(30);
  });

  it('keeps storage embedding dimensions upgrade-safe by default', async () => {
    const { getConfig, getConfiguredEmbeddingDimensions } = await import('./config');

    const config = getConfig();

    expect(config.EMBEDDER_PROVIDER).toBe('openai');
    expect(config.OLLAMA_EMBEDDING_MODEL).toBe('nomic-embed-text');
    expect(config.TEI_BASE_URL).toBe('http://tei:80');
    expect(config.TEI_EMBEDDING_MODEL).toBe('text-embeddings-inference');
    expect(config.EXTRACTOR_BASE_URL).toBe('https://api.openai.com/v1');
    expect(config.EXTRACTOR_MODEL).toBe('gpt-4o-mini');
    expect(config.CURATOR_MODEL).toBe('claude-sonnet-4-5');
    expect(config.STORAGE_EMBEDDING_DIMENSIONS).toBe(1536);
    expect(config.TELEMETRY_PROVIDER).toBe('none');
    expect(getConfiguredEmbeddingDimensions(config)).toBe(1536);
  });

  it('keeps Ollama storage dimensions upgrade-safe without an explicit dimension override', async () => {
    const config = await parseConfig({
      EMBEDDER_PROVIDER: 'ollama',
      OPENAI_API_KEY: ''
    });

    expect(config.OLLAMA_EMBEDDING_MODEL).toBe('nomic-embed-text');
    expect(config.STORAGE_EMBEDDING_DIMENSIONS).toBe(1536);
  });

  it('allows TEI embeddings without OpenAI credentials', async () => {
    const config = await parseConfig({
      EMBEDDER_PROVIDER: 'tei',
      OPENAI_API_KEY: '',
      TEI_BASE_URL: 'http://tei.internal',
      TEI_EMBEDDING_MODEL: 'text-embeddings-inference',
      STORAGE_EMBEDDING_DIMENSIONS: '1024'
    });

    expect(config.EMBEDDER_PROVIDER).toBe('tei');
    expect(config.TEI_BASE_URL).toBe('http://tei.internal');
    expect(config.TEI_EMBEDDING_MODEL).toBe('text-embeddings-inference');
    expect(config.STORAGE_EMBEDDING_DIMENSIONS).toBe(1024);
  });

  it('allows Vertex embeddings without OpenAI credentials', async () => {
    const config = await parseConfig({
      EMBEDDER_PROVIDER: 'vertex',
      OPENAI_API_KEY: '',
      VERTEX_PROJECT_ID: 'persistio',
      VERTEX_LOCATION: 'europe-west2',
      VERTEX_EMBEDDING_MODEL: 'gemini-embedding-001',
      VERTEX_EMBEDDING_DIMENSIONS: '1536'
    });

    expect(config.EMBEDDER_PROVIDER).toBe('vertex');
    expect(config.VERTEX_PROJECT_ID).toBe('persistio');
    expect(config.VERTEX_LOCATION).toBe('europe-west2');
    expect(config.VERTEX_EMBEDDING_MODEL).toBe('gemini-embedding-001');
    expect(config.VERTEX_EMBEDDING_DIMENSIONS).toBe(1536);
  });

  it('requires a project id for Vertex embeddings', async () => {
    await expect(parseConfig({
      EMBEDDER_PROVIDER: 'vertex',
      OPENAI_API_KEY: ''
    })).rejects.toThrow('VERTEX_PROJECT_ID is required when EMBEDDER_PROVIDER=vertex');
  });

  it('allows native storage dimensions for alternate embedding providers', async () => {
    process.env.EMBEDDER_PROVIDER = 'ollama';
    process.env.STORAGE_EMBEDDING_DIMENSIONS = '1024';

    const { getConfig, getConfiguredEmbeddingDimensions } = await import('./config');

    const config = getConfig();

    expect(config.STORAGE_EMBEDDING_DIMENSIONS).toBe(1024);
    expect(getConfiguredEmbeddingDimensions(config)).toBe(1024);
  });

  it('rejects embedding dimensions that pgvector indexes cannot support', async () => {
    await expect(parseConfig({
      STORAGE_EMBEDDING_DIMENSIONS: '3072'
    })).rejects.toThrow('Number must be less than or equal to 2000');
  });

  it('requires role-specific provider overrides to be configured as complete triplets', async () => {
    await expect(parseConfig({
      ESCALATION_BASE_URL: 'https://api.anthropic.com/v1/',
      ESCALATION_MODEL: 'claude-sonnet-4-6'
    })).rejects.toThrow('ESCALATION_BASE_URL, ESCALATION_API_KEY, and ESCALATION_MODEL must be set together or all left empty');
  });

  it('requires Azure Key Vault settings only for the Azure key provider', async () => {
    await expect(parseConfig({
      ENCRYPTION_ENABLED: 'true',
      KEY_PROVIDER: 'azure_key_vault'
    })).rejects.toThrow('KEY_VAULT_URI is required when ENCRYPTION_ENABLED=true and KEY_PROVIDER=azure_key_vault');
  });

  it('accepts GCP KMS settings for encryption without Azure Key Vault settings', async () => {
    const config = await parseConfig({
      ENCRYPTION_ENABLED: 'true',
      KEY_PROVIDER: 'gcp_kms',
      GCP_KMS_KEY_NAME: 'projects/persistio/locations/europe-west2/keyRings/persistio/cryptoKeys/vault-dek'
    });

    expect(config.KEY_PROVIDER).toBe('gcp_kms');
    expect(config.GCP_KMS_KEY_NAME).toContain('/cryptoKeys/vault-dek');
  });

  it('requires a GCS bucket when raw chunk storage uses GCS', async () => {
    await expect(parseConfig({
      RAW_CHUNK_STORAGE_PROVIDER: 'gcs'
    })).rejects.toThrow('RAW_CHUNK_GCS_BUCKET is required when RAW_CHUNK_STORAGE_PROVIDER=gcs');
  });

  it('accepts GCS raw chunk storage config', async () => {
    const config = await parseConfig({
      RAW_CHUNK_STORAGE_PROVIDER: 'gcs',
      RAW_CHUNK_GCS_BUCKET: 'persistio-raw-chunks'
    });

    expect(config.RAW_CHUNK_STORAGE_PROVIDER).toBe('gcs');
    expect(config.RAW_CHUNK_GCS_BUCKET).toBe('persistio-raw-chunks');
  });

  it('keeps legacy Application Insights deployments on Azure telemetry', async () => {
    const config = await parseConfig({
      APPLICATIONINSIGHTS_CONNECTION_STRING: 'InstrumentationKey=test;IngestionEndpoint=https://example.com/'
    });

    expect(config.TELEMETRY_PROVIDER).toBe('azure_monitor');
  });

  it('treats an empty telemetry provider as unset for legacy Azure deployments', async () => {
    const config = await parseConfig({
      TELEMETRY_PROVIDER: '',
      APPLICATIONINSIGHTS_CONNECTION_STRING: 'InstrumentationKey=test;IngestionEndpoint=https://example.com/'
    });

    expect(config.TELEMETRY_PROVIDER).toBe('azure_monitor');
  });

  it('requires an Application Insights connection string for explicit Azure telemetry', async () => {
    await expect(parseConfig({
      TELEMETRY_PROVIDER: 'azure_monitor'
    })).rejects.toThrow('APPLICATIONINSIGHTS_CONNECTION_STRING is required when TELEMETRY_PROVIDER=azure_monitor');
  });

  it('accepts GCP OTLP telemetry with localhost collector defaults', async () => {
    const config = await parseConfig({
      TELEMETRY_PROVIDER: 'gcp_otlp',
      OTEL_SERVICE_NAME: 'persistio-api'
    });

    expect(config.TELEMETRY_PROVIDER).toBe('gcp_otlp');
    expect(config.OTEL_SERVICE_NAME).toBe('persistio-api');
    expect(config.OTEL_EXPORTER_OTLP_ENDPOINT).toBe('http://localhost:4318');
    expect(config.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).toBe('');
    expect(config.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT).toBe('');
  });

  it('rejects invalid OTLP per-signal endpoint overrides', async () => {
    await expect(parseConfig({
      TELEMETRY_PROVIDER: 'gcp_otlp',
      OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: 'localhost:4318/v1/traces'
    })).rejects.toThrow('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT must be a valid http(s) URL when set');
  });

  it('rejects non-http OTLP collector endpoints', async () => {
    await expect(parseConfig({
      TELEMETRY_PROVIDER: 'gcp_otlp',
      OTEL_EXPORTER_OTLP_ENDPOINT: 'ftp://localhost:4318'
    })).rejects.toThrow('OTEL_EXPORTER_OTLP_ENDPOINT must be a valid http(s) URL when set');
  });
});

describe('event publisher config', () => {
  afterEach(() => {
    process.env = originalEnv;
    vi.resetModules();
  });

  it('allows API-only mode without worker event publisher settings', async () => {
    const config = await parseConfig({
      EVENT_PUBLISHER: 'webhook',
      PERSISTIO_MODE: 'api'
    });

    expect(config.EVENT_PUBLISHER).toBe('webhook');
    expect(config.PERSISTIO_MODE).toBe('api');
  });

  it('requires webhook settings in worker-capable modes', async () => {
    await expect(parseConfig({
      EVENT_PUBLISHER: 'webhook',
      PERSISTIO_MODE: 'worker'
    })).rejects.toThrow('PLATFORM_EVENTS_WEBHOOK_URL is required when EVENT_PUBLISHER=webhook');
  });

  it('requires valid absolute webhook URLs in worker-capable modes', async () => {
    await expect(parseConfig({
      EVENT_PUBLISHER: 'webhook',
      PERSISTIO_MODE: 'worker',
      PLATFORM_EVENTS_WEBHOOK_SECRET: 'secret',
      PLATFORM_EVENTS_WEBHOOK_URL: 'app.example/events'
    })).rejects.toThrow('PLATFORM_EVENTS_WEBHOOK_URL must be a valid http(s) URL when EVENT_PUBLISHER=webhook');
  });

  it('accepts valid webhook URLs in worker-capable modes', async () => {
    const config = await parseConfig({
      EVENT_PUBLISHER: 'webhook',
      PERSISTIO_MODE: 'worker',
      PLATFORM_EVENTS_WEBHOOK_SECRET: 'secret',
      PLATFORM_EVENTS_WEBHOOK_URL: 'https://app.example/events'
    });

    expect(config.PLATFORM_EVENTS_WEBHOOK_URL).toBe('https://app.example/events');
  });

  it('uses the default webhook timeout in worker-capable modes', async () => {
    const config = await parseConfig({
      EVENT_PUBLISHER: 'webhook',
      PERSISTIO_MODE: 'worker',
      PLATFORM_EVENTS_WEBHOOK_SECRET: 'secret',
      PLATFORM_EVENTS_WEBHOOK_URL: 'https://app.example/events'
    });

    expect(config.PLATFORM_EVENTS_WEBHOOK_TIMEOUT_MS).toBe(30000);
  });

  it('requires Azure Service Bus settings in worker-capable modes', async () => {
    await expect(parseConfig({
      EVENT_PUBLISHER: 'azure_service_bus',
      PERSISTIO_MODE: 'combined'
    })).rejects.toThrow('AZURE_SERVICE_BUS_TOPIC_OR_QUEUE is required when EVENT_PUBLISHER=azure_service_bus');
  });

  it('requires a valid Service Bus namespace FQDN for managed identity', async () => {
    await expect(parseConfig({
      AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE: 'sb-persistio-prod',
      AZURE_SERVICE_BUS_TOPIC_OR_QUEUE: 'platform-events',
      EVENT_PUBLISHER: 'azure_service_bus',
      PERSISTIO_MODE: 'worker'
    })).rejects.toThrow('AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE must be a valid Service Bus namespace FQDN when EVENT_PUBLISHER=azure_service_bus');

    await expect(parseConfig({
      AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE: 'https://sb-persistio-prod.servicebus.windows.net',
      AZURE_SERVICE_BUS_TOPIC_OR_QUEUE: 'platform-events',
      EVENT_PUBLISHER: 'azure_service_bus',
      PERSISTIO_MODE: 'worker'
    })).rejects.toThrow('AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE must be a valid Service Bus namespace FQDN when EVENT_PUBLISHER=azure_service_bus');
  });

  it('accepts a valid Service Bus namespace FQDN for managed identity', async () => {
    const config = await parseConfig({
      AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE: 'sb-persistio-prod.servicebus.windows.net',
      AZURE_SERVICE_BUS_TOPIC_OR_QUEUE: 'platform-events',
      EVENT_PUBLISHER: 'azure_service_bus',
      PERSISTIO_MODE: 'worker'
    });

    expect(config.AZURE_SERVICE_BUS_FULLY_QUALIFIED_NAMESPACE).toBe('sb-persistio-prod.servicebus.windows.net');
  });

  it('requires a Pub/Sub topic for GCP event publishing in worker-capable modes', async () => {
    await expect(parseConfig({
      EVENT_PUBLISHER: 'gcp_pubsub',
      PERSISTIO_MODE: 'worker'
    })).rejects.toThrow('GCP_PUBSUB_TOPIC is required when EVENT_PUBLISHER=gcp_pubsub');
  });

  it('accepts GCP Pub/Sub event publishing config', async () => {
    const config = await parseConfig({
      EVENT_PUBLISHER: 'gcp_pubsub',
      GCP_PUBSUB_PROJECT_ID: 'persistio',
      GCP_PUBSUB_TOPIC: 'platform-events',
      PERSISTIO_MODE: 'worker'
    });

    expect(config.EVENT_PUBLISHER).toBe('gcp_pubsub');
    expect(config.GCP_PUBSUB_PROJECT_ID).toBe('persistio');
    expect(config.GCP_PUBSUB_TOPIC).toBe('platform-events');
  });
});
