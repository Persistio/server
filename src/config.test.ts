import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const originalEnv = process.env;
const appWorkerPolicy = JSON.stringify([{
  client_id: 'test-app-worker-client-id',
  allowed_scopes: [
    'platform:vaults:create',
    'platform:vaults:read',
    'platform:vaults:update',
    'platform:vaults:delete',
    'platform:vault_keys:rotate',
    'platform:vaults:stats:read',
    'platform:plans:read',
    'platform:plans:write',
    'platform:analytics:read'
  ],
  allow_delegation: true,
  require_account_context: true,
  allow_global_access: false
}]);

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

  it('uses a bounded pool connection timeout by default and accepts an override', async () => {
    expect((await parseConfig()).DB_POOL_CONNECTION_TIMEOUT_MS).toBe(5000);
    expect(
      (await parseConfig({ DB_POOL_CONNECTION_TIMEOUT_MS: '2500' })).DB_POOL_CONNECTION_TIMEOUT_MS
    ).toBe(2500);
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

  it('keeps platform auth in legacy API-key mode by default', async () => {
    const config = await parseConfig();

    expect(config.PLATFORM_AUTH_MODE).toBe('api_key');
    expect(config.PLATFORM_OAUTH_CLIENT_POLICIES).toBe('');
  });

  it('requires OAuth issuer and audience in platform OAuth mode', async () => {
    await expect(parseConfig({
      PLATFORM_AUTH_MODE: 'oauth'
    })).rejects.toThrow('PLATFORM_OAUTH_ISSUER is required when platform OAuth is enabled');
  });

  it('accepts complete platform OAuth settings', async () => {
    const config = await parseConfig({
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: appWorkerPolicy
    });

    expect(config.PLATFORM_AUTH_MODE).toBe('oauth');
    expect(config.PLATFORM_OAUTH_CLIENT_POLICIES).toBe(appWorkerPolicy);
  });

  it('requires explicit OAuth client policies in platform OAuth mode', async () => {
    await expect(parseConfig({
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    })).rejects.toThrow('PLATFORM_OAUTH_CLIENT_POLICIES is required when platform OAuth is enabled');
  });

  it('rejects whitespace-only OAuth client policies in platform OAuth mode', async () => {
    await expect(parseConfig({
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: '   '
    })).rejects.toThrow('PLATFORM_OAUTH_CLIENT_POLICIES must contain at least one OAuth client policy');
  });

  it('requires explicit OAuth client policies in dual mode when OAuth is configured', async () => {
    await expect(parseConfig({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    })).rejects.toThrow('PLATFORM_OAUTH_CLIENT_POLICIES is required when platform OAuth is enabled');
  });

  it('rejects whitespace-only OAuth client policies in dual mode when OAuth is configured', async () => {
    await expect(parseConfig({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: '   '
    })).rejects.toThrow('PLATFORM_OAUTH_CLIENT_POLICIES must contain at least one OAuth client policy');
  });

  it('allows dual mode without OAuth settings for legacy migration deployments', async () => {
    const config = await parseConfig({
      PLATFORM_AUTH_MODE: 'dual'
    });

    expect(config.PLATFORM_AUTH_MODE).toBe('dual');
    expect(config.PLATFORM_OAUTH_CLIENT_POLICIES).toBe('');
  });

  it('requires platform database and admin secrets outside analytics API mode', async () => {
    await expect(parseConfig({
      DATABASE_URL: ''
    })).rejects.toThrow('DATABASE_URL is required unless PERSISTIO_MODE=analytics-api');

    await expect(parseConfig({
      ADMIN_API_KEY: ''
    })).rejects.toThrow('ADMIN_API_KEY is required unless PERSISTIO_MODE=analytics-api');
  });

  it('allows analytics API mode without platform database, admin, storage, embedder, or KMS secrets', async () => {
    const config = await parseConfig({
      ADMIN_API_KEY: '',
      ANALYTICS_BIGQUERY_PROJECT_ID: 'persistio',
      DATABASE_URL: '',
      EMBEDDER_PROVIDER: 'openai',
      ENCRYPTION_ENABLED: 'true',
      GCP_KMS_KEY_NAME: '',
      KEY_PROVIDER: 'gcp_kms',
      OPENAI_API_KEY: '',
      PERSISTIO_MODE: 'analytics-api',
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: appWorkerPolicy,
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      RAW_CHUNK_GCS_BUCKET: '',
      RAW_CHUNK_STORAGE_PROVIDER: 'gcs'
    });

    expect(config.PERSISTIO_MODE).toBe('analytics-api');
    expect(config.DATABASE_URL).toBe('');
    expect(config.ADMIN_API_KEY).toBe('');
    expect(config.PLATFORM_AUTH_MODE).toBe('oauth');
    expect(config.ANALYTICS_FIRESTORE_SNAPSHOT_ENABLED).toBe(false);
    expect(config.ANALYTICS_FIRESTORE_DATABASE_ID).toBe('(default)');
    expect(config.ANALYTICS_FIRESTORE_SNAPSHOT_COLLECTION).toBe('customer_metric_snapshots');
  });

  it('accepts Firestore analytics snapshot serving settings', async () => {
    const config = await parseConfig({
      ADMIN_API_KEY: '',
      ANALYTICS_BIGQUERY_PROJECT_ID: 'persistio',
      ANALYTICS_FIRESTORE_DATABASE_ID: '(default)',
      ANALYTICS_FIRESTORE_PROJECT_ID: 'persistio',
      ANALYTICS_FIRESTORE_SNAPSHOT_COLLECTION: 'customer_metric_snapshots',
      ANALYTICS_FIRESTORE_SNAPSHOT_ENABLED: 'true',
      ANALYTICS_FIRESTORE_SNAPSHOT_TTL_SECONDS: '900',
      DATABASE_URL: '',
      OPENAI_API_KEY: '',
      PERSISTIO_MODE: 'analytics-api',
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: appWorkerPolicy,
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/'
    });

    expect(config.ANALYTICS_FIRESTORE_SNAPSHOT_ENABLED).toBe(true);
    expect(config.ANALYTICS_FIRESTORE_PROJECT_ID).toBe('persistio');
    expect(config.ANALYTICS_FIRESTORE_SNAPSHOT_TTL_SECONDS).toBe(900);
  });

  it('requires OAuth-capable auth for analytics API mode', async () => {
    await expect(parseConfig({
      ANALYTICS_BIGQUERY_PROJECT_ID: 'persistio',
      PERSISTIO_MODE: 'analytics-api',
      PLATFORM_AUTH_MODE: 'api_key'
    })).rejects.toThrow('PLATFORM_AUTH_MODE must be oauth or dual when PERSISTIO_MODE=analytics-api');
  });

  it('rejects invalid platform OAuth client policy JSON', async () => {
    await expect(parseConfig({
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: '[not json'
    })).rejects.toThrow('PLATFORM_OAUTH_CLIENT_POLICIES must be valid JSON');
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

describe('customer metric publisher config', () => {
  afterEach(() => {
    process.env = originalEnv;
    vi.resetModules();
  });

  it('defaults customer metric publishing to noop', async () => {
    const config = await parseConfig();

    expect(config.CUSTOMER_METRICS_PUBLISHER).toBe('noop');
    expect(config.CUSTOMER_METRICS_BATCH_SIZE).toBe(100);
    expect(config.CUSTOMER_METRICS_FLUSH_INTERVAL_MS).toBe(5000);
  });

  it('accepts customer metric log settings', async () => {
    const config = await parseConfig({
      CUSTOMER_METRICS_BATCH_SIZE: '25',
      CUSTOMER_METRICS_FLUSH_INTERVAL_MS: '1500',
      CUSTOMER_METRICS_PUBLISHER: 'log'
    });

    expect(config.CUSTOMER_METRICS_BATCH_SIZE).toBe(25);
    expect(config.CUSTOMER_METRICS_FLUSH_INTERVAL_MS).toBe(1500);
    expect(config.CUSTOMER_METRICS_PUBLISHER).toBe('log');
  });

  it('requires a Pub/Sub topic when customer metrics use Pub/Sub publishing', async () => {
    await expect(parseConfig({
      CUSTOMER_METRICS_PUBLISHER: 'gcp_pubsub'
    })).rejects.toThrow('CUSTOMER_METRICS_GCP_PUBSUB_TOPIC is required when CUSTOMER_METRICS_PUBLISHER=gcp_pubsub');
  });

  it('accepts customer metric Pub/Sub settings', async () => {
    const config = await parseConfig({
      CUSTOMER_METRICS_GCP_PUBSUB_PROJECT_ID: 'persistio',
      CUSTOMER_METRICS_GCP_PUBSUB_TOPIC: 'customer-metrics',
      CUSTOMER_METRICS_PUBLISHER: 'gcp_pubsub'
    });

    expect(config.CUSTOMER_METRICS_GCP_PUBSUB_PROJECT_ID).toBe('persistio');
    expect(config.CUSTOMER_METRICS_GCP_PUBSUB_TOPIC).toBe('customer-metrics');
    expect(config.CUSTOMER_METRICS_PUBLISHER).toBe('gcp_pubsub');
  });
});
