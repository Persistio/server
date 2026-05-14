import { z } from 'zod';

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
  EMBEDDER_PROVIDER: z.enum(['openai', 'ollama']).default('openai'),
  OPENAI_API_KEY: z.string().default(''),
  OPENAI_EMBEDDING_MODEL: z.string().default('text-embedding-3-small'),
  OLLAMA_BASE_URL: z.string().url().default('http://ollama:11434'),
  OLLAMA_EMBEDDING_MODEL: z.string().default('nomic-embed-text'),
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
  CURATOR_AUTO_RUN: booleanFlag,
  CURATOR_BASE_URL: z.string().default(''),
  CURATOR_API_KEY: z.string().default(''),
  CURATOR_MODEL: z.string().default('claude-sonnet-4-5'),
  CURATOR_PROMPT_FILE: z.string().default('prompts/curator.txt'),
  PROMPTS_DIR: z.string().default('/prompts'),
  APPLICATIONINSIGHTS_CONNECTION_STRING: z.string().default(''),
  EXTRACTION_INTERVAL_MS: z.coerce.number().int().positive().default(30000),
  EXTRACTION_BATCH_SIZE: z.coerce.number().int().positive().default(20),
  MAX_EXTRACTION_RETRIES: z.coerce.number().int().positive().default(5),
  CURATION_INTERVAL_MS: z.coerce.number().int().positive().default(2000),
  CURATION_BATCH_SIZE: z.coerce.number().int().positive().default(5),
  WORKER_CONCURRENCY: z.coerce.number().int().positive().max(20).default(4),
  ARBITRATION_BATCH_SIZE: z.coerce.number().int().positive().default(15),
  PG_POOL_MAX: z.coerce.number().int().positive().default(20),
  EXTRACTION_SCORE_THRESHOLD: z.coerce.number().int().min(1).max(10).default(5),
  SEGMENTATION_THRESHOLD: z.coerce.number().min(0).max(1).default(0.75),
  DEFAULT_TOKEN_BUDGET: z.coerce.number().int().positive().default(2000),
  DEFAULT_RECALL_TOP_K: z.coerce.number().int().positive().default(10),
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
  KEY_VAULT_URI: z.string().default(''),
  KEK_KEY_NAME: z.string().default('')
}).superRefine((value, ctx) => {
  if (value.EMBEDDER_PROVIDER === 'openai' && !value.OPENAI_API_KEY) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'OPENAI_API_KEY is required when EMBEDDER_PROVIDER=openai',
      path: ['OPENAI_API_KEY']
    });
  }

  if (value.ENCRYPTION_ENABLED && !value.KEY_VAULT_URI) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'KEY_VAULT_URI is required when ENCRYPTION_ENABLED=true',
      path: ['KEY_VAULT_URI']
    });
  }

  if (value.ENCRYPTION_ENABLED && !value.KEK_KEY_NAME) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'KEK_KEY_NAME is required when ENCRYPTION_ENABLED=true',
      path: ['KEK_KEY_NAME']
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
});

export type AppConfig = z.infer<typeof configSchema>;

let cachedConfig: AppConfig | undefined;

export function getConfig(): AppConfig {
  cachedConfig ??= configSchema.parse(process.env);
  return cachedConfig;
}

export function getConfiguredEmbeddingDimensions(config = getConfig()): number {
  return config.EMBEDDER_PROVIDER === 'openai' ? 1536 : 768;
}

export const STORAGE_EMBEDDING_DIMENSIONS = 1536;
