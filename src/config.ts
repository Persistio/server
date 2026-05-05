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
  EXTRACTOR_PROMPT_FILE: z.string().default(''),
  PROMPTS_DIR: z.string().default('/prompts'),
  APPLICATIONINSIGHTS_CONNECTION_STRING: z.string().default(''),
  EXTRACTION_INTERVAL_MS: z.coerce.number().int().positive().default(30000),
  EXTRACTION_BATCH_SIZE: z.coerce.number().int().positive().default(20),
  EXTRACTION_SCORE_THRESHOLD: z.coerce.number().int().min(1).max(10).default(5),
  DEFAULT_TOKEN_BUDGET: z.coerce.number().int().positive().default(2000),
  DEFAULT_RECALL_TOP_K: z.coerce.number().int().positive().default(10),
  MEMORY_ARCHIVE_TTL_DAYS: z.coerce.number().int().positive().default(90),
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
