import fs from 'node:fs';
import path from 'node:path';
import { Pool, type PoolClient, type PoolConfig, type QueryResultRow } from 'pg';
import pgvector from 'pgvector/pg';

import { getConfig } from '../config';

const config = getConfig();
const registeredPgvectorClients = new WeakSet<PoolClient>();
let pgvectorTypeRegistrationEnabled = false;
let lastPoolWarningAt = 0;

type PoolConfigWithVerify = PoolConfig & {
  verify?: (client: PoolClient, callback: (error?: Error) => void) => void;
};

type PgvectorRegisterType = (client: PoolClient) => Promise<void>;

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

async function registerPgvectorTypes(
  client: PoolClient,
  registerType: PgvectorRegisterType = pgvector.registerType
): Promise<void> {
  if (registeredPgvectorClients.has(client)) {
    return;
  }

  await registerType(client);
  registeredPgvectorClients.add(client);
}

export function createPgvectorVerifyHook(
  registerType: PgvectorRegisterType = pgvector.registerType,
  isEnabled = () => pgvectorTypeRegistrationEnabled
): (client: PoolClient, callback: (error?: Error) => void) => void {
  return (client, callback) => {
    if (!isEnabled() || registeredPgvectorClients.has(client)) {
      callback();
      return;
    }

    registerPgvectorTypes(client, registerType)
      .then(() => callback())
      .catch((error: unknown) => callback(toError(error)));
  };
}

export const pool = new Pool({
  connectionString: config.DATABASE_URL,
  connectionTimeoutMillis: config.DB_POOL_CONNECTION_TIMEOUT_MS,
  max: getConfiguredPoolMax(config),
  verify: createPgvectorVerifyHook()
} as PoolConfigWithVerify);

export function createPoolErrorHandler(
  poolState: Pick<Pool, 'totalCount' | 'idleCount' | 'waitingCount'>,
  log: (message: string) => void = console.error
): (error: Error) => void {
  return (error) => {
    const code = (error as Error & { code?: unknown }).code;
    log(JSON.stringify({
      level: 50,
      msg: 'postgres idle connection failed; removed from pool',
      error: error.message,
      stack: error.stack,
      code: typeof code === 'string' ? code : undefined,
      total: poolState.totalCount,
      idle: poolState.idleCount,
      waiting: poolState.waitingCount
    }));
  };
}

pool.on('error', createPoolErrorHandler(pool));

export async function query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]) {
  warnIfPoolNearExhaustion();
  return pool.query<T>(text, values);
}

export async function withTransaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
  warnIfPoolNearExhaustion();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    try {
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  } finally {
    client.release();
  }
}

export async function runMigrations() {
  const client = await pool.connect();

  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);

    const migrationsDir = path.resolve(__dirname, 'migrations');
    const filenames = fs.readdirSync(migrationsDir)
      .filter((filename) => filename.endsWith('.sql'))
      .sort();

    await client.query(
      `SELECT set_config('persistio.storage_embedding_dimensions', $1, false)`,
      [String(config.STORAGE_EMBEDDING_DIMENSIONS)]
    );

    for (const filename of filenames) {
      const existing = await client.query<{ filename: string }>(
        `SELECT filename
         FROM schema_migrations
         WHERE filename = $1
         LIMIT 1`,
        [filename]
      );
      if (existing.rowCount) {
        continue;
      }

      const sql = fs.readFileSync(path.join(migrationsDir, filename), 'utf8');
      await client.query('BEGIN');
      try {
        await client.query(`SELECT set_config('persistio.skip_migration_record', 'false', true)`);
        await client.query(sql);
        const skipRecord = await client.query<{ skip_migration_record: string | null }>(
          `SELECT current_setting('persistio.skip_migration_record', true) AS skip_migration_record`
        );
        if (skipRecord.rows[0]?.skip_migration_record !== 'true') {
          await client.query(
            `INSERT INTO schema_migrations (filename)
             VALUES ($1)`,
            [filename]
          );
        }
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
    await validateStorageEmbeddingDimensions(client);
    pgvectorTypeRegistrationEnabled = true;
    await registerPgvectorTypes(client);
  } finally {
    client.release();
  }
}

export async function closePool() {
  await pool.end();
}

export function getConfiguredPoolMax(appConfig = config): number {
  return appConfig.DB_POOL_MAX;
}

export function getConfiguredPoolConnectionTimeout(appConfig = config): number {
  return appConfig.DB_POOL_CONNECTION_TIMEOUT_MS;
}

export async function validateStorageEmbeddingDimensions(
  client: Pick<PoolClient, 'query'>,
  targetDimensions = config.STORAGE_EMBEDDING_DIMENSIONS
): Promise<void> {
  const targetType = `vector(${targetDimensions})`;
  const result = await client.query<{ table_name: string; column_type: string }>(
    `SELECT c.relname AS table_name,
            format_type(a.atttypid, a.atttypmod) AS column_type
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = ANY($1::text[])
       AND a.attname = 'embedding'
       AND NOT a.attisdropped
     ORDER BY c.relname`,
    [['raw_chunks', 'memories', 'memory_embeddings', 'entity_aliases']]
  );
  const mismatches = result.rows.filter((row) => row.column_type !== targetType);
  if (mismatches.length > 0) {
    throw new Error(
      `Configured STORAGE_EMBEDDING_DIMENSIONS=${targetDimensions} does not match pgvector columns: ${
        mismatches.map((row) => `${row.table_name}.${row.column_type}`).join(', ')
      }. Re-embed or restore the matching storage dimension before startup.`
    );
  }
}

export function warnIfPoolNearExhaustion(now = Date.now()) {
  const max = getPoolMax(pool);
  if (max <= 0 || pool.totalCount < max * 0.8) {
    return;
  }

  if (now - lastPoolWarningAt < 60_000) {
    return;
  }

  lastPoolWarningAt = now;
  console.warn(JSON.stringify({
    level: 40,
    msg: 'postgres pool nearing capacity',
    total: pool.totalCount,
    idle: pool.idleCount,
    waiting: pool.waitingCount,
    max
  }));
}

function getPoolMax(poolLike: Pool): number {
  return Number((poolLike as unknown as { options?: { max?: number } }).options?.max ?? config.DB_POOL_MAX);
}
