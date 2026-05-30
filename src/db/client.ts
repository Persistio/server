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
  max: getConfiguredPoolMax(config),
  verify: createPgvectorVerifyHook()
} as PoolConfigWithVerify);

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
        await client.query(sql);
        await client.query(
          `INSERT INTO schema_migrations (filename)
           VALUES ($1)`,
          [filename]
        );
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
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
  return Math.min(appConfig.DB_POOL_MAX, appConfig.EXTRACTION_WORKER_CONCURRENCY * 2 + 2);
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
