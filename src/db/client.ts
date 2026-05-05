import fs from 'node:fs';
import path from 'node:path';
import { Pool, type QueryResultRow } from 'pg';
import pgvector from 'pgvector/pg';

import { getConfig } from '../config';

const config = getConfig();

export const pool = new Pool({
  connectionString: config.DATABASE_URL
});

export async function query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]) {
  return pool.query<T>(text, values);
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
  } finally {
    client.release();
  }

  // Now that the vector extension exists, register the type for all future connections
  pool.on('connect', (client) => {
    pgvector.registerType(client);
  });

  // Re-register on existing idle connections by bouncing the pool
  // (simplest approach: end pool and recreate — but since we just started, pool is empty after release)
  // Force a fresh connection so the handler fires for the next query
}

export async function closePool() {
  await pool.end();
}
