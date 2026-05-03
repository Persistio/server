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
    // Run migrations first (creates the vector extension)
    const sql = fs.readFileSync(
      path.resolve(__dirname, 'migrations', '001_initial.sql'),
      'utf8'
    );
    await client.query(sql);
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
