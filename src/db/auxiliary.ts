import { Pool, type PoolClient, type QueryResultRow } from 'pg';
import { getConfig } from '../config';

let pool: Pool | undefined;
/** Optional per-request work never queues behind the business pool. No pgvector
 * registration, transaction or provider work is allowed on these connections. */
function auxiliaryPool(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: getConfig().DATABASE_URL,
      application_name: 'persistio:api:auxiliary',
      max: 2,
      connectionTimeoutMillis: 250,
      statement_timeout: 500,
      lock_timeout: 100,
      query_timeout: 750
    });
    pool.on('error', () => { /* pg removes broken idle clients. No payload logging. */ });
  }
  return pool;
}

export async function withAuxiliaryClient<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await auxiliaryPool().connect();
  let failed = false;
  try { return await work(client); }
  catch (error) { failed = true; throw error; }
  // Client query timeout alone does NOT cancel SQL. Destroy the socket on failure,
  // never reuse a client with an indeterminate autocommit. No retries here.
  finally { client.release(failed); }
}

export function auxiliaryQuery<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]) {
  return withAuxiliaryClient(client => client.query<T>(text, values));
}

export async function closeAuxiliaryPool(): Promise<void> {
  const current = pool;
  pool = undefined;
  await current?.end();
}
