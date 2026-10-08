import type { Pool, QueryResultRow } from 'pg';

/** Short dispatcher-only transaction. Bounds scans/row-lock waits on the server;
 * no network transport is invoked until this connection has been released.
 * The host must also configure pool connection/overall request deadlines (PR3). */
export async function workerWakeQuery<Row extends QueryResultRow = QueryResultRow>(
  db: Pick<Pool, 'connect'>, sql: string, values?: unknown[]
) {
  const client = await db.connect();
  let discard: Error | undefined;
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL statement_timeout = '5s'");
    await client.query("SET LOCAL lock_timeout = '1s'");
    const result = await client.query<Row>(sql, values);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try { await client.query('ROLLBACK'); }
    catch { discard = new Error('Wake transaction cleanup failed'); }
    throw error;
  } finally {
    client.release(discard);
  }
}
