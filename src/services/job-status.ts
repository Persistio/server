import type { PoolClient, QueryResultRow } from 'pg';

import { query } from '../db/client';

interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<{ rowCount: number | null; rows: T[] }>;
}

export async function markPersistentJobRunning(jobId: string | null): Promise<void> {
  if (!jobId) {
    return;
  }

  await query(
    `UPDATE jobs
     SET status = 'running', updated_at = now()
     WHERE id = $1
       AND status = 'queued'`,
    [jobId]
  );
}

export async function completePersistentJobIfReady(client: PoolClient, jobId: string | null): Promise<void> {
  if (!jobId || !(await lockPersistentJob(client, jobId))) {
    return;
  }

  const remaining = await client.query<{ count: string }>(
    `SELECT COUNT(*)::text AS count
     FROM extraction_queue
     WHERE job_id = $1`,
    [jobId]
  );
  const failed = await client.query<{ count: string }>(
    `SELECT COUNT(*)::text AS count
     FROM extraction_dead_letter
     WHERE job_id = $1`,
    [jobId]
  );

  if (Number(remaining.rows[0]?.count ?? 0) !== 0 || Number(failed.rows[0]?.count ?? 0) !== 0) {
    return;
  }

  await client.query(
    `UPDATE jobs
     SET status = 'completed', updated_at = now(), error = NULL
     WHERE id = $1
       AND status <> 'failed'`,
    [jobId]
  );
}

export async function failPersistentJob(client: PoolClient, jobId: string | null, lastError: string): Promise<void> {
  if (!jobId || !(await lockPersistentJob(client, jobId))) {
    return;
  }

  await client.query(
    `UPDATE jobs
     SET status = 'failed', updated_at = now(), error = $2
     WHERE id = $1`,
    [jobId, lastError]
  );
}

async function lockPersistentJob(client: Queryable, jobId: string): Promise<boolean> {
  const result = await client.query<{ id: string }>(
    `SELECT id
     FROM jobs
     WHERE id = $1
     FOR UPDATE`,
    [jobId]
  );
  return Boolean(result.rowCount);
}
