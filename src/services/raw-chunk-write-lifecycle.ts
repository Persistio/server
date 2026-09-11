import type { PoolClient } from 'pg';

import { query, withTransaction } from '../db/client';
import type { RawChunkStorage } from './raw-chunk-storage';

// Includes active uploads and indeterminate writes retained for safe cleanup.
export const MAX_OUTSTANDING_RAW_CHUNK_WRITES_PER_VAULT = 10_000;
export const RAW_CHUNK_DELETE_TIMEOUT_MS = 30_000;
export const MAX_IN_FLIGHT_RAW_CHUNK_DELETES = 4;
// These slots follow the underlying IO, not the caller's deadline. A hung
// provider cannot accumulate another set of requests on every sweep. Do not
// reuse an older DELETE as proof for an upload confirmed after that DELETE began.
const inFlightDeletes = new Set<string>();

export type WritePhase = 'prepared' | 'uploading' | 'uploaded';
type Intent = { id: string; blob_store: string; blob_key: string; write_phase: WritePhase };
export type CleanupOutcome = 'busy' | 'committed' | 'deleted' | 'quarantined' | 'failed';

export async function registerRawChunkWrites(
  vaultId: string,
  blobStore: RawChunkStorage['store'],
  blobKeys: string[],
  sourceKeys: Array<string | null>
): Promise<void> {
  if (blobKeys.length === 0) return;
  await withTransaction(async (client) => {
    const vault = await client.query('SELECT id FROM vaults WHERE id = $1 FOR UPDATE', [vaultId]);
    if (!vault.rowCount) throw new Error('Vault disappeared before raw upload registration');
    const count = await client.query<{ outstanding: string }>(
      'SELECT count(*)::text AS outstanding FROM raw_chunk_blob_write_intents WHERE vault_id = $1', [vaultId]
    );
    if (Number(count.rows[0].outstanding) + blobKeys.length > MAX_OUTSTANDING_RAW_CHUNK_WRITES_PER_VAULT) {
      throw Object.assign(new Error('Raw upload backlog requires reconciliation before accepting new content'), { statusCode: 503 });
    }
    await client.query(
      `INSERT INTO raw_chunk_blob_write_intents (vault_id, blob_store, blob_key, source_event_key, write_phase)
       SELECT $1, $2, input.blob_key, input.source_event_key, 'prepared'
       FROM UNNEST($3::text[], $4::text[]) AS input(blob_key, source_event_key)`,
      [vaultId, blobStore, blobKeys, sourceKeys]
    );
  });
}

export async function beginRawChunkUploads(blobStore: RawChunkStorage['store'], blobKeys: string[]): Promise<void> {
  if (blobKeys.length === 0) return;
  await withTransaction(async (client) => {
    const started = await client.query(
      `UPDATE raw_chunk_blob_write_intents SET write_phase = 'uploading', updated_at = now()
       WHERE blob_store = $1 AND blob_key = ANY($2::text[]) AND write_phase = 'prepared' AND NOT revoked
       RETURNING id`, [blobStore, blobKeys]
    );
    if (started.rowCount !== blobKeys.length) throw new Error('Raw upload ownership was lost before upload');
  });
}

export async function confirmRawChunkUpload(blobStore: RawChunkStorage['store'], blobKey: string): Promise<void> {
  // Record terminal success even after revocation: cleanup then knows another
  // delete pass can safely retire this marker. Revocation is never cleared.
  const result = await query(
    `UPDATE raw_chunk_blob_write_intents SET write_phase = 'uploaded', updated_at = now()
     WHERE blob_store = $1 AND blob_key = $2 AND write_phase = 'uploading' RETURNING id`, [blobStore, blobKey]
  );
  if (result.rowCount !== 1) throw new Error('Raw upload completion could not be recorded');
}

export async function lockCompletedRawChunkWrites(client: PoolClient, blobStore: RawChunkStorage['store'], blobKeys: string[]): Promise<void> {
  if (blobKeys.length === 0) return;
  const locked = await client.query(
    `SELECT id FROM raw_chunk_blob_write_intents
     WHERE blob_store = $1 AND blob_key = ANY($2::text[]) AND write_phase = 'uploaded' AND NOT revoked
     ORDER BY blob_key FOR UPDATE`, [blobStore, blobKeys]
  );
  if (locked.rowCount !== blobKeys.length) throw new Error('Raw upload ownership was lost before persistence');
}

export async function cleanupRawChunkWrite(
  storage: RawChunkStorage,
  selector: { id: string; staleAfterMs?: number } | { blobKey: string },
  transaction: typeof withTransaction = withTransaction,
  timeoutMs = RAW_CHUNK_DELETE_TIMEOUT_MS
): Promise<CleanupOutcome> {
  const claimed = await transaction(async (client): Promise<Intent | 'busy' | 'committed'> => {
    const byId = 'id' in selector;
    const found = await client.query<Intent>(
      `SELECT id, blob_store, blob_key, write_phase FROM raw_chunk_blob_write_intents
       WHERE ${byId ? 'id = $1' : 'blob_store = $1 AND blob_key = $2'}
       ${byId && selector.staleAfterMs !== undefined ? "AND updated_at <= now() - ($2::bigint * interval '1 millisecond')" : ''}
       FOR UPDATE SKIP LOCKED`,
      byId ? (selector.staleAfterMs === undefined ? [selector.id] : [selector.id, selector.staleAfterMs]) : [storage.store, selector.blobKey]
    );
    const intent = found.rows[0];
    if (!intent) return 'busy';
    // A separate statement after the intent lock obtains a fresh READ COMMITTED
    // snapshot. An outer-join snapshot from before waiting on a writer is unsafe.
    const reference = await client.query(
      'SELECT id FROM raw_chunks WHERE blob_store = $1 AND blob_key = $2 LIMIT 1',
      [intent.blob_store, intent.blob_key]
    );
    if (reference.rowCount) {
      await client.query('DELETE FROM raw_chunk_blob_write_intents WHERE id = $1', [intent.id]);
      return 'committed';
    }
    await client.query(
      'UPDATE raw_chunk_blob_write_intents SET revoked = true, updated_at = now() WHERE id = $1', [intent.id]
    );
    return intent;
  });
  if (typeof claimed === 'string') return claimed;

  let failure: string | undefined;
  if (claimed.blob_store !== storage.store) {
    failure = 'Configured provider does not own this write intent';
  } else {
    try {
      // Timeout bounds worker occupancy, NOT the underlying deletion. The SQL
      // fence is already committed, so a late delete cannot race a new raw commit.
      await boundedDelete(storage, claimed.blob_key, timeoutMs);
    } catch {
      failure = 'Object deletion failed or timed out; durable ownership retained';
    }
  }

  return transaction(async (client) => {
    if (!failure && claimed.write_phase !== 'uploading') {
      await client.query('DELETE FROM raw_chunk_blob_write_intents WHERE id = $1 AND revoked', [claimed.id]);
      return 'deleted';
    }
    // Do not consult the NEW phase here. A PUT can finish after DELETE and before
    // this statement; only completion known before that DELETE permits retirement.
    await client.query(
      'UPDATE raw_chunk_blob_write_intents SET last_error = $2, updated_at = now() WHERE id = $1 AND revoked',
      [claimed.id, failure ?? 'Upload outcome was indeterminate before deletion; another reconciliation is required']
    );
    return failure ? 'failed' : 'quarantined';
  });
}

async function boundedDelete(storage: RawChunkStorage, key: string, timeoutMs: number): Promise<void> {
  const identity = JSON.stringify([storage.store, key]);
  if (inFlightDeletes.has(identity) || inFlightDeletes.size >= MAX_IN_FLIGHT_RAW_CHUNK_DELETES) {
    throw new Error('Raw deletion capacity is still occupied by earlier IO');
  }
  inFlightDeletes.add(identity);
  const operation = Promise.resolve().then(() => storage.delete(key)).finally(() => {
    inFlightDeletes.delete(identity);
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error('Deletion deadline exceeded')), timeoutMs); })
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
