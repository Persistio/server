import crypto from 'node:crypto';
import type { PoolClient } from 'pg';

import { query, withTransaction } from '../db/client';
import type { VaultContext } from '../middleware/auth';
import { decryptForVault } from './crypto';
import { getRawChunkStorage } from './raw-chunk-storage';

export type ReplayPayload = { role: string; content: string };
export type StoredIngestRow = {
  id: string;
  created_at: string;
  source_event_key: string;
  source_event_payload_sha256: string;
  role: string;
  blob_store: string | null;
  blob_key: string | null;
};
const LEGACY_HASH = '0'.repeat(64);
const REPLAY_COLUMNS = 'id, created_at, source_event_key, source_event_payload_sha256, role, blob_store, blob_key';

export function ingestPayloadHash(chunk: ReplayPayload): string {
  // Released fingerprint contract: import-job/context metadata is intentionally
  // excluded. Replays preserve the original evidence rather than overwriting it.
  return crypto.createHash('sha256').update(JSON.stringify({ role: chunk.role, content: chunk.content })).digest('hex');
}

export async function classifyIngestReplay(
  vault: VaultContext, chunks: ReplayPayload[], sourceKeys: string[]
): Promise<Map<string, StoredIngestRow>> {
  const existing = await query<StoredIngestRow>(
    `SELECT ${REPLAY_COLUMNS} FROM raw_chunks WHERE vault_id = $1 AND source_event_key = ANY($2::text[])`,
    [vault.id, sourceKeys]
  );
  const byKey = new Map<string, StoredIngestRow>();
  for (const row of existing.rows) {
    const verified = row.source_event_payload_sha256 === LEGACY_HASH ? await attestStoredPayload(vault, row) : row;
    byKey.set(verified.source_event_key, verified);
  }
  chunks.forEach((chunk, index) => {
    const row = byKey.get(sourceKeys[index]);
    if (row) assertIngestReplayMatches(row, chunk, index);
  });
  return byKey;
}

export async function loadLockedIngestRows(client: PoolClient, vaultId: string, keys: string[]): Promise<Map<string, StoredIngestRow>> {
  const result = await client.query<StoredIngestRow>(
    `SELECT ${REPLAY_COLUMNS} FROM raw_chunks
     WHERE vault_id = $1 AND source_event_key = ANY($2::text[])
     ORDER BY source_event_key FOR UPDATE`, [vaultId, keys]
  );
  return new Map(result.rows.map(row => [row.source_event_key, row]));
}

export function assertIngestReplayMatches(row: StoredIngestRow, chunk: ReplayPayload, index: number): void {
  if (!/^[a-f0-9]{64}$/.test(row.source_event_payload_sha256) || row.source_event_payload_sha256 === LEGACY_HASH) {
    throw unavailable('Stored source evidence requires verification before replay');
  }
  if (row.source_event_payload_sha256 !== ingestPayloadHash(chunk)) {
    throw Object.assign(new Error(`Source event identity collision for input chunk ${index}`), { statusCode: 409 });
  }
}

async function attestStoredPayload(vault: VaultContext, original: StoredIngestRow): Promise<StoredIngestRow> {
  let storage: ReturnType<typeof getRawChunkStorage>;
  try {
    storage = getRawChunkStorage();
  } catch {
    throw unavailable('Original source storage is unavailable for replay verification');
  }
  if (!original.blob_key || original.blob_store !== storage.store) {
    throw unavailable('Original source storage is unavailable for replay verification');
  }
  let originalContent: string;
  try {
    originalContent = await decryptForVault(vault, await storage.get(original.blob_key));
  } catch {
    throw unavailable('Original source content could not be verified');
  }
  const attestedHash = ingestPayloadHash({ role: original.role, content: originalContent });
  return withTransaction(async client => {
    const current = (await loadLockedIngestRows(client, vault.id, [original.source_event_key])).get(original.source_event_key);
    if (!current || current.id !== original.id || current.role !== original.role
      || current.blob_store !== original.blob_store || current.blob_key !== original.blob_key) {
      throw unavailable('Original source evidence changed during replay verification');
    }
    if (current.source_event_payload_sha256 !== LEGACY_HASH) return current;
    const updated = await client.query(
      `UPDATE raw_chunks SET source_event_payload_sha256 = $3
       WHERE vault_id = $1 AND id = $2 AND source_event_payload_sha256 = $4 RETURNING id`,
      [vault.id, original.id, attestedHash, LEGACY_HASH]
    );
    if (updated.rowCount !== 1) throw unavailable('Original source verification could not be recorded');
    return { ...current, source_event_payload_sha256: attestedHash };
  });
}

function unavailable(message: string): Error & { statusCode: number } {
  return Object.assign(new Error(message), { statusCode: 503 });
}
