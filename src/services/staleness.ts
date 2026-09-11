import { getConfig } from '../config';
import { query } from '../db/client';
import { recordMemoryCountDelta } from './usage';

export async function archiveStaleMemories() {
  const config = getConfig();
  const expiredArchived = await query<{ vault_id: string; account_id: string | null; archived_count: string }>(
    `WITH archived AS (
       UPDATE memories
       SET archived_at = now(),
           updated_at = now()
       FROM vaults
       WHERE memories.vault_id = vaults.id
         AND memories.archived_at IS NULL
         AND memories.valid_until < (now() AT TIME ZONE 'UTC')::date
       RETURNING memories.vault_id::text AS vault_id, vaults.account_id::text AS account_id
     )
     SELECT vault_id, account_id, COUNT(*)::text AS archived_count
     FROM archived
     GROUP BY vault_id, account_id`
  );
  recordArchivedMemoryDeltas(expiredArchived.rows);

  // A scheduled memory cannot accumulate inactivity before its UTC start date.
  const ttlArchived = await query<{ vault_id: string; account_id: string | null; archived_count: string }>(
    `WITH archived AS (
       UPDATE memories
       SET archived_at = now(),
           updated_at = now()
       FROM vaults
       WHERE memories.vault_id = vaults.id
         AND memories.archived_at IS NULL
         AND GREATEST(
           memories.valid_from::timestamp AT TIME ZONE 'UTC',
           COALESCE(memories.last_recalled, memories.updated_at, memories.created_at)
         ) < now() - ($1::text || ' days')::interval
       RETURNING memories.vault_id::text AS vault_id, vaults.account_id::text AS account_id
     )
     SELECT vault_id, account_id, COUNT(*)::text AS archived_count
     FROM archived
     GROUP BY vault_id, account_id`,
    [config.MEMORY_ARCHIVE_TTL_DAYS]
  );
  recordArchivedMemoryDeltas(ttlArchived.rows);

  // Preserve the existing decay step, but never leave zero confidence active:
  // the activation policy rejects that state even during background cleanup.
  await query(
    `UPDATE memories
     SET confidence = GREATEST(confidence - 1, 0),
         status = CASE WHEN status = 'active' AND confidence <= 1 THEN 'needs_review' ELSE status END,
         last_decayed_at = now(),
         updated_at = now()
     WHERE archived_at IS NULL
       AND confidence > 0
       AND GREATEST(
         valid_from::timestamp AT TIME ZONE 'UTC',
         COALESCE(last_recalled, updated_at, created_at)
       ) < now() - ($1::text || ' days')::interval
       AND (
         last_decayed_at IS NULL
         OR last_decayed_at < now() - ($1::text || ' days')::interval
       )`,
    [config.CONFIDENCE_DECAY_INTERVAL_DAYS]
  );

  const decayedArchived = await query<{ vault_id: string; account_id: string | null; archived_count: string }>(
    `WITH archived AS (
       UPDATE memories
       SET archived_at = now(),
           updated_at = now()
       FROM vaults
       WHERE memories.vault_id = vaults.id
         AND memories.archived_at IS NULL
         AND (memories.valid_from IS NULL OR memories.valid_from <= (now() AT TIME ZONE 'UTC')::date)
         AND memories.confidence <= 0
         AND memories.salience < $1
       RETURNING memories.vault_id::text AS vault_id, vaults.account_id::text AS account_id
     )
     SELECT vault_id, account_id, COUNT(*)::text AS archived_count
     FROM archived
     GROUP BY vault_id, account_id`,
    [config.CONFIDENCE_DECAY_AUTO_ARCHIVE_SALIENCE_THRESHOLD]
  );
  recordArchivedMemoryDeltas(decayedArchived.rows);
}

function recordArchivedMemoryDeltas(rows: Array<{ vault_id: string; account_id: string | null; archived_count: string }>) {
  for (const row of rows) {
    recordMemoryCountDelta(row.vault_id, row.account_id, -Number(row.archived_count), 'extraction_worker');
  }
}
