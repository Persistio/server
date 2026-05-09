import { getConfig } from '../config';
import { query } from '../db/client';

export async function archiveStaleMemories() {
  const config = getConfig();
  await query(
    `UPDATE memories
     SET archived_at = now(),
         updated_at = now()
     WHERE archived_at IS NULL
       AND COALESCE(last_recalled, updated_at, created_at) < now() - ($1::text || ' days')::interval`,
    [config.MEMORY_ARCHIVE_TTL_DAYS]
  );

  await query(
    `UPDATE memories
     SET confidence = confidence - 1,
         last_decayed_at = now(),
         updated_at = now()
     WHERE archived_at IS NULL
       AND confidence > 0
       AND COALESCE(last_recalled, updated_at, created_at) < now() - ($1::text || ' days')::interval
       AND (
         last_decayed_at IS NULL
         OR last_decayed_at < now() - ($1::text || ' days')::interval
       )`,
    [config.CONFIDENCE_DECAY_INTERVAL_DAYS]
  );

  await query(
    `UPDATE memories
     SET archived_at = now(),
         updated_at = now()
     WHERE archived_at IS NULL
       AND confidence <= 0
       AND salience < $1`,
    [config.CONFIDENCE_DECAY_AUTO_ARCHIVE_SALIENCE_THRESHOLD]
  );
}
