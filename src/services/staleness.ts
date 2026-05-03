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
}
