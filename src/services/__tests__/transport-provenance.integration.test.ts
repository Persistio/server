import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

import { sourceEventKey } from '../transport-provenance';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('transport provenance idempotency (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();

  beforeAll(async () => {
    const migration = await pool.query<{ applied: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = '048_transport_provenance.sql') AS applied`
    );
    if (!migration.rows[0]?.applied) throw new Error('Test database must be migrated through 048_transport_provenance.sql');
    const idempotencyMigration = await pool.query<{ applied: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = '050_ingest_idempotency.sql') AS applied`
    );
    if (!idempotencyMigration.rows[0]?.applied) throw new Error('Test database must be migrated through 050_ingest_idempotency.sql');
    await pool.query(
      `INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)`,
      [vaultId, `transport-${vaultId}`, crypto.randomUUID()]
    );
  });

  afterAll(async () => {
    await pool.query('DELETE FROM vaults WHERE id = $1', [vaultId]);
    await pool.end();
  });

  it('replays one source event twice as one logical ingestion', async () => {
    const sourceEvent = { namespace: 'persistio-replay-v2', id: 'event-123', message_id: 'message-123', ordinal: 0 };
    const key = sourceEventKey(vaultId, sourceEvent);
    for (let replay = 0; replay < 2; replay += 1) {
      await pool.query(
        `INSERT INTO raw_chunks (
           vault_id, session_id, role, content, source_event_namespace,
           source_event_id, source_message_id, source_event_key,
           source_event_ordinal, source_event_payload_sha256
         ) VALUES ($1, 'replay-session', 'user', 'historical payload', $2, $3, $4, $5, $6, $7)
         ON CONFLICT (vault_id, source_event_key) WHERE source_event_key IS NOT NULL DO NOTHING`,
        [vaultId, sourceEvent.namespace, sourceEvent.id, sourceEvent.message_id, key, sourceEvent.ordinal, 'a'.repeat(64)]
      );
    }

    const result = await pool.query<{ count: string; message_id: string }>(
      `SELECT count(*)::text AS count, max(source_message_id) AS message_id
       FROM raw_chunks WHERE vault_id = $1 AND source_event_key = $2`,
      [vaultId, key]
    );
    expect(result.rows[0]).toEqual({ count: '1', message_id: 'message-123' });
  });

  it('gives separate chunks of one source event separate durable identities', () => {
    const first = sourceEventKey(vaultId, { namespace: 'openclaw-capture', id: 'message-123', ordinal: 0 });
    const second = sourceEventKey(vaultId, { namespace: 'openclaw-capture', id: 'message-123', ordinal: 120 });
    expect(first).not.toBe(second);
  });
});
