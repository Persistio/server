import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

import { recallSourceProvenanceSql } from './recall';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('structured recall provenance projection (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  const foreignVaultId = crypto.randomUUID();
  const memoryId = crypto.randomUUID();
  const chunkIds = [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()];
  const foreignChunkId = crypto.randomUUID();

  beforeAll(async () => {
    const migration = await pool.query<{ applied: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = '048_transport_provenance.sql') AS applied`
    );
    if (!migration.rows[0]?.applied) throw new Error('Test database must be migrated through 048_transport_provenance.sql');
    await pool.query(
      `INSERT INTO vaults (id, name, api_key_hash)
       VALUES ($1, $2, $3), ($4, $5, $6)`,
      [vaultId, `recall-provenance-${vaultId}`, crypto.randomUUID(),
        foreignVaultId, `recall-provenance-${foreignVaultId}`, crypto.randomUUID()]
    );
    await pool.query(
      `INSERT INTO raw_chunks (id, vault_id, session_id, role, provenance)
       VALUES
         ($1, $5, 'agent:main:subagent:first', 'user', '{"source_class":"agent_subagent","authorship":"generated"}'::jsonb),
         ($2, $5, 'agent:main:subagent:second', 'user', '{"source_class":"agent_subagent","authorship":"generated"}'::jsonb),
         ($3, $5, 'C123-topic-456', 'user', '{"source_class":"thread_conversation","authorship":"original"}'::jsonb),
         ($4, $6, 'foreign', 'user', '{"source_class":"agent_cron","authorship":"imported"}'::jsonb)`,
      [...chunkIds, foreignChunkId, vaultId, foreignVaultId]
    );
  });

  afterAll(async () => {
    await pool.query('DELETE FROM vaults WHERE id = ANY($1::uuid[])', [[vaultId, foreignVaultId]]);
    await pool.end();
  });

  it('batches, deduplicates, orders, and vault-bounds structural provenance', async () => {
    const result = await pool.query<{
      memory_id: string;
      source_classes: string[];
      authorships: string[];
    }>(recallSourceProvenanceSql(), [
      JSON.stringify([{
        memory_id: memoryId,
        source_chunk_ids: [...chunkIds, foreignChunkId]
      }]),
      vaultId
    ]);

    expect(result.rows).toEqual([{
      memory_id: memoryId,
      source_classes: ['agent_subagent', 'thread_conversation'],
      authorships: ['generated', 'original']
    }]);
  });
});
