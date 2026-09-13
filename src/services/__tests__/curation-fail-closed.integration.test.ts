import crypto from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('active-memory database boundary (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID(), segmentId = crypto.randomUUID();
  beforeAll(async () => {
    await pool.query("INSERT INTO vaults(id,name,api_key_hash) VALUES ($1,'active-boundary',$2)",[vaultId,crypto.randomUUID()]);
    await pool.query("INSERT INTO segments(id,vault_id,session_id,chunk_ids) VALUES ($1,$2,'active-boundary','{}'::uuid[])",[segmentId,vaultId]);
  });
  afterAll(async () => {
    await pool.query('DELETE FROM vaults WHERE id=$1',[vaultId]);
    await pool.end();
  });

  it.each([
    ['rejected evidence','low',0.9,null,{policy_rejections:[{code:'untrusted_provenance'}]},/Rejected evidence/],
    ['malformed evidence','low',0.9,null,{policy_rejections:'invalid'},/Rejected evidence/],
    ['restricted sensitivity','restricted',0.9,null,{},/Invalid active memory/],
    ['zero confidence','low',0,null,{},/Invalid active memory/],
    ['future source','low',0.9,new Date(Date.now()+10*60_000).toISOString(),{},/Invalid active memory/]
  ])('refuses %s directly without creating a pending memory',async (_name,sensitivity,confidence,timestamp,evidence,message) => {
    const id=crypto.randomUUID();
    await expect(pool.query(
      "INSERT INTO memories(id,vault_id,data,subject,hash,scope,status,sensitivity,confidence,source_timestamp,evidence) VALUES ($1,$2,'fact','subject',$3,'global','active',$4,$5,$6,$7::jsonb)",
      [id,vaultId,crypto.randomUUID(),sensitivity,confidence,timestamp,JSON.stringify(evidence)]
    )).rejects.toThrow(message as RegExp);
    expect((await pool.query('SELECT 1 FROM memories WHERE id=$1',[id])).rowCount).toBe(0);
  });

  it('rolls back an earlier active refinement and its revision when a later action violates the boundary',async () => {
    const ids=[crypto.randomUUID(),crypto.randomUUID()];
    for (const id of ids) await pool.query(
      "INSERT INTO memories(id,vault_id,data,subject,hash,scope,status) VALUES ($1,$2,'original','subject',$3,'global','active')",
      [id,vaultId,crypto.randomUUID()]);
    const client=await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("UPDATE memories SET data='refined' WHERE id=$1",[ids[0]]);
      await expect(client.query("UPDATE memories SET sensitivity='restricted' WHERE id=$1",[ids[1]])).rejects.toThrow(/Invalid active memory/);
      await client.query('ROLLBACK');
    } finally { client.release(); }
    expect((await pool.query('SELECT data,status,revision::text FROM memories WHERE id=ANY($1::uuid[])',[ids])).rows)
      .toEqual([{data:'original',status:'active',revision:'1'},{data:'original',status:'active',revision:'1'}]);
  });

  it.each(['active','contradicted','superseded','needs_review','candidate'])('never stores inverted intervals under %s, even archived',async status => {
    for (const archived of [false,true]) await expect(pool.query(
      "INSERT INTO memories(vault_id,data,subject,hash,scope,status,valid_from,valid_until,archived_at) VALUES ($1,'inverted','subject',$2,'global',$3,'2026-07-01','2026-06-30',CASE WHEN $4 THEN now() END)",
      [vaultId,crypto.randomUUID(),status,archived]
    )).rejects.toMatchObject({code:'23514'});
  });

  it('persists versioned validation metadata and before/after audit state', async () => {
    const run = await pool.query<{ id: string }>(
      `INSERT INTO curation_review_runs (
         vault_id, segment_id, model, schema_version, prompt_version, prompt_hash,
         validation_status, validation_errors, raw_response, before_state, after_state, applied_at
       ) VALUES ($1, $2, 'test-model', 'curation-plan.v2', 'curation-active.v2', $3,
                 'applied', '[]'::jsonb, '{"response":"encrypted-or-plain"}'::jsonb,
                 '[]'::jsonb, '[]'::jsonb, now())
       RETURNING id`,
      [vaultId, segmentId, 'a'.repeat(64)]
    );
    const stored = await pool.query<{
      schema_version: string;
      prompt_version: string;
      validation_status: string;
      has_before: boolean;
      has_after: boolean;
    }>(
      `SELECT schema_version, prompt_version, validation_status,
              before_state IS NOT NULL AS has_before,
              after_state IS NOT NULL AS has_after
       FROM curation_review_runs WHERE id = $1`,
      [run.rows[0].id]
    );
    expect(stored.rows[0]).toEqual({
      schema_version: 'curation-plan.v2',
      prompt_version: 'curation-active.v2',
      validation_status: 'applied',
      has_before: true,
      has_after: true
    });
  });
});
