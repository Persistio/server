import crypto from 'node:crypto';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import Fastify from 'fastify';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const fixture = vi.hoisted(() => ({
  vaultId: '', loseCommit: false, loseReplay: false, storageUnavailable: false, blobs: new Map<string, string>(),
  embed: vi.fn(), put: vi.fn(), get: vi.fn(), remove: vi.fn(), unwrap:vi.fn(),
  encryptedDek:null as string|null, encryptionEnabled:false
}));
vi.mock('../middleware/auth', () => ({ requireVaultWriteAuth: async (request: { vault: unknown }) => {
  request.vault = { id: fixture.vaultId, name: 'ingest test', purpose: null, settings: {}, plan_id: 'unlimited',
    status: 'active', encrypted_dek: fixture.encryptedDek, vault_encryption_enabled: fixture.encryptionEnabled };
} }));
vi.mock('@google-cloud/kms',()=>({KeyManagementServiceClient:class {decrypt=fixture.unwrap;}}));
vi.mock('../services/embedder', () => ({
  OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT: 8192, estimateEmbeddingTokens: (s: string) => s.length,
  getEmbedder: () => ({ embedBatch: fixture.embed })
}));
vi.mock('../services/raw-chunk-storage', async original => ({
  ...await original<typeof import('../services/raw-chunk-storage')>(),
  getRawChunkStorage: () => {
    if (fixture.storageUnavailable) throw new Error('Provider client cannot initialize');
    return { store: 'local', put: fixture.put, get: fixture.get, delete: fixture.remove };
  }
}));
vi.mock('../db/client', async original => {
  const actual = await original<typeof import('../db/client')>();
  return { ...actual, withTransaction: async <T>(run: Parameters<typeof actual.withTransaction<T>>[0]) => {
    let wroteRaw = false;
    const result = await actual.withTransaction(client => run(Object.assign(Object.create(client), {
      query: async (sql: string, values?: unknown[]) => {
        if (sql.includes('WITH input AS')) wroteRaw = true;
        if(fixture.loseReplay && sql==='SELECT id FROM vaults WHERE id = $1 FOR UPDATE'){
          fixture.loseReplay=false;
          const result=await client.query(sql,values);
          await client.query('DELETE FROM segments WHERE vault_id=$1',[fixture.vaultId]);
          await client.query('DELETE FROM raw_chunks WHERE vault_id=$1',[fixture.vaultId]);
          return result;
        }
        return client.query(sql, values);
      }
    })));
    if (wroteRaw && fixture.loseCommit) { fixture.loseCommit = false; throw new Error('Simulated lost COMMIT acknowledgement'); }
    return result;
  } };
});

import { pool } from '../db/client';
import { registerIngestRoutes } from './ingest';
import { registerPlatformErrorHandler } from '../http-error-handler';
import { cleanupRawChunkWrite, registerRawChunkWrites, beginRawChunkUploads, MAX_OUTSTANDING_RAW_CHUNK_WRITES_PER_VAULT } from '../services/raw-chunk-write-lifecycle';
import { reconcileStaleRawChunkBlobWrites } from '../services/raw-chunk-blob-reconciler';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('ingest ownership and replay (real PostgreSQL)', () => {
  const app = Fastify();
  const triggerExtraction = vi.fn();
  const storage = { store: 'local' as const, put: fixture.put, get: fixture.get, delete: fixture.remove };
  const chunk = (id = 'message', content = 'durable payload') => ({ role: 'user' as const, content,
    timestamp: '2026-06-01T00:00:00.000Z', source_event: { namespace: 'integration', id, ordinal: 0 } });
  const post = (chunks = [chunk()], url = '/v1/ingest') => app.inject({ method: 'POST', url,
    payload: { session_id: 'test-session', context:{trigger_type:'backfill'}, chunks } });
  const counts = async () => (await pool.query(
    `SELECT (SELECT count(*)::int FROM raw_chunks WHERE vault_id=$1) AS raw,
      (SELECT count(*)::int FROM segments WHERE vault_id=$1) AS segments,
      (SELECT count(*)::int FROM extraction_queue WHERE vault_id=$1) AS queue,
      (SELECT count(*)::int FROM raw_chunk_blob_write_intents WHERE vault_id=$1) AS intents,
      COALESCE((SELECT ingest_events FROM vault_usage WHERE vault_id=$1),0)::int AS charges`, [fixture.vaultId]
  )).rows[0];

  beforeAll(async () => {
    registerPlatformErrorHandler(app);
    const parsed=new URL(databaseUrl!);
    if (databaseUrl !== process.env.DATABASE_URL || !['localhost','127.0.0.1'].includes(parsed.hostname)
      || !/^\/(persistio_test_pr369|persistio_restoration_\d{8}_[a-z])$/.test(parsed.pathname)) {
      throw new Error('Use the same approved isolated loopback test database for both database variables');
    }
    const migrated = await pool.query("SELECT 1 FROM schema_migrations WHERE filename='055_raw_chunk_write_ownership.sql'");
    if (!migrated.rowCount) throw new Error('Apply migration 055 to the isolated test database first');
    await registerIngestRoutes(app, triggerExtraction);
  });
  beforeEach(async () => {
    fixture.encryptedDek=null;fixture.encryptionEnabled=false;
    (await import('../config')).getConfig().ENCRYPTION_ENABLED=false;
    fixture.vaultId = crypto.randomUUID(); fixture.loseCommit = false; fixture.loseReplay=false; fixture.blobs.clear();
    fixture.storageUnavailable = false;
    triggerExtraction.mockReset();
    fixture.embed.mockReset().mockImplementation(async (texts: string[]) => texts.map(() => [1, ...Array(1535).fill(0)]));
    fixture.put.mockReset().mockImplementation(async (key: string, content: string) => {
      fixture.blobs.set(key, content); return { blobStore: 'local', blobKey: key };
    });
    fixture.get.mockReset().mockImplementation(async (key: string) => {
      if (!fixture.blobs.has(key)) throw new Error('missing original'); return fixture.blobs.get(key)!;
    });
    fixture.remove.mockReset().mockImplementation(async (key: string) => { fixture.blobs.delete(key); });
    await pool.query(`INSERT INTO vaults (id,name,api_key_hash,plan_id,rate_limit_override)
      VALUES ($1,'PR369 test',$2,'unlimited','{"ingest_events_per_month":1}')`, [fixture.vaultId, crypto.randomUUID()]);
  });
  afterEach(async () => {
    await pool.query('DELETE FROM vaults WHERE id=$1', [fixture.vaultId]);
    await pool.query('DELETE FROM raw_chunk_blob_write_intents WHERE vault_id=$1', [fixture.vaultId]);
    await pool.query('DELETE FROM raw_chunk_blob_deletion_queue WHERE vault_id=$1', [fixture.vaultId]);
  });
  afterAll(async () => { await app.close(); await pool.end(); });

  it.each(['/v1/ingest', '/v1/ingest/bulk'])('replays at exhausted quota without provider work at %s', async url => {
    const first = await post([chunk()], url);
    expect(first.statusCode, first.body).toBe(202);
    fixture.embed.mockRejectedValue(new Error('provider down')); fixture.put.mockRejectedValue(new Error('store down'));
    fixture.storageUnavailable = true;
    const second = await post([chunk()], url);
    expect(second.statusCode, second.body).toBe(202);
    expect(second.json()).toMatchObject({ inserted: 0, replayed: 1, chunks: [{ id: first.json().chunks[0].id }] });
    if (url.endsWith('bulk')) expect(second.json().job_id).toBe(first.json().job_id);
    expect(fixture.embed).toHaveBeenCalledTimes(1); expect(fixture.put).toHaveBeenCalledTimes(1);
    expect(await counts()).toEqual({ raw: 1, segments: 1, queue: 1, intents: 0, charges: 1 });
  });

  it('accepts concurrent identical requests with one lineage and one final quota unit', async () => {
    let release!: () => void; const barrier = new Promise<void>(resolve => { release = resolve; }); let calls = 0;
    fixture.embed.mockImplementation(async () => { if (++calls === 2) release(); await barrier; return [[1, ...Array(1535).fill(0)]]; });
    const responses = await Promise.all([post(), post()]);
    expect(responses.map(r => r.statusCode)).toEqual([202, 202]);
    expect(responses.map(r => r.json().inserted).sort()).toEqual([0, 1]);
    expect(await counts()).toEqual({ raw: 1, segments: 1, queue: 1, intents: 0, charges: 1 });
    expect(fixture.blobs.size).toBe(1);
  });

  it('rolls back a different concurrent event when only one quota unit remains', async () => {
    let release!: () => void; const barrier = new Promise<void>(resolve => { release = resolve; }); let calls = 0;
    fixture.embed.mockImplementation(async () => { if (++calls === 2) release(); await barrier; return [[1, ...Array(1535).fill(0)]]; });
    const responses = await Promise.all([post([chunk('a')]), post([chunk('b')])]);
    expect(responses.map(r => r.statusCode).sort()).toEqual([202, 429]);
    expect(await counts()).toEqual({ raw: 1, segments: 1, queue: 1, intents: 0, charges: 1 });
    expect(fixture.blobs.size).toBe(1);
  });

  it('does not refund a committed charge after lost COMMIT acknowledgement', async () => {
    fixture.loseCommit = true;
    expect((await post()).statusCode).toBe(500);
    expect(await counts()).toEqual({ raw: 1, segments: 1, queue: 1, intents: 0, charges: 1 });
    const retry = await post(); expect(retry.statusCode, retry.body).toBe(202);
    expect(retry.json()).toMatchObject({ inserted: 0, replayed: 1 });
    expect((await counts()).charges).toBe(1); expect(fixture.blobs.size).toBe(1);
  });

  it('catches a live PUT completing after cleanup takeover', async () => {
    let started!: () => void, release!: () => void, key = '';
    const putStarted = new Promise<void>(r => { started = r; }); const finishPut = new Promise<void>(r => { release = r; });
    fixture.put.mockImplementation(async (k: string, content: string) => {
      key = k; started(); await finishPut; fixture.blobs.set(k, content); return { blobStore: 'local', blobKey: k };
    });
    const request = post(); await putStarted;
    expect(await cleanupRawChunkWrite(storage, { blobKey: key })).toBe('quarantined');
    release(); expect((await request).statusCode).toBe(500);
    expect(await counts()).toEqual({ raw: 0, segments: 0, queue: 0, intents: 0, charges: 0 });
    expect(fixture.blobs.size).toBe(0); expect(fixture.remove).toHaveBeenCalledTimes(2);
  });

  it('enforces revocation even for an older writer that ignores the new phase', async () => {
    await registerRawChunkWrites(fixture.vaultId, 'local', ['legacy-attempt'], ['a'.repeat(64)]);
    await beginRawChunkUploads('local', ['legacy-attempt']);
    expect(await cleanupRawChunkWrite(storage, { blobKey: 'legacy-attempt' })).toBe('quarantined');
    await expect(pool.query(`INSERT INTO raw_chunks (vault_id,session_id,role,blob_store,blob_key)
      VALUES ($1,'old-writer','user','local','legacy-attempt')`, [fixture.vaultId])).rejects.toMatchObject({ code: '23514' });
    expect((await counts()).raw).toBe(0);
  });

  it('moves an unavailable provider behind other eligible cleanup work', async () => {
    await pool.query(`INSERT INTO raw_chunk_blob_write_intents (vault_id,blob_store,blob_key,write_phase,updated_at)
      VALUES ($1,'gcs','unavailable','uploaded',now()-interval '2 hours'),
             ($1,'local','ready','uploaded',now()-interval '1 hour')`, [fixture.vaultId]);
    const first = await reconcileStaleRawChunkBlobWrites(1, 1000, storage);
    const second = await reconcileStaleRawChunkBlobWrites(1, 1000, storage);
    expect(first.failed).toBe(1); expect(second.deleted).toBe(1);
    expect(fixture.remove).toHaveBeenCalledExactlyOnceWith('ready');
  });

  it('bounds indeterminate ownership records before permitting another upload', async () => {
    await pool.query(`INSERT INTO raw_chunk_blob_write_intents (vault_id,blob_store,blob_key)
      SELECT $1,'local','quarantine-' || n FROM generate_series(1,$2::int) n`,
      [fixture.vaultId, MAX_OUTSTANDING_RAW_CHUNK_WRITES_PER_VAULT]);
    await expect(registerRawChunkWrites(fixture.vaultId, 'local', ['overflow'], ['a'.repeat(64)]))
      .rejects.toMatchObject({ statusCode: 503 });
    expect((await counts()).intents).toBe(MAX_OUTSTANDING_RAW_CHUNK_WRITES_PER_VAULT);
    for (const url of ['/v1/ingest', '/v1/ingest/bulk']) {
      const response = await post([chunk('capacity-overflow')], url);
      expect(response.statusCode, response.body).toBe(503);
    }
    expect(fixture.embed).not.toHaveBeenCalled();
    expect(fixture.put).not.toHaveBeenCalled();
  });
  it('preserves 503 if an accepted replay disappears between preflight and locked verification',async()=>{
    expect((await post()).statusCode).toBe(202);
    fixture.loseReplay=true;
    const res=await post();expect(res.statusCode,res.body).toBe(503);
    expect(res.json()).toEqual({error:'Internal server error',code:'http_503'});
    expect(await counts()).toEqual({raw:1,segments:1,queue:1,intents:0,charges:1});
  });

  it.each(['failed', 'completed', 'deleted'])('bulk replay preserves acceptance when submission metadata is %s', async status => {
    const original = await post([chunk()], '/v1/ingest/bulk');
    expect(original.statusCode).toBe(202);
    const job = original.json().job_id;
    if (status === 'deleted') await pool.query('DELETE FROM jobs WHERE id=$1', [job]);
    else await pool.query('UPDATE jobs SET status=$2 WHERE id=$1', [job, status]);
    const replay = await post([chunk()], '/v1/ingest/bulk');
    expect(replay.statusCode, replay.body).toBe(202);
    expect(replay.json()).toMatchObject({ job_id: job, inserted: 0, replayed: 1,
      chunks: [{ id: original.json().chunks[0].id, outcome: 'replayed' }] });
    expect(fixture.embed).toHaveBeenCalledTimes(1);
    expect(fixture.put).toHaveBeenCalledTimes(1);
    expect((await counts()).charges).toBe(1);
    expect((await counts()).raw).toBe(1);
  });

  it.each(['/v1/ingest', '/v1/ingest/bulk'])('prepares and queues only the new part of a mixed batch at %s', async url => {
    await pool.query(`UPDATE vaults SET rate_limit_override='{"ingest_events_per_month":2}' WHERE id=$1`, [fixture.vaultId]);
    const first = await post([chunk('old')], url);
    const original = (await pool.query('SELECT * FROM raw_chunks WHERE vault_id=$1', [fixture.vaultId])).rows[0];
    const mixed = await post([chunk('new', 'new payload'), chunk('old')], url);
    expect(mixed.statusCode, mixed.body).toBe(202);
    expect(mixed.json()).toMatchObject({ inserted: 1, replayed: 1,
      chunks: [{ outcome: 'inserted' }, { id: first.json().chunks[0].id, outcome: 'replayed' }] });
    expect(fixture.embed.mock.calls.map(call => call[0])).toEqual([['durable payload'], ['new payload']]);
    expect(fixture.put).toHaveBeenCalledTimes(2);
    expect(await counts()).toEqual({ raw: 2, segments: 2, queue: 2, intents: 0, charges: 2 });
    expect((await pool.query('SELECT * FROM raw_chunks WHERE id=$1', [original.id])).rows[0]).toEqual(original);
    const groupedReplay = await post([chunk('old'), chunk('new', 'new payload')], '/v1/ingest/bulk');
    expect(groupedReplay.statusCode, groupedReplay.body).toBe(202);
    expect(groupedReplay.json()).toMatchObject({ inserted: 0, replayed: 2 });
    expect((await pool.query('SELECT status FROM jobs WHERE id=$1', [groupedReplay.json().job_id])).rows[0].status).toBe('completed');
    expect(triggerExtraction).toHaveBeenCalledTimes(url.endsWith('bulk') ? 2 : 0);
    expect(await counts()).toEqual({ raw: 2, segments: 2, queue: 2, intents: 0, charges: 2 });
  });

  it('defers a large rollback cleanup without losing ownership or the quota error', async () => {
    let started!: () => void, release!: () => void;
    const preparing = new Promise<void>(resolve => { started = resolve; });
    const finish = new Promise<void>(resolve => { release = resolve; });
    fixture.embed.mockImplementation(async (texts: string[]) => {
      if (texts.length > 1) { started(); await finish; }
      return texts.map(() => [1, ...Array(1535).fill(0)]);
    });
    const large = post(Array.from({ length: 2048 }, (_value, index) => chunk(`large-${index}`)), '/v1/ingest/bulk');
    await Promise.race([preparing, large.then(response => { throw new Error(`Batch stopped before preparation: ${response.statusCode} ${response.body}`); })]);
    let response;
    try { expect((await post([chunk('winner')])).statusCode).toBe(202); }
    finally { release(); response = await large; }
    expect(response.statusCode, response.body).toBe(429);
    expect(fixture.remove).toHaveBeenCalledTimes(4);
    expect(await counts()).toEqual({ raw: 1, segments: 1, queue: 1, intents: 2044, charges: 1 });
    await pool.query("UPDATE raw_chunk_blob_write_intents SET updated_at=now()-interval '1 hour' WHERE vault_id=$1", [fixture.vaultId]);
    expect((await reconcileStaleRawChunkBlobWrites(1000, 1000, storage)).deleted).toBe(1000);
    expect((await reconcileStaleRawChunkBlobWrites(1000, 1000, storage)).deleted).toBe(1000);
    expect((await reconcileStaleRawChunkBlobWrites(1000, 1000, storage)).deleted).toBe(44);
    expect(fixture.blobs.size).toBe(1);
    expect(await counts()).toEqual({ raw: 1, segments: 1, queue: 1, intents: 0, charges: 1 });
    // Thousands of real SQL ownership/cleanup transactions exceed Vitest's
    // generic five-second unit-test budget on shared CI runners. Production
    // deadlines, 2,048 inputs, concurrency and every state assertion are unchanged.
  }, 30_000);

  it.each([true, false])('downgrade preserves unresolved ownership (pending=%s)', async pending => {
    if (pending) await registerRawChunkWrites(fixture.vaultId, 'local', ['downgrade-proof'], ['a'.repeat(64)]);
    const client = await pool.connect();
    const sql = readFileSync(path.join(__dirname, '../db/migrations/down/055_raw_chunk_write_ownership.sql'), 'utf8');
    try {
      await client.query('BEGIN');
      if (pending) {
        await expect(client.query(sql)).rejects.toMatchObject({ code: 'P0001' });
      } else {
        await client.query(sql);
        const columns = await client.query(`SELECT column_name FROM information_schema.columns
          WHERE table_name='raw_chunk_blob_write_intents' AND column_name IN ('revoked','write_phase')`);
        expect(columns.rows).toHaveLength(0);
      }
    } finally {
      // Exercise real DDL without downgrading the shared isolated test database.
      await client.query('ROLLBACK'); client.release();
    }
    expect((await counts()).intents).toBe(pending ? 1 : 0);
    expect((await pool.query(`SELECT column_name FROM information_schema.columns
      WHERE table_name='raw_chunk_blob_write_intents' AND column_name IN ('revoked','write_phase')`)).rows).toHaveLength(2);
  });
  it('refuses accepted metadata changes instead of repairing or reattesting history',async()=>{
    expect((await post()).statusCode).toBe(202);
    for(const assignment of ["source_event_payload_sha256=repeat('0',64)","blob_key='replacement'","blob_store='gcs'","capture_context='{}'::jsonb","role='assistant'"]) {
      await expect(pool.query('UPDATE raw_chunks SET '+assignment+' WHERE vault_id=$1',[fixture.vaultId]))
        .rejects.toThrow(/Accepted source metadata is immutable/);
    }
    fixture.blobs.clear();
    expect((await post()).statusCode).toBe(202);
    expect(fixture.get).not.toHaveBeenCalled();expect(fixture.put).toHaveBeenCalledTimes(1);
  });
  it('rejects changed payload or capture context under the same identity without provider work',async()=>{
    expect((await post()).statusCode).toBe(202);
    expect((await post([chunk('message','Conflicting submitted content')])).statusCode).toBe(409);
    const response=await app.inject({method:'POST',url:'/v1/ingest',payload:{session_id:'test-session',
      context:{trigger_type:'backfill',project_id:'different'},chunks:[chunk()]}});
    expect(response.statusCode).toBe(409);
    expect(fixture.get).not.toHaveBeenCalled();expect(fixture.embed).toHaveBeenCalledTimes(1);expect(fixture.put).toHaveBeenCalledTimes(1);
    expect((await counts()).charges).toBe(1);
  });
  it.each(['flag','wrapped-key'])('rejects a changed vault %s after preparation without accepting ciphertext or charging',async change=>{
    fixture.embed.mockImplementation(async()=>{
      await pool.query(change==='flag'?'UPDATE vaults SET vault_encryption_enabled=true WHERE id=$1':
        "UPDATE vaults SET encrypted_dek='different-key' WHERE id=$1",[fixture.vaultId]);
      return [[1,...Array(1535).fill(0)]];
    });
    expect((await post()).statusCode).toBe(500);
    expect(await counts()).toEqual({raw:0,segments:0,queue:0,intents:0,charges:0});
    expect(fixture.blobs.size).toBe(0);
  });
  it('prepares one encryption key before locks, encrypts raw and segment data, and performs no key work for replay',async()=>{
    const config=(await import('../config')).getConfig(),crypt=await import('../services/crypto');
    const key=Buffer.alloc(32,27);config.ENCRYPTION_ENABLED=true;config.KEY_PROVIDER='gcp_kms';
    config.GCP_KMS_KEY_NAME='projects/test/locations/global/keyRings/test/cryptoKeys/test';
    fixture.encryptionEnabled=true;fixture.encryptedDek=Buffer.from('synthetic-wrapped-ingest-key').toString('base64');
    await pool.query('UPDATE vaults SET vault_encryption_enabled=true,encrypted_dek=$2 WHERE id=$1',[fixture.vaultId,fixture.encryptedDek]);
    fixture.unwrap.mockReset().mockImplementation(async()=>{
      const client=await pool.connect();
      try{await client.query('BEGIN');await client.query('SELECT id FROM vaults WHERE id=$1 FOR UPDATE NOWAIT',[fixture.vaultId]);}
      finally{await client.query('ROLLBACK');client.release();}
      return [{plaintext:key}];
    });
    await crypt.initCryptoClient();
    expect((await post()).statusCode).toBe(202);
    expect(fixture.unwrap).toHaveBeenCalledOnce();
    const raw=(await pool.query('SELECT blob_key FROM raw_chunks WHERE vault_id=$1',[fixture.vaultId])).rows[0];
    expect(crypt.decryptField(fixture.blobs.get(raw.blob_key)!,key)).toBe('durable payload');
    const segment=(await pool.query('SELECT context FROM segments WHERE vault_id=$1',[fixture.vaultId])).rows[0];
    if(segment.context)expect(crypt.decryptField(segment.context,key)).toContain('durable payload');
    fixture.unwrap.mockRejectedValue(new Error('Synthetic key provider offline'));
    expect((await post()).statusCode).toBe(202);expect(fixture.unwrap).toHaveBeenCalledOnce();
  });

});
