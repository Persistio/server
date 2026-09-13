import crypto from 'node:crypto';
import {z} from 'zod';
import {query,withTransaction} from '../db/client';
import {lockMemoryWriteVault} from './dedup';
import {enqueueCurationWork} from './curation-work';
import {getRawChunkStorage} from './raw-chunk-storage';
import {prepareVaultCrypto,type VaultEncryptionContext} from './crypto';

const recoverySchema=z.object({kind:z.enum(['extraction','curation']),failureId:z.string().uuid(),vaultId:z.string().uuid(),
  actorId:z.string().trim().min(1).max(512),reason:z.string().trim().min(1).max(2000)}).strict();
const targetsSchema=z.array(z.object({memory_id:z.string().uuid(),revision:z.string().regex(/^[1-9][0-9]*$/)}).strict()).min(1).max(500);
type Failure={id:string;vault_id:string;source_queue_id:string|null;segment_id:string|null;chunk_id?:string|null;context_chunk_ids?:string[]|null;targets?:unknown};
type Source={id:string;blob_key:string|null;blob_store:string|null;storage_bytes:string;processed:boolean};

export async function listMemoryJobFailures(vaultId:string,limit=50){
  z.string().uuid().parse(vaultId);z.number().int().min(1).max(200).parse(limit);
  return(await query(`SELECT f.kind,f.id,f.retry_count,f.dead_lettered_at,r.queue_id AS recovery_queue_id
    FROM (SELECT 'extraction' AS kind,id,vault_id,retry_count,dead_lettered_at FROM extraction_dead_letter
      UNION ALL SELECT 'curation',id,vault_id,retry_count,dead_lettered_at FROM curation_dead_letter) f
    LEFT JOIN memory_job_recoveries r ON r.failure_kind=f.kind AND r.failure_id=f.id AND r.vault_id=f.vault_id
    WHERE f.vault_id=$1 ORDER BY f.dead_lettered_at DESC,f.id LIMIT $2`,[vaultId,limit])).rows;
}

/** Explicit technical retry; never changes memory eligibility or consumes history files. */
export async function retryMemoryJobFailure(value:z.infer<typeof recoverySchema>):Promise<{queueId:string;alreadyRetried:boolean}>{
  const input=recoverySchema.parse(value);
  const table=input.kind==='extraction'?'extraction_dead_letter':'curation_dead_letter';
  const prior=(await query<{queue_id:string}>('SELECT queue_id FROM memory_job_recoveries WHERE vault_id=$1 AND failure_kind=$2 AND failure_id=$3',
    [input.vaultId,input.kind,input.failureId])).rows[0];
  if(prior)return{queueId:prior.queue_id,alreadyRetried:true};
  const failure=(await query<Failure>(`SELECT * FROM ${table} WHERE id=$1 AND vault_id=$2`,[input.failureId,input.vaultId])).rows[0];
  if(!failure?.source_queue_id)throw new Error('Failure has no recoverable worker receipt');
  const vault=(await query<VaultEncryptionContext>('SELECT id,encrypted_dek,vault_encryption_enabled FROM vaults WHERE id=$1',[input.vaultId])).rows[0];
  if(!vault)throw new Error('Recovery vault is unavailable');
  const prepared = await prepareVaultCrypto(vault);
  let sourceIds:string[]=[];
  let targets:z.infer<typeof targetsSchema>=[];
  if(input.kind==='extraction'){
    if(failure.segment_id){
      const segment=(await query<{chunk_ids:string[]}>('SELECT chunk_ids FROM segments WHERE id=$1 AND vault_id=$2',[failure.segment_id,input.vaultId])).rows[0];
      if(!segment?.chunk_ids.length)throw new Error('Extraction sources are unavailable');sourceIds=segment.chunk_ids;
    }else if(failure.chunk_id)sourceIds=[failure.chunk_id];
    else throw new Error('Extraction source is unavailable');
  }else{
    const parsed=targetsSchema.safeParse(failure.targets);if(!parsed.success)throw new Error('Curation failure has no retryable target snapshot');
    targets=parsed.data;
    sourceIds=(await query<{source_chunks:string[]}>('SELECT source_chunks FROM memories WHERE vault_id=$1 AND id=ANY($2::uuid[])',
      [input.vaultId,targets.map(t=>t.memory_id)])).rows.flatMap(m=>m.source_chunks ?? []);
  }
  sourceIds=[...new Set(sourceIds)];
  // Verify existing owned objects outside locks. Immutable accepted source metadata
  // and final row comparison bind this proof; missing/corrupt objects are refused.
  const sources=(await query<Source>('SELECT id,blob_key,blob_store,storage_bytes::text,processed FROM raw_chunks WHERE vault_id=$1 AND id=ANY($2::uuid[])',
    [input.vaultId,sourceIds])).rows;
  if(sources.length!==sourceIds.length)throw new Error('Recovery source lineage is incomplete');
  if(sources.length){
    const storage=getRawChunkStorage();
    for(const source of sources){
      if(!source.blob_key || source.blob_store!==storage.store || Number(source.storage_bytes)>1048576)throw new Error('Recovery source object is unavailable or outside read capacity');
      try{prepared.decrypt(vault,await storage.get(source.blob_key));}catch{throw new Error('Recovery source object is missing or corrupt');}
    }
  }
  return withTransaction(async client=>{
    await lockMemoryWriteVault(client,input.vaultId);
    await prepared.assertCurrent(client);
    const already=(await client.query<{queue_id:string}>('SELECT queue_id FROM memory_job_recoveries WHERE vault_id=$1 AND failure_kind=$2 AND failure_id=$3',
      [input.vaultId,input.kind,input.failureId])).rows[0];
    if(already)return{queueId:already.queue_id,alreadyRetried:true};
    const proof=await client.query(`SELECT queue_id FROM worker_action_receipts
      WHERE queue_kind=$1 AND queue_id=$2 AND action_key='dead-letter'`,[input.kind,failure.source_queue_id]);
    if(proof.rowCount!==1)throw new Error('Failure lacks a completed dead-letter action');
    const currentSources=(await client.query<Source>('SELECT id,blob_key,blob_store,storage_bytes::text,processed FROM raw_chunks WHERE vault_id=$1 AND id=ANY($2::uuid[]) ORDER BY id FOR KEY SHARE',
      [input.vaultId,sourceIds])).rows;
    if(currentSources.length!==sources.length || currentSources.some(s=>!sources.some(old=>old.id===s.id && old.blob_key===s.blob_key && old.blob_store===s.blob_store && old.storage_bytes===s.storage_bytes)))throw new Error('Recovery source changed during verification');
    let queueId:string;
    if(input.kind==='extraction'){
      if(currentSources.some(s=>s.processed))throw new Error('Extraction evidence has already been processed');
      const busy=await client.query(`SELECT q.id FROM extraction_queue q
        LEFT JOIN segments s ON s.id=q.segment_id AND s.vault_id=q.vault_id WHERE q.vault_id=$1 AND
        (q.id=$2 OR (q.segment_id=$3 AND $3::uuid IS NOT NULL) OR q.chunk_id=ANY($4::uuid[]) OR s.chunk_ids && $4::uuid[]) LIMIT 1`,
        [input.vaultId,failure.source_queue_id,failure.segment_id,sourceIds]);
      if(busy.rowCount)throw new Error('Extraction source already has queued or claimed work');
      queueId=crypto.randomUUID();
      // The original bulk job remains a truthful failed historical attempt. This
      // separately audited retry is tracked by its queue identity, not a reset job.
      await client.query(`INSERT INTO extraction_queue(id,vault_id,chunk_id,segment_id,context_chunk_ids)
        VALUES($1,$2,$3,$4,$5::uuid[])`,[queueId,input.vaultId,failure.chunk_id ?? null,failure.segment_id,failure.context_chunk_ids ?? null]);
    }else{
      const ids=targets.map(t=>t.memory_id);
      const current=(await client.query<{id:string;revision:string}>(`SELECT id,revision::text FROM memories WHERE vault_id=$1 AND id=ANY($2::uuid[])
        AND status='active' AND archived_at IS NULL ORDER BY id FOR UPDATE`,[input.vaultId,ids])).rows;
      if(current.length!==targets.length || current.some(m=>!targets.some(t=>t.memory_id===m.id && t.revision===m.revision)))throw new Error('Curation target revision is no longer current');
      if((await client.query(`SELECT 1 FROM curation_queue_items WHERE vault_id=$1 AND memory_id=ANY($2::uuid[]) LIMIT 1`,[input.vaultId,ids])).rowCount)throw new Error('Curation targets already have queued or claimed work');
      const queued=await enqueueCurationWork(client,{vaultId:input.vaultId,workKey:'recovery:'+input.failureId,memoryIds:ids,segmentId:failure.segment_id});
      if(!queued)throw new Error('Curation is not entitled or recovery work already exists');queueId=queued;
    }
    await client.query(`INSERT INTO memory_job_recoveries(vault_id,failure_kind,failure_id,queue_id,actor_id,reason)
      VALUES($1,$2,$3,$4,$5,$6)`,[input.vaultId,input.kind,input.failureId,queueId,input.actorId,input.reason]);
    return{queueId,alreadyRetried:false};
  });
}
