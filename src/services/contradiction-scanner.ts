import type { Client,PoolClient } from 'pg';
import { query,withTransaction } from '../db/client';
import { getConfig } from '../config';
import { decryptForVault,prepareVaultCrypto,type VaultEncryptionContext } from './crypto';
import type { ExtractorService,ConflictResolution } from './extractor';
import { enqueueCurationWork } from './curation-work';
import { publishCommittedWorkerEffects } from './worker-effects';
import crypto from 'node:crypto';

type ScanClient=Client|PoolClient;
interface MemoryCandidateRow extends VaultEncryptionContext {
  memory_id:string;data:string;status:string;similarity:number;scope:string;scope_key:string|null;row_version:string;
  source_timestamp:string|null;valid_from:string|null;valid_until:string|null;created_at:string;account_id:string|null;
  type:string|null;polarity:string;
}
export interface ContradictionScanOptions {client?:ScanClient;budget?:{remaining:number};maxArbitrations?:number}
export interface ContradictionScanResult {completedMemoryIds:string[];deferredMemoryIds:string[]}
const eligible=(a:string)=>`${a}.status='active' AND ${a}.archived_at IS NULL AND ${a}.sensitivity<>'restricted'
  AND ${a}.confidence>0 AND ${a}.confidence<=1
  AND (${a}.source_timestamp IS NULL OR ${a}.source_timestamp<=clock_timestamp()+interval '5 minutes')`;
const columns=(a:string)=>`${a}.id AS memory_id,${a}.data,${a}.status,${a}.scope,${a}.scope_key,${a}.revision::text AS row_version,
  ${a}.source_timestamp::text,${a}.valid_from::text,${a}.valid_until::text,${a}.created_at::text,${a}.type,${a}.polarity`;
const sameMeaning=(a:MemoryCandidateRow,b:MemoryCandidateRow)=>a.valid_from===b.valid_from && a.valid_until===b.valid_until && a.type===b.type && a.polarity===b.polarity;

export async function scanForContradictions(vaultId:string,memoryIds:string[],extractor:ExtractorService,options:ContradictionScanOptions={}):Promise<ContradictionScanResult>{
  const config=getConfig(),budget=options.budget ?? {remaining:config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH};
  const execute=options.client ? options.client.query.bind(options.client):query;
  const result:ContradictionScanResult={completedMemoryIds:[],deferredMemoryIds:[]};
  const transaction=async<T>(fn:(client:ScanClient)=>Promise<T>):Promise<T>=>{
    if(!options.client)return withTransaction(fn);
    await options.client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    try{const value=await fn(options.client);await options.client.query('COMMIT');return value;}
    catch(error){await options.client.query('ROLLBACK');throw error;}
  };
  for(const memoryId of memoryIds){
    if(!config.CONTRADICTION_SCAN_ENABLED || budget.remaining<=0){result.deferredMemoryIds.push(memoryId);continue;}
    const current=(await execute<MemoryCandidateRow>(`SELECT ${columns('m')},1.0 AS similarity,
      v.id,v.encrypted_dek,v.vault_encryption_enabled,v.account_id::text FROM memories m JOIN vaults v ON v.id=m.vault_id
      WHERE m.vault_id=$1 AND m.id=$2 AND ${eligible('m')}`,[vaultId,memoryId])).rows[0];
    if(!current){result.completedMemoryIds.push(memoryId);continue;}
    // Corrupt technical input fails this scan; never mutate memory into a review state.
    const currentFact=await decryptForVault(current,current.data);
    const limit=Math.min(budget.remaining,options.maxArbitrations ?? config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH);
    const candidates=(await execute<MemoryCandidateRow>(`SELECT ${columns('m')},
      1-(m.embedding <=> original.embedding) AS similarity,v.id,v.encrypted_dek,v.vault_encryption_enabled,v.account_id::text
      FROM memories original JOIN memories m ON m.vault_id=original.vault_id JOIN vaults v ON v.id=m.vault_id
      WHERE original.vault_id=$1 AND original.id=$2 AND original.revision=$3::bigint AND ${eligible('original')}
        AND m.id<>original.id AND m.scope=original.scope AND m.scope_key IS NOT DISTINCT FROM original.scope_key
        AND ${eligible('m')} AND original.embedding IS NOT NULL AND m.embedding IS NOT NULL
        AND 1-(m.embedding <=> original.embedding)>=$4
        AND NOT EXISTS(SELECT 1 FROM contradiction_scan_log l WHERE l.vault_id=$1
          AND ((l.memory_id_a=original.id AND l.revision_a=original.revision AND l.memory_id_b=m.id AND l.revision_b=m.revision)
            OR (l.memory_id_b=original.id AND l.revision_b=original.revision AND l.memory_id_a=m.id AND l.revision_a=m.revision)))
      ORDER BY similarity DESC,m.id LIMIT $5`,[vaultId,memoryId,current.row_version,config.CONTRADICTION_SCAN_MIN_SIMILARITY,limit+1])).rows;
    let complete=candidates.length<=limit;
    for(const candidate of candidates.slice(0,limit)){
      const fact=await decryptForVault(candidate,candidate.data);
      let decision:ConflictResolution='keep_both';
      if(sameMeaning(current,candidate)){
        if(fact===currentFact)decision='merge';
        else{
          budget.remaining--;
          const temporal=(m:MemoryCandidateRow)=>({sourceTimestamp:m.source_timestamp,validFrom:m.valid_from,validUntil:m.valid_until,createdAt:m.created_at});
          decision=await extractor.arbitrateConflict(fact,currentFact,vaultId,{existing:temporal(candidate),incoming:temporal(current)});
        }
      }
      if(!['supersede_old','discard_new','keep_both','merge'].includes(decision))throw new Error('Invalid conflict decision');
      const prepared=await prepareVaultCrypto(current);
      let retired=false;
      await transaction(async client=>{
        await client.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE',[vaultId]);
        // These helpers use only the pg query interface on the same advisory-lock
        // owning connection, not a second pool transaction.
        await prepared.assertCurrent(client as PoolClient);
        const locked=await client.query(`SELECT id FROM memories m WHERE vault_id=$1
          AND ((id=$2 AND revision=$4::bigint) OR (id=$3 AND revision=$5::bigint))
          AND ${eligible('m')} ORDER BY id FOR UPDATE`,[vaultId,current.memory_id,candidate.memory_id,current.row_version,candidate.row_version]);
        if(locked.rowCount!==2)throw new Error('Conflict inputs changed after arbitration');
        if(decision!=='keep_both'){
          const survivor=decision==='supersede_old'?current.memory_id:candidate.memory_id;
          const removed=decision==='supersede_old'?candidate.memory_id:current.memory_id;
          if(decision==='merge'){
            await client.query(`UPDATE memories target SET source_chunks=ARRAY(
              SELECT DISTINCT source FROM memories m,unnest(m.source_chunks) source WHERE m.vault_id=$1 AND m.id=ANY($3::uuid[]) ORDER BY source),
              sensitivity=CASE WHEN EXISTS(SELECT 1 FROM memories WHERE vault_id=$1 AND id=ANY($3::uuid[]) AND sensitivity='high') THEN 'high'
                WHEN EXISTS(SELECT 1 FROM memories WHERE vault_id=$1 AND id=ANY($3::uuid[]) AND sensitivity='medium') THEN 'medium' ELSE 'low' END,
              source_timestamp=(SELECT max(source_timestamp) FROM memories WHERE vault_id=$1 AND id=ANY($3::uuid[])),updated_at=now()
              WHERE target.vault_id=$1 AND target.id=$2`,[vaultId,survivor,[current.memory_id,candidate.memory_id]]);
            await enqueueCurationWork(client as PoolClient,{vaultId,workKey:'conflict:'+crypto.randomUUID(),memoryIds:[survivor]});
          }
          await client.query(`UPDATE memories SET status=$3,archived_at=now(),updated_at=now() WHERE vault_id=$1 AND id=$2`,
            [vaultId,removed,decision==='merge'?'superseded':'contradicted']);
          retired=true;
        }
        // A is the first model input, B the second; log exact resulting revisions.
        await client.query(`INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b)
          SELECT $1,a.id,b.id,$4,$5,a.revision,b.revision FROM memories a,memories b
          WHERE a.vault_id=$1 AND b.vault_id=$1 AND a.id=$2 AND b.id=$3`,
          [vaultId,candidate.memory_id,current.memory_id,decision,candidate.similarity]);
      });
      if(retired)publishCommittedWorkerEffects([{kind:'memory-count',vaultId,accountId:current.account_id,delta:-1,source:'extraction_worker'}]);
      if(decision==='merge' || decision==='discard_new'){complete=true;break;}
    }
    // An empty neighbour query is not completion of a concurrently changed input.
    if(!candidates.length){
      const unchanged=await execute('SELECT 1 FROM memories WHERE vault_id=$1 AND id=$2 AND revision=$3::bigint',[vaultId,memoryId,current.row_version]);
      complete=unchanged.rowCount===1;
    }
    (complete?result.completedMemoryIds:result.deferredMemoryIds).push(memoryId);
  }
  return result;
}
