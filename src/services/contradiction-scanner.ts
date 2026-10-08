import { unresolvedPairSql } from './contradiction-pairs';
import { prepareConflictEvidence, conflictStillCurrent } from './conflict-evidence';
import { validatedConflictAssessment } from './extractor';
import { BEHAVIORAL_MEMORY_TYPES,isMemoryTemporallyRecallable,memoryTemporalState } from './memory-temporal-recall';
import type { Client,PoolClient } from 'pg';
import { query,withTransaction } from '../db/client';
import { getConfig } from '../config';
import { decryptForVault,prepareVaultCrypto,type VaultEncryptionContext } from './crypto';
import type { ExtractorService,ConflictResolution } from './extractor';
import { enqueueCurationWork } from './curation-work';
import { memoryCountEffect, captureShadowWorkerEffects, type WorkerEffect, publishCommittedWorkerEffects } from './worker-effects';
import crypto from 'node:crypto';

type ScanClient=Client|PoolClient;
interface MemoryCandidateRow extends VaultEncryptionContext {
  memory_id:string;data:string;status:string;similarity:number;scope:string;scope_key:string|null;row_version:string;
  source_timestamp:string|null;valid_from:string|null;valid_until:string|null;created_at:string;account_id:string|null;
  type:string|null;polarity:string;
}
/** Declared stream: same-vault/binding eligible rows at the configured cosine
 * threshold OR retained unresolved pair neighbors, excluding exact current pair
 * completions. History only rediscovers a pair; fresh source evidence/receipts
 * still govern every decision. Aliases do not participate in this stream.
 * Order: cosine descending (missing embeddings = -1), UUID ascending.
 * Completion exhausts this policy, not all possible contradictions.
 * Total input/candidate payload rows per invocation, including lookahead: */
export const CONTRADICTION_MAX_CANDIDATES_PER_PASS=64;
export interface ContradictionScanCursor {
  revision:string;referenceDate:string;similarity:number;memoryId:string;
}
export interface ContradictionScanOptions {
  client?:ScanClient;budget?:{remaining:number};maxArbitrations?:number;now?:Date;
  cursors?:Record<string,ContradictionScanCursor>;
  admitArbitration?:()=>Promise<boolean>;
}
export interface ContradictionScanResult {
  completedMemoryIds:string[];deferredMemoryIds:string[];
  cursors?:Record<string,ContradictionScanCursor>;
  /** Exhausted this snapshot, but start again (without a cursor) at applicability. */
  reactivations?:Record<string,{revision:string;referenceDate:string;validFrom:string}>;
}
const eligible=(a:string)=>`${a}.status='active' AND ${a}.archived_at IS NULL AND ${a}.sensitivity<>'restricted'
  AND ${a}.confidence>0 AND ${a}.confidence<=1
  AND (${a}.source_timestamp IS NULL OR ${a}.source_timestamp<=clock_timestamp()+interval '5 minutes')`;
const columns=(a:string)=>`${a}.id AS memory_id,${a}.data,${a}.status,${a}.scope,${a}.scope_key,${a}.revision::text AS row_version,
  ${a}.source_timestamp::text,${a}.valid_from::text,${a}.valid_until::text,${a}.created_at::text,${a}.type,${a}.polarity`;
const sameMeaning=(a:MemoryCandidateRow,b:MemoryCandidateRow)=>a.valid_from===b.valid_from && a.valid_until===b.valid_until && a.type===b.type && a.polarity===b.polarity;
// pgvector cosine is NaN for zero-norm vectors; neither NaN nor NULL is relevance.
const candidateSimilarity="COALESCE(NULLIF(1-(m.embedding <=> original.embedding),'NaN'::double precision),-1)";

export async function scanForContradictions(vaultId:string,memoryIds:string[],extractor:ExtractorService,options:ContradictionScanOptions={}):Promise<ContradictionScanResult>{
  const now=options.now ?? new Date(),referenceDate=now.toISOString().slice(0,10);
  let remainingReads=CONTRADICTION_MAX_CANDIDATES_PER_PASS;
  const config=getConfig(),budget=options.budget ?? {remaining:config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH};
  const execute=options.client ? options.client.query.bind(options.client):query;
  const result:ContradictionScanResult={completedMemoryIds:[],deferredMemoryIds:[]};
  // One durable reminder per future behavioral memory, not one lossy minimum
  // over a partially consumed candidate stream. Its own pass retains it even if
  // the SQL shortlist is empty; neighbors discovered in bounded pages recover
  // missing reminders without replacing due work or another scanner's cursor.
  const isFutureBehavior=(memory:MemoryCandidateRow)=>(BEHAVIORAL_MEMORY_TYPES as readonly unknown[]).includes(memory.type)
    && memoryTemporalState(memory,now)==='future';
  const preserveFutureReminders=async(memories:MemoryCandidateRow[])=>{
    const future=memories.filter(isFutureBehavior);
    if(!future.length)return;
    // Do this BEFORE consuming a cursor or logging a pair: a crash after either
    // must not hide a neighbor whose old reminder was already removed. Lock
    // exact observed revisions so concurrent retire/type/window changes cannot
    // resurrect stale work after their trigger removed it. This atomic bounded
    // insert never updates existing (possibly due or partially scanned) work.
    // Take the vault lock before memory locks, like decision transactions, so
    // vault deletion cannot invert memory/FK lock acquisition during enqueue.
    await execute(`WITH future_memories AS MATERIALIZED (
      SELECT m.id,m.vault_id,m.revision,m.valid_from FROM memories m
      JOIN (SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE) v ON v.id=m.vault_id
      JOIN jsonb_to_recordset($2::jsonb) AS observed(id uuid,revision bigint) ON m.id=observed.id AND m.revision=observed.revision
      WHERE m.vault_id=$1 AND ${eligible('m')}
      ORDER BY m.id FOR NO KEY UPDATE OF m
    ) INSERT INTO memory_contradiction_schedule(memory_id,vault_id,revision,available_at)
      SELECT id,vault_id,revision,valid_from::timestamp AT TIME ZONE 'UTC' FROM future_memories
      ON CONFLICT(memory_id) DO NOTHING`,[vaultId,JSON.stringify(future.map(m=>({id:m.memory_id,revision:m.row_version})))]);
  };
  const transaction=async<T>(fn:(client:ScanClient)=>Promise<T>):Promise<T>=>{
    if(!options.client)return withTransaction(fn);
    await options.client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
    try{const value=await fn(options.client);await options.client.query('COMMIT');return value;}
    catch(error){await options.client.query('ROLLBACK');throw error;}
  };
  for(const memoryId of memoryIds){
    if(!config.CONTRADICTION_SCAN_ENABLED || budget.remaining<=0 || remainingReads<=0){result.deferredMemoryIds.push(memoryId);continue;}
    remainingReads--;
    const current=(await execute<MemoryCandidateRow>(`SELECT ${columns('m')},1.0 AS similarity,
      v.id,v.encrypted_dek,v.vault_encryption_enabled,v.account_id::text FROM memories m JOIN vaults v ON v.id=m.vault_id
      WHERE m.vault_id=$1 AND m.id=$2 AND ${eligible('m')}`,[vaultId,memoryId])).rows[0];
    if(!current){result.completedMemoryIds.push(memoryId);continue;}
    await preserveFutureReminders([current]);
    // Corrupt technical input fails this scan; never mutate memory into a review state.
    const currentFact=await decryptForVault(current,current.data);
    const limit=Math.min(budget.remaining,options.maxArbitrations ?? config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH);
    const currentRecallable=isMemoryTemporallyRecallable(current,now);
    // A hash is only a shortlist hint. Bound *all* rows, including skipped hints
    // and actionable lookahead, not just decisions. Persist only consumed keys:
    // an unprocessed actionable lookahead must be revisited on the next pass.
    const saved=options.cursors?.[memoryId];
    let cursor=saved?.revision===current.row_version && saved.referenceDate===referenceDate ? saved : undefined;
    let complete=true,currentRetired=false,processed=0;
    const position=(candidate:MemoryCandidateRow):ContradictionScanCursor=>({
      revision:current.row_version,referenceDate,similarity:candidate.similarity,memoryId:candidate.memory_id
    });
    async function* actionableCandidates(){
      for(;;){
        if(remainingReads<=0){complete=false;return;}
        const pageSize=Math.min(limit+1,remainingReads);
        const candidates=(await execute<MemoryCandidateRow>(`SELECT ${columns('m')},
          ${candidateSimilarity} AS similarity,v.id,v.encrypted_dek,v.vault_encryption_enabled,v.account_id::text
          FROM memories original JOIN memories m ON m.vault_id=original.vault_id JOIN vaults v ON v.id=m.vault_id
          WHERE original.vault_id=$1 AND original.id=$2 AND original.revision=$3::bigint AND ${eligible('original')}
            AND m.id<>original.id AND m.scope=original.scope AND m.scope_key IS NOT DISTINCT FROM original.scope_key
            AND ${eligible('m')}
            AND ((original.embedding IS NOT NULL AND m.embedding IS NOT NULL
              AND ${candidateSimilarity}>=$4)
              OR (${unresolvedPairSql('original','m',false)}))
            AND (ROW(m.valid_from,m.valid_until,m.type,m.polarity)
              IS DISTINCT FROM ROW(original.valid_from,original.valid_until,original.type,original.polarity)
              OR $6::boolean OR m.data=original.data OR m.hash=original.hash)
            AND ($7::double precision IS NULL OR ${candidateSimilarity}<$7
              OR (${candidateSimilarity}=$7 AND m.id>$8::uuid))
            AND NOT EXISTS(SELECT 1 FROM contradiction_scan_log l WHERE l.vault_id=$1 AND l.send_attempt IS NULL
              AND ((l.memory_id_a=original.id AND l.revision_a=original.revision AND l.memory_id_b=m.id AND l.revision_b=m.revision)
                OR (l.memory_id_b=original.id AND l.revision_b=original.revision AND l.memory_id_a=m.id AND l.revision_a=m.revision)))
          ORDER BY similarity DESC,m.id LIMIT $5`,[vaultId,memoryId,current.row_version,config.CONTRADICTION_SCAN_MIN_SIMILARITY,
          pageSize,currentRecallable,cursor?.similarity??null,cursor?.memoryId??null])).rows;
        remainingReads-=candidates.length;
        await preserveFutureReminders(candidates);
        for(const candidate of candidates){
          const fact=await decryptForVault(candidate,candidate.data);
          // Same metadata means the SQL shortlist can use currentRecallable for
          // both rows. Recheck in application code before any provider call.
          if(sameMeaning(current,candidate)
            && (!currentRecallable || !isMemoryTemporallyRecallable(candidate,now))){cursor=position(candidate);continue;}
          yield {candidate,fact};
          cursor=position(candidate);
        }
        if(candidates.length<pageSize)return;
      }
    }
    for await(const {candidate,fact} of actionableCandidates()){
      if(processed===limit){complete=false;break;}
      processed++;
      let decision:ConflictResolution|'incomplete'='incomplete';
      let receipt:ReturnType<typeof validatedConflictAssessment>;
      if(sameMeaning(current,candidate) && currentRecallable && isMemoryTemporallyRecallable(candidate,now)){
        decision='incomplete';
        const evidence=await prepareConflictEvidence(vaultId,candidate.memory_id,current.memory_id,{query:execute} as never);
        if(evidence){
          if(options.admitArbitration && !await options.admitArbitration()){complete=false;break;}
          budget.remaining--;
          const temporal=(m:MemoryCandidateRow)=>({sourceTimestamp:m.source_timestamp,validFrom:m.valid_from,validUntil:m.valid_until,createdAt:m.created_at});
          receipt=validatedConflictAssessment(await extractor.arbitrateConflict(fact,currentFact,vaultId,
            {evidence,existing:temporal(candidate),incoming:temporal(current)}));
          if(receipt)decision=receipt.decision;
        }
      }

      const prepared=await prepareVaultCrypto(current);
      const effects: WorkerEffect[] = [];
      await transaction(async client=>{
        await client.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE',[vaultId]);
        // These helpers use only the pg query interface on the same advisory-lock
        // owning connection, not a second pool transaction.
        await prepared.assertCurrent(client as PoolClient);
        const locked=await client.query(`SELECT id FROM memories m WHERE vault_id=$1
          AND ((id=$2 AND revision=$4::bigint) OR (id=$3 AND revision=$5::bigint))
          AND ${eligible('m')} ORDER BY id FOR UPDATE`,[vaultId,current.memory_id,candidate.memory_id,current.row_version,candidate.row_version]);
        if(locked.rowCount!==2)throw new Error('Conflict inputs changed after arbitration');
        if(receipt && !await conflictStillCurrent(receipt.evidence,client as PoolClient,vaultId,candidate.memory_id,current.memory_id))decision='incomplete';
        if(decision!=='keep_both' && decision!=='incomplete'){
          const survivor=decision==='supersede_old'?current.memory_id:candidate.memory_id;
          const removed=decision==='supersede_old'?candidate.memory_id:current.memory_id;
          if(decision==='merge' || decision==='discard_new'){
            // Both decisions subsume B's supported information into A. Preserve
            // complete support, but enqueue only a genuinely changed survivor.
            const changed=await client.query(`WITH support AS (
              SELECT ARRAY(SELECT DISTINCT source FROM memories m,unnest(m.source_chunks) source
                WHERE m.vault_id=$1 AND m.id=ANY($3::uuid[]) ORDER BY source) AS sources,
                CASE WHEN bool_or(sensitivity='high') THEN 'high'
                  WHEN bool_or(sensitivity='medium') THEN 'medium' ELSE 'low' END AS sensitivity,
                max(source_timestamp) AS source_timestamp
              FROM memories WHERE vault_id=$1 AND id=ANY($3::uuid[])
            ) UPDATE memories target SET source_chunks=support.sources,sensitivity=support.sensitivity,
              source_timestamp=support.source_timestamp,updated_at=now() FROM support
              WHERE target.vault_id=$1 AND target.id=$2
                AND (NOT (target.source_chunks @> support.sources AND target.source_chunks <@ support.sources)
                  OR ROW(target.sensitivity,target.source_timestamp)
                    IS DISTINCT FROM ROW(support.sensitivity,support.source_timestamp))`,
              [vaultId,survivor,[current.memory_id,candidate.memory_id]]);
            if(changed.rowCount)await enqueueCurationWork(client as PoolClient,{vaultId,workKey:'conflict:'+crypto.randomUUID(),memoryIds:[survivor]});
          }
          await client.query(`UPDATE memories SET status=$3,archived_at=now(),updated_at=now() WHERE vault_id=$1 AND id=$2`,
            [vaultId,removed,decision==='discard_new'?'contradicted':'superseded']);
          effects.push(memoryCountEffect(vaultId,current.account_id,-1,'extraction_worker'));
        }
        // A is the first model input, B the second; log exact resulting revisions.
        await client.query(`INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b)
          SELECT $1,a.id,b.id,$4,$5,a.revision,b.revision FROM memories a,memories b
          WHERE a.vault_id=$1 AND b.vault_id=$1 AND a.id=$2 AND b.id=$3`,
          [vaultId,candidate.memory_id,current.memory_id,decision,candidate.similarity]);
        await captureShadowWorkerEffects(client as PoolClient,effects);
      });
      if(effects.length)publishCommittedWorkerEffects(effects);
      if(decision==='merge' || decision==='discard_new'){currentRetired=true;break;}
    }
    // Neither completion nor progress is valid for changed input. Retirement
    // already validated the revision under lock; the scheduler also fences the
    // generation when saving a cursor or deleting the reminder.
    if(!currentRetired){
      const unchanged=await execute('SELECT 1 FROM memories WHERE vault_id=$1 AND id=$2 AND revision=$3::bigint',[vaultId,memoryId,current.row_version]);
      if(unchanged.rowCount!==1){complete=false;cursor=undefined;}
    }
    if(!complete && cursor)(result.cursors??={})[memoryId]=cursor;
    if(complete && !currentRetired && isFutureBehavior(current)){
      (result.reactivations??={})[memoryId]={revision:current.row_version,referenceDate,validFrom:current.valid_from!};
      complete=false;
    }
    (complete?result.completedMemoryIds:result.deferredMemoryIds).push(memoryId);
  }
  return result;
}
