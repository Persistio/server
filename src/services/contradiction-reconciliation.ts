import {z} from 'zod';
import {MIGRATION_ADVISORY_LOCK_ID,withTransaction} from '../db/client';
import {memoryTemporalRecallPredicateSql} from './memory-temporal-recall';

export const CONTRADICTION_RECONCILIATION_PAGE_SIZE=64;
const requestSchema=z.object({vaultId:z.string().uuid(),after:z.string().uuid().nullable().optional(),
  through:z.string().uuid().nullable().optional()}).strict();

/** Explicit operator maintenance, never startup or request-triggered arbitration.
 * Stop/drain writers and schedulers first. One transaction repairs <=64 metadata
 * rows; the returned fixed high-water UUID bounds resumed pages. Existing current
 * generations/cursors/failures/reminders are untouched. No schema change needed. */
export async function reconcileContradictionSchedulePage(input:z.input<typeof requestSchema>){
  const request=requestSchema.parse(input);
  if(request.after && (!request.through || request.after.toLowerCase()>request.through.toLowerCase()))
    throw new Error('Invalid reconciliation cursor');
  return withTransaction(async client=>{
    // Share the production migration exclusion, without becoming a migration
    // runner or changing its ledger. Fail rather than race an active drain.
    const migration=await client.query('SELECT pg_try_advisory_xact_lock($1::bigint) AS locked',[MIGRATION_ADVISORY_LOCK_ID]);
    if(!migration.rows[0].locked)throw new Error('Schema maintenance is active');
    const scheduler=await client.query('SELECT pg_try_advisory_xact_lock(hashtextextended($1,0)) AS locked',[
      'persistio:contradiction-activation:'+request.vaultId]);
    if(!scheduler.rows[0].locked)throw new Error('Contradiction scheduler is active');
    const vault=await client.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE',[request.vaultId]);
    if(!vault.rowCount)throw new Error('Reconciliation vault is unavailable');
    const through=request.through??(await client.query('SELECT id FROM memories WHERE vault_id=$1 ORDER BY id DESC LIMIT 1',[request.vaultId])).rows[0]?.id??null;
    if(!through)return{examined:0,scheduled:0,nextAfter:null,through:null,done:true};
    const rows=await client.query<{id:string;revision:string}>(`SELECT m.id,m.revision::text FROM memories m
      WHERE m.vault_id=$1 AND ($2::uuid IS NULL OR m.id>$2::uuid) AND m.id<=$3::uuid
        AND m.status='active' AND m.archived_at IS NULL AND m.sensitivity<>'restricted'
        AND m.confidence>0 AND m.confidence<=1
        AND (m.source_timestamp IS NULL OR m.source_timestamp<=statement_timestamp()+interval '5 minutes')
        AND ${memoryTemporalRecallPredicateSql('m',"GREATEST(statement_timestamp(),m.valid_from::timestamp AT TIME ZONE 'UTC')")}
      ORDER BY m.id LIMIT $4 FOR NO KEY UPDATE`,[request.vaultId,request.after??null,through,CONTRADICTION_RECONCILIATION_PAGE_SIZE]);
    const repaired=await client.query(`INSERT INTO memory_contradiction_schedule(memory_id,vault_id,revision)
      SELECT id,$1,revision FROM jsonb_to_recordset($2::jsonb) AS observed(id uuid,revision bigint) ORDER BY id
      ON CONFLICT(memory_id) DO UPDATE SET revision=EXCLUDED.revision,generation=gen_random_uuid(),available_at=now(),failures=0
        WHERE memory_contradiction_schedule.vault_id=EXCLUDED.vault_id
          AND memory_contradiction_schedule.revision<>EXCLUDED.revision`,[request.vaultId,JSON.stringify(rows.rows)]);
    return{examined:rows.rowCount??0,scheduled:repaired.rowCount??0,nextAfter:rows.rows.at(-1)?.id??request.after??null,through,
      done:rows.rows.length<CONTRADICTION_RECONCILIATION_PAGE_SIZE || rows.rows.at(-1)?.id===through};
  });
}
