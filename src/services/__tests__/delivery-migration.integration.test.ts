import crypto from 'node:crypto';
import { readFileSync,readdirSync } from 'node:fs';
import { Client } from 'pg';
import { beforeAll,afterAll,describe,it,expect } from 'vitest';
import { DELIVERY_HEALTH_SQL } from '../delivery-health';
import { readDeliveryAudit } from '../../../../../scripts/lib/memory-observability-audit.mjs';

const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
const protectedTables=['memory_mutation_events','memory_delivery_runs','memory_delivery_events','memory_authority_events',
  'memory_scope_change_log','contradiction_scan_log','curation_action_log','curation_review_runs','curation_dead_letter',
  'extraction_dead_letter','memory_delivery_acknowledgements','memory_delivery_pending','memories'];
const directory=new URL('../../db/migrations/',import.meta.url);
const migration='056_delivery_completion_and_audit_guards.sql';
describe.skipIf(!databaseUrl)('fresh and populated delivery migration / all retained-table guards',()=>{
  const name=`persistio_test_pr371_${crypto.randomUUID().replaceAll('-','')}`;
  let admin:Client,db:Client,created=false;
  const vault=crypto.randomUUID(),memory=crypto.randomUUID(),segment=crypto.randomUUID();
  const runs:Record<string,string>={};
  async function migrate(filename:string){
    if((await db.query('SELECT 1 FROM schema_migrations WHERE filename=$1',[filename])).rowCount)return;
    await db.query('BEGIN');
    try{
      await db.query("SELECT set_config('persistio.skip_migration_record','false',true)");
      await db.query(readFileSync(new URL(filename,directory),'utf8'));
      const skip=(await db.query("SELECT current_setting('persistio.skip_migration_record',true) AS skip")).rows[0].skip;
      if(skip!=='true')await db.query('INSERT INTO schema_migrations(filename) VALUES($1)',[filename]);
      await db.query('COMMIT');
    }catch(error){await db.query('ROLLBACK');throw error;}
  }
  beforeAll(async()=>{
    const url=new URL(databaseUrl!);
    if(!['localhost','127.0.0.1','::1','[::1]'].includes(url.hostname)||!/(test|pr369)/.test(url.pathname))throw Error('Requires isolated local test database');
    admin=new Client({connectionString:databaseUrl});await admin.connect();
    await admin.query(`CREATE DATABASE "${name}"`);created=true;url.pathname=`/${name}`;
    db=new Client({connectionString:url.toString()});await db.connect();
    await db.query('CREATE TABLE schema_migrations(filename TEXT PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())');
    await db.query("SELECT set_config('persistio.storage_embedding_dimensions','1536',false)");
    for(const filename of readdirSync(directory).filter(file=>file.endsWith('.sql')&&file<migration).sort())await migrate(filename);
  },30000);
  afterAll(async()=>{
    if(db)await db.end();
    // Only the unique database created above, never the configured test database.
    if(created)await admin.query(`DROP DATABASE "${name}"`);
    if(admin)await admin.end();
  });
  async function rejected(sql:string,pattern=/append-only|immutable|protected|audit mutation is not permitted/){
    await db.query('SAVEPOINT denied');await expect(db.query(sql),sql).rejects.toThrow(pattern);await db.query('ROLLBACK TO SAVEPOINT denied');
  }
  it('protects even empty tables and rolls back the complete TRUNCATE statement',async()=>{
    await db.query('BEGIN');
    try{
      await db.query(readFileSync(new URL(migration,directory),'utf8'));
      for(const table of protectedTables)await rejected(`TRUNCATE ${table} RESTART IDENTITY CASCADE`);
      await rejected('TRUNCATE vaults,raw_chunks CASCADE');
      for(const table of protectedTables)expect((await db.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n).toBe(0);
    }finally{await db.query('ROLLBACK');}
    expect((await db.query("SELECT to_regclass('memory_delivery_acknowledgements') AS table_name")).rows[0].table_name).toBeNull();
  });
  async function legacy(kind:string,count:number){
    const id=crypto.randomUUID();runs[kind]=id;
    await db.query(`INSERT INTO memory_delivery_runs(id,vault_id,query_hash,response_format,mode,top_k,min_similarity,global_rule_policy,
      include_global_rules_requested,include_global_rules_effective,selected_count,global_selected_count,created_at)
      VALUES($1,$2,$3,'json','agent',2,0.5,'off',false,false,$4,0,clock_timestamp()-interval '1 day')`,[id,vault,'a'.repeat(64),count]);
    const ids=Array.from({length:count},()=>crypto.randomUUID());
    for(const stage of ['selected','returned','rendered'])for(const [index,mid]of ids.entries()){
      if(stage==='returned'&&kind==='missing-returned')continue;
      if(stage==='rendered'&&(kind==='pending'||kind==='missing-returned'||(kind==='partial'&&index===1)))continue;
      await db.query(`INSERT INTO memory_delivery_events(delivery_id,vault_id,memory_id,authority_version,stage,section,memory_type,
        retrieval_reason,scope,authority_state,authority_required,authority_approval_valid,similarity,token_budget,rendered_tokens,truncated,render_target)
        VALUES($1,$2,$3,$4,$5,'historical_facts','system_fact','semantic','global','untrusted',false,false,0.8,$6,$7,$8,$9)`,
      [id,vault,stage==='rendered'&&kind==='wrong-id'&&index===1?crypto.randomUUID():mid,
        stage==='rendered'&&kind==='snapshot'?2:1,stage,stage==='rendered'?100:null,
        stage==='rendered'?(kind==='metadata'&&index===1?11:10):null,stage==='rendered'?false:null,stage==='rendered'?'tool_response':null]);
    }
  }
  it('classifies old complete, pending, partial, wrong-ID, snapshot, metadata and empty evidence without rewriting it',async()=>{
    await db.query('INSERT INTO vaults(id,name,api_key_hash) VALUES($1,\'migration\',$2)',[vault,crypto.randomUUID()]);
    await db.query("INSERT INTO segments(id,vault_id,session_id,chunk_ids) VALUES($1,$2,'migration','{}')",[segment,vault]);
    await db.query("INSERT INTO memories(id,vault_id,data,subject,hash,type,scope,status,source_segment_id) VALUES($1,$2,'fixture','fixture',$3,'system_fact','global','active',$4)",[memory,vault,crypto.randomUUID(),segment]);
    await db.query("INSERT INTO memory_authority_events(vault_id,memory_id,event_type,new_state,new_version,actor_type,source,reason) VALUES($1,$2,'approve','approved',1,'user','api','fixture')",[vault,memory]);
    await db.query("INSERT INTO memory_scope_change_log(vault_id,memory_id,old_scope,new_scope,actor_type,source,reason) VALUES($1,$2,'session','global','user','api','fixture')",[vault,memory]);
    await db.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity) VALUES($1,$2,$2,'needs_review',0.9)",[vault,memory]);
    await db.query("INSERT INTO curation_action_log(vault_id,segment_id,action_type,memory_id,raw_curator_response,applied_at) VALUES($1,$2,'update',$3,'{}',now())",[vault,segment,memory]);
    await db.query("INSERT INTO curation_review_runs(vault_id,segment_id,model,schema_version,prompt_version,prompt_hash,validation_status,raw_response) VALUES($1,$2,'test','v1','v1',$3,'valid','{}')",[vault,segment,'a'.repeat(64)]);
    for(const table of ['curation_dead_letter','extraction_dead_letter'])await db.query(`INSERT INTO ${table}(vault_id,segment_id,retry_count,last_error) VALUES($1,$2,3,'fixture')`,[vault,segment]);
    for(const kind of ['complete','pending','partial','wrong-id','metadata','snapshot','missing-returned','empty'])await legacy(kind,kind==='empty'?0:2);
    const before=(await db.query('SELECT to_jsonb(e) AS row FROM memory_delivery_events e ORDER BY id')).rows;
    await db.query('BEGIN');
    await db.query(readFileSync(new URL(migration,directory),'utf8'));
    await expect(db.query('SELECT deliberately_missing_migration_function()')).rejects.toThrow();await db.query('ROLLBACK');
    expect((await db.query("SELECT 1 FROM information_schema.columns WHERE table_name='memory_delivery_runs' AND column_name='completion_protocol'")).rowCount).toBe(0);
    await migrate(migration);await migrate(migration);
    expect((await db.query('SELECT to_jsonb(e) AS row FROM memory_delivery_events e ORDER BY id')).rows).toEqual(before);
    const ack=(await db.query('SELECT * FROM memory_delivery_acknowledgements')).rows;
    expect(ack).toHaveLength(1);expect(ack[0]).toMatchObject({delivery_id:runs.complete,origin:'legacy_terminal_events'});
    expect(ack[0].original_terminal_at.getTime()).toBeLessThanOrEqual(ack[0].recorded_at.getTime());
    const pending=(await db.query('SELECT * FROM memory_delivery_pending')).rows;
    expect(pending).toHaveLength(6);
    for(const kind of ['partial','wrong-id','metadata','snapshot','missing-returned'])expect(pending.find(p=>p.delivery_id===runs[kind])?.integrity_error).toBe(true);
    expect(pending.find(p=>p.delivery_id===runs.pending)?.integrity_error).toBe(false);
    expect((await db.query('SELECT DISTINCT completion_protocol FROM memory_delivery_runs')).rows).toEqual([{completion_protocol:0}]);
    expect(await readDeliveryAudit(db,vault)).toMatchObject({delivery_runs:8,legacy_empty_outcome_unknown:1,invalid_partitions:5,pending_projection_mismatches:0});
  });
  it('monitors only indexed pending state despite thousands of retained complete deliveries',async()=>{
    await db.query('BEGIN');
    try{
      const historyVault=crypto.randomUUID();
      await db.query(`INSERT INTO memory_delivery_runs(vault_id,query_hash,response_format,mode,top_k,min_similarity,global_rule_policy,
        include_global_rules_requested,include_global_rules_effective,selected_count,global_selected_count,created_at)
        SELECT $1,$2,'json','agent',1,0.5,'off',false,false,0,0,clock_timestamp()-interval '1 day' FROM generate_series(1,2000)`,[historyVault,'b'.repeat(64)]);
      await db.query(`INSERT INTO memory_delivery_acknowledgements(delivery_id,vault_id,outcome,origin)
        SELECT id,vault_id,'{"rendered_ids":[],"dropped":[],"token_budget":0,"rendered_tokens":0,"truncated":false,"render_target":"prompt_context"}'::jsonb,'client_ack'
        FROM memory_delivery_runs WHERE vault_id=$1`,[historyVault]);
      await db.query('ANALYZE memory_delivery_pending');
      const result=(await db.query(DELIVERY_HEALTH_SQL)).rows[0];expect(result).toEqual({overdue:1,integrity_error:5});
      const fixed=(await db.query('SELECT clock_timestamp() AS now')).rows[0].now;
      for(const delta of [-1,0,1])await db.query(`INSERT INTO memory_delivery_runs(vault_id,query_hash,response_format,mode,top_k,min_similarity,global_rule_policy,
        include_global_rules_requested,include_global_rules_effective,selected_count,global_selected_count,created_at)
        VALUES($1,$2,'json','agent',1,0.5,'off',false,false,0,0,$3::timestamptz-interval '5 minutes'+$4::int*interval '1 millisecond')`,[historyVault,'c'.repeat(64),fixed,delta]);
      // Only the strictly older row qualifies; neither exact boundary nor newer.
      expect((await db.query(DELIVERY_HEALTH_SQL.replace('clock_timestamp()','$1::timestamptz'),[fixed])).rows[0]).toEqual({overdue:2,integrity_error:5});
      const plan=JSON.stringify((await db.query(`EXPLAIN (ANALYZE,FORMAT JSON) ${DELIVERY_HEALTH_SQL}`)).rows);
      expect(plan).toContain('memory_delivery_pending');expect(plan).not.toContain('memory_delivery_events');expect(plan).not.toContain('memory_delivery_runs');
      // Tiny fixture backlogs may legitimately choose a sequential scan. Prove
      // both selective access paths are indexable without forcing production plans.
      await db.query('SET LOCAL enable_seqscan=off');
      const indexed=JSON.stringify((await db.query(`EXPLAIN (FORMAT JSON) ${DELIVERY_HEALTH_SQL}`)).rows);
      expect(indexed).toContain('idx_memory_delivery_pending_age');expect(indexed).toContain('idx_memory_delivery_pending_invalid');
    }finally{await db.query('ROLLBACK');}
  },15000);
  it('blocks destructive operations on populated evidence while retaining legitimate review and domain deletion',async()=>{
    await db.query('BEGIN');
    try{
      for(const table of protectedTables){
        expect((await db.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n).toBeGreaterThan(0);
        await rejected(`TRUNCATE ${table} RESTART IDENTITY CASCADE`);
      }
      await rejected(`TRUNCATE vaults,memories,memory_delivery_runs CASCADE`);
      for(const table of protectedTables.filter(t=>!['memories','memory_delivery_pending','curation_review_runs'].includes(t))){
        await rejected(`DELETE FROM ${table}`);await rejected(`UPDATE ${table} SET vault_id=vault_id`);
      }
      await db.query("UPDATE curation_review_runs SET before_state='{}' WHERE validation_status='valid'");
      await db.query("UPDATE curation_review_runs SET validation_status='applied',after_state='{}',applied_at=now() WHERE validation_status='valid'");
      await rejected('DELETE FROM curation_review_runs');await rejected("UPDATE curation_review_runs SET raw_response='{}'");
      const counts:Array<readonly [string,number]>=[];
      for(const table of protectedTables.filter(t=>t!=='memories'))counts.push([table,(await db.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n]);
      await db.query('DELETE FROM segments WHERE id=$1',[segment]);await db.query('DELETE FROM memories WHERE id=$1',[memory]);await db.query('DELETE FROM vaults WHERE id=$1',[vault]);
      for(const [table,n]of counts)expect((await db.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n).toBeGreaterThanOrEqual(n);
    }finally{await db.query('ROLLBACK');}
  });
});
