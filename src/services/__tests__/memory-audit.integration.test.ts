import crypto from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Client } from 'pg';
import { beforeAll,afterAll,describe,it,expect } from 'vitest';
import {readGlobalAudit,readDeliveryAudit,quarantineAuditedRules} from '../../../../../scripts/lib/memory-observability-audit.mjs';
const execute=promisify(execFile);
const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
const script=new URL('../../../../../scripts/audit-memory-observability.mjs',import.meta.url).pathname;
describe.skipIf(!databaseUrl)('operator audit and quarantine against current authority (PostgreSQL)',()=>{
  const db=new Client({connectionString:databaseUrl});const vault=crypto.randomUUID(),other=crypto.randomUUID();
  const ids:Record<string,string>={};let delivery:string;
  const options={vaultId:vault,actorId:'fixture-operator',reason:'fixture quarantine'};
  const human={actor_type:'human',authorship:'original',trigger_type:'direct',artifact_type:'message',cadence:'one_off',source_class:'thread_conversation'};
  beforeAll(async()=>{
    await db.connect();for(const id of [vault,other])await db.query('INSERT INTO vaults(id,name,api_key_hash) VALUES($1,\'audit-fixture\',$2)',[id,crypto.randomUUID()]);
    for(const kind of ['manual','unapproved','revoked','state-only','human','forged','invalid','missing']){
      const id=crypto.randomUUID();ids[kind]=id;let chunks:string[]=[];
      if(['human','forged','invalid','missing'].includes(kind)){
        const chunk=crypto.randomUUID();chunks=[chunk];
        await db.query("INSERT INTO raw_chunks(id,vault_id,session_id,role,content,provenance) VALUES($1,$2,'audit','user','not emitted',$3::jsonb)",
          [chunk,vault,JSON.stringify(kind==='invalid'?{bad:true}:kind==='forged'?{...human,transport:{initiator_actor_type:'agent',receiver_actor_type:'agent'},payload_author:{actor_type:'agent',authorship:'generated',is_user:false}}:human)]);
      }
      await db.query("INSERT INTO memories(id,vault_id,data,subject,hash,type,scope,status,source_chunks) VALUES($1,$2,'not emitted','not emitted',$3,'user_rule','global','active',$4)",[id,vault,crypto.randomUUID(),chunks]);
      if(kind!=='unapproved')await db.query("UPDATE memories SET authority_state='approved' WHERE id=$1",[id]);
      if(!['unapproved','state-only'].includes(kind))await db.query("INSERT INTO memory_authority_events(vault_id,memory_id,event_type,new_state,new_version,actor_type,source,reason) SELECT vault_id,id,'approve','approved',authority_version,'user','api','fixture' FROM memories WHERE id=$1",[id]);
      if(kind==='revoked')await db.query("INSERT INTO memory_authority_events(vault_id,memory_id,event_type,new_state,new_version,actor_type,source,reason) SELECT vault_id,id,'revoke','revoked',authority_version,'user','api','fixture' FROM memories WHERE id=$1",[id]);
      if(kind==='missing')await db.query('DELETE FROM raw_chunks WHERE id=ANY($1::uuid[])',[chunks]);
    }
    delivery=crypto.randomUUID();await db.query(`INSERT INTO memory_delivery_runs(id,vault_id,query_hash,response_format,mode,top_k,min_similarity,global_rule_policy,
      include_global_rules_requested,include_global_rules_effective,selected_count,global_selected_count,created_at)
      VALUES($1,$2,$3,'json','agent',1,0.5,'off',false,false,0,0,clock_timestamp()-interval '6 minutes')`,[delivery,vault,'a'.repeat(64)]);
  });
  afterAll(async()=>{await db.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[[vault,other]]);await db.end();});
  async function cli(args:string[]){
    try{const result=await execute(process.execPath,[script,...args],{env:{...process.env,DATABASE_URL:databaseUrl},maxBuffer:2*1024*1024});return{code:0,stdout:result.stdout};}
    catch(error:any){return{code:Number(error.code),stdout:error.stdout};}
  }
  it('distinguishes current approval, event-only revocation, nested provenance and unknown lineage',async()=>{
    const rows=await readGlobalAudit(db,vault);const byId=new Map(rows.map((r:any)=>[r.id,r]));
    for(const kind of ['manual','human','missing'])expect(byId.get(ids[kind])).toMatchObject({suspicious:false});
    for(const kind of ['unapproved','revoked','state-only','forged','invalid'])expect(byId.get(ids[kind])).toMatchObject({suspicious:true});
    for(const kind of ['manual','missing'])expect(byId.get(ids[kind])).toMatchObject({lineage_unknown:true});
    expect(await readGlobalAudit(db,other)).toEqual([]);
    expect(await readDeliveryAudit(db,vault)).toMatchObject({unacknowledged_after_five_minutes:1,pending_projection_mismatches:0});
  });
  it('scopes every detail read, retains deleted-vault evidence and verifies CLI exit codes',async()=>{
    const mismatch=await cli(['--vault-id',other,'--memory-id',ids.manual,'--delivery-id',delivery]);expect(mismatch.code).toBe(0);
    expect(JSON.parse(mismatch.stdout)).toMatchObject({active_global_rules:[],memory_mutations:[],memory_deliveries:[],delivery:null,delivery_events:[],delivery_acknowledgement:null});
    const own=await cli(['--vault-id',vault,'--memory-id',ids.manual,'--delivery-id',delivery,'--fail-on-suspicious']);expect(own.code).toBe(3);
    const report=JSON.parse(own.stdout);expect(report.memory_mutations.length).toBeGreaterThan(0);expect(report.delivery.id).toBe(delivery);expect(own.stdout).not.toContain('not emitted');
    expect((await cli(['--quarantine-suspicious'])).code).toBe(2);expect((await cli(['--vault-id','bad'])).code).toBe(2);
    await db.query('BEGIN');
    try{await db.query('DELETE FROM vaults WHERE id=$1',[vault]);expect((await readDeliveryAudit(db,vault)).unacknowledged_after_five_minutes).toBe(1);}
    finally{await db.query('ROLLBACK');}
  });
  it.each(['version','authority','provenance'])('aborts the entire stale %s target set',async kind=>{
    const selected=(await readGlobalAudit(db,vault,[ids.unapproved,ids.forged])).filter((r:any)=>r.suspicious);
    if(kind==='version')await db.query("UPDATE memories SET data=data||' changed' WHERE id=$1",[ids.unapproved]);
    if(kind==='authority')await db.query("INSERT INTO memory_authority_events(vault_id,memory_id,event_type,new_state,new_version,actor_type,source,reason) SELECT vault_id,id,'revoke','revoked',authority_version,'user','api','new evidence' FROM memories WHERE id=$1",[ids.unapproved]);
    if(kind==='provenance')await db.query("UPDATE raw_chunks SET provenance=jsonb_set(provenance,'{source_class}','\"agent_other\"') WHERE id=ANY((SELECT source_chunks FROM memories WHERE id=$1)::uuid[])",[ids.forged]);
    await expect(quarantineAuditedRules(db,options,selected)).rejects.toThrow(/evidence changed/);
    expect((await db.query('SELECT count(*)::int AS n FROM memories WHERE id=ANY($1::uuid[]) AND archived_at IS NOT NULL',[[ids.unapproved,ids.forged]])).rows[0].n).toBe(0);
  });
  it('rolls back mutation and authority together on failure, then quarantines only the unchanged audited set',async()=>{
    const selected=await readGlobalAudit(db,vault,[ids.unapproved,ids.forged]);
    const failing={query:async(sql:string,params?:unknown[])=>{
      if(sql.startsWith('COMMIT'))throw Error('injected commit failure');return db.query(sql,params);
    }};
    await expect(quarantineAuditedRules(failing,options,selected)).rejects.toThrow(/injected/);
    expect((await db.query('SELECT count(*)::int AS n FROM memories WHERE id=ANY($1::uuid[]) AND archived_at IS NOT NULL',[[ids.unapproved,ids.forged]])).rows[0].n).toBe(0);
    expect((await quarantineAuditedRules(db,options,selected)).sort()).toEqual([ids.unapproved,ids.forged].sort());
    expect((await db.query("SELECT count(*)::int AS n FROM memory_authority_events WHERE actor_id=$1 AND reason=$2 AND vault_id=$3",[options.actorId,options.reason,vault])).rows[0].n).toBe(2);
    expect((await readGlobalAudit(db,vault,[ids.manual]))[0]).toMatchObject({suspicious:false});
  });
  it('independently catches derived-projection loss',async()=>{
    await db.query('BEGIN');
    try{
      // Deliberate owner-level corruption, rolled back; never a runtime repair.
      await db.query('ALTER TABLE memory_delivery_pending DISABLE TRIGGER memory_delivery_pending_guard');
      await db.query('DELETE FROM memory_delivery_pending WHERE delivery_id=$1',[delivery]);
      expect((await readDeliveryAudit(db,vault)).pending_projection_mismatches).toBe(1);
    }finally{await db.query('ROLLBACK');}
  });
  it('independently validates empty ACK shape even if an owner bypassed the insertion guard',async()=>{
    await db.query('BEGIN');
    try{
      await db.query('ALTER TABLE memory_delivery_acknowledgements DISABLE TRIGGER memory_delivery_ack_reference_guard');
      await db.query(`INSERT INTO memory_delivery_acknowledgements(delivery_id,vault_id,outcome,origin)
        VALUES($1,$2,$3::jsonb,'client_ack')`,[delivery,vault,JSON.stringify({rendered_ids:[crypto.randomUUID()],dropped:[],token_budget:0,rendered_tokens:0,truncated:false,render_target:'tool_response'})]);
      expect((await readDeliveryAudit(db,vault)).invalid_partitions).toBe(1);
    }finally{await db.query('ROLLBACK');}
  });
  it('prevents concurrent source or event-only approval changes during locked quarantine',async()=>{
    const selected=await readGlobalAudit(db,vault,[ids['state-only']]);
    let reached!:()=>void,resume!:()=>void;
    const paused=new Promise<void>(resolve=>{reached=resolve;}),release=new Promise<void>(resolve=>{resume=resolve;});
    const wrapped={query:async(sql:string,params?:unknown[])=>{
      const result=await db.query(sql,params);
      if(sql.startsWith('SELECT m.id,m.vault_id')){reached();await release;}return result;
    }};
    const operation=quarantineAuditedRules(wrapped,options,selected);await paused;
    const writer=new Client({connectionString:databaseUrl});await writer.connect();await writer.query("SET lock_timeout='100ms'");
    try{
      await expect(writer.query("UPDATE raw_chunks SET provenance=provenance WHERE vault_id=$1",[vault])).rejects.toMatchObject({code:'55P03'});
      await expect(writer.query("INSERT INTO memory_authority_events(vault_id,memory_id,event_type,new_state,new_version,actor_type,source,reason) SELECT vault_id,id,'approve','approved',authority_version,'user','api','concurrent fixture' FROM memories WHERE id=$1",[ids['state-only']])).rejects.toMatchObject({code:'55P03'});
    }finally{resume();await operation;await writer.end();}
  });
});
