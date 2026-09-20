import crypto from 'node:crypto';
import {Pool,Client} from 'pg';
import {afterAll,afterEach,beforeAll,describe,expect,it,vi} from 'vitest';
const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('bounded scheduler-metadata reconciliation (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:url}),vaults:string[]=[];
  let db:typeof import('../../db/client');
  beforeAll(async()=>{process.env.DATABASE_URL=url;db=await import('../../db/client');await db.runMigrations();});
  afterEach(async()=>{vi.restoreAllMocks();await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;});
  afterAll(async()=>{await pool.end();await db?.closePool();});
  async function fixture(count=70){
    const v=crypto.randomUUID();vaults.push(v);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'schedule-repair',$2,'unlimited')",[v,crypto.randomUUID()]);
    await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,type,scope,status)
      SELECT $1,'Preserved factual knowledge','topic',n::text,'system_fact','global','active' FROM generate_series(1,$2) n`,[v,count]);
    return v;
  }
  it('repairs missing work in finite idempotent pages without touching customer data or retained outcomes',async()=>{
    const v=await fixture(),ids=(await pool.query('SELECT id FROM memories WHERE vault_id=$1 ORDER BY id',[v])).rows.map(r=>r.id);
    await pool.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b) VALUES($1,$2,$3,'keep_both',1,1,1)",[v,ids[0],ids[1]]);
    await pool.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,arbitration_job_key,send_attempt) VALUES($1,$2,NULL,'incomplete',0,1,$3,1)",[v,ids[0],'f'.repeat(64)]);
    await pool.query("INSERT INTO entity_aliases(vault_id,alias,canonical,scope) VALUES($1,'preserved','topic','global')",[v]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1',[v]);
    // Old scheduler alone loses this historical work: no provider call or repair.
    const config=(await import('../../config')).getConfig();config.CONTRADICTION_SCAN_ENABLED=true;
    const extractor={arbitrateConflict:vi.fn()};await (await import('../contradiction-activation')).drainDueContradictionActivations(extractor as never);
    expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect((await pool.query('SELECT 1 FROM memory_contradiction_schedule WHERE vault_id=$1',[v])).rowCount).toBe(0);
    const tables=['memories','contradiction_scan_log','entity_aliases','vault_usage','memory_mutation_events'];
    const snapshot=()=>Promise.all(tables.map(t=>pool.query(`SELECT to_jsonb(t) AS row FROM ${t} t WHERE vault_id=$1 ORDER BY to_jsonb(t)::text`,[v]).then(r=>r.rows)));
    const before=await snapshot();
    const {reconcileContradictionSchedulePage:repair}=await import('../contradiction-reconciliation');
    const first=await repair({vaultId:v});expect(first.examined).toBe(64);expect(first.scheduled).toBe(64);expect(first.done).toBe(false);
    const second=await repair({vaultId:v,after:first.nextAfter,through:first.through});
    expect(second.examined).toBe(6);expect(second.scheduled).toBe(6);expect(second.done).toBe(true);
    const scheduled=(await pool.query('SELECT * FROM memory_contradiction_schedule WHERE vault_id=$1 ORDER BY memory_id',[v])).rows;
    expect((await repair({vaultId:v,through:first.through})).scheduled).toBe(0);
    expect((await pool.query('SELECT * FROM memory_contradiction_schedule WHERE vault_id=$1 ORDER BY memory_id',[v])).rows).toEqual(scheduled);
    expect(await snapshot()).toEqual(before);expect(extractor.arbitrateConflict).not.toHaveBeenCalled();
    expect((await pool.query('SELECT pending_count::int FROM memory_contradiction_pending_vaults WHERE vault_id=$1',[v])).rows[0].pending_count).toBe(70);
  });
  it('preserves current cursors/future reminders, rejects active drains and never crosses a vault',async()=>{
    const v=await fixture(2),other=await fixture(1);
    const ids=(await pool.query('SELECT id FROM memories WHERE vault_id=$1 ORDER BY id',[v])).rows.map(r=>r.id);
    await pool.query("UPDATE memories SET type='workflow',valid_from='2099-01-01' WHERE id=$1",[ids[0]]);
    await pool.query(`UPDATE memory_contradiction_schedule SET available_at='2099-01-01',failures=3,
      scan_reference_date='2026-09-20',scan_similarity=1,scan_memory_id=$2 WHERE memory_id=$1`,[ids[0],ids[1]]);
    const before=(await pool.query('SELECT * FROM memory_contradiction_schedule ORDER BY memory_id')).rows;
    const {reconcileContradictionSchedulePage:repair}=await import('../contradiction-reconciliation');
    const owner=new Client({connectionString:url});await owner.connect();
    try{await owner.query('SELECT pg_advisory_lock(hashtextextended($1,0))',['persistio:contradiction-activation:'+v]);
      await expect(repair({vaultId:v})).rejects.toThrow('Contradiction scheduler is active');
    }finally{await owner.end();}
    expect((await repair({vaultId:v})).scheduled).toBe(0);
    expect((await pool.query('SELECT * FROM memory_contradiction_schedule ORDER BY memory_id')).rows).toEqual(before);
    expect((await pool.query('SELECT count(*)::int AS n FROM memory_contradiction_schedule WHERE vault_id=$1',[other])).rows[0].n).toBe(1);
  });
});
