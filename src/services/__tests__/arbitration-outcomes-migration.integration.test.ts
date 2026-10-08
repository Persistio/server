import crypto from 'node:crypto';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {Pool} from 'pg';
import {afterAll,beforeAll,describe,expect,it,vi} from 'vitest';
const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('committed outcomes migration histories (production runner)',()=>{
  const admin=new Pool({connectionString:url});
  const name='persistio_test_outcomes_'+crypto.randomBytes(6).toString('hex');
  let db:typeof import('../../db/client'),pool:Pool;
  const filename='061_committed_arbitration_outcomes.sql';
  const down=()=>readFileSync(resolve(__dirname,'../../db/migrations/down',filename),'utf8');
  beforeAll(async()=>{
    if(!/test/.test(new URL(url!).pathname))throw new Error('Explicit test database required');
    await admin.query(`CREATE DATABASE "${name}"`);
    const isolated=new URL(url!);isolated.pathname='/'+name;
    process.env.DATABASE_URL=isolated.toString();vi.resetModules();
    db=await import('../../db/client');pool=new Pool({connectionString:isolated.toString()});await db.runMigrations();
  });
  afterAll(async()=>{
    await pool?.end();await db?.closePool();await admin.query(`DROP DATABASE IF EXISTS "${name}"`);await admin.end();process.env.DATABASE_URL=url;
  });
  it('preserves populated 060 data and attempts, permits only truthful null-B outcomes and refuses lossy downgrade',async()=>{
    expect((await pool.query('SELECT filename FROM schema_migrations WHERE filename=$1',[filename])).rowCount).toBe(1);
    const v=crypto.randomUUID(),foreign=crypto.randomUUID(),a=crypto.randomUUID(),b=crypto.randomUUID();
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) SELECT id,'outcome-migration',id::text,'unlimited' FROM unnest($1::uuid[]) id",[[v,foreign]]);
    await pool.query("INSERT INTO memories(id,vault_id,data,subject,hash,scope,status,type) SELECT id,$2,'Stored fact','topic',id::text,'global','active','system_fact' FROM unnest($1::uuid[]) id",[[a,b],v]);
    await pool.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b) VALUES($1,$2,$3,'keep_both',1,1,1)",[v,a,b]);
    await pool.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,arbitration_job_key,send_attempt) VALUES($1,$2,NULL,'incomplete',0,1,$3,1)",[v,a,'f'.repeat(64)]);
    await pool.query('BEGIN');
    try{await pool.query(down());await pool.query('DELETE FROM schema_migrations WHERE filename=$1',[filename]);await pool.query('COMMIT');}
    catch(error){await pool.query('ROLLBACK');throw error;}
    const before=(await pool.query('SELECT id,decision,arbitration_job_key,send_attempt FROM contradiction_scan_log WHERE vault_id=$1 ORDER BY id',[v])).rows;
    const memories=(await pool.query('SELECT * FROM memories WHERE vault_id=$1 ORDER BY id',[v])).rows;
    // Upgrading populated 060 needs no new schema for Part 7-3 repair. Simulate
    // historical reminder loss and use the same explicit metadata-only entry point.
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1',[v]);
    await db.runMigrations();await db.runMigrations();
    const schema=(await pool.query('SELECT * FROM schema_migrations ORDER BY filename')).rows;
    const {reconcileContradictionSchedulePage:repair}=await import('../contradiction-reconciliation');
    expect(await repair({vaultId:foreign})).toMatchObject({examined:0,scheduled:0,done:true});
    expect(await repair({vaultId:v})).toMatchObject({examined:2,scheduled:2,done:true});
    expect(await repair({vaultId:v})).toMatchObject({scheduled:0,done:true});
    expect((await pool.query('SELECT * FROM schema_migrations ORDER BY filename')).rows).toEqual(schema);
    expect(schema.at(-1).filename).toBe('066_production_customer_metrics.sql');
    expect((await pool.query('SELECT id,decision,arbitration_job_key,send_attempt FROM contradiction_scan_log WHERE vault_id=$1 ORDER BY id',[v])).rows).toEqual(before);
    expect((await pool.query('SELECT * FROM memories WHERE vault_id=$1 ORDER BY id',[v])).rows).toEqual(memories);
    const insert=(key:string,decision='merge',owner=v)=>pool.query('INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,dedup_input_key) VALUES($1,$2,NULL,$3,1,1,$4)',[owner,a,decision,key]);
    await insert('a'.repeat(64));await insert('b'.repeat(64),'discard_new');
    await expect(insert('a'.repeat(64))).rejects.toHaveProperty('code','23505');
    await expect(insert('c'.repeat(64),'keep_both')).rejects.toHaveProperty('code','23514');
    await expect(insert('d'.repeat(64),'supersede_old')).rejects.toHaveProperty('code','23514');
    await expect(insert('bad')).rejects.toHaveProperty('code','23514');
    // Even a normal two-memory shape cannot turn an incomplete assessment into
    // a permanent consumption key. Test the exact previously admitted shape.
    await expect(pool.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b,dedup_input_key) VALUES($1,$2,$3,'incomplete',1,1,1,$4)",[v,a,b,'c'.repeat(64)]))
      .rejects.toHaveProperty('code','23514');
    await expect(insert('e'.repeat(64),'merge',foreign)).rejects.toHaveProperty('code','23503');
    await expect(pool.query('DELETE FROM contradiction_scan_log WHERE dedup_input_key IS NOT NULL')).rejects.toThrow('append-only');
    await expect(pool.query("UPDATE contradiction_scan_log SET decision='discard_new' WHERE dedup_input_key IS NOT NULL")).rejects.toThrow('append-only');
    const client=await pool.connect();try{await client.query('BEGIN');await expect(client.query(down())).rejects.toThrow('retained committed arbitration');}finally{await client.query('ROLLBACK');client.release();}
    expect((await pool.query('SELECT count(*)::int AS n FROM contradiction_scan_log WHERE dedup_input_key IS NOT NULL')).rows[0].n).toBe(2);
  });
});
