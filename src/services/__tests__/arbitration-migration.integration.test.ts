import crypto from 'node:crypto';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {Pool} from 'pg';
import {afterAll,beforeAll,describe,expect,it,vi} from 'vitest';
const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('arbitration migration supported histories (PostgreSQL production runner)',()=>{
  const admin=new Pool({connectionString:url});
  const name='persistio_test_arbitration_'+crypto.randomBytes(6).toString('hex');
  let db:typeof import('../../db/client'),pool:Pool;
  const down=()=>readFileSync(resolve(__dirname,'../../db/migrations/down/060_conflict_arbitration_attempts.sql'),'utf8');
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
  it('runs fresh migrations, preserves populated 059 data and fails closed on retained history downgrade',async()=>{
    expect((await pool.query("SELECT filename FROM schema_migrations WHERE filename='060_conflict_arbitration_attempts.sql'")).rowCount).toBe(1);
    const v=crypto.randomUUID(),a=crypto.randomUUID(),b=crypto.randomUUID();
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'migration',$2,'unlimited')",[v,crypto.randomUUID()]);
    await pool.query("INSERT INTO memories(id,vault_id,data,subject,hash,scope,status,type) VALUES($1::uuid,$3::uuid,'Old fact','subject',$1::uuid::text,'global','active','system_fact'),($2::uuid,$3::uuid,'Other fact','subject',$2::uuid::text,'global','active','system_fact')",[a,b,v]);
    await pool.query("INSERT INTO entity_aliases(vault_id,alias,canonical,scope) VALUES($1,'preserved legacy','canonical',NULL),($1,'scoped alias','canonical','global')",[v]);
    await pool.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b) VALUES($1,$2,$3,'keep_both',1,1,1)",[v,a,b]);
    const prior=(await pool.query('SELECT id,decision,revision_a,revision_b FROM contradiction_scan_log WHERE vault_id=$1',[v])).rows;
    await pool.query('BEGIN');
    try{await pool.query(readFileSync(resolve(__dirname,'../../db/migrations/down/061_committed_arbitration_outcomes.sql'),'utf8'));await pool.query("DELETE FROM schema_migrations WHERE filename='061_committed_arbitration_outcomes.sql'");await pool.query(down());await pool.query("DELETE FROM schema_migrations WHERE filename='060_conflict_arbitration_attempts.sql'");await pool.query('COMMIT');}
    catch(error){await pool.query('ROLLBACK');throw error;}
    await db.runMigrations();await db.runMigrations();
    expect((await pool.query('SELECT id,decision,revision_a,revision_b FROM contradiction_scan_log WHERE vault_id=$1',[v])).rows).toEqual(prior);
    expect((await pool.query('SELECT alias,scope FROM entity_aliases WHERE vault_id=$1 ORDER BY alias',[v])).rows).toEqual([{alias:'preserved legacy',scope:null},{alias:'scoped alias',scope:'global'}]);
    const key='f'.repeat(64);
    const insert=(attempt:number,owner=v)=>pool.query("INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,arbitration_job_key,send_attempt) VALUES($1,$2,NULL,'incomplete',0,1,$3,$4)",[owner,a,key,attempt]);
    for(const attempt of [1,2,3])await insert(attempt);
    await expect(insert(4)).rejects.toHaveProperty('code','23514');
    await expect(insert(3)).rejects.toHaveProperty('code','23505');
    await expect(insert(1,crypto.randomUUID())).rejects.toHaveProperty('code','23503');
    await expect(pool.query('DELETE FROM contradiction_scan_log WHERE arbitration_job_key=$1',[key])).rejects.toThrow('append-only');
    await expect(pool.query("UPDATE contradiction_scan_log SET decision='keep_both' WHERE arbitration_job_key=$1",[key])).rejects.toThrow('append-only');
    const client=await pool.connect();try{await client.query('BEGIN');await expect(client.query(down())).rejects.toThrow('retained arbitration');}finally{await client.query('ROLLBACK');client.release();}
  });
});
