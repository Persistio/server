import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import {Client} from 'pg';
import {afterAll,afterEach,beforeAll,beforeEach,describe,expect,it} from 'vitest';

const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('real pre-058 summary boundary upgrade',()=>{
  const database='persistio_test_summary_upgrade_'+crypto.randomUUID().replaceAll('-','');
  const vault=crypto.randomUUID(),otherVault=crypto.randomUUID();
  const dir=path.resolve(__dirname,'../../db/migrations');
  const migration=fs.readFileSync(path.join(dir,'058_extraction_summary_boundaries.sql'),'utf8');
  const down=fs.readFileSync(path.join(dir,'down/058_extraction_summary_boundaries.sql'),'utf8');
  let admin:Client,client:Client;
  let created=false;
  let legacy:Record<string,unknown>[];
  beforeAll(async()=>{
    const parsed=new URL(url!);
    if(!['localhost','127.0.0.1'].includes(parsed.hostname)||!/^\/persistio_(test|restoration)_/.test(parsed.pathname))
      throw new Error('Upgrade tests require an isolated loopback test database');
    parsed.pathname='/postgres';admin=new Client({connectionString:parsed.toString()});await admin.connect();
    await admin.query(`CREATE DATABASE ${database}`);created=true;
    parsed.pathname='/'+database;client=new Client({connectionString:parsed.toString()});await client.connect();
    await client.query('CREATE TABLE schema_migrations(filename TEXT PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())');
    await client.query("SELECT set_config('persistio.storage_embedding_dimensions','1536',false)");
    for(const filename of fs.readdirSync(dir).filter(f=>f.endsWith('.sql')&&f<'058').sort()){
      await client.query('BEGIN');
      try{
        await client.query(fs.readFileSync(path.join(dir,filename),'utf8'));
        await client.query('INSERT INTO schema_migrations(filename) VALUES($1)',[filename]);
        await client.query('COMMIT');
      }catch(error){await client.query('ROLLBACK');throw error;}
    }
    for(const id of [vault,otherVault]){
      await client.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'summary upgrade',$2,'unlimited')",[id,`summary-upgrade-${id}`]);
      await client.query("INSERT INTO session_contexts(vault_id,session_id,context) VALUES($1,'shared-session',$2)",[id,`Retained legacy summary ${id}`]);
    }
    legacy=(await client.query('SELECT * FROM session_contexts ORDER BY vault_id')).rows;
  },30000);
  beforeEach(async()=>{await client.query('BEGIN');await client.query(migration);});
  afterEach(async()=>{await client?.query('ROLLBACK');});
  afterAll(async()=>{
    await client?.end();
    // Never clear or drop the supplied database; only this fixture's random DB.
    try{if(created)await admin.query(`DROP DATABASE ${database}`);}finally{await admin?.end();}
  });
  async function insert(boundary:unknown,id=vault,session='shared-session'){
    return client.query('INSERT INTO session_contexts(vault_id,session_id,context,context_boundary) VALUES($1,$2,$3,$4::jsonb)',
      [id,session,'New isolated summary',JSON.stringify(boundary)]);
  }
  async function rejects(operation:()=>Promise<unknown>,code:string,constraint?:string){
    await client.query('SAVEPOINT rejected_write');
    try{await expect(operation()).rejects.toMatchObject({code,...(constraint?{constraint}:{})});}
    finally{await client.query('ROLLBACK TO SAVEPOINT rejected_write');await client.query('RELEASE SAVEPOINT rejected_write');}
  }
  async function schema(){
    return {
      columns:(await client.query("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='session_contexts' ORDER BY ordinal_position")).rows,
      indexes:(await client.query("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename='session_contexts' ORDER BY indexname")).rows,
      constraints:(await client.query("SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid='public.session_contexts'::regclass ORDER BY conname")).rows,
    };
  }
  it('preserves every legacy field and leaves unknown boundaries SQL NULL, not [null,null]',async()=>{
    expect((await client.query('SELECT * FROM session_contexts ORDER BY vault_id')).rows)
      .toEqual(legacy.map(row=>({...row,context_boundary:null})));
    expect((await client.query('SELECT 1 FROM session_contexts WHERE context_boundary IS NULL')).rowCount).toBe(2);
    expect((await client.query("SELECT 1 FROM session_contexts WHERE context_boundary='[null,null]'::jsonb")).rowCount).toBe(0);
  });
  it('coexists across exact project/task, explicit unbound, legacy and vault boundaries',async()=>{
    const boundaries=[['project','task'],['project','other-task'],['other-project','task'],['task','project'],['project',null],[null,'task'],[null,null]];
    for(const id of [vault,otherVault]){
      for(const boundary of boundaries)await insert(boundary,id);
      const rows=(await client.query('SELECT context_boundary FROM session_contexts WHERE vault_id=$1 AND session_id=$2',[id,'shared-session'])).rows;
      expect(rows).toHaveLength(boundaries.length+1);
      expect(rows).toEqual(expect.arrayContaining([null,...boundaries].map(context_boundary=>({context_boundary}))));
    }
  });
  it('rejects exact duplicate boundaries and duplicate legacy rows, but not another session',async()=>{
    for(const boundary of [['project','task'],[null,null]]){
      await insert(boundary);
      await rejects(()=>insert(boundary),'23505','session_contexts_boundary_unique');
      await insert(boundary,vault,'different-session');
    }
    await rejects(()=>client.query("INSERT INTO session_contexts(vault_id,session_id,context) VALUES($1,'shared-session','duplicate legacy')",[vault]),
      '23505','session_contexts_legacy_unique');
  });
  it.each([
    ['JSON null',null],['object',{}],['scalar','project'],['empty tuple',[]],['short tuple',['project']],['long tuple',['project','task',null]],
    ['numeric project',[1,'task']],['boolean task',['project',true]],['nested project',[[],null]],['nested task',[null,{}]],
    ['empty project',['','task']],['empty task',['project','']],
    ['oversized ASCII project',['p'.repeat(513),null]],['oversized ASCII task',[null,'t'.repeat(513)]],
    ['oversized Unicode project',['😀'.repeat(513),null]],['oversized Unicode task',[null,'😀'.repeat(513)]],
  ])('rejects invalid boundary: %s',async(_name,boundary)=>{
    await rejects(()=>insert(boundary),'23514','session_contexts_boundary_shape');
  });
  it('rejects malformed JSON at the database boundary',async()=>{
    await rejects(()=>client.query("INSERT INTO session_contexts(vault_id,session_id,context,context_boundary) VALUES($1,'invalid-json','invalid',$2::jsonb)",
      [vault,'["project",']),'22P02');
  });
  it('indexes 512 Unicode code points in both boundary fields and the session without a btree size error',async()=>{
    // Varied astral code points prevent a repeated-character fixture hiding index-size regressions via compression.
    const identity=(offset:number)=>Array.from({length:512},(_,i)=>String.fromCodePoint(0x10000+(i*7919+offset)%0xf0000)).join('');
    const session=identity(0),boundary=[identity(1),identity(2)];
    await insert(boundary,vault,session);
    const row=(await client.query(`SELECT context_boundary,length(session_id) AS session_length,
      length(context_boundary->>0) AS project_length,length(context_boundary->>1) AS task_length
      FROM session_contexts WHERE vault_id=$1 AND session_id=$2 AND context_boundary=$3::jsonb`,[vault,session,JSON.stringify(boundary)])).rows[0];
    expect(row).toEqual({context_boundary:boundary,session_length:512,project_length:512,task_length:512});
    await rejects(()=>insert(boundary,vault,session),'23505','session_contexts_boundary_unique');
  });
  it.each(['legacy','bound','unbound'])('refuses downgrade with only %s summaries and retains schema/data',async kind=>{
    if(kind!=='legacy'){
      await client.query('DELETE FROM session_contexts');
      await insert(kind==='bound'?['project','task']:[null,null]);
    }
    const beforeSchema=await schema(),beforeRows=(await client.query('SELECT * FROM session_contexts ORDER BY id')).rows;
    await client.query('SAVEPOINT downgrade');
    await expect(client.query(down)).rejects.toThrow('Cannot roll back summary isolation with retained summaries');
    await client.query('ROLLBACK TO SAVEPOINT downgrade');
    expect(await schema()).toEqual(beforeSchema);
    expect((await client.query('SELECT * FROM session_contexts ORDER BY id')).rows).toEqual(beforeRows);
  });
  it('downgrades only after explicitly emptying the synthetic database and permits reapplying up',async()=>{
    const upgradedSchema=await schema();
    await client.query('DELETE FROM session_contexts');
    await client.query(down);
    expect((await client.query("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='session_contexts' AND column_name='context_boundary'")).rowCount).toBe(0);
    expect((await client.query('SELECT * FROM session_contexts')).rows).toEqual([]);
    const legacyInsert=()=>client.query("INSERT INTO session_contexts(vault_id,session_id,context) VALUES($1,'shared-session','downgraded summary')",[vault]);
    await legacyInsert();
    await rejects(legacyInsert,'23505','session_contexts_vault_id_session_id_key');
    await client.query(migration);
    expect(await schema()).toEqual(upgradedSchema);
    expect((await client.query('SELECT context,context_boundary FROM session_contexts')).rows)
      .toEqual([{context:'downgraded summary',context_boundary:null}]);
    await insert(['project','task']);await insert([null,null]);
    await rejects(()=>insert(['project','task']),'23505','session_contexts_boundary_unique');
    expect((await client.query('SELECT 1 FROM session_contexts')).rowCount).toBe(3);
  });
});
