import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import {Client} from 'pg';
import {afterAll,afterEach,beforeAll,beforeEach,describe,expect,it} from 'vitest';

const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('real pre-057 restoration upgrade',()=>{
  const database='persistio_test_upgrade_'+crypto.randomUUID().replaceAll('-','');
  const vault=crypto.randomUUID();
  let admin:Client,client:Client;
  let created=false;
  const dir=path.resolve(__dirname,'../../db/migrations');
  const migration=fs.readFileSync(path.join(dir,'057_restore_platform_pipeline.sql'),'utf8');
  beforeAll(async()=>{
    const parsed=new URL(url!);
    if(!['localhost','127.0.0.1'].includes(parsed.hostname)||!/^\/persistio_(test|restoration)_/.test(parsed.pathname))
      throw new Error('Upgrade tests require an isolated loopback test database');
    parsed.pathname='/postgres';admin=new Client({connectionString:parsed.toString()});await admin.connect();
    await admin.query(`CREATE DATABASE ${database}`);created=true;
    parsed.pathname='/'+database;client=new Client({connectionString:parsed.toString()});await client.connect();
    await client.query('CREATE TABLE schema_migrations(filename TEXT PRIMARY KEY,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())');
    await client.query("SELECT set_config('persistio.storage_embedding_dimensions','1536',false)");
    for(const filename of fs.readdirSync(dir).filter(f=>f.endsWith('.sql')&&f<'057').sort()){
      await client.query('BEGIN');
      try{
        await client.query(fs.readFileSync(path.join(dir,filename),'utf8'));
        await client.query('INSERT INTO schema_migrations(filename) VALUES($1)',[filename]);
        await client.query('COMMIT');
      }catch(error){await client.query('ROLLBACK');throw error;}
    }
    await client.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'preserved control plane','preserved-key','unlimited')",[vault]);
  },30000);
  beforeEach(async()=>{await client.query('BEGIN');});
  afterEach(async()=>{await client?.query('ROLLBACK');});
  afterAll(async()=>{
    await client?.end();
    // Only the random database created above belongs to this fixture.
    if(created)await admin.query(`DROP DATABASE ${database}`);
    await admin?.end();
  });
  async function refuses(){
    await client.query('SAVEPOINT replacement');
    await expect(client.query(migration)).rejects.toThrow('approved empty memory domain');
    await client.query('ROLLBACK TO SAVEPOINT replacement');
    expect((await client.query("SELECT column_name FROM information_schema.columns WHERE table_name='memories' AND column_name='authority_state'")).rowCount).toBe(1);
    expect((await client.query("SELECT 1 FROM schema_migrations WHERE filename LIKE '057%'")).rowCount).toBe(0);
    expect((await client.query('SELECT api_key_hash,plan_id FROM vaults WHERE id=$1',[vault])).rows[0])
      .toEqual({api_key_hash:'preserved-key',plan_id:'unlimited'});
  }
  it('refuses a session-summary-only domain without deleting its content or changing schema',async()=>{
    await client.query("INSERT INTO session_contexts(vault_id,session_id,context) VALUES($1,'old-session','Old auxiliary summary')",[vault]);
    await refuses();
    expect((await client.query('SELECT context FROM session_contexts WHERE vault_id=$1',[vault])).rows)
      .toEqual([{context:'Old auxiliary summary'}]);
  });
  it.each(['memories','raw_chunks','segments','entity_aliases'] as const)('refuses independent live input in %s',async table=>{
    if(table==='memories')await client.query("INSERT INTO memories(vault_id,data,subject,hash,scope,scope_key,status) VALUES($1,'fact','topic','hash','session','s1','superseded')",[vault]);
    if(table==='raw_chunks')await client.query("INSERT INTO raw_chunks(vault_id,session_id,role,blob_store,blob_key,storage_bytes) VALUES($1,'s1','user','local','old-object',1)",[vault]);
    if(table==='segments')await client.query("INSERT INTO segments(vault_id,session_id) VALUES($1,'s1')",[vault]);
    if(table==='entity_aliases')await client.query("INSERT INTO entity_aliases(vault_id,alias,canonical) VALUES($1,'old','older')",[vault]);
    await refuses();expect((await client.query(`SELECT 1 FROM ${table} WHERE vault_id=$1`,[vault])).rowCount).toBe(1);
  });
  it('upgrades a clean domain while preserving control plane and inert worker receipts',async()=>{
    const queue=crypto.randomUUID();
    await client.query("INSERT INTO worker_action_receipts(queue_kind,queue_id,action_key,claim_token) VALUES('extraction',$1,'dead-letter',$2)",[queue,crypto.randomUUID()]);
    await client.query(migration);
    expect((await client.query("SELECT column_name FROM information_schema.columns WHERE table_name='memories' AND column_name='revision'")).rowCount).toBe(1);
    expect((await client.query("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'memory_delivery_%'")).rows).toEqual([]);
    expect((await client.query('SELECT queue_id FROM worker_action_receipts WHERE queue_id=$1',[queue])).rowCount).toBe(1);
    expect((await client.query('SELECT api_key_hash FROM vaults WHERE id=$1',[vault])).rows[0].api_key_hash).toBe('preserved-key');
  });
  it('locks session summaries against concurrent insertion for the entire replacement transaction',async()=>{
    await client.query(migration);
    const parsed=new URL(url!);parsed.pathname='/'+database;
    const other=new Client({connectionString:parsed.toString()});await other.connect();
    try{
      await other.query("SET lock_timeout='100ms'");
      await expect(other.query("INSERT INTO session_contexts(vault_id,session_id,context) VALUES($1,'late','must wait')",[vault]))
        .rejects.toMatchObject({code:'55P03'});
    }finally{await other.end();}
  });
});
