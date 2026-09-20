import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('extraction subject temporal context (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:databaseUrl,options:'-c timezone=America/Los_Angeles'});
  const vault=crypto.randomUUID();
  let db:typeof import('../db/client');
  let getVaultSubjectList:typeof import('../services/entity-resolver').getVaultSubjectList;
  beforeAll(async()=>{
    process.env.DATABASE_URL=databaseUrl;
    db=await import('../db/client');await db.runMigrations();
    (await import('../config')).getConfig().ENCRYPTION_ENABLED=false;
    // Exercise the production query construction in a deliberately non-UTC session.
    vi.spyOn(db,'query').mockImplementation((sql,values)=>pool.query(sql,values) as never);
    ({getVaultSubjectList}=await import('../services/entity-resolver'));
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'temporal-subject-test',$2,'unlimited')",[vault,crypto.randomUUID()]);
  });
  afterAll(async()=>{
    await pool.query('DELETE FROM vaults WHERE id=$1',[vault]);
    vi.restoreAllMocks();await pool.end();await db?.closePool();
  });
  it('filters before subject ranking, retaining dated facts and inclusive UTC-day behavior',async()=>{
    const expected:string[]=[];
    const types=['user_rule','user_preference','task_pattern','workflow','constraint','decision','project','system_fact','domain_knowledge'];
    for(const type of types){
      for(const [state,from,until] of [['past',null,'2026-09-17'],['future','2026-09-19',null],['today','2026-09-18','2026-09-18']]){
        const subject=`${type}-${state}`;
        await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,type,scope,status,valid_from,valid_until,confidence)
          VALUES($1,$2,$2,$3,$4,'global','active',$5::date,$6::date,0.9)`,[vault,subject,crypto.randomUUID(),type,from,until]);
        if(state==='today'||['project','system_fact','domain_knowledge'].includes(type))expected.push(subject);
      }
    }
    await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,type,scope,status,confidence)
      VALUES($1,'unknown type','unknown type',$2,NULL,'global','active',0.9)`,[vault,crypto.randomUUID()]);
    const subjects=await getVaultSubjectList(vault,100,100,'global',null,new Date('2026-09-18T00:30:00Z'));
    expect(subjects.map(s=>s.canonical).sort()).toEqual(expected.sort());
    expect((await pool.query('SELECT count(*)::int AS count FROM memories WHERE vault_id=$1',[vault])).rows[0].count).toBe(28);
  });
});
