import crypto from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {Pool} from 'pg';
import {beforeAll,afterAll,describe,it,expect} from 'vitest';
import {readMemoryAudit} from '../../../../../scripts/lib/memory-observability-audit.mjs';
const run=promisify(execFile),url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('read-only aggregate memory audit (PostgreSQL)',()=>{
  const pool=new Pool({connectionString:url}),vaults=[crypto.randomUUID(),crypto.randomUUID()];
  beforeAll(async()=>{
    for(const vault of vaults)await pool.query("INSERT INTO vaults(id,name,api_key_hash) VALUES($1,'aggregate-audit',$2)",[vault,crypto.randomUUID()]);
    for(const [index,vault] of vaults.entries())await pool.query(`INSERT INTO memories(vault_id,data,subject,hash,status,scope,scope_key)
      SELECT $1,'PRIVATE memory sentinel','PRIVATE subject sentinel',gen_random_uuid()::text,'active','session','PRIVATE binding sentinel'
      FROM generate_series(1,$2::integer)`,[vault,index+1]);
  });
  afterAll(async()=>{await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);await pool.end();});
  it('reports only owned aggregate state, mutation counts and queued work, never per-memory content/identity',async()=>{
    const report=await readMemoryAudit(pool,vaults[0]);
    expect(report.memory_state).toEqual([{status:'active',scope:'session',count:1,archived:0}]);
    expect(report.invalid_bindings_or_dates).toBe(0);expect(report.mutation_counts).toEqual([{event_type:'create',count:1}]);
    expect(report.queued_work).toEqual([{kind:'extraction',pending:0},{kind:'curation',pending:0}]);
    expect(JSON.stringify(report)).not.toContain('PRIVATE');expect(JSON.stringify(report)).not.toContain(vaults[0]);
  });
  it('CLI is read-only, returns aggregate JSON and rejects removed quarantine/ACK arguments',async()=>{
    const cli=new URL('../../../../../scripts/audit-memory-observability.mjs',import.meta.url).pathname;
    const before=(await pool.query('SELECT id,revision::text,status FROM memories WHERE vault_id=$1',[vaults[0]])).rows;
    const output=await run(process.execPath,[cli,'--vault-id',vaults[0],'--fail-on-invalid'],{env:{...process.env,DATABASE_URL:url}});
    expect(JSON.parse(output.stdout).memory_state[0].count).toBe(1);expect(output.stdout+output.stderr).not.toContain('PRIVATE');
    expect((await pool.query('SELECT id,revision::text,status FROM memories WHERE vault_id=$1',[vaults[0]])).rows).toEqual(before);
    for(const args of [['--apply-quarantine'],['--delivery-id',crypto.randomUUID()],['--vault-id','not-uuid']]){
      await expect(run(process.execPath,[cli,...args],{env:{...process.env,DATABASE_URL:url}})).rejects.toMatchObject({code:2});
    }
  });
  it('retains mutation evidence after vault deletion without retaining a live review queue',async()=>{
    await pool.query('DELETE FROM vaults WHERE id=$1',[vaults[1]]);
    const report=await readMemoryAudit(pool,vaults[1]);expect(report.memory_state).toEqual([]);
    expect(report.mutation_counts).toEqual([{event_type:'create',count:2},{event_type:'delete',count:2}]);
    expect(report.queued_work.every((row:{pending:number})=>row.pending===0)).toBe(true);
  });
});
