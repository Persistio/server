import { beforeEach, describe, expect, it, vi } from 'vitest';
const mocks=vi.hoisted(()=>({query:vi.fn(),scan:vi.fn()}));
vi.mock('pg',()=>({Client:class {query=mocks.query;connect=vi.fn();end=vi.fn();on=vi.fn();}}));
vi.mock('../db/client',()=>({query:vi.fn(),withTransaction:vi.fn()}));
vi.mock('../config',()=>({getConfig:()=>({CONTRADICTION_SCAN_ENABLED:true,CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH:4})}));
vi.mock('./contradiction-scanner',()=>({scanForContradictions:mocks.scan}));
import { drainDueContradictionActivations } from './contradiction-activation';
const memory={memory_id:'memory',generation:'generation',revision:'7',failures:0,
  reference_now:new Date('2026-09-18T00:01:00Z'),scan_reference_date:'2026-09-18',scan_similarity:0.9,scan_memory_id:'consumed'};
const cursor={revision:'7',referenceDate:'2026-09-18',similarity:0.85,memoryId:'next-consumed'};
describe('durable contradiction progress scheduling',()=>{
  beforeEach(()=>{
    vi.clearAllMocks();
    mocks.query.mockImplementation(async(sql:string)=>{
      if(sql.includes('SELECT vault_id, revision::text'))return{rows:[{vault_id:'vault',revision:'3'}]};
      if(sql.includes('pg_try_advisory_lock'))return{rows:[{locked:true}]};
      if(sql.includes('SELECT memory_id, generation, failures'))return{rows:[memory]};
      return{rows:[],rowCount:1};
    });
    mocks.scan.mockResolvedValue({completedMemoryIds:[],deferredMemoryIds:['memory'],cursors:{memory:cursor}});
  });
  it('passes stored revision/date/key and database instant, then saves the next consumed key with generation/revision/UTC fences',async()=>{
    await drainDueContradictionActivations({} as never);
    expect(mocks.scan.mock.calls[0][3]).toMatchObject({now:memory.reference_now,maxArbitrations:1,cursors:{memory:{
      revision:'7',referenceDate:'2026-09-18',similarity:0.9,memoryId:'consumed'
    }}});
    const [sql,params]=mocks.query.mock.calls.find(([sql])=>sql.includes('SET available_at = now()'))!;
    expect(sql).toContain('generation = $2 AND revision = $5::bigint');
    expect(sql).toContain("(statement_timestamp() AT TIME ZONE 'UTC')::date = $6::date");
    expect(params).toEqual(['memory','generation',60000,false,'7','2026-09-18',0.85,'next-consumed']);
    expect(mocks.query.mock.calls.some(([sql])=>sql.includes('pg_advisory_unlock'))).toBe(true);
  });
  it('clears progress when the scanner cannot certify a current revision',async()=>{
    mocks.scan.mockResolvedValue({completedMemoryIds:[],deferredMemoryIds:['memory']});
    await drainDueContradictionActivations({} as never);
    expect(mocks.query.mock.calls.find(([sql])=>sql.includes('SET available_at = now()'))![1])
      .toEqual(['memory','generation',60000,false,'7',null,null,null]);
  });
  it('never saves a cursor carrying another revision',async()=>{
    mocks.scan.mockResolvedValue({completedMemoryIds:[],deferredMemoryIds:['memory'],cursors:{memory:{...cursor,revision:'8'}}});
    await drainDueContradictionActivations({} as never);
    expect(mocks.query.mock.calls.find(([sql])=>sql.includes('SET available_at = now()'))![1].slice(5)).toEqual([null,null,null]);
  });
  it('preserves the previous durable cursor on technical failure and backs off',async()=>{
    mocks.scan.mockRejectedValueOnce(Error('Temporary failure'));
    await drainDueContradictionActivations({} as never);
    const [sql,params]=mocks.query.mock.calls.find(([sql])=>sql.includes('SET available_at = now()'))!;
    expect(params).toEqual(['memory','generation',60000,true,'7',null,null,null]);
    expect(sql).toContain('CASE WHEN $4 THEN scan_reference_date');
    expect(sql).toContain('CASE WHEN $4 THEN scan_similarity');expect(sql).toContain('CASE WHEN $4 THEN scan_memory_id');
  });
  it('guards completion against generation, revision, and a UTC midnight crossing',async()=>{
    mocks.scan.mockResolvedValue({completedMemoryIds:['memory'],deferredMemoryIds:[]});
    await drainDueContradictionActivations({} as never);
    const [sql,params]=mocks.query.mock.calls.find(([sql])=>sql.startsWith('DELETE FROM memory_contradiction_schedule'))!;
    expect(params).toEqual(['memory','generation','7','2026-09-18']);
    expect(sql).toContain('revision = $3::bigint');
    expect(sql).toContain("(statement_timestamp() AT TIME ZONE 'UTC')::date = $4::date");
  });
  it('reschedules an exhausted future scan at UTC valid_from and clears every cursor field under the same fences',async()=>{
    mocks.scan.mockResolvedValue({completedMemoryIds:[],deferredMemoryIds:['memory'],reactivations:{memory:{
      revision:'7',referenceDate:'2026-09-18',validFrom:'2099-01-01'
    }}});
    await drainDueContradictionActivations({} as never);
    const [sql,params]=mocks.query.mock.calls.find(([sql])=>sql.includes('SET available_at = $5::date'))!;
    expect(params).toEqual(['memory','generation','7','2026-09-18','2099-01-01']);
    expect(sql).toContain("$5::date::timestamp AT TIME ZONE 'UTC'");
    expect(sql).toContain('generation = $2 AND revision = $3::bigint');
    expect(sql).toContain("(statement_timestamp() AT TIME ZONE 'UTC')::date = $4::date");
    expect(sql).toContain('scan_reference_date = NULL, scan_similarity = NULL, scan_memory_id = NULL');
    expect(mocks.query.mock.calls.some(([sql])=>sql.startsWith('DELETE FROM memory_contradiction_schedule'))).toBe(false);
  });
  it('does not delay a replacement revision using a stale future result',async()=>{
    mocks.scan.mockResolvedValue({completedMemoryIds:[],deferredMemoryIds:['memory'],reactivations:{memory:{
      revision:'8',referenceDate:'2026-09-18',validFrom:'2099-01-01'
    }}});
    await drainDueContradictionActivations({} as never);
    expect(mocks.query.mock.calls.some(([sql])=>sql.includes('SET available_at = $5::date'))).toBe(false);
    expect(mocks.query.mock.calls.find(([sql])=>sql.includes('SET available_at = now()'))![1])
      .toEqual(['memory','generation',60000,false,'7',null,null,null]);
  });
});
