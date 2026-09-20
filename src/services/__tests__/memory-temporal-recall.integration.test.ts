import { Pool } from 'pg';
import { afterAll, describe, expect, it } from 'vitest';
import { BEHAVIORAL_MEMORY_TYPES, FACTUAL_MEMORY_TYPES, isMemoryTemporallyRecallable,
  memoryTemporalRecallPredicateSql } from '../memory-temporal-recall';

const databaseUrl=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!databaseUrl)('temporal recall SQL/TypeScript parity (PostgreSQL 17)',()=>{
  const pool=new Pool({connectionString:databaseUrl});
  afterAll(()=>pool.end());
  const types=[...BEHAVIORAL_MEMORY_TYPES,...FACTUAL_MEMORY_TYPES,null,'unknown'];
  const windows=[
    [null,null],['2024-02-28','2024-02-28'],['2024-03-01',null],['2024-02-29',null],
    [null,'2024-02-29'],['2024-02-29','2024-02-29'],['2024-03-01','2024-02-28'],
    ['infinity',null],[null,'-infinity'],['10000-01-01',null],['0001-01-01 BC',null]
  ];
  it.each(['Pacific/Honolulu','Asia/Tokyo','UTC'])('agrees across complete taxonomy/windows with session timezone %s',async timezone=>{
    const client=await pool.connect();
    try{
      await client.query("SELECT set_config('TimeZone',$1,false)",[timezone]);
      for(const instant of ['2024-02-29T00:00:00Z','2024-03-01T01:59:59.999+02:00','2024-03-01T00:00:00Z']){
        const fixtures=types.flatMap(type=>windows.map(([valid_from,valid_until])=>({type,valid_from,valid_until})));
        const result=await client.query(`SELECT x.ordinality,COALESCE(${memoryTemporalRecallPredicateSql('m','$2')},false) AS eligible
          FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY x(value,ordinality)
          CROSS JOIN LATERAL (SELECT x.value->>'type' AS type,(x.value->>'valid_from')::date AS valid_from,
            (x.value->>'valid_until')::date AS valid_until) m ORDER BY x.ordinality`,[JSON.stringify(fixtures),instant]);
        expect(result.rows.map(row=>row.eligible)).toEqual(fixtures.map(row=>isMemoryTemporallyRecallable(row,new Date(instant))));
      }
    }finally{client.release();}
  });
  it.each(['2024-02-30','2023-02-29','0000-01-01'])('PostgreSQL itself rejects malformed date %s; application also fails closed',async date=>{
    await expect(pool.query('SELECT $1::date',[date])).rejects.toHaveProperty('code');
    for(const type of types)expect(isMemoryTemporallyRecallable({type,valid_from:date,valid_until:null},new Date('2024-02-29'))).toBe(false);
  });
  it('keeps the request clock authoritative after a wait and across a transaction clock',async()=>{
    const client=await pool.connect();
    try{
      await client.query('BEGIN');
      await client.query("SET LOCAL TIME ZONE 'Pacific/Honolulu'");
      await client.query('SELECT pg_sleep(0.02)');
      const result=await client.query(`SELECT ${memoryTemporalRecallPredicateSql('m','$1')} AS eligible
        FROM (SELECT 'user_rule'::text AS type,DATE '2024-02-29' AS valid_from,DATE '2024-02-29' AS valid_until) m`,
      ['2024-02-29T00:00:00Z']);
      expect(result.rows).toEqual([{eligible:true}]);
    }finally{await client.query('ROLLBACK');client.release();}
  });
});
