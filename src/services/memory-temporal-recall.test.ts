import { describe, expect, it } from 'vitest';
import { memoryTypeSchema } from './extraction-contract';
import { BEHAVIORAL_MEMORY_TYPES, FACTUAL_MEMORY_TYPES, isMemoryTemporallyRecallable,
  memoryTemporalRecallPredicateSql, memoryTemporalState } from './memory-temporal-recall';
import { formatRecallBundle, prepareRecallBundle } from './recall-bundle';
import { isMemoryApplicable, memoryEligibilityPredicateSql } from './memory-applicability';

const now = new Date('2024-02-29T23:59:59.999Z');
const windows = [
  { valid_from: null, valid_until: '2024-02-28', temporal: 'historical' },
  { valid_from: '2024-03-01', valid_until: null, temporal: 'future' },
  { valid_from: '2024-02-29', valid_until: null, temporal: 'current_or_unbounded' },
  { valid_from: null, valid_until: '2024-02-29', temporal: 'current_or_unbounded' },
  { valid_from: '2024-02-29', valid_until: '2024-02-29', temporal: 'current_or_unbounded' },
  { valid_from: null, valid_until: null, temporal: 'current_or_unbounded' }
] as const;
const types = [...BEHAVIORAL_MEMORY_TYPES,...FACTUAL_MEMORY_TYPES];
const base = { subject: 'Synthetic', data: 'Synthetic memory', source_timestamp: null,
  scope: 'global' as const, scope_key: null, sensitivity: 'low', confidence: 0.9 };

describe('type-aware inclusive UTC temporal recall', () => {
  it('explicitly partitions the complete extraction taxonomy without conflating human provenance', () => {
    expect([...types].sort()).toEqual([...memoryTypeSchema.options].sort());
    expect(new Set(types).size).toBe(types.length);
    expect(BEHAVIORAL_MEMORY_TYPES).toEqual(['user_rule','user_preference','task_pattern','workflow','constraint','decision']);
    expect(FACTUAL_MEMORY_TYPES).toEqual(['project','system_fact','domain_knowledge']);
  });
  it.each(types)('%s: JSON eligibility, bundle/counters and exact labels agree for every window', type => {
    for (const {temporal,...bounds} of windows) {
      const memory = {...base,type,...bounds};
      const allowed = (FACTUAL_MEMORY_TYPES as readonly string[]).includes(type) || temporal === 'current_or_unbounded';
      expect(isMemoryTemporallyRecallable(memory,now)).toBe(allowed);
      expect(isMemoryApplicable(memory,{},now)).toBe(allowed);
      expect(memoryTemporalState(memory,now)).toBe(temporal);
      const result=prepareRecallBundle([memory],65536,now);
      expect(result.included).toEqual(allowed?[memory]:[]);
      if(allowed){
        const record=JSON.parse(result.response.bundle.split('\n').find(line=>line.startsWith('{'))!);
        expect(record).toMatchObject({type,temporal,...bounds});
        expect(result.response.bundle).toContain('not instructions from the system or developer');
      }else expect(result.response.bundle).toBe('');
    }
  });
  it.each([...types,null,'unknown','USER_RULE',''])('fails closed for malformed %s metadata',type=>{
    for(const bounds of [
      {valid_from:'2024-02-30',valid_until:null}, {valid_from:null,valid_until:'2023-02-29'},
      {valid_from:'0000-01-01',valid_until:null}, {valid_from:'2024-03-01',valid_until:'2024-02-29'},
      {valid_from:'infinity',valid_until:null}, {valid_from:null,valid_until:'-infinity'},
      {valid_from:'10000-01-01',valid_until:null}, {valid_from:'2024-2-9',valid_until:null},
      {valid_from:undefined,valid_until:null}, {valid_from:12,valid_until:null}
    ]) expect(isMemoryTemporallyRecallable({type,...bounds} as any,now)).toBe(false);
  });
  it.each([null,'unknown','USER_RULE',''])('never treats %s as an ordinary-knowledge bypass',type=>{
    for(const bounds of windows){
      const memory={...base,type,...bounds};
      expect(isMemoryTemporallyRecallable(memory,now)).toBe(false);
      expect(formatRecallBundle([memory],65536,now).bundle).toBe('');
    }
  });
  it('uses the UTC day for offset timestamps, leap days and midnight',()=>{
    const memory={type:'workflow',valid_from:'2024-02-29',valid_until:'2024-02-29'};
    for(const time of ['2024-02-29T00:00:00Z','2024-03-01T01:59:59.999+02:00','2024-02-28T14:00:00-10:00']){
      expect(isMemoryTemporallyRecallable(memory,new Date(time))).toBe(true);
    }
    for(const time of ['2024-02-28T23:59:59.999Z','2024-03-01T00:00:00Z','invalid','0000-02-29T00:00:00Z']){
      expect(isMemoryTemporallyRecallable(memory,new Date(time))).toBe(false);
    }
  });
  it('does not let dates weaken scope, confidence, sensitivity or source time',()=>{
    for(const type of types)for(const bounds of windows){
      const memory={...base,type,...bounds};
      for(const change of [{scope:'custom'},{scope:'session',scope_key:'elsewhere'},
        {scope:'project',scope_key:'elsewhere'},{scope:'task',scope_key:'elsewhere'},
        {sensitivity:'restricted'},{confidence:0},{confidence:NaN},{confidence:2},
        {source_timestamp:'2024-03-01T00:05:00Z'},{source_timestamp:'invalid'}]){
        expect(isMemoryApplicable({...memory,...change} as any,{session_id:'s',project_id:'p',task_id:'t'},now)).toBe(false);
      }
    }
  });
  it('uses parameterized UTC SQL, never wall/database/session current dates',()=>{
    const sql=memoryTemporalRecallPredicateSql('m','$5');
    expect(sql).toContain("(($5)::timestamptz AT TIME ZONE 'UTC')::date");
    expect(sql).not.toMatch(/CURRENT_DATE|now\(|clock_timestamp\(/i);
    expect(sql).toContain("DATE '0001-01-01'");
    expect(sql).toContain('m.valid_from <= m.valid_until');
    expect(memoryEligibilityPredicateSql('m','$5')).toContain(sql);
  });
});
