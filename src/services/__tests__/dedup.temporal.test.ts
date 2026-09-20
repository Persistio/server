// This unit suite isolates selection/timing; real evidence and receipt forgery are covered by arbitration-*.integration.test.ts.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DedupInput, DedupOptions } from '../dedup';

const mocks = vi.hoisted(() => ({ reserve: vi.fn() }));
vi.mock('../../db/client', () => ({ query: vi.fn(() => { throw new Error('Unexpected global DB access'); }), withTransaction: vi.fn() }));
vi.mock('../crypto', () => ({ isVaultEncryptionActive: (v: {vault_encryption_enabled:boolean}) => v.vault_encryption_enabled }));
vi.mock('../entity-resolver', () => ({ normaliseSubject: (s:string) => s, resolveCanonical: async () => null }));
vi.mock('../usage', () => ({ reserveMemoryCreationInTransaction: mocks.reserve }));
vi.mock('../worker-effects', () => ({ publishCommittedWorkerEffects: vi.fn() }));
vi.mock('../extractor', () => ({ ExtractorService: class {},validatedConflictAssessment:(decision:unknown)=>typeof decision==='string'?{decision,evidence:{}}:undefined }));
vi.mock('../conflict-evidence',async original=>({...await original() as object,prepareConflictEvidence:async()=>({pairId:'test'}),conflictStillCurrent:async()=>true}));
import { deduplicateMemoryInTransaction, fingerprintDedupInput, getDedupEscalationRequest } from '../dedup';

const NOW = new Date('2026-09-18T00:30:00Z');
const input = (extra:Partial<DedupInput> = {}):DedupInput => ({
  vaultId:'vault', fact:'Use the new workflow.', subject:'topic', embedding:[1,0], sourceChunks:['fixture-source'],
  score:8, salience:0.8, sensitivity:'low', type:'workflow', scope:'session', scopeKey:'s1',
  polarity:'neutral', status:'active', volatility:'low', validFrom:null, validUntil:null, ...extra
});
const row = (extra:Record<string,unknown> = {}) => ({
  id:'current', data:'Use the existing workflow.', type:'workflow', valid_from:null as string|null,
  valid_until:null as string|null, polarity:'neutral', source_timestamp:null, source_chunks:['fixture-source'],
  sensitivity:'low', similarity:0.85, row_version:'1', ...extra
});
type Row = ReturnType<typeof row>;
// Independent oracle for the mocked SQL boundary, not the production policy.
function eligible(r:Row, date:string) {
  return ['project','system_fact','domain_knowledge'].includes(r.type)
    || (['user_rule','user_preference','task_pattern','workflow','constraint','decision'].includes(r.type)
      && (!r.valid_from || r.valid_from <= date) && (!r.valid_until || r.valid_until >= date));
}
function fixture(rows:Row[], encrypted = false) {
  const prepared = {
    assertCurrent:vi.fn(async () => {}), encrypt:(_v:unknown,s:string) => s,
    decrypt:vi.fn((_v:unknown,s:string) => { if(s === 'poison') throw new Error('Excluded ciphertext decoded'); return s; }),
    subject:() => null, subjectMatch:(_v:unknown,s:string) => encrypted ? 'subject-hmac' : s
  };
  const db = { query:vi.fn(async (sql:string, values:unknown[] = []) => {
    if(sql.includes('FROM vaults')) return {rowCount:1,rows:[{id:'vault',vault_encryption_enabled:encrypted,encrypted_dek:encrypted?'wrapped':null}]};
    if(sql.includes('FROM memories m WHERE')) {
      let matches = rows;
      if(sql.includes("AT TIME ZONE 'UTC'")) {
        matches = matches.filter(r => eligible(r,String(values[9]).slice(0,10)) && r.similarity >= 0.8);
        if(sql.includes('m.id=$11::uuid')) matches = matches.filter(r => r.id === values[10]);
      }
      matches = matches.slice(0,sql.includes('LIMIT 1') && !sql.includes('LIMIT 10') ? 1 : 10);
      return {rowCount:matches.length,rows:matches};
    }
    if(sql.includes('FROM raw_chunks')) {
      const ids = values[1] as string[];
      return {rowCount:ids.length,rows:ids.map(id => ({id}))};
    }
    if(sql.includes('FOR UPDATE')) {
      const target = rows.find(r => r.id === values[0]);
      return {rowCount:target?1:0,rows:target?[{row_version:target.row_version}]:[]};
    }
    if(sql.includes('INSERT INTO memories')) return {rowCount:1,rows:[{id:'inserted'}]};
    return {rowCount:1,rows:[]};
  }) };
  return { db, prepared, rows };
}
const options = (request:NonNullable<Awaited<ReturnType<typeof getDedupEscalationRequest>>>, decision:DedupOptions['precomputedConflictDecision'] = 'merge'):DedupOptions => ({
  precomputedConflictDecision:decision, precomputedConflictMemoryId:request.existingMemoryId,
  precomputedConflictMemoryRevision:request.existingMemoryRevision, precomputedConflictInput:request.inputFingerprint
});
const selects = (f:ReturnType<typeof fixture>) => f.db.query.mock.calls.filter(([sql]) => sql.includes('FROM memories m WHERE'));
const updates = (f:ReturnType<typeof fixture>) => f.db.query.mock.calls.filter(([sql]) => sql.startsWith('UPDATE memories'));

describe('dedup arbitration selection independent of storage matching', () => {
  beforeEach(() => { vi.useFakeTimers({toFake:['Date']}); vi.setSystemTime(NOW); mocks.reserve.mockResolvedValue({}); });
  afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

  it.each(['expired','future'] as const)('selects eligible related below more than the storage cap of %s rows', async kind => {
    const excluded = Array.from({length:14},(_,i) => row({id:'excluded-'+i,similarity:0.99,
      ...(kind === 'expired' ? {valid_until:'2026-09-17'} : {valid_from:'2026-09-19'})}));
    const f = fixture([...excluded,row({id:'best',similarity:0.9}),row({id:'runner-up',similarity:0.85})]);
    const request = await getDedupEscalationRequest(input(),'test',f.db as never,f.prepared,NOW);
    expect(request).toMatchObject({existingMemoryId:'best',existingMemoryRevision:'1'});
    const queries = selects(f);
    expect(queries).toHaveLength(2);
    expect(queries[0][0]).toContain('LIMIT 10');
    expect(queries[0][0]).not.toContain("AT TIME ZONE 'UTC'");
    expect(queries[1][0]).toContain('LIMIT 1');
    expect(queries[1][0].indexOf("AT TIME ZONE 'UTC'")).toBeLessThan(queries[1][0].indexOf('ORDER BY'));
    expect(queries[1][1]?.[9]).toBe(NOW.toISOString());
    expect(queries[1][0]).not.toMatch(/current_date|now\(\)/);
  });

  it.each(['merge','discard_new','supersede_old'] as const)('applies %s to the prepared eligible target, never the storage leader', async decision => {
    const f = fixture([row({id:'expired',valid_until:'2020-01-01',similarity:1}),row()]);
    const request = await getDedupEscalationRequest(input(),'test',f.db as never,f.prepared,NOW);
    expect(request?.existingMemoryId).toBe('current');
    // A newly higher-ranked eligible row must not redirect or hide the decision.
    f.rows.splice(1,0,row({id:'new-leader',similarity:0.95}));
    const result = await deduplicateMemoryInTransaction(input(),f.db as never,f.prepared,[],options(request!,decision));
    expect(result).toEqual(decision === 'supersede_old' ? {action:'inserted',memoryId:'inserted'} : {action:'updated',memoryId:'current'});
    expect(updates(f)).toHaveLength(1);
    expect(updates(f)[0][1]?.[0]).toBe('current');
    expect(f.db.query.mock.calls.find(([sql]) => sql.includes('FOR UPDATE'))?.[1]?.[0]).toBe('current');
    expect(selects(f).at(-1)?.[0]).toContain('m.id=$11::uuid');
  });

  it.each(['type','window','polarity','revision','incoming','expired','deleted'] as const)('rejects prepared mutation after %s change', async change => {
    const f = fixture([row({id:'expired',valid_until:'2020-01-01',similarity:1}),row()]);
    let value = input();
    const request = await getDedupEscalationRequest(value,'test',f.db as never,f.prepared,NOW);
    expect(request).not.toBeNull();
    const target = f.rows[1];
    if(change === 'type') target.type = 'system_fact';
    if(change === 'window') target.valid_from = '2026-09-01';
    if(change === 'polarity') target.polarity = 'negative';
    if(change === 'revision') target.row_version = '2';
    if(change === 'incoming') value = input({fact:'A different new workflow.'});
    if(change === 'expired') target.valid_until = '2026-09-17';
    if(change === 'deleted') f.rows.pop();
    expect((await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[],options(request!,'supersede_old'))).action).toBe('inserted');
    expect(updates(f)).toEqual([]);
  });

  it('rechecks temporal eligibility when arbitration crosses UTC midnight', async () => {
    const value = input({validUntil:'2026-09-18'}), f = fixture([row({valid_until:'2026-09-18'})]);
    const request = await getDedupEscalationRequest(value,'test',f.db as never,f.prepared,NOW);
    expect(request).not.toBeNull();
    vi.setSystemTime(new Date('2026-09-19T00:00:00Z'));
    await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[],options(request!,'supersede_old'));
    expect(updates(f)).toEqual([]);
  });

  it('leaves no-decision storage matching and historical exact retries untouched', async () => {
    const f = fixture([row({id:'expired',valid_until:'2020-01-01',similarity:1}),row()]);
    await deduplicateMemoryInTransaction(input(),f.db as never,f.prepared,[]);
    expect(selects(f)).toHaveLength(1);
    const log = f.db.query.mock.calls.find(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'));
    expect(log?.[1]?.[1]).toBe('expired');
    const historical = input({validUntil:'2020-01-01'});
    const exact = fixture([row({id:'old-exact',valid_until:'2020-01-01',data:historical.fact})]);
    expect(await getDedupEscalationRequest(historical,'test',exact.db as never,exact.prepared,NOW)).toBeNull();
    expect(exact.db.query).not.toHaveBeenCalled();
    expect(await deduplicateMemoryInTransaction(historical,exact.db as never,exact.prepared,[])).toEqual({action:'skipped',memoryId:'old-exact'});
  });

  it.each([false,true].flatMap(encrypted =>
    ['user_rule','user_preference','task_pattern','workflow','constraint','decision'].map(type => ({encrypted,type}))))
  ('inserts future same-metadata $type without certifying pending arbitration (encrypted=$encrypted)', async ({encrypted,type}) => {
    const value = input({type:type as DedupInput['type'],validFrom:'2026-09-19',validUntil:'2026-09-19',
      sourceChunks:['source'],evidence:JSON.stringify({quote:'Use the new workflow.'})});
    const f = fixture([row({type,valid_from:value.validFrom,valid_until:value.validUntil})],encrypted);
    expect(await getDedupEscalationRequest(value,'test',f.db as never,f.prepared,NOW)).toBeNull();
    expect(f.db.query).not.toHaveBeenCalled();
    const effects:import('../worker-effects').WorkerEffect[] = [];
    expect(await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,effects)).toEqual({action:'inserted',memoryId:'inserted'});
    expect(f.db.query.mock.calls.filter(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'))).toEqual([]);
    expect(updates(f)).toEqual([]);
    expect(mocks.reserve).toHaveBeenCalledExactlyOnceWith(f.db,value.vaultId);
    expect(effects.map(e => e.kind)).toEqual(['quota','memory-count']);
    const inserted = f.db.query.mock.calls.find(([sql]) => sql.includes('INSERT INTO memories'))!;
    expect(inserted[1]?.[1]).toBe(value.fact);
    expect(inserted[1]?.[7]).toEqual(value.sourceChunks);
    expect(JSON.parse(inserted[1]?.[17] as string)).toMatchObject({quote:'Use the new workflow.'});
    expect(inserted[1]?.slice(18,20)).toEqual([value.validFrom,value.validUntil]);
    expect(f.db.query.mock.calls.some(([sql]) => sql.includes('INSERT INTO memory_embeddings'))).toBe(true);
  });

  it.each([
    {name:'different type',incoming:{},existing:{type:'constraint'}},
    {name:'different start',incoming:{},existing:{valid_from:'2026-09-20'}},
    {name:'different end',incoming:{},existing:{valid_until:'2026-09-21'}},
    {name:'different polarity',incoming:{},existing:{polarity:'negative'}},
    ...['project','system_fact','domain_knowledge',null].map(type =>
      ({name:'non-behavioral '+type,incoming:{type},existing:{type}})),
    {name:'historical behavior',incoming:{validFrom:'2026-09-16',validUntil:'2026-09-17'},
      existing:{valid_from:'2026-09-16',valid_until:'2026-09-17'}},
    {name:'inclusive current behavior',incoming:{validFrom:'2026-09-18'},existing:{valid_from:'2026-09-18'}}
  ])('records incomplete assessment for $name', async ({incoming,existing}) => {
    const value = input({validFrom:'2026-09-19',...incoming} as Partial<DedupInput>);
    const f = fixture([row({valid_from:'2026-09-19',...existing})]);
    expect((await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[])).action).toBe('inserted');
    const logs = f.db.query.mock.calls.filter(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'));
    expect(logs).toHaveLength(1); expect(logs[0][1]?.slice(1,4)).toEqual(['current','inserted','incomplete']);
    expect(updates(f)).toEqual([]);
  });

  it('does not let an inapplicable prepared decision certify a future pair', async () => {
    const value = input({validFrom:'2026-09-19'}), f = fixture([row({valid_from:'2026-09-19'})]);
    // Even a fingerprint/revision-bound decision from another instant cannot
    // apply before this behavioral pair's window is current.
    const {fingerprintDedupInput} = await import('../dedup');
    await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[],{
      precomputedConflictDecision:'supersede_old',precomputedConflictMemoryId:'current',
      precomputedConflictMemoryRevision:'1',precomputedConflictInput:fingerprintDedupInput(value)
    });
    expect(updates(f)).toEqual([]);
    expect(f.db.query.mock.calls.some(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'))).toBe(false);
  });

  it('preserves future exact retries but admits distinct evidence separately without a false pair log', async () => {
    const value = input({validFrom:'2026-09-19'});
    const f = fixture([row({data:value.fact,valid_from:value.validFrom})]);
    expect(await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[])).toEqual({action:'skipped',memoryId:'current'});
    expect(await deduplicateMemoryInTransaction({...value,sourceChunks:['new-source']},f.db as never,f.prepared,[]))
      .toEqual({action:'inserted',memoryId:'inserted'});
    expect(updates(f)).toHaveLength(0);
    expect(mocks.reserve).toHaveBeenCalledOnce();
    expect(f.db.query.mock.calls.some(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'))).toBe(false);
  });

  it.each(['user_rule','user_preference','task_pattern','workflow','constraint','decision'] as const)
  ('preserves future %s preflight intent across UTC midnight without relaxing apply fences', async type => {
    const value = input({type,validFrom:'2026-09-19',validUntil:'2026-09-19'});
    const f = fixture([row({type,valid_from:value.validFrom,valid_until:value.validUntil})]);
    const preflight = {inputFingerprint:fingerprintDedupInput(value),referenceTime:NOW.toISOString()};
    expect(await getDedupEscalationRequest(value,'test',f.db as never,f.prepared,NOW)).toBeNull();
    vi.setSystemTime(new Date('2026-09-19T00:00:00Z'));
    await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[],{preflight});
    expect(f.db.query.mock.calls.filter(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'))).toEqual([]);
    expect(updates(f)).toEqual([]);
    expect(mocks.reserve).toHaveBeenCalledOnce();
  });

  it.each(['fingerprint','invalid-time','current','historical','different-metadata'] as const)
  ('does not extend the future-log exception for %s preflight controls', async control => {
    const value = input({validFrom:'2026-09-19',validUntil:'2026-09-19'});
    const f = fixture([row({valid_from:value.validFrom,valid_until:value.validUntil,
      ...(control === 'different-metadata' ? {polarity:'negative'} : {})})]);
    const preflight = {inputFingerprint:control === 'fingerprint' ? 'wrong' : fingerprintDedupInput(value),
      referenceTime:control === 'invalid-time' ? 'invalid' : control === 'current' ? '2026-09-19T00:00:00Z'
        : control === 'historical' ? '2026-09-20T00:00:00Z' : NOW.toISOString()};
    vi.setSystemTime(new Date('2026-09-19T00:00:00Z'));
    await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[],{preflight});
    expect(f.db.query.mock.calls.filter(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'))).toHaveLength(1);
  });

  it('does not use the retained preflight clock to authorize an expired prepared decision', async () => {
    const value = input({validUntil:'2026-09-18'}), f = fixture([row({valid_until:value.validUntil})]);
    const request = await getDedupEscalationRequest(value,'test',f.db as never,f.prepared,NOW);
    const preflight = {inputFingerprint:fingerprintDedupInput(value),referenceTime:NOW.toISOString()};
    vi.setSystemTime(new Date('2026-09-19T00:00:00Z'));
    await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[],{...options(request!,'supersede_old'),preflight});
    expect(updates(f)).toEqual([]);
    expect(f.db.query.mock.calls.filter(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'))).toHaveLength(1);
  });

  it('logs an applicable keep_both decision on the inclusive activation date', async () => {
    const value = input({validFrom:'2026-09-19'}), f = fixture([row({valid_from:value.validFrom})]);
    vi.setSystemTime(new Date('2026-09-19T00:00:00Z'));
    const request = await getDedupEscalationRequest(value,'test',f.db as never,f.prepared);
    expect(request).not.toBeNull();
    await deduplicateMemoryInTransaction(value,f.db as never,f.prepared,[],options(request!,'keep_both'));
    expect(f.db.query.mock.calls.filter(([sql]) => sql.includes('INSERT INTO contradiction_scan_log'))).toHaveLength(1);
  });

  it('gives exact storage precedence over an already prepared lower related decision', async () => {
    const f = fixture([row({id:'expired',valid_until:'2020-01-01',similarity:1}),row()]);
    const request = await getDedupEscalationRequest(input(),'test',f.db as never,f.prepared,NOW);
    expect(request).not.toBeNull();
    f.rows.unshift(row({id:'exact',data:input().fact}));
    f.db.query.mockClear();
    expect(await deduplicateMemoryInTransaction(input(),f.db as never,f.prepared,[],options(request!,'supersede_old'))).toEqual({action:'skipped',memoryId:'exact'});
    expect(selects(f)).toHaveLength(1);
    expect(updates(f)).toEqual([]);
    expect(await getDedupEscalationRequest(input(),'test',f.db as never,f.prepared,NOW)).toBeNull();
  });

  it.each(['project','system_fact','domain_knowledge'] as const)('keeps historical/future %s eligible rather than using a blanket date filter', async type => {
    for(const dates of [{valid_from:null,valid_until:'2020-01-01'},{valid_from:'2099-01-01',valid_until:null}]) {
      const f = fixture([row({id:'behavior',valid_until:'2020-01-01',similarity:1}),row({type,...dates})]);
      expect(await getDedupEscalationRequest(input(),'test',f.db as never,f.prepared,NOW)).toMatchObject({existingMemoryId:'current'});
    }
  });

  it('uses encrypted subject matching and never decodes excluded temporal rows for arbitration', async () => {
    const f = fixture([row({id:'expired',valid_until:'2020-01-01',similarity:1,data:'poison'}),row()],true);
    expect(await getDedupEscalationRequest(input(),'test',f.db as never,f.prepared,NOW)).toMatchObject({existingFact:'Use the existing workflow.'});
    expect(f.prepared.decrypt.mock.calls.every(([,s]) => s !== 'poison')).toBe(true);
    for(const [sql,values] of selects(f)) {
      expect(sql).toContain('m.subject_hmac=$5'); expect(values?.[4]).toBe('subject-hmac');
      expect(sql).toContain('m.scope_key IS NOT DISTINCT FROM $3::text');
      expect(sql).toContain("m.status='active'"); expect(sql).toContain('m.archived_at IS NULL');
      expect(sql).toContain('m.valid_until >= $8::date');
    }
  });
});
