import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import http from 'node:http';
import {Pool} from 'pg';
import {afterAll,afterEach,beforeAll,beforeEach,describe,expect,it,vi} from 'vitest';
import type {DedupInput} from '../dedup';
import {CONFLICT_BOUNDS as B} from '../conflict-evidence';
const url=process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('complete arbitration evidence and durable dispatch (PostgreSQL + real SDK)',()=>{
  const pool=new Pool({connectionString:url}),vaults:string[]=[],keys:string[]=[];
  let db:typeof import('../../db/client'),dedup:typeof import('../dedup'),evidence:typeof import('../conflict-evidence'),
    model:typeof import('../extractor'),storage:ReturnType<typeof import('../raw-chunk-storage').getRawChunkStorage>,config:ReturnType<typeof import('../../config').getConfig>;
  let vector:number[],requests:any[]=[],mode='supersede_old',hook:(()=>Promise<void>)|undefined;
  const server=http.createServer(async(req,res)=>{
    const chunks:Buffer[]=[];for await(const chunk of req)chunks.push(chunk);
    const body=JSON.parse(Buffer.concat(chunks).toString());requests.push(body);
    const pair=JSON.parse(body.messages[1].content);await hook?.();
    if(mode==='timeout'){req.socket.destroy();return;}
    if(mode==='503'){res.writeHead(503);res.end('{"error":{"message":"unavailable"}}');return;}
    const answer:any={pairId:pair.pairId,decision:['merge','discard_new','supersede_old','keep_both'].includes(mode)?mode:'merge',
      citationsA:[{sourceId:pair.A.sources[0].id,quote:pair.A.sources[0].text.slice(0,64).trim()}],
      citationsB:[{sourceId:pair.B.sources[0].id,quote:pair.B.sources[0].text.slice(0,64).trim()}]};
    if(mode==='wrong-pair')answer.pairId='0'.repeat(64);
    if(mode==='wrong-side')answer.citationsA=answer.citationsB;
    if(mode==='missing-citation')answer.citationsB=[];
    if(mode==='unknown')answer.decision='approve';
    if(mode==='extra')answer.untrusted='instruction';
    if(mode==='duplicate')answer.citationsA.push(answer.citationsA[0]);
    if(mode==='whitespace')answer.citationsA[0].quote=' ';
    if(mode==='punctuation')answer.citationsA[0].quote='.';
    if(mode==='one-letter')answer.citationsA[0].quote='A';
    const content=mode==='malformed'?'not json':mode==='partial'?JSON.stringify({pairId:pair.pairId}):JSON.stringify(answer);
    res.writeHead(200,{'content-type':'application/json'});
    res.end(JSON.stringify({id:'fake-local',object:'chat.completion',created:1,model:body.model,
      choices:[{index:0,finish_reason:mode==='length'?'length':'stop',message:{role:'assistant',content,
        ...(mode==='refusal'?{refusal:'No'}:{})}}]}));
  });
  beforeAll(async()=>{
    process.env.DATABASE_URL=url;db=await import('../../db/client');await db.runMigrations();
    config=(await import('../../config')).getConfig();config.ENCRYPTION_ENABLED=false;
    vector=[1,...Array(config.STORAGE_EMBEDDING_DIMENSIONS-1).fill(0)];
    storage=(await import('../raw-chunk-storage')).getRawChunkStorage();
    dedup=await import('../dedup');evidence=await import('../conflict-evidence');model=await import('../extractor');
    await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));
    config.ESCALATION_BASE_URL=`http://127.0.0.1:${(server.address() as any).port}/v1`;
    config.ESCALATION_API_KEY='local-test-not-a-credential';config.ESCALATION_MODEL='deterministic-local';
  });
  beforeEach(()=>{requests=[];mode='supersede_old';hook=undefined;});
  afterEach(async()=>{
    vi.restoreAllMocks();
    for(const key of keys)await storage.delete(key);keys.length=0;
    await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])',[vaults]);vaults.length=0;
  });
  afterAll(async()=>{await new Promise<void>(resolve=>server.close(()=>resolve()));await pool.end();await db?.closePool();});
  async function source(vaultId:string,text:string,extra:Record<string,unknown>={}){
    const id=crypto.randomUUID(),key=`vaults/${vaultId}/sessions/test/chunks/${id}.txt`;keys.push(key);
    await storage.put(key,text);
    await pool.query(`INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes,provenance)
      VALUES($1,$2,'test','user',$3,$4,$5,'{}')`,[id,vaultId,Object.hasOwn(extra,'store')?extra.store:storage.store,extra.key??key,extra.bytes??Buffer.byteLength(text)]);
    return id;
  }
  async function fixture(old='Project Atlas uses PostgreSQL.',fresh='Correction: Project Atlas, now called Beacon, uses SQLite.'){
    const vaultId=crypto.randomUUID();vaults.push(vaultId);
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id) VALUES($1,'arbitration-evidence',$2,'unlimited')",[vaultId,crypto.randomUUID()]);
    const a=await source(vaultId,old),b=await source(vaultId,fresh);
    const input:DedupInput={vaultId,fact:old,subject:'atlas',embedding:vector,sourceChunks:[a],score:8,salience:0.8,sensitivity:'low',type:'system_fact',scope:'global',scopeKey:null,polarity:'neutral',status:'active',volatility:'low',validFrom:null,validUntil:null};
    const first=await dedup.deduplicateMemory(input);
    return{input:{...input,fact:fresh,sourceChunks:[b]},a,b,first};
  }
  async function invoke(input:DedupInput){
    const request=await dedup.getDedupEscalationRequest(input,'p');expect(request).not.toBeNull();
    const assessment=await new model.ExtractorService().arbitrateConflict(request!.existingFact,request!.newFact,input.vaultId,request!.context);
    return{request:request!,assessment};
  }
  async function apply(input:DedupInput,r:Awaited<ReturnType<typeof invoke>>){
    return dedup.deduplicateMemory(input,undefined,{precomputedConflictDecision:r.assessment,precomputedConflictInput:r.request.inputFingerprint,
      precomputedConflictMemoryId:r.request.existingMemoryId,precomputedConflictMemoryRevision:r.request.existingMemoryRevision});
  }
  const active=async(v:string)=>(await pool.query("SELECT count(*)::int AS n FROM memories WHERE vault_id=$1 AND status='active'",[v])).rows[0].n;
  it.each(['503','timeout'])('applies a successful prefix despite a later %s and never resends it',async failure=>{
    const f=await fixture(),other=await source(f.input.vaultId,'Beacon previously used PostgreSQL.');
    const vectorB=[0,1,...vector.slice(2)];
    const bInput={...f.input,fact:'Beacon previously used PostgreSQL.',subject:'beacon',embedding:vectorB,sourceChunks:[other]};
    await dedup.deduplicateMemory(bInput);
    const incoming={...bInput,fact:'Beacon now uses SQLite.',sourceChunks:[await source(f.input.vaultId,'Beacon now uses SQLite.')]};
    const pairs=await Promise.all([f.input,incoming].map((input,i)=>dedup.getDedupEscalationRequest(input,String(i))));
    hook=async()=>{mode=JSON.parse(requests.at(-1).messages[1].content).B.subject==='beacon'?failure:'supersede_old';};
    const receipts=await new model.ExtractorService().arbitrateConflictsBatch(pairs as NonNullable<typeof pairs[0]>[],f.input.vaultId);
    expect(receipts.get('0')?.state).toBe('complete');expect(receipts.get('1')?.state).toBe('incomplete');
    for(let i=0;i<2;i++)await apply([f.input,incoming][i],{request:pairs[i]!,assessment:receipts.get(String(i))!});
    expect(await active(f.input.vaultId)).toBe(3);
    expect((await pool.query('SELECT status FROM memories WHERE id=$1',[f.first.memoryId])).rows[0].status).toBe('superseded');
    const sent=requests.length;
    await dedup.deduplicateMemory(f.input,new model.ExtractorService());
    await dedup.deduplicateMemory(incoming,new model.ExtractorService());
    expect(requests).toHaveLength(sent);
    expect((await pool.query('SELECT send_attempt FROM contradiction_scan_log WHERE vault_id=$1 AND send_attempt IS NOT NULL',[f.input.vaultId])).rows).toEqual([{send_attempt:1},{send_attempt:1}]);
  });
  it('rejects crypto identity drift even when the apply caller prepares a new context',async()=>{
    const f=await fixture(),r=await invoke(f.input);
    await pool.query("UPDATE vaults SET encrypted_dek='changed-prepared-identity' WHERE id=$1",[f.input.vaultId]);
    await apply(f.input,r);expect(await active(f.input.vaultId)).toBe(2);
  });
  it('permits a complete source-supported correction/rename, with provider work outside locks',async()=>{
    const f=await fixture();
    hook=async()=>{const client=await pool.connect();try{await client.query('BEGIN');await client.query('SELECT id FROM vaults WHERE id=$1 FOR UPDATE NOWAIT',[f.input.vaultId]);await client.query('SELECT id FROM memories WHERE vault_id=$1 FOR UPDATE NOWAIT',[f.input.vaultId]);}finally{await client.query('ROLLBACK');client.release();}};
    const r=await invoke(f.input);expect(r.assessment.state).toBe('complete');await apply(f.input,r);
    expect(await active(f.input.vaultId)).toBe(1);expect(requests).toHaveLength(1);
    expect(JSON.parse(requests[0].messages[1].content).B.sources[0].text).toContain('now called Beacon');
  });
  it.each(['merge','discard_new','keep_both'])('preserves the ordinary validated %s decision',async verdict=>{
    mode=verdict;const f=await fixture();const r=await invoke(f.input);await apply(f.input,r);
    expect(await active(f.input.vaultId)).toBe(verdict==='keep_both'?2:1);
  });
  it.each(['wrong-pair','wrong-side','missing-citation','unknown','extra','duplicate','malformed','partial','length','refusal','whitespace','punctuation','one-letter'])('rejects %s without an authorizing verdict',async failure=>{
    mode=failure;const f=await fixture();const r=await invoke(f.input);
    expect(r.assessment.state).toBe('incomplete');await apply(f.input,r);expect(await active(f.input.vaultId)).toBe(2);
    const logs=(await pool.query('SELECT decision FROM contradiction_scan_log WHERE vault_id=$1',[f.input.vaultId])).rows;
    expect(logs.every(row=>row.decision==='incomplete')).toBe(true);
  });
  it.each(['spread','swap','substantive','source-delete'])('cannot apply a %s receipt',async change=>{
    const f=await fixture(),r=await invoke(f.input);
    if(change==='spread')r.assessment={...r.assessment};
    if(change==='swap'){f.input.fact=r.request.existingFact;r.request.inputFingerprint=dedup.fingerprintDedupInput(f.input);}
    if(change==='substantive')await pool.query("UPDATE memories SET data='Changed concurrently' WHERE id=$1",[f.first.memoryId]);
    if(change==='source-delete')await pool.query('DELETE FROM raw_chunks WHERE id=$1',[f.a]);
    await apply(f.input,r);expect(await active(f.input.vaultId)).toBe(2);
  });
  it('invalidates a concurrent change made during provider wait',async()=>{
    const f=await fixture();hook=async()=>{await pool.query("UPDATE memories SET data='Concurrent correction' WHERE id=$1",[f.first.memoryId]);};
    const r=await invoke(f.input);await apply(f.input,r);expect(await active(f.input.vaultId)).toBe(2);
  });
  it('keeps identical wording for distinct source-supported entities despite canonical equality',async()=>{
    const f=await fixture('Entity one: the service uses PostgreSQL.','Entity two: the service uses PostgreSQL.');
    await pool.query("UPDATE memories SET data='The service uses PostgreSQL.',hash=$2 WHERE id=$1",[f.first.memoryId,crypto.createHash('sha256').update('The service uses PostgreSQL.').digest('hex')]);
    f.input.fact='The service uses PostgreSQL.';mode='keep_both';
    await dedup.deduplicateMemory(f.input,new model.ExtractorService());expect(await active(f.input.vaultId)).toBe(2);
    expect(JSON.parse(requests[0].messages[1].content).A.sources[0].text).toContain('Entity one');
  });
  it.each(['missing','foreign','unreadable','wrong-store','wrong-size','object-overflow','aggregate-overflow','manifest-overflow'])('rejects %s before dispatch',async failure=>{
    const f=await fixture();
    if(failure==='missing')f.input.sourceChunks=[];
    if(failure==='foreign'){const other=await fixture();f.input.sourceChunks=[other.a];}
    if(failure==='unreadable'){await storage.delete(keys[keys.length-1]);}
    if(failure==='wrong-store')f.input.sourceChunks=[await source(f.input.vaultId,'Source text',{store:'gcs'})];
    if(failure==='wrong-size')f.input.sourceChunks=[await source(f.input.vaultId,'Source text',{bytes:1})];
    if(failure==='object-overflow')f.input.sourceChunks=[await source(f.input.vaultId,'x'.repeat(B.objectBytes+1))];
    if(failure==='aggregate-overflow')f.input.sourceChunks=[await source(f.input.vaultId,'x'.repeat(B.decodedBytes))];
    if(failure==='manifest-overflow')f.input.sourceChunks=await Promise.all(Array.from({length:B.manifest+1},(_,i)=>source(f.input.vaultId,'Source '+i)));
    expect(await dedup.getDedupEscalationRequest(f.input,'p')).toBeNull();expect(requests).toHaveLength(0);
  });
  it('includes the final contradictory source at the manifest bound and rejects bound+1',async()=>{
    const f=await fixture();
    f.input.sourceChunks=[];for(let i=0;i<B.manifest;i++)f.input.sourceChunks.push(await source(f.input.vaultId,i===B.manifest-1?'FINAL CORRECTION: different entity.':'Observation '+i));
    mode='keep_both';await invoke(f.input);
    expect(JSON.parse(requests[0].messages[1].content).B.sources.map((s:any)=>s.text)).toContain('FINAL CORRECTION: different entity.');
    f.input.sourceChunks.push(await source(f.input.vaultId,'One beyond the finite manifest'));
    expect(await dedup.getDedupEscalationRequest(f.input,'overflow')).toBeNull();expect(requests).toHaveLength(1);
  });
  it.each([B.decodedBytes,B.decodedBytes+1])('admits aggregate decoded bytes exactly at the edge: %s',async bytes=>{
    const f=await fixture('a','b');f.input.sourceChunks=[await source(f.input.vaultId,'x'.repeat(bytes-1))];
    const r=await dedup.getDedupEscalationRequest(f.input,'p');expect(Boolean(r)).toBe(bytes===B.decodedBytes);
  });
  it.each(['503','timeout','malformed'])('caps %s sends across new service instances and records only incomplete attempts',async failure=>{
    mode=failure;const f=await fixture();
    for(let i=0;i<4;i++){
      try{await invoke(f.input);}catch(error){if(failure==='malformed')throw error;}
    }
    expect(requests).toHaveLength(3);
    const rows=(await pool.query('SELECT decision,send_attempt FROM contradiction_scan_log WHERE vault_id=$1 ORDER BY send_attempt',[f.input.vaultId])).rows;
    expect(rows).toEqual([1,2,3].map(send_attempt=>({decision:'incomplete',send_attempt})));
    expect((await pool.query('SELECT count(*)::int AS n FROM vault_model_usage WHERE vault_id=$1',[f.input.vaultId])).rows[0].n).toBe(0);
  });
  it('shares three slots across reversed pairs, embedding/counter/alias churn and concurrent workers',async()=>{
    const f=await fixture();const second=await dedup.deduplicateMemory(f.input);mode='malformed';
    const usage=await import('../usage'),prepareAdmission=usage.prepareAiBudgetAdmission;let charged=0;
    vi.spyOn(usage,'prepareAiBudgetAdmission').mockImplementation(async(...args)=>{const value=await prepareAdmission(...args);return{...value,reserve:()=>{charged++;return value.reserve();}};});
    const send=async(reverse=false)=>{
      const a=reverse?second.memoryId!:f.first.memoryId!,b=reverse?f.first.memoryId!:second.memoryId!;
      const prepared=await evidence.prepareConflictEvidence(f.input.vaultId,a,b);expect(prepared).not.toBeNull();
      const left=reverse?f.input.fact:'Project Atlas uses PostgreSQL.',right=reverse?'Project Atlas uses PostgreSQL.':f.input.fact;
      const meta={sourceTimestamp:null,validFrom:null,validUntil:null,createdAt:null};
      return new model.ExtractorService().arbitrateConflict(left,right,f.input.vaultId,{existing:meta,incoming:meta,evidence:prepared!});
    };
    await Promise.all([send(),send(true)]);
    await pool.query('UPDATE memories SET embedding=$2::vector,recall_count=recall_count+1,subject=$3 WHERE id=$1',[f.first.memoryId,JSON.stringify(vector),'new canonical alias']);
    await Promise.all([send(true),send(),send(true)]);
    expect(requests).toHaveLength(3);expect(charged).toBe(3);
  });
  it('keeps reconsideration finite across scheduler restarts and embedding revisions with the same three-send allowance',async()=>{
    const f=await fixture(),second=await dedup.deduplicateMemory(f.input);mode='keep_both';
    const v=f.input.vaultId;
    config.CONTRADICTION_SCAN_ENABLED=true;config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH=4;
    await pool.query(`INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b)
      SELECT $1,a.id,b.id,'keep_both',1,a.revision,b.revision FROM memories a,memories b WHERE a.id=$2 AND b.id=$3`,[v,f.first.memoryId,second.memoryId]);
    await pool.query('DELETE FROM memory_contradiction_schedule WHERE vault_id=$1',[v]);
    const {drainDueContradictionActivations:drain}=await import('../contradiction-activation');
    for(let pass=0;pass<4;pass++){
      await pool.query('UPDATE memories SET embedding=$2::vector WHERE id=$1',[second.memoryId,JSON.stringify([0,pass+1,...vector.slice(2)])]);
      await pool.query("UPDATE memory_contradiction_pending_vaults SET next_visit_at=now()-interval '1 second' WHERE vault_id=$1",[v]);
      await drain(new model.ExtractorService());
      expect(requests).toHaveLength(Math.min(pass+1,3));
      expect((await pool.query('SELECT 1 FROM memory_contradiction_schedule WHERE vault_id=$1',[v])).rowCount).toBe(0);
    }
    expect((await pool.query('SELECT send_attempt FROM contradiction_scan_log WHERE vault_id=$1 AND send_attempt IS NOT NULL ORDER BY send_attempt',[v])).rows)
      .toEqual([{send_attempt:1},{send_attempt:2},{send_attempt:3}]);
    expect(await active(v)).toBe(2);
  });
  it.each(['capacity','budget','circuit'])('consumes zero durable send slots for local %s rejection',async failure=>{
    const f=await fixture();
    const usage=await import('../usage');
    if(failure==='capacity')vi.spyOn(usage,'prepareAiBudgetAdmission').mockResolvedValue({fitsCapacity:false,reserve:vi.fn()});
    if(failure==='budget')vi.spyOn(usage,'prepareAiBudgetAdmission').mockResolvedValue({fitsCapacity:true,reserve:()=>{throw new Error('Local budget rejection');}});
    if(failure==='circuit')vi.spyOn((await import('../ai-resilience')).ServiceCircuitBreaker.prototype,'beforeRequest').mockImplementation(()=>{throw new Error('Local circuit rejection');});
    try{const result=await invoke(f.input);expect(result.assessment.state).toBe('incomplete');}catch(error){if(failure==='capacity')throw error;}
    expect(requests).toHaveLength(0);
    expect((await pool.query('SELECT count(*)::int AS n FROM contradiction_scan_log WHERE vault_id=$1 AND send_attempt IS NOT NULL',[f.input.vaultId])).rows[0].n).toBe(0);
  });
  it('rejects a serialized request that cannot fit without truncating evidence',async()=>{
    const f=await fixture();const original=config.LLM_SYSTEM_PROMPT_PREFIX;
    config.LLM_SYSTEM_PROMPT_PREFIX='x'.repeat(B.requestBytes);
    try{expect((await invoke(f.input)).assessment.state).toBe('incomplete');}
    finally{config.LLM_SYSTEM_PROMPT_PREFIX=original;}
    expect(requests).toHaveLength(0);
    expect((await pool.query('SELECT count(*)::int AS n FROM contradiction_scan_log WHERE vault_id=$1',[f.input.vaultId])).rows[0].n).toBe(0);
  });
  it.each(['sensitivity','sources','vault'])('cannot rebind a receipt by forging public %s fingerprints',async change=>{
    const f=await fixture(),r=await invoke(f.input);
    if(change==='sensitivity')f.input.sensitivity='high';
    if(change==='sources')f.input.sourceChunks=[await source(f.input.vaultId,'A different source snapshot')];
    if(change==='vault'){const other=await fixture();f.input.vaultId=other.input.vaultId;f.input.sourceChunks=other.input.sourceChunks;}
    r.request.inputFingerprint=dedup.fingerprintDedupInput(f.input);
    await apply(f.input,r);expect(await active(f.input.vaultId)).toBe(2);
  });
  it('preserves one allowance across canonical alias rebinding and incoming-to-stored conversion',async()=>{
    const f=await fixture();mode='malformed';await invoke(f.input);await invoke(f.input);
    await pool.query("INSERT INTO entity_aliases(vault_id,alias,canonical,scope,scope_key) VALUES($1,'atlas','beacon','global',NULL)",[f.input.vaultId]);
    const second=await dedup.deduplicateMemory(f.input);
    await pool.query("UPDATE entity_aliases SET canonical='another name' WHERE vault_id=$1",[f.input.vaultId]);
    const meta={sourceTimestamp:null,validFrom:null,validUntil:null,createdAt:null};
    for(let i=0;i<2;i++){
      const prepared=await evidence.prepareConflictEvidence(f.input.vaultId,second.memoryId!,f.first.memoryId!);
      await new model.ExtractorService().arbitrateConflict(f.input.fact,'Project Atlas uses PostgreSQL.',f.input.vaultId,{existing:meta,incoming:meta,evidence:prepared!});
    }
    expect(requests).toHaveLength(3);
  });

  it.each([B.manifest,B.manifest+1])('checks the entire persisted old-side manifest before selecting rows: %i',async count=>{
    const f=await fixture();const ids:string[]=[];
    for(let i=0;i<count;i++)ids.push(await source(f.input.vaultId,i===count-1?'FINAL CORRECTION: these refer to distinct entities.':'Prior observation '+i));
    await pool.query('UPDATE memories SET source_chunks=$2::uuid[] WHERE id=$1',[f.first.memoryId,ids]);
    const request=await dedup.getDedupEscalationRequest(f.input,'p');
    if(count>B.manifest){expect(request).toBeNull();expect(requests).toHaveLength(0);}
    else{mode='keep_both';await invoke(f.input);expect(JSON.parse(requests[0].messages[1].content).A.sources.map((s:any)=>s.text)).toContain('FINAL CORRECTION: these refer to distinct entities.');}
  });

  it('rechecks raw content after provider wait before authorizing a destructive receipt',async()=>{
    const f=await fixture();
    hook=async()=>{
      const {resolveRawChunkLocalRoot}=await import('../raw-chunk-storage-config');
      const file=path.join(resolveRawChunkLocalRoot(config.RAW_CHUNK_LOCAL_DIR),keys[0]);
      const old=await fs.readFile(file);await fs.writeFile(file,Buffer.alloc(old.length,120));
    };
    const r=await invoke(f.input);expect(r.assessment.state).toBe('incomplete');await apply(f.input,r);
    expect(await active(f.input.vaultId)).toBe(2);
  });
  it('refuses overwriting a retained object key through the storage writer',async()=>{
    await fixture();await expect(storage.put(keys[0],'Replacement source')).rejects.toThrow();
    expect(await storage.get(keys[0])).toBe('Project Atlas uses PostgreSQL.');
  });
  it('discovers exhausted durable allowance before consulting a depleted AI bucket',async()=>{
    const f=await fixture();mode='malformed';
    await pool.query(`UPDATE vaults SET rate_limit_override='{"ai_requests_per_minute":3,"ai_tokens_per_minute":5000000,"ai_escalation_weight":1}' WHERE id=$1`,[f.input.vaultId]);
    for(let i=0;i<3;i++)expect((await invoke(f.input)).assessment.state).toBe('incomplete');
    expect((await invoke(f.input)).assessment.state).toBe('incomplete');expect(requests).toHaveLength(3);
  });
  it('does not infer an ambiguous legacy source backend from current configuration',async()=>{
    const f=await fixture();f.input.sourceChunks=[await source(f.input.vaultId,'Readable but backend-unbound source',{store:null})];
    expect(await dedup.getDedupEscalationRequest(f.input,'p')).toBeNull();expect(requests).toHaveLength(0);
  });
  it('rejects over-bound raw incoming manifests before normalizing duplicate IDs',async()=>{
    const f=await fixture();f.input.sourceChunks=Array(B.manifest+1).fill(f.b);
    expect(await dedup.getDedupEscalationRequest(f.input,'p')).toBeNull();expect(requests).toHaveLength(0);
  });

  it('refunds local bucket reservation after a known pre-dispatch database rollback',async()=>{
    const f=await fixture();mode='malformed';
    await pool.query(`UPDATE vaults SET rate_limit_override='{"ai_requests_per_minute":1,"ai_tokens_per_minute":5000000,"ai_escalation_weight":1}' WHERE id=$1`,[f.input.vaultId]);
    const transaction=db.withTransaction;
    vi.spyOn(db,'withTransaction').mockImplementationOnce(work=>transaction(async client=>{await work(client);throw new Error('Known pre-dispatch rollback');}));
    await expect(invoke(f.input)).rejects.toThrow('Known pre-dispatch rollback');expect(requests).toHaveLength(0);
    expect((await pool.query('SELECT count(*)::int AS n FROM contradiction_scan_log WHERE vault_id=$1',[f.input.vaultId])).rows[0].n).toBe(0);
    expect((await invoke(f.input)).assessment.state).toBe('incomplete');expect(requests).toHaveLength(1);
  });
  it('does not certify a model verdict after source deletion during provider work',async()=>{
    const f=await fixture();hook=()=>storage.delete(keys[0]);
    const r=await invoke(f.input);expect(r.assessment.state).toBe('incomplete');await apply(f.input,r);
    expect(await active(f.input.vaultId)).toBe(2);
  });

});
