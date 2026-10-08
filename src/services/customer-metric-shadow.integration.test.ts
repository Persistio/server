import Fastify from 'fastify';
import { recordCustomerApiRequestObservation } from './customer-api-request-metrics';
import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, afterEach, describe, expect, it, vi } from 'vitest';
import { captureShadowCustomerMetric, ShadowCustomerMetricDispatcher } from './customer-metric-shadow';
import { buildCustomerMetricEvent, type CustomerMetricEvent } from './customer-metrics';
import { observeWorkerShadow, recordShadowCycleSuccess } from './shadow-observability';
const url = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!url)('shadow accounting and observation protocol (PostgreSQL)', () => {
  const pool = new Pool({connectionString:url,max:4});
  let db: typeof import('../db/client');
  const vault=crypto.randomUUID(),workspace=crypto.randomUUID();
  const input = (): CustomerMetricEvent => buildCustomerMetricEvent({event_id:crypto.randomUUID(),event_ts:new Date(),
    event_type:'quota_delta',operation:'searches',searches_delta:1,source:'api',vault_id:vault,workspace_id:workspace});
  const insert = async (event=input()) => {
    event=buildCustomerMetricEvent(event);
    const client=await pool.connect();
    try { await client.query('BEGIN'); await captureShadowCustomerMetric(client,event,true); await client.query('COMMIT'); }
    catch(error){await client.query('ROLLBACK');throw error;} finally{client.release();}
    return event;
  };
  const rows = async()=> (await pool.query('SELECT * FROM customer_metric_shadow_outbox ORDER BY event_id')).rows;
  beforeAll(async()=>{
    if (!url || process.env.DATABASE_URL!==url || !new URL(url).pathname.startsWith('/persistio_test_')) throw new Error('Use isolated matching test DB URLs');
    db=await import('../db/client');await db.runMigrations();
    await pool.query("INSERT INTO vaults(id,name,api_key_hash,plan_id,account_id) VALUES($1,'shadow-test',$2,'unlimited',$3)",[vault,crypto.randomUUID(),workspace]);
  });
  beforeEach(async()=>{
    await pool.query('DELETE FROM customer_metric_shadow_outbox');
    (await import('../config')).getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE=false;
  });
  afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();});
  afterAll(async()=>{
    await pool.query('DELETE FROM customer_metric_shadow_outbox');
    await pool.query('DELETE FROM vaults WHERE id=$1',[vault]);
    await pool.end();await db?.closePool();
  });
  it('disabled capture does no SQL and disabled observation produces no false healthy heartbeat',async()=>{
    const query=vi.fn();await captureShadowCustomerMetric({query} as never,input(),false);
    expect(query).not.toHaveBeenCalled();
    expect(await observeWorkerShadow({connect:query} as never)).toEqual([]);expect(query).not.toHaveBeenCalled();
  });
  it('rolls business and event back together; committed identity survives duplicate capture unchanged',async()=>{
    const event=input();const client=await pool.connect();
    try{
      await client.query('BEGIN');await client.query("UPDATE vaults SET name='rollback-sentinel' WHERE id=$1",[vault]);
      await captureShadowCustomerMetric(client,event,true);await client.query('ROLLBACK');
      expect(await rows()).toEqual([]);
      expect((await pool.query('SELECT name FROM vaults WHERE id=$1',[vault])).rows[0].name).toBe('shadow-test');
    }finally{client.release();}
    await insert(event);await insert(event);
    expect(await rows()).toHaveLength(1);expect((await rows())[0].payload).toEqual(buildCustomerMetricEvent(event));
    await expect(insert({...event,searches_delta:2} as CustomerMetricEvent)).rejects.toThrow('identity conflict');
    expect((await rows())[0].payload.searches_delta).toBe(1);
  });
  it('retains IDs and original timestamps across publish-before-ack ambiguity',async()=>{
    const event=await insert();const received:unknown[]=[];
    const publisher={publish:vi.fn(async events=>{received.push(events[0]);throw new Error('accepted but receipt lost');})};
    const first=await new ShadowCustomerMetricDispatcher(pool,publisher).dispatchOnce();expect(first.retried).toBe(1);
    await pool.query("UPDATE customer_metric_shadow_outbox SET available_at=now()-interval '1 second'");
    publisher.publish.mockImplementation(async events=>{received.push(events[0]);});
    expect((await new ShadowCustomerMetricDispatcher(pool,publisher).dispatchOnce()).delivered).toBe(1);
    expect(received).toEqual([buildCustomerMetricEvent(event),buildCustomerMetricEvent(event)]);
    expect((await rows())[0].status).toBe('delivered');
  });
  it('fences an old successful publisher after another owner takes over',async()=>{
    await insert();
    const replacement=crypto.randomUUID();
    const publisher={publish:async()=>{await pool.query("UPDATE customer_metric_shadow_outbox SET lease_owner=$1,lease_expires_at=now()+interval '1 minute'",[replacement]);}};
    expect((await new ShadowCustomerMetricDispatcher(pool,publisher).dispatchOnce()).stale).toBe(1);
    expect((await rows())[0]).toMatchObject({status:'pending',lease_owner:replacement});
  });
  it('does not hold a SQL connection during transport and concurrent dispatchers claim distinct events',async()=>{
    await insert();await insert();
    const seen:string[]=[];let active=0;let resolve!:()=>void;const both=new Promise<void>(r=>resolve=r);
    const publisher={publish:async(events:any[])=>{seen.push(events[0].event_id);if(++active===2)resolve();await both;}};
    const result=await Promise.all([new ShadowCustomerMetricDispatcher(pool,publisher,{batchSize:1}).dispatchOnce(),
      new ShadowCustomerMetricDispatcher(pool,publisher,{batchSize:1}).dispatchOnce()]);
    expect(new Set(seen).size).toBe(2);expect(result.map(r=>r.delivered)).toEqual([1,1]);
  });
  it('bounds a hung publisher and poisons reuse; a late result cannot acknowledge the row',async()=>{
    await insert();let finish!:()=>void;
    const dispatcher=new ShadowCustomerMetricDispatcher(pool,{publish:()=>new Promise<void>(r=>finish=r)},{publishTimeoutMs:20});
    expect((await dispatcher.dispatchOnce()).retried).toBe(1);finish();await Promise.resolve();
    expect((await rows())[0].status).toBe('pending');await expect(dispatcher.dispatchOnce()).rejects.toThrow('unavailable');
  });
  it('recovers an expired lease; caps retries, quarantines beyond lookback and bounds retention',async()=>{
    await insert();await pool.query("UPDATE customer_metric_shadow_outbox SET lease_owner=gen_random_uuid(),lease_expires_at=now()-interval '1 second'");
    expect((await new ShadowCustomerMetricDispatcher(pool,{publish:async()=>{}}).dispatchOnce()).delivered).toBe(1);
    await pool.query("UPDATE customer_metric_shadow_outbox SET settled_at=now()-interval '31 days'");
    await insert({...input(),event_ts:new Date(Date.now()-7*3600_000).toISOString()});
    const exhausted=await insert();await pool.query('UPDATE customer_metric_shadow_outbox SET attempts=10 WHERE event_id=$1',[exhausted.event_id]);
    const publisher={publish:vi.fn()};const report=await new ShadowCustomerMetricDispatcher(pool,publisher).dispatchOnce();
    expect(report).toMatchObject({dead:2,purged:1});expect(publisher.publish).not.toHaveBeenCalled();
    expect((await rows()).every(row=>row.status==='dead')).toBe(true);
  });
  it('captures real quota/refund/model transactions with field parity and accountless exclusion',async()=>{
    const config=(await import('../config')).getConfig();config.CUSTOMER_METRICS_SHADOW_CAPTURE=true;
    const usage=await import('./usage');
    const reservation=await usage.reserveApiQuota(vault,'searches');await usage.refundApiQuotaReservation(reservation);
    await usage.recordModelUsage({vaultId:vault,provider:'openai',model:'test-model',modelRole:'embedding',source:'api',embeddingInputTokens:3,requestCount:1});
    const events=(await rows()).map(row=>row.payload);
    expect(events.filter(e=>e.event_type==='quota_delta').map(e=>e.searches_delta).sort()).toEqual([-1,1]);
    expect(events.find(e=>e.event_type==='model_usage')).toMatchObject({model:'test-model',embedding_input_tokens:3,model_request_count:1,workspace_id:workspace});
    expect(events.every(e=>e.contract_version===1&&e.event_ts&&e.event_date===e.event_ts.slice(0,10))).toBe(true);
    const before=events.length;
    await expect(db.withTransaction(async client=>{await usage.reserveApiQuotaInTransaction(client,vault,'searches');throw new Error('rollback');})).rejects.toThrow('rollback');
    expect(await rows()).toHaveLength(before);
    await pool.query('UPDATE vaults SET account_id=NULL WHERE id=$1',[vault]);
    await usage.reserveApiQuota(vault,'searches');expect(await rows()).toHaveLength(before);
    await pool.query('UPDATE vaults SET account_id=$2 WHERE id=$1',[vault,workspace]);
  });
  it.each(['quota','refund','increment','model'] as const)('preserves the entire %s envelope when COMMIT crosses UTC midnight',async kind=>{
    (await import('../config')).getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE=true;
    const usage=await import('./usage'),metrics=await import('./customer-metrics');
    const reservation=kind==='refund'?await usage.reserveApiQuota(vault,'searches'):undefined;
    await pool.query('DELETE FROM customer_metric_shadow_outbox');
    const legacy:CustomerMetricEvent[]=[];
    vi.spyOn(metrics,'recordCustomerMetric').mockImplementation(event=>legacy.push(metrics.buildCustomerMetricEvent(event)));
    const before='2026-10-06T23:59:59.999Z',after='2026-10-07T00:00:00.001Z';
    vi.useFakeTimers({toFake:['Date']});vi.setSystemTime(new Date(before));
    const transact=db.withTransaction;
    vi.spyOn(db,'withTransaction').mockImplementation(work=>transact(async client=>{
      const result=await work(client);vi.setSystemTime(new Date(after));return result;
    }));
    if(kind==='quota')await usage.reserveApiQuota(vault,'searches');
    if(kind==='refund')await usage.refundApiQuotaReservation(reservation!);
    if(kind==='increment')await usage.incrementUsage(vault,'searches');
    if(kind==='model')await usage.recordModelUsage({vaultId:vault,provider:'openai',model:'boundary',modelRole:'embedding',source:'api',requestCount:1});
    const shadow=(await rows()).map(row=>row.payload);
    expect(Date.now()).toBe(Date.parse(after));expect(legacy).toHaveLength(1);expect(shadow).toEqual(legacy);
    expect(shadow[0]).toMatchObject({event_ts:before,event_date:'2026-10-06'});
    if(reservation)expect(shadow[0].event_id).not.toBe(reservation.metricEvent!.event_id);
  });
  it('retains memory-count effect time and identity across delayed commit/publication',async()=>{
    (await import('../config')).getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE=true;
    const effects=await import('./worker-effects'),metrics=await import('./customer-metrics');
    const legacy:CustomerMetricEvent[]=[];
    vi.spyOn(metrics,'recordCustomerMetric').mockImplementation(event=>legacy.push(metrics.buildCustomerMetricEvent(event)));
    vi.useFakeTimers({toFake:['Date']});vi.setSystemTime(new Date('2026-10-06T12:59:59.999Z'));
    const effect=effects.memoryCountEffect(vault,workspace,-1,'curation_worker');
    await db.withTransaction(client=>effects.captureShadowWorkerEffects(client,[effect]));
    vi.setSystemTime(new Date('2026-10-07T00:00:00Z'));effects.publishCommittedWorkerEffects([effect]);
    expect((await rows()).map(row=>row.payload)).toEqual(legacy);
    expect(legacy[0]).toMatchObject({event_ts:'2026-10-06T12:59:59.999Z',event_date:'2026-10-06'});
  });
  it('reuses the request envelope when after-response SQL capture crosses UTC midnight',async()=>{
    (await import('../config')).getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE=true;
    const metrics=await import('./customer-metrics');const legacy:CustomerMetricEvent[]=[];
    vi.useFakeTimers({toFake:['Date']});vi.setSystemTime(new Date('2026-10-06T23:59:59.999Z'));
    vi.spyOn(metrics,'recordCustomerMetric').mockImplementation(event=>{
      legacy.push(metrics.buildCustomerMetricEvent(event));vi.setSystemTime(new Date('2026-10-07T00:00:00.001Z'));
    });
    await recordCustomerApiRequestObservation({routeOptions:{url:'/v1/recall'},method:'POST',vault:{id:vault,account_id:workspace},log:{warn:vi.fn()}} as any,
      {elapsedTime:12,statusCode:200} as any);
    expect(legacy).toHaveLength(1);expect((await rows()).map(row=>row.payload)).toEqual(legacy);
    expect(legacy[0]).toMatchObject({event_ts:'2026-10-06T23:59:59.999Z',event_date:'2026-10-06'});
  });
  it('preserves the actual HTTP response and v1 request fields when shadow capture succeeds or fails',async()=>{
    (await import('../config')).getConfig().CUSTOMER_METRICS_SHADOW_CAPTURE=true;
    const app=Fastify();const captures:Promise<void>[]=[];
    app.addHook('onResponse',(request,reply)=>{const capture=recordCustomerApiRequestObservation(request,reply);captures.push(capture);return capture;});
    app.post('/v1/recall',async request=>{
      request.vault={id:vault,account_id:workspace} as any;
      return {answer:'PRIVATE response'};
    });
    try{
      expect((await app.inject({method:'POST',url:'/v1/recall'})).json()).toEqual({answer:'PRIVATE response'});
      await Promise.all(captures);
      const shadow=await rows();expect(shadow).toHaveLength(1);
      expect(shadow[0].payload).toMatchObject({event_type:'api_request',api_request_count:1,
        workspace_id:workspace,vault_id:vault,operation:'recall',route:'/v1/recall',status_code:200});
      expect(JSON.stringify(shadow)).not.toContain('PRIVATE');
      const failing=vi.spyOn(db,'withTransaction').mockRejectedValueOnce(new Error('capture unavailable'));
      try{expect((await app.inject({method:'POST',url:'/v1/recall'})).statusCode).toBe(200);await Promise.all(captures);}finally{failing.mockRestore();}
      expect(await rows()).toHaveLength(1);
    }finally{await Promise.all(captures);await app.close();}
  });
  it('isolates a failed last-cycle diagnostic with a real PostgreSQL savepoint',async()=>{
    const config=(await import('../config')).getConfig();config.TELEMETRY_SHADOW_ENABLED=true;
    const client=await pool.connect();
    try{
      await client.query('BEGIN');await client.query("DELETE FROM worker_observation_success WHERE kind='usage_periods'");
      const original=client.query.bind(client);let fail=true;
      const spy=vi.spyOn(client,'query').mockImplementation((...args:any[])=>{
        if(fail&&String(args[0]).startsWith('INSERT INTO worker_observation_success')){fail=false;return original('SELECT 1/0') as any;}
        return (original as any)(...args);
      });
      try{await recordShadowCycleSuccess(client,'usage_periods');}finally{spy.mockRestore();}
      expect((await client.query("SELECT 1 FROM worker_observation_success WHERE kind='usage_periods'")).rowCount).toBe(0);
      await recordShadowCycleSuccess(client,'usage_periods');
      expect((await client.query("SELECT 1 FROM worker_observation_success WHERE kind='usage_periods'")).rowCount).toBe(1);
      await client.query('ROLLBACK');
    }finally{client.release();config.TELEMETRY_SHADOW_ENABLED=false;}
  });
  it('does not lose terminal worker evidence between slow observations, and bounds recent history',async()=>{
    const client=await pool.connect();
    try{
      for(const kind of ['extraction','curation']){
        await client.query(`CREATE TEMP TABLE ${kind}_dead_letter (LIKE public.${kind}_dead_letter INCLUDING ALL)`);
        await client.query(`INSERT INTO ${kind}_dead_letter(vault_id,segment_id,retry_count,dead_lettered_at)
          VALUES($1,gen_random_uuid(),1,now()-interval '10 minutes'),($1,gen_random_uuid(),1,now()-interval '26 hours')`,[vault]);
      }
      const connection=Object.assign(Object.create(client),{query:client.query.bind(client),release:()=>{}});
      const result=await observeWorkerShadow({connect:async()=>connection},{enabled:true,curationEnabled:true,emit:()=>{}});
      for(const kind of ['extraction','curation'])expect(result.find(row=>row.shadow_kind===kind))
        .toMatchObject({shadow_health:'ok',dead_letters:1,recent_dead_letter_window_ms:25*3600_000});
    }finally{client.release(true);}
  });
  it('emits real zero/deferral/dead observations; failure has no fabricated values; recovery is explicit',async()=>{
    await insert({...input(),event_ts:new Date(Date.now()-7*3600_000).toISOString()});
    await new ShadowCustomerMetricDispatcher(pool,{publish:async()=>{}}).dispatchOnce();
    await insert();await pool.query("UPDATE customer_metric_shadow_outbox SET available_at=now()+interval '1 hour' WHERE status='pending'");
    const observations=await observeWorkerShadow(pool,{enabled:true,curationEnabled:true,contradictionEnabled:true,emit:()=>{}});
    expect(observations).toHaveLength(7);expect(observations.every(o=>o.shadow_health==='ok')).toBe(true);
    expect(observations.find(o=>o.shadow_kind==='customer_metrics')).toMatchObject({eligible_depth:0,deferred_depth:1,dead_letters:1,oldest_due_age_ms:0});
    expect(observations.find(o=>o.shadow_kind==='customer_metrics')).not.toHaveProperty('last_cycle_success_age_ms');
    await pool.query("UPDATE customer_metric_shadow_outbox SET event_ts=now()-interval '10 minutes',available_at=now()-interval '1 second' WHERE status='pending'");
    const overdue=await observeWorkerShadow(pool,{enabled:true,emit:()=>{}});
    expect(overdue.find(o=>o.shadow_kind==='customer_metrics')!.oldest_due_age_ms).toBeGreaterThanOrEqual(600000);

    const failed=await observeWorkerShadow({connect:async()=>{throw new Error('PRIVATE SQL DATA');}} as never,{enabled:true,emit:()=>{}});
    expect(failed.every(o=>o.shadow_health==='monitor_error'&&!('eligible_depth' in o))).toBe(true);
    expect(JSON.stringify(failed)).not.toContain('PRIVATE');
    const recovered=await observeWorkerShadow(pool,{enabled:true,emit:()=>{}});expect(recovered.every(o=>o.shadow_health==='ok')).toBe(true);
  });
});
