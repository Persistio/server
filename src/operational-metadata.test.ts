import {Writable} from 'node:stream';
import pino from 'pino';
import Fastify from 'fastify';
import {trace,metrics,context} from '@opentelemetry/api';
import {BasicTracerProvider,InMemorySpanExporter,SimpleSpanProcessor} from '@opentelemetry/sdk-trace-base';
import {MeterProvider,InMemoryMetricExporter,PeriodicExportingMetricReader,AggregationTemporality} from '@opentelemetry/sdk-metrics';
import {afterEach,describe,it,expect,vi} from 'vitest';
import {z} from 'zod';
import {createOperationalLogger,operationalLoggerOptions,operationalMetadata,safeLogArguments} from './operational-metadata';
import {ModelOutputContractError} from './services/model-output-error';
import {withSpan,meter} from './telemetry';
const privateText='PRIVATE_QUERY_MEMORY_SUBJECT_PROVIDER_SENTINEL';
const vault='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
afterEach(()=>{vi.restoreAllMocks();trace.disable();metrics.disable();context.disable();});
describe('actual operational exports contain metadata only',()=>{
  it('retains fixed proposal exclusion counts and reasons through repeated filtering without content',()=>{
    const expected={event:'extraction pipeline attrition',vault_id:vault,raw_facts:3,
      after_source_filter:1,excluded_context_only:1,excluded_unsupported_human_intent:1,
      after_score_filter:1,after_secret_filter:1,after_sensitivity_filter:1,after_deterministic_filter:1,threshold:5};
    const input={...expected,msg:expected.event,subject:privateText,fact:privateText,
      source_refs:[privateText],excluded_candidates:[privateText],reason:privateText};
    expect(operationalMetadata(input)).toEqual(expected);
    expect(operationalMetadata(operationalMetadata(input))).toEqual(expected);
    const lines:string[]=[];vi.spyOn(console,'log').mockImplementation(value=>lines.push(String(value)));
    createOperationalLogger('extraction-worker').log(input);
    expect(JSON.parse(lines[0])).toMatchObject(expected);expect(lines.join('')).not.toContain(privateText);
    for(const reason of ['context_only','unsupported_human_intent']){
      const labels={status:'dropped',reason,count:1};
      expect(operationalMetadata(operationalMetadata(labels))).toEqual(labels);
    }
  });
  it('exports bounded model-output metadata through worker logs and repeated filtering',()=>{
    const parsed=z.object({facts:z.array(z.object({type:z.enum(['decision'])}).strict())})
      .safeParse({facts:[{type:privateText,[privateText]:privateText}]});
    if(parsed.success)throw new Error('Invalid fixture');
    const failure=new ModelOutputContractError('extraction','schema',parsed.error.issues);
    const metadata=safeLogArguments([{error:failure,vault_id:vault},'Extraction job failed']);
    expect(metadata).toMatchObject({error_code:'invalid_model_output',model_output_operation:'extraction',
      model_output_stage:'schema',model_output_issue_count:2,model_output_issue_0_code:'invalid_enum_value',
      model_output_issue_0_path:'facts.[].type',model_output_issue_1_code:'unrecognized_keys'});
    expect(operationalMetadata(metadata)).toEqual(metadata);
    expect(safeLogArguments([JSON.stringify(metadata)])).toEqual(metadata);
    const lines:string[]=[];vi.spyOn(console,'error').mockImplementation(value=>{lines.push(String(value));});
    createOperationalLogger('extraction-worker').error({error:failure,source:privateText});
    expect(JSON.parse(lines[0])).toMatchObject({error_code:'invalid_model_output',model_output_issue_count:2});
    expect(lines.join('')).not.toContain(privateText);
  });
  it('recognizes an actual nested output failure without trusting wrapper text or plain-object impostors',()=>{
    const failure=new ModelOutputContractError('curation','json');
    const wrapped=Object.assign(new Error(privateText),{outputFailure:failure,audit:{rawResponse:privateText}});
    expect(safeLogArguments([wrapped])).toEqual({error_code:'invalid_model_output',
      model_output_operation:'curation',model_output_stage:'json',model_output_issue_count:0});
    expect(safeLogArguments([{err:wrapped}])).toEqual(safeLogArguments([wrapped]));
    const impostor=Object.assign(new Error(privateText),{outputFailure:{name:'ModelOutputContractError',
      operation:'curation',stage:'json',issueCount:0,issues:[]}});
    expect(safeLogArguments([impostor])).toEqual({error_code:'operation_failed'});
    expect(JSON.stringify(safeLogArguments([wrapped]))).not.toContain(privateText);
  });
  it('rejects untrusted flattened diagnostic values and unbounded issue fields',()=>{
    const output=operationalMetadata({model_output_operation:privateText,model_output_stage:privateText,
      model_output_issue_0_code:privateText,model_output_issue_0_path:`facts.${privateText}`,
      model_output_issue_8_code:'custom',model_output_issue_8_path:'facts',model_output_issue_count:Infinity,
      model_output_issue_7_code:'custom',model_output_issue_7_path:'root',error:new Error(privateText)});
    expect(output).toEqual({model_output_issue_7_code:'custom',model_output_issue_7_path:'root',error_code:'operation_failed'});
  });
  it('keeps model-output diagnostics intact through actual pino hooks without leaking rejected data',()=>{
    const lines:string[]=[];const stream=new Writable({write(chunk,_encoding,done){lines.push(String(chunk));done();}});
    const logger=pino(operationalLoggerOptions() as any,stream);
    const failure=new ModelOutputContractError('curation','schema',[{code:'custom',path:['update',0,'memory','valid_from'],message:privateText}]);
    logger.error({err:Object.assign(new Error(privateText),{outputFailure:failure}),response:privateText},privateText);
    const record=JSON.parse(lines.join('').trim());
    expect(record).toMatchObject({error_code:'invalid_model_output',model_output_operation:'curation',
      model_output_stage:'schema',model_output_issue_0_path:'update.[].memory.valid_from'});
    expect(lines.join('')).not.toContain(privateText);
  });
  it('filters Fastify/pino objects, child bindings, request bodies and errors in emitted records',async()=>{
    const lines:string[]=[];const stream=new Writable({write(chunk,_encoding,done){lines.push(String(chunk));done();}});
    const logger=pino(operationalLoggerOptions(()=>({trace_id:'a'.repeat(32),query:privateText})) as any,stream);
    const app=Fastify({loggerInstance:logger,disableRequestLogging:true});
    app.post('/v1/recall',async request=>{
      request.log.info({vault_id:vault,route:'/v1/recall',status:200,result_count:1,query:privateText,memories:[privateText],
        memory_id:vault,'memory.subject':privateText,err:Object.assign(new Error(privateText),{code:'23514'})},privateText);
      return{bundle:privateText};
    });
    const res=await app.inject({method:'POST',url:'/v1/recall',payload:{query:privateText}});
    expect(res.statusCode).toBe(200);expect(res.body).toContain(privateText);await app.close();
    expect(lines.length).toBeGreaterThan(0);expect(lines.join('')).not.toContain(privateText);
    const records=lines.flatMap(line=>line.trim().split('\n').map(s=>JSON.parse(s)));
    expect(records.some(r=>r.vault_id===vault&&r.result_count===1&&r.error_code==='23514')).toBe(true);
    expect(records.every(r=>!('memory_id'in r)&&!('memories'in r)&&!('err'in r))).toBe(true);
  });
  it('filters worker console emissions and keeps a failed sink out of business control flow',()=>{
    const lines:string[]=[];vi.spyOn(console,'error').mockImplementation((value)=>{lines.push(String(value));});
    const logger=createOperationalLogger('extraction-worker');logger.error({error:new Error(privateText),subject:privateText,vault_id:vault},'Extraction job failed');
    expect(JSON.parse(lines[0])).toMatchObject({component:'extraction-worker',vault_id:vault,error_code:'operation_failed'});
    expect(lines.join('')).not.toContain(privateText);
    vi.mocked(console.error).mockImplementation(()=>{throw new Error('Sink unavailable');});
    expect(()=>logger.error(new Error(privateText))).not.toThrow();
  });
  it('exports safe actual spans without exception messages, stacks, subjects or query text',async()=>{
    const exporter=new InMemorySpanExporter(),provider=new BasicTracerProvider({spanProcessors:[new SimpleSpanProcessor(exporter)]});
    trace.setGlobalTracerProvider(provider);
    try{
      await expect(withSpan('recall.operation',{vault_id:vault,query:privateText,'memory.subject':privateText},async span=>{
        span.setAttribute('memory.subject',privateText);span.setAttribute('result_count',1);span.setStatus({code:2,message:privateText});
        throw Object.assign(new Error(privateText),{code:'ETIMEDOUT'});
      })).rejects.toThrow(privateText);
      await provider.forceFlush();
      const output=exporter.getFinishedSpans().map(s=>({name:s.name,attributes:s.attributes,status:s.status,events:s.events}));
      expect(output).toHaveLength(1);expect(output[0].attributes).toMatchObject({vault_id:vault,result_count:1,error_code:'ETIMEDOUT'});
      expect(JSON.stringify(output)).not.toContain(privateText);expect(output[0].events).toEqual([]);
    }finally{await provider.shutdown();}
  });
  it('exports bounded metric labels, never source/session/query payloads',async()=>{
    const exporter=new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
    const reader=new PeriodicExportingMetricReader({exporter,exportIntervalMillis:60000});
    const provider=new MeterProvider({readers:[reader]});metrics.setGlobalMeterProvider(provider);
    try{
      meter.createCounter('restoration.safe.count').add(1,{vault_id:vault,status:'accepted',session_id:privateText,memory_id:privateText,query:privateText});
      meter.createHistogram('restoration.safe.duration').record(10,{route:'/v1/recall',status_code:'200',subject:privateText});
      await provider.forceFlush();const output=JSON.stringify(exporter.getMetrics());
      expect(output).toContain('restoration.safe.count');expect(output).toContain(vault);expect(output).not.toContain(privateText);
    }finally{await provider.shutdown();}
  });
  it('runs an action exactly once despite tracer/context/export failures',async()=>{
    vi.spyOn(trace,'getTracer').mockImplementation(()=>{throw new Error('Exporter unavailable');});
    vi.spyOn(context,'active').mockImplementation(()=>{throw new Error('Context unavailable');});
    const action=vi.fn(async()=>42);expect(await withSpan('safe.operation',{},action)).toBe(42);expect(action).toHaveBeenCalledOnce();
    const metricFailure=vi.spyOn(metrics,'getMeter').mockImplementation(()=>{throw new Error('Metric export unavailable');});
    expect(()=>meter.createCounter('safe.counter').add(1)).not.toThrow();expect(metricFailure).toHaveBeenCalled();
  });
});
