import type {FastifyServerOptions} from 'fastify';
import {ModelOutputContractError,isSafeModelOutputIssueCode,isSafeModelOutputIssuePath} from './services/model-output-error';
/** Operational export only. Domain evidence/audits and HTTP bodies are separate. */
const numericKeys=new Set(['status','status_code','duration_ms','response_bytes','result_count','count','total','idle','waiting',
  'prompt_tokens','completion_tokens','total_tokens','pairs_count','candidates_count','active_memories_count',
  'accepted','inserted','replayed','deferred','retry_count','retry_after_ms','wait_ms','batch_size',
  'raw_facts','after_source_filter','excluded_context_only','excluded_unsupported_human_intent',
  'after_score_filter','after_secret_filter','after_sensitivity_filter','after_deterministic_filter','threshold',
  'embedding.input_count','embedding.request_count','embedding.duration_ms','extraction.batch_limit','extraction.batch_size',
  'extraction.memories_created','extraction.candidates.extracted','extraction.candidates.accepted','extraction.candidates.dropped']);
const idKeys=new Set(['vault_id','vaultId','vault.id']);
const enumValues:Record<string,ReadonlySet<string>>={
  method:new Set(['GET','POST','PATCH','PUT','DELETE','HEAD','OPTIONS']),
  mode:new Set(['agent','factual']),source:new Set(['api','extraction_worker','curation_worker']),
  model_role:new Set(['extraction','escalation','embedding','curation']),
  model_output_operation:new Set(['extraction','curation']),
  model_output_stage:new Set(['schema','json','completion','truncated','refusal']),
  role:new Set(['extraction','escalation','embedding','curation']),
  queue:new Set(['extraction','curation']),
  provider:new Set(['openai','vertex','ollama','tei']),
  status:new Set(['accepted','dropped','success','error','failed','dead']),
  reason:new Set(['context_only','unsupported_human_intent','empty','duplicate','secret_like','low_salience','implementation_detail']),
  dedup_action:new Set(['skipped','updated','inserted','conflict']),
  'embedding.provider':new Set(['openai','vertex','ollama','tei']),
  event:new Set(['scope_widening_attempt','invalid_source','invalid_scope','invalid_model_output']),
  outcome:new Set(['accepted','rejected','deferred','completed','failed','cycle','attempted','delivered','dead'])
};
const safeMessages=new Set(['Recall served','Extraction job failed','Curation job failed','Extraction loop iteration failed',
  'extraction pipeline attrition',
  'Curation loop iteration failed','Contradiction activation iteration failed','Extraction worker terminated','Curation worker terminated',
  'curator token usage','arbitration token usage','batch arbitration token usage','circuit_breaker_open',
  'settle_ai_usage_overage','failed to record model usage','worker lease renewal failed',
  'postgres idle connection failed; removed from pool','contradiction activation connection lost','contradiction activation deferred',
  'discarding stale curation worker result','deferring curation job for ai budget','skipping curation job while circuit breaker is open',
  'raw upload cleanup deferred after failed ingest','Committed worker metric publication failed; business result retained',
  'Ingest accepted; nonessential acceptance telemetry unavailable','[persistio] Worker shutdown incomplete']);

export function safeErrorCode(error:unknown):string {
  if(modelOutputFailure(error))return 'invalid_model_output';
  if(error instanceof SyntaxError)return 'invalid_syntax';
  if(error && typeof error==='object'){
    const e=error as {code?:unknown;statusCode?:unknown;name?:unknown};
    if(e.name==='ZodError')return 'invalid_request';
    const codes=new Set(['23503','23505','23514','40001','40P01','57014','ECONNRESET','ECONNREFUSED','ETIMEDOUT','ENOTFOUND']);
    if(typeof e.code==='string' && codes.has(e.code))return e.code;
    if(typeof e.statusCode==='number' && [400,401,403,404,409,413,429,503].includes(e.statusCode))return 'http_'+e.statusCode;
  }
  return 'operation_failed';
}

function modelOutputFailure(error:unknown):ModelOutputContractError|undefined {
  if(error instanceof ModelOutputContractError)return error;
  if(error && typeof error==='object'){
    const failure=(error as {outputFailure?:unknown}).outputFailure;
    if(failure instanceof ModelOutputContractError)return failure;
  }
  return undefined;
}

function safeErrorMetadata(error:unknown):Record<string,string|number|boolean> {
  const output:Record<string,string|number|boolean>={error_code:safeErrorCode(error)};
  const failure=modelOutputFailure(error);
  if(!failure)return output;
  output.model_output_operation=failure.operation;
  output.model_output_stage=failure.stage;
  output.model_output_issue_count=failure.issueCount;
  failure.issues.forEach((issue,index)=>{
    output[`model_output_issue_${index}_code`]=issue.code;
    output[`model_output_issue_${index}_path`]=issue.path;
  });
  return output;
}

export function operationalMetadata(record:unknown):Record<string,string|number|boolean> {
  const output:Record<string,string|number|boolean>={};
  if(!record || typeof record!=='object')return output;
  for(const [key,value] of Object.entries(record)){
    if((key==='error'||key==='err') && value){Object.assign(output,safeErrorMetadata(value));continue;}
    if(key==='model_output_issue_count' && typeof value==='number' && Number.isSafeInteger(value) && value>=0){output[key]=value;continue;}
    if(/^model_output_issue_[0-7]_code$/.test(key) && isSafeModelOutputIssueCode(value)){output[key]=value;continue;}
    if(/^model_output_issue_[0-7]_path$/.test(key) && isSafeModelOutputIssuePath(value)){output[key]=value;continue;}
    if(numericKeys.has(key) && typeof value==='number' && Number.isFinite(value)){output[key]=value;continue;}
    if(key==='status_code' && typeof value==='string' && /^[1-5][0-9]{2}$/.test(value)){output[key]=value;continue;}
    if(idKeys.has(key) && typeof value==='string' && /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(value)){output[key]=value;continue;}
    if(enumValues[key]?.has(String(value))){output[key]=String(value);continue;}
    if(key==='route' && typeof value==='string' && value.startsWith('/') && !/[?#\r\n]/.test(value) && value.length<=150){
      // Only callers may supply the registered route template, never request.url.
      output.route=value;continue;
    }
    if((key==='request_id'||key==='reqId') && typeof value==='string' && /^req-[a-z0-9]+$/.test(value)){output.request_id=value;continue;}
    if(key==='trace_id' && typeof value==='string' && /^[0-9a-f]{32}$/.test(value)){output.trace_id=value;continue;}
    if((key==='msg'||key==='event') && typeof value==='string' && safeMessages.has(value))output.event=value;
    if(key==='error_code' && typeof value==='string' && /^(operation_failed|invalid_model_output|invalid_request|invalid_syntax|http_(400|401|403|404|409|413|429|503)|23503|23505|23514|40001|40P01|57014|ECONNRESET|ECONNREFUSED|ETIMEDOUT|ENOTFOUND)$/.test(value))output.error_code=value;
  }
  return output;
}

export function safeLogArguments(args:unknown[]):Record<string,string|number|boolean>{
  const result:Record<string,string|number|boolean>={};
  for(let value of args){
    if(value instanceof Error){Object.assign(result,safeErrorMetadata(value));continue;}
    if(typeof value==='string'){
      if(safeMessages.has(value)){result.event=value;continue;}
      try{value=JSON.parse(value);}catch{continue;}
    }
    Object.assign(result,operationalMetadata(value));
  }
  return result;
}

export function createOperationalLogger(component:string){
  const emit=(level:'log'|'info'|'warn'|'error'|'debug',args:unknown[])=>{
    try{console[level](JSON.stringify({level,component,...safeLogArguments(args)}));}catch{/* Export is best-effort, never business control flow. */}
  };
  return{log:(...args:unknown[])=>emit('log',args),info:(...args:unknown[])=>emit('info',args),
    warn:(...args:unknown[])=>emit('warn',args),error:(...args:unknown[])=>emit('error',args),debug:(...args:unknown[])=>emit('debug',args)};
}

export function operationalLoggerOptions(traceContext:()=>Record<string,unknown>=()=>({})):
  Exclude<FastifyServerOptions['logger'],boolean|undefined> {
  return{
    level:process.env.LOG_LEVEL??'info',
    hooks:{logMethod(args,method){try{method.call(this,safeLogArguments(args),'platform_event');}catch{}}},
    formatters:{bindings:operationalMetadata},
    mixin(){try{return operationalMetadata(traceContext());}catch{return {};}}
  };
}
