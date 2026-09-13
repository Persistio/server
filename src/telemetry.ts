import fs from 'node:fs';
import { operationalMetadata,safeErrorCode } from './operational-metadata';
import path from 'node:path';

import { context, metrics, SpanStatusCode, trace } from '@opentelemetry/api';

export interface SpanLike {
  setAttribute(key: string, value: string | number | boolean): void;
  setStatus(status: { code: number; message?: string }): void;
  recordException(error: Error): void;
  end(): void;
}

export interface HistogramLike {
  record(value: number, attributes?: Record<string, string>): void;
}

export interface CounterLike {
  add(value: number, attributes?: Record<string, string>): void;
}

export interface ObservableResultLike {
  observe(value: number, attributes?: Record<string, string>): void;
}

export interface ObservableGaugeLike {
  addCallback(callback: (result: ObservableResultLike) => void | Promise<void>): void;
}

export interface MeterLike {
  createHistogram(name: string, options?: Record<string, unknown>): HistogramLike;
  createCounter(name: string, options?: Record<string, unknown>): CounterLike;
  createObservableGauge(name: string, options?: Record<string, unknown>): ObservableGaugeLike;
}

const noopSpan: SpanLike = {
  setAttribute() {},
  setStatus() {},
  recordException() {},
  end() {}
};

const serverPackageJsonPath = path.resolve(__dirname, '..', 'package.json');
const serverVersion = JSON.parse(fs.readFileSync(serverPackageJsonPath, 'utf8')) as { version?: string };
const instrumentationVersion = serverVersion.version ?? '0.0.0';

export function getTracer() {
  return trace.getTracer('persistio-server', instrumentationVersion);
}

/** Export failures and arbitrary labels never control pipeline availability. */
function metricMetadata(value: unknown): Record<string,string> {
  return Object.fromEntries(Object.entries(operationalMetadata(value)).map(([key,entry])=>[key,String(entry)]));
}
export const meter: MeterLike = {
  createCounter(name, options) {
    let instrument:ReturnType<ReturnType<typeof metrics.getMeter>['createCounter']>|undefined;
    try{instrument=metrics.getMeter('persistio-server',instrumentationVersion).createCounter(name,options);}catch{}
    return{add(value,attributes){try{if(Number.isFinite(value))instrument?.add(value,metricMetadata(attributes));}catch{}}};
  },
  createHistogram(name, options) {
    let instrument:ReturnType<ReturnType<typeof metrics.getMeter>['createHistogram']>|undefined;
    try{instrument=metrics.getMeter('persistio-server',instrumentationVersion).createHistogram(name,options);}catch{}
    return{record(value,attributes){try{if(Number.isFinite(value))instrument?.record(value,metricMetadata(attributes));}catch{}}};
  },
  createObservableGauge(name, options) {
    let instrument:ReturnType<ReturnType<typeof metrics.getMeter>['createObservableGauge']>|undefined;
    try{instrument=metrics.getMeter('persistio-server',instrumentationVersion).createObservableGauge(name,options);}catch{}
    return{addCallback(callback){try{instrument?.addCallback(async result=>{
      try{await callback({observe(value,attributes){try{if(Number.isFinite(value))result.observe(value,metricMetadata(attributes));}catch{}}});}catch{}
    });}catch{}}};
  }
};

export function getTraceId() {
  try{return trace.getActiveSpan()?.spanContext().traceId;}catch{return undefined;}
}

export function getSpanAttributes(record: Record<string, unknown>) {
  const traceId = getTraceId();
  return operationalMetadata(traceId ? {...record,trace_id:traceId}:record);
}

export async function withSpan<T>(
  name: string,
  attributes: Record<string, string | number | boolean | undefined>,
  fn: (span: SpanLike) => Promise<T>
): Promise<T> {
  let raw:ReturnType<ReturnType<typeof getTracer>['startSpan']>|undefined;
  try{raw=getTracer().startSpan(name,{attributes:operationalMetadata(attributes)});}catch{/* export unavailable */}
  const span:SpanLike={
    setAttribute(key,value){try{const allowed=operationalMetadata({[key]:value});if(allowed[key]!==undefined)raw?.setAttribute(key,allowed[key]);}catch{}},
    setStatus(status){try{raw?.setStatus({code:status.code});}catch{}},
    recordException(error){try{raw?.setAttribute('error_code',safeErrorCode(error));}catch{}},
    end(){try{raw?.end();}catch{}}
  };
  const run=async()=>{
    try{const result=await fn(span);span.setStatus({code:SpanStatusCode.OK});return result;}
    catch(error){span.recordException(error instanceof Error?error:new Error());span.setStatus({code:SpanStatusCode.ERROR});throw error;}
    finally{span.end();}
  };
  // A failing exporter/context lookup must not run the business action twice.
  let active:ReturnType<typeof context.active>;
  try{active=context.active();if(raw)active=trace.setSpan(active,raw);}catch{return run();}
  let started=false;
  try{return await context.with(active,()=>{started=true;return run();});}
  catch(error){if(started)throw error;return run();}
}
