import { getConfig } from './config';
import { createOperationalLogger } from './operational-metadata';
import { meter, type CounterLike, type HistogramLike } from './telemetry';
const logger = createOperationalLogger('shadow-metrics');
// Fixed instrument names only; never pass arbitrary OTel attributes into logs.
export const SHADOW_METRIC_NAMES = [
  'persistio.recall.duration','persistio.ingest.chunks.total','persistio.extraction.jobs.total',
  'persistio.extraction.lag_ms','persistio.extraction.candidates.total','persistio.ai_budget.wait_ms',
  'persistio.ai_budget.throttled_jobs.total','persistio.embedding.duration',
  'persistio.event_outbox.dispatch.total','persistio.event_outbox.publish_failures.total','persistio.event_outbox.lag_ms'
] as const;
function emit(name: string, value: number, attributes?: Record<string,string>) {
  try {
    if (!getConfig().TELEMETRY_SHADOW_ENABLED || !SHADOW_METRIC_NAMES.includes(name as typeof SHADOW_METRIC_NAMES[number])) return;
    // Budget producers use role; normalize only the shadow copy, before enum filtering.
    logger.info({event:'shadow_domain_metric',shadow_metric:name,value,
      status:attributes?.status,outcome:attributes?.outcome,queue:attributes?.queue,mode:attributes?.mode,
      reason:attributes?.reason,model_role:attributes?.model_role ?? attributes?.role,provider:attributes?.provider});
  } catch { /* Operational observations never alter domain outcomes. */ }
}
export function shadowCounter(name:string, options:Record<string,unknown>):CounterLike {
  const instrument=meter.createCounter(name,options);
  return {add(value,attributes){instrument.add(value,attributes);emit(name,value,attributes);}};
}
export function shadowHistogram(name:string, options:Record<string,unknown>):HistogramLike {
  const instrument=meter.createHistogram(name,options);
  return {record(value,attributes){instrument.record(value,attributes);emit(name,value,attributes);}};
}
