// Explicit one-shot utility: never imported by the server's startup path.
import { getConfig } from './config';
import { pool, closePool } from './db/client';
import { observeWorkerShadow } from './services/shadow-observability';
import { ShadowCustomerMetricDispatcher } from './services/customer-metric-shadow';
import { runShadowObservabilityPass } from './services/shadow-observability-pass';
import { ShadowPubSubPublisher } from './services/shadow-customer-publisher';

export async function main(args = process.argv.slice(2)) {
  const config = getConfig();
  let publisher: ShadowPubSubPublisher | undefined;
  try {
    if (!config.TELEMETRY_SHADOW_ENABLED) throw new Error('Shadow telemetry must be explicitly enabled');
    if (args.length && (args.length !== 2 || args[0] !== '--publish-shadow-topic')) throw new Error('Invalid shadow utility arguments');
    if (args.length) {
      publisher = new ShadowPubSubPublisher(args[1],config.CUSTOMER_METRICS_GCP_PUBSUB_TOPIC);
    }
    const dispatcher = publisher ? new ShadowCustomerMetricDispatcher(pool,publisher) : undefined;
    process.exitCode = await runShadowObservabilityPass({
      dispatch: dispatcher ? () => dispatcher.dispatchOnce() : undefined,
      observe: () => observeWorkerShadow(pool,{enabled:true,
        curationEnabled:config.CURATOR_AUTO_RUN,contradictionEnabled:config.CONTRADICTION_SCAN_ENABLED})
    });
  } finally {
    try { await publisher?.close(); } finally { await closePool(); }
  }
}
// This executable owns the process. A stalled SDK/connection must not outlive its
// bounded pass; SQL leases make forced exit recoverable without a new event identity.
if (require.main === module) {
  const hardDeadline=setTimeout(()=>process.exit(1),60_000);
  hardDeadline.unref();
  void main().catch(() => { console.error('Shadow pass failed'); process.exitCode=1; }).finally(()=>clearTimeout(hardDeadline));
}
