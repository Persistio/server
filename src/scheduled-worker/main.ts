import { Pool } from 'pg';
import { z } from 'zod';
import { getConfig } from '../config';
import { requestWorkerConfig } from '../request-worker/config';
import { createWorkerAuthenticator } from '../request-worker/auth';
import { ShadowPubSubPublisher } from '../services/shadow-customer-publisher';
import { ShadowCustomerMetricDispatcher } from '../services/customer-metric-shadow';
import { runShadowObservabilityPass } from '../services/shadow-observability-pass';
import { observeWorkerShadow } from '../services/shadow-observability';
import { WorkerWakeReconciliation } from '../services/worker-wake-reconciliation';
import { WORKER_WAKE_KINDS } from '../services/worker-wake-types';
import { createScheduledWorkerServer } from './server';

export async function startScheduledWorker(): Promise<void> {
  const config = getConfig();
  const identity = requestWorkerConfig();
  if (!config.TELEMETRY_SHADOW_ENABLED || config.TELEMETRY_PROVIDER !== 'none' ||
      config.CUSTOMER_METRICS_PUBLISHER !== 'noop') throw new Error('Invalid scheduled worker telemetry');
  // Validate destination before any database/listener activity or credential discovery.
  const topic = process.env.CUSTOMER_METRICS_SHADOW_TOPIC ?? '';
  const publisher = new ShadowPubSubPublisher(topic, config.CUSTOMER_METRICS_GCP_PUBSUB_TOPIC);
  await publisher.close();
  const pool = new Pool({ connectionString: config.DATABASE_URL, max: 1,
    application_name: 'persistio:scheduled-control', connectionTimeoutMillis: 3000,
    statement_timeout: 5000, lock_timeout: 1000, idle_in_transaction_session_timeout: 5000 });
  pool.on('error', () => process.exit(1));
  // Explicit deployment migration gate. Do not run migrations on cold start.
  await pool.query('SELECT event_id FROM customer_metric_shadow_outbox LIMIT 0');
  await pool.query('SELECT kind FROM worker_wake_intents LIMIT 0');
  await pool.query('SELECT kind FROM worker_observation_success LIMIT 0');
  const observationFlags = z.object({
    OBSERVE_CURATION_ENABLED: z.enum(['true', 'false']),
    OBSERVE_CONTRADICTION_ENABLED: z.enum(['true', 'false'])
  }).parse(process.env);
  const observationOptions = { enabled: true,
    curationEnabled: observationFlags.OBSERVE_CURATION_ENABLED === 'true',
    contradictionEnabled: observationFlags.OBSERVE_CONTRADICTION_ENABLED === 'true' };
  const reconciliation = new WorkerWakeReconciliation(pool, observationOptions);
  const runtime = createScheduledWorkerServer({
    authenticate: createWorkerAuthenticator(identity), fatal: () => process.exit(1),
    async observe() {
      // Each request owns and closes its publisher, including poisoned/ambiguous sends.
      const transport = new ShadowPubSubPublisher(topic, config.CUSTOMER_METRICS_GCP_PUBSUB_TOPIC);
      try {
        const dispatcher = new ShadowCustomerMetricDispatcher(pool, transport);
        return await runShadowObservabilityPass({
          dispatch: () => dispatcher.dispatchOnce(),
          observe: () => observeWorkerShadow(pool, observationOptions)
        });
      } finally { await transport.close(); }
    },
    async reconcile() {
      // Read-only deployed probe. No task creation, intent mutation or business work.
      for (const kind of WORKER_WAKE_KINDS) await reconciliation.dueAt(kind);
    }
  });
  let stopping = false;
  const shutdown = () => {
    if (stopping) return;
    stopping = true;
    setTimeout(() => process.exit(1), 8000);
    void runtime.stop().then(() => pool.end()).then(() => process.exit(0), () => process.exit(1));
  };
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);
  await runtime.app.listen({ port: config.PORT, host: '0.0.0.0' });
}
if (require.main === module) {
  const startupDeadline = setTimeout(() => process.exit(1), 55_000);
  void startScheduledWorker().then(() => clearTimeout(startupDeadline), () => process.exit(1));
}
