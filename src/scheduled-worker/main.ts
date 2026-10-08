import { Pool } from 'pg';
import { z } from 'zod';
import { getConfig } from '../config';
import { requestWorkerConfig } from '../request-worker/config';
import { createWorkerAuthenticator } from '../request-worker/auth';
import { configuredWakeDispatcher } from '../services/worker-wake-runtime';
import { ShadowPubSubPublisher, ProductionCustomerPublisher } from '../services/shadow-customer-publisher';
import { ShadowCustomerMetricDispatcher } from '../services/customer-metric-shadow';
import { runShadowObservabilityPass } from '../services/shadow-observability-pass';
import { observeWorkerShadow } from '../services/shadow-observability';
import { WorkerWakeReconciliation } from '../services/worker-wake-reconciliation';
import { WORKER_WAKE_KINDS } from '../services/worker-wake-types';
import { createScheduledWorkerServer, ScheduledCleanupError } from './server';

export async function startScheduledWorker(): Promise<void> {
  const config = getConfig();
  const identity = requestWorkerConfig();
  if (!config.TELEMETRY_SHADOW_ENABLED || config.TELEMETRY_PROVIDER !== 'none' ||
      config.CUSTOMER_METRICS_PUBLISHER !== 'noop') throw new Error('Invalid scheduled worker telemetry');
  // Validate destination before any database/listener activity or credential discovery.
  const productionDelivery = z.enum(['true', 'false']).default('false').parse(process.env.CUSTOMER_METRICS_DURABLE_DELIVERY) === 'true';
  const productionTopic = process.env.CUSTOMER_METRICS_PRODUCTION_TOPIC ?? '';
  if (productionDelivery) await new ProductionCustomerPublisher(productionTopic).close();
  const topic = process.env.CUSTOMER_METRICS_SHADOW_TOPIC ?? '';
  const publisher = new ShadowPubSubPublisher(topic, config.CUSTOMER_METRICS_GCP_PUBSUB_TOPIC);
  await publisher.close();
  const pool = new Pool({ connectionString: config.DATABASE_URL, max: 1,
    application_name: 'persistio:scheduled-control', connectionTimeoutMillis: 3000,
    statement_timeout: 5000, lock_timeout: 1000, idle_in_transaction_session_timeout: 5000 });
  pool.on('error', () => process.exit(1));
  // Explicit deployment migration gate. Do not run migrations on cold start.
  await pool.query('SELECT event_id FROM customer_metric_shadow_outbox LIMIT 0');
  if (productionDelivery) await pool.query('SELECT event_id FROM customer_metric_outbox LIMIT 0');
  await pool.query('SELECT kind FROM worker_wake_intents LIMIT 0');
  await pool.query('SELECT kind FROM worker_observation_success LIMIT 0');
  const observationFlags = z.object({
    OBSERVE_CURATION_ENABLED: z.enum(['true', 'false']),
    OBSERVE_CONTRADICTION_ENABLED: z.enum(['true', 'false'])
  }).parse(process.env);
  const observationOptions = { enabled: true, productionMetrics: productionDelivery,
    curationEnabled: observationFlags.OBSERVE_CURATION_ENABLED === 'true',
    contradictionEnabled: observationFlags.OBSERVE_CONTRADICTION_ENABLED === 'true' };
  const reconciliation = new WorkerWakeReconciliation(pool, observationOptions);
  const wakeDispatcher = configuredWakeDispatcher(pool, observationOptions);
  const runtime = createScheduledWorkerServer({
    authenticate: createWorkerAuthenticator(identity), fatal: () => process.exit(1),
    async observe() {
      // One scheduled request owns all control work. Separate schedules on this
      // concurrency-one host would repeatedly collide. Independent network work
      // overlaps, but SQL remains bounded by the single control connection.
      const shadow = new ShadowPubSubPublisher(topic, config.CUSTOMER_METRICS_GCP_PUBSUB_TOPIC);
      const production = productionDelivery ? new ProductionCustomerPublisher(productionTopic) : undefined;
      try {
        const deliveries = [new ShadowCustomerMetricDispatcher(pool, shadow, { batchSize: 100 })];
        if (production) deliveries.push(new ShadowCustomerMetricDispatcher(pool, production, { production: true, batchSize: 100 }));
        return await runShadowObservabilityPass({
          async dispatch() {
            const results = await Promise.allSettled([
              ...deliveries.map(dispatcher => dispatcher.dispatchOnce()),
              ...(wakeDispatcher ? [wakeDispatcher.dispatchOnce().then(report => {
                if (Object.keys(report.failures).length) throw new Error('Wake dispatch failed');
                return { delivered: 0, retried: 0, stale: 0, dead: 0, purged: 0 };
              })] : [])
            ]);
            // Await every sibling before propagating failure or closing SDKs.
            if (results.some(result => result.status === 'rejected')) throw new Error('Control pass failed');
            return results.reduce((sum, result) => {
              if (result.status === 'fulfilled') for (const key of ['delivered','retried','stale','dead','purged'] as const) sum[key] += result.value[key];
              return sum;
            }, { delivered: 0, retried: 0, stale: 0, dead: 0, purged: 0 });
          },
          observe: () => observeWorkerShadow(pool, observationOptions)
        });
      } finally {
        const closed = await Promise.allSettled([shadow.close(), production?.close()]);
        const failed = closed.find(result => result.status === 'rejected');
        if (failed?.status === 'rejected') throw new ScheduledCleanupError('Publisher cleanup failed');
      }
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
