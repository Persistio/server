import { Pool } from 'pg';
import { configuredWakeDispatcher } from '../services/worker-wake-runtime';
import { getConfig } from '../config';
import { requestWorkerConfig } from './config';
import { createWorkerAuthenticator } from './auth';
import { WorkerExecutionStore } from './store';
import { createRequestWorkerServer } from './server';

export async function startRequestWorker(): Promise<void> {
  const config = getConfig();
  const requestConfig = requestWorkerConfig();
  if (config.TELEMETRY_PROVIDER !== 'none' || config.CUSTOMER_METRICS_PUBLISHER !== 'noop') {
    throw new Error('Request-worker observability requires the PR4 delivery contract');
  }
  if (requestConfig.REQUEST_WORKER_CUTOVER_ENABLED === 'true' && !config.CUSTOMER_METRICS_DURABLE_CAPTURE) {
    throw new Error('Cutover admission requires durable production capture');
  }
  const pool = new Pool({
    connectionString: config.DATABASE_URL, application_name: 'persistio:request-control',
    max: 1, connectionTimeoutMillis: 3000, statement_timeout: 2000, lock_timeout: 1000,
    idle_in_transaction_session_timeout: 5000
  });
  pool.on('error', () => { process.exit(1); });
  // Migrations are explicit deployment work, never a cold-start side effect.
  await pool.query("SELECT id FROM worker_execution_slot WHERE id=1");
  if (config.CUSTOMER_METRICS_DURABLE_CAPTURE) await pool.query("SELECT event_id FROM customer_metric_outbox LIMIT 0");
  const dispatcher = configuredWakeDispatcher(pool, {
    curationEnabled: config.CURATOR_AUTO_RUN, contradictionEnabled: config.CONTRADICTION_SCAN_ENABLED });
  const runtime = createRequestWorkerServer({
    continueWork: dispatcher ? kind => dispatcher.dispatchOnce([kind]) : undefined,
    config: requestConfig, store: new WorkerExecutionStore(pool, config.CUSTOMER_METRICS_DURABLE_CAPTURE),
    authenticate: createWorkerAuthenticator(requestConfig), fatal: () => process.exit(1)
  });
  let stopping = false;
  const shutdown = () => {
    if (stopping) return;
    stopping = true;
    // Remain inside Cloud Run's termination allowance even on unknown I/O.
    const cutoff = setTimeout(() => process.exit(1), 8000);
    void runtime.stop().then(() => pool.end()).then(() => {
      clearTimeout(cutoff); process.exit(0);
    }, () => process.exit(1));
  };
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);
  await runtime.app.listen({ port: config.PORT, host: '0.0.0.0' });
}
