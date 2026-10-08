import path from 'node:path';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

const url = process.env.PERSISTIO_TEST_DATABASE_URL;
const built = path.resolve(__dirname, '../..', 'dist');
describe.skipIf(!url)('built scheduled foundation startup (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: url });
  beforeAll(async () => {
    if (!url || url !== process.env.DATABASE_URL || !/^persistio_test_/.test(new URL(url).pathname.slice(1))) throw new Error('Use matching isolated persistio_test_ URLs');
    const db = await import('../db/client'); try { await db.runMigrations(); } finally { await db.closePool(); }
  });
  afterAll(() => pool.end());
  it('starts without migrations, credentials for AI or idle work; private probe rejects unsigned requests', async () => {
    const reserve = net.createServer(); await new Promise<void>(resolve => reserve.listen(0, '127.0.0.1', resolve));
    const port = (reserve.address() as net.AddressInfo).port;
    await new Promise<void>(resolve => reserve.close(() => resolve()));
    const lock = await pool.connect(); await lock.query('SELECT pg_advisory_lock(734920174600360)');
    const child = spawn(process.execPath, ['--require', path.join(built, 'preload.js'), path.join(built, 'scheduled-worker/main.js')], {
      env: { PATH: process.env.PATH, NODE_ENV: 'test', DATABASE_URL: url!, PORT: String(port),
        PERSISTIO_MODE: 'request-worker', EMBEDDER_PROVIDER: 'vertex', VERTEX_PROJECT_ID: 'persistio-test',
        TELEMETRY_PROVIDER: 'none', TELEMETRY_SHADOW_ENABLED: 'true', CUSTOMER_METRICS_PUBLISHER: 'noop',
        CUSTOMER_METRICS_GCP_PUBSUB_TOPIC: 'customer-metrics', CUSTOMER_METRICS_SHADOW_TOPIC: 'projects/persistio-test/topics/customer-metrics-shadow',
        OBSERVE_CURATION_ENABLED: 'true', OBSERVE_CONTRADICTION_ENABLED: 'false',
        REQUEST_WORKER_ENABLED: 'true', REQUEST_WORKER_ADMISSION_ENABLED: 'false',
        REQUEST_WORKER_AUDIENCE: 'https://observer-example.run.app',
        REQUEST_WORKER_INVOKER_EMAIL: 'scheduler@persistio-test.iam.gserviceaccount.com',
        REQUEST_WORKER_INVOKER_SUBJECT: '123456789012345678901' }, stdio: ['ignore', 'pipe', 'pipe']
    });
    let output = '';
    child.stdout.on('data', chunk => { output += chunk; }); child.stderr.on('data', chunk => { output += chunk; });
    const exit = new Promise<number | null>(resolve => child.once('exit', resolve));
    try {
      await vi.waitFor(async () => {
        expect(child.exitCode, output).toBeNull();
        expect((await fetch(`http://127.0.0.1:${port}/health`)).status).toBe(200);
      }, { timeout: 10000, interval: 100 });
      for (const endpoint of ['observe', 'reconcile']) {
        expect((await fetch(`http://127.0.0.1:${port}/internal/${endpoint}`, { method: 'POST' })).status).toBe(401);
      }
      expect(output).not.toMatch(/shadow_worker_observation|shadow_customer_delivery|worker started|Running migration|OTLP HTTP initialised/);
      const sessions = (await pool.query("SELECT application_name FROM pg_stat_activity WHERE datname=current_database() AND application_name LIKE 'persistio:%'")).rows;
      expect(sessions).toEqual([{ application_name: 'persistio:scheduled-control' }]);
      child.kill('SIGTERM'); expect(await exit).toBe(0);
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
      await exit; await lock.query('SELECT pg_advisory_unlock(734920174600360)'); lock.release();
    }
  }, 15000);
});
