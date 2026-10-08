import path from 'node:path';
import net from 'node:net';
import { spawn, type ChildProcess } from 'node:child_process';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

const url = process.env.PERSISTIO_TEST_DATABASE_URL;
const built = path.resolve(__dirname, '../../..', 'dist');
describe.skipIf(!url)('built private-worker startup (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: url });
  beforeAll(async () => {
    if (!url || url !== process.env.DATABASE_URL || !/^persistio_test_/.test(new URL(url).pathname.slice(1))) throw new Error('Use matching isolated persistio_test_ URLs');
    const db = await import('../../db/client'); try { await db.runMigrations(); } finally { await db.closePool(); }
  });
  afterAll(() => pool.end());
  async function start(extra: Record<string, string> = {}) {
    const reserve = net.createServer(); await new Promise<void>(resolve => reserve.listen(0, '127.0.0.1', resolve));
    const port = (reserve.address() as net.AddressInfo).port; await new Promise<void>(resolve => reserve.close(() => resolve()));
    const child = spawn(process.execPath, ['--require', path.join(built, 'preload.js'), path.join(built, 'index.js')], {
      env: { ...process.env, DATABASE_URL: url!, ADMIN_API_KEY: '', ENCRYPTION_ENABLED: 'false', PERSISTIO_MODE: 'request-worker', PORT: String(port),
        TELEMETRY_PROVIDER: 'none', CUSTOMER_METRICS_PUBLISHER: 'noop', REQUEST_WORKER_ENABLED: 'true',
        REQUEST_WORKER_AUDIENCE: 'https://worker-example.run.app', REQUEST_WORKER_INVOKER_EMAIL: 'tasks@example-project.iam.gserviceaccount.com',
        REQUEST_WORKER_INVOKER_SUBJECT: '123456789012345678901', ...extra }, stdio: ['ignore', 'pipe', 'pipe']
    });
    let output = ''; child.stdout!.on('data', chunk => { output += chunk; }); child.stderr!.on('data', chunk => { output += chunk; });
    const exit = new Promise<number | null>(resolve => child.once('exit', resolve));
    return { child, exit, base: 'http://127.0.0.1:' + port, output: () => output };
  }
  it('starts while the migration lock is held, exposes no customer routes and idles without business connections', async () => {
    const lock = await pool.connect(); await lock.query('SELECT pg_advisory_lock(734920174600360)');
    const run = await start({ REQUEST_WORKER_ADMISSION_ENABLED: 'false', REQUEST_WORKER_CUTOVER_ENABLED: 'true', CUSTOMER_METRICS_DURABLE_CAPTURE: 'true', PLATFORM_AUTH_MODE: 'oauth', PLATFORM_OAUTH_ISSUER: '', PLATFORM_OAUTH_AUDIENCE: '', PLATFORM_OAUTH_CLIENT_POLICIES: '' });
    try {
      await vi.waitFor(async () => {
        expect(run.child.exitCode, run.output()).toBeNull();
        expect((await fetch(run.base + '/health')).status).toBe(200);
      }, { timeout: 7000, interval: 100 });
      expect((await fetch(run.base + '/v1/memories')).status).toBe(404);
      expect((await fetch(run.base + '/internal/worker/wake', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' })).status).toBe(401);
      await new Promise(resolve => setTimeout(resolve, 500));
      const sessions = (await pool.query("SELECT application_name FROM pg_stat_activity WHERE datname=current_database() AND application_name LIKE 'persistio:%'")).rows;
      expect(sessions).toEqual([{ application_name: 'persistio:request-control' }]);
      expect(run.output()).not.toMatch(/worker started|Running migration|OTLP HTTP initialised|Azure Monitor initialised/);
      run.child.kill('SIGTERM'); expect(await run.exit).toBe(0);
    } finally {
      if (run.child.exitCode === null) run.child.kill('SIGKILL');
      await run.exit; await lock.query('SELECT pg_advisory_unlock(734920174600360)'); lock.release();
    }
  }, 12000);
  it.each([
    { REQUEST_WORKER_ENABLED: 'false' }, { REQUEST_WORKER_CUTOVER_ENABLED: 'true', CUSTOMER_METRICS_DURABLE_CAPTURE: 'false' }, { TELEMETRY_PROVIDER: 'gcp_otlp' }, { CUSTOMER_METRICS_PUBLISHER: 'log' }
  ])('refuses unsafe request-mode startup %j', async extra => {
    const run = await start(extra);
    try {
      await vi.waitFor(() => expect(run.child.exitCode).not.toBeNull(), { timeout: 7000 });
      expect(await run.exit).toBe(1);
      expect(run.output()).not.toMatch(/OTLP HTTP initialised|Azure Monitor initialised/);
    } finally { if (run.child.exitCode === null) { run.child.kill('SIGKILL'); await run.exit; } }
  }, 10000);
});
