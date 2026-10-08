import Fastify from 'fastify';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { getConfig } from '../config';
import { auxiliaryQuery, closeAuxiliaryPool } from '../db/auxiliary';
import { registerCustomerRequestObservations } from './customer-api-request-metrics';

const url = process.env.PERSISTIO_TEST_DATABASE_URL;
describe.skipIf(!url)('request-billed API durable handoff (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: url, max: 3 });
  const config = getConfig(); let db: typeof import('../db/client');
  beforeAll(async () => {
    if (url !== process.env.DATABASE_URL || !new URL(url!).pathname.startsWith('/persistio_test_')) throw new Error('Use an isolated matching test database');
    db = await import('../db/client'); await db.runMigrations();
    config.API_REQUEST_BILLING = true; config.CUSTOMER_METRICS_DURABLE_CAPTURE = true;
  });
  afterAll(async () => {
    config.API_REQUEST_BILLING = false; config.CUSTOMER_METRICS_DURABLE_CAPTURE = false;
    await pool.query("DELETE FROM customer_metric_outbox WHERE payload->>'workspace_id'='api-billing-test'");
    await closeAuxiliaryPool(); await pool.end(); await db?.closePool();
  });
  it('commits an observation before response and keeps business response on outbox lock timeout', async () => {
    const api = Fastify(); registerCustomerRequestObservations(api, true);
    api.addHook('preHandler', async request => { request.auth = { account_id: 'api-billing-test', method: 'oauth' } as any; });
    api.get('/v1/memories', async () => ({ committed: true }));
    try {
      expect((await api.inject('/v1/memories')).json()).toEqual({ committed: true });
      expect((await pool.query("SELECT payload FROM customer_metric_outbox WHERE payload->>'workspace_id'='api-billing-test'")).rows).toHaveLength(1);
      const lock = await pool.connect(); await lock.query('BEGIN'); await lock.query('LOCK customer_metric_outbox IN ACCESS EXCLUSIVE MODE');
      try {
        const before = Date.now();
        expect((await api.inject('/v1/memories')).json()).toEqual({ committed: true });
        expect(Date.now() - before).toBeLessThan(1500);
      } finally { await lock.query('ROLLBACK'); lock.release(); }
      expect((await pool.query("SELECT payload FROM customer_metric_outbox WHERE payload->>'workspace_id'='api-billing-test'")).rows).toHaveLength(1);
    } finally { await api.close(); }
  });
  it('server timeout settles SQL, destroys the failed connection, and a subsequent operation succeeds', async () => {
    const pid = (await auxiliaryQuery('SELECT pg_backend_pid() AS pid')).rows[0].pid;
    await expect(auxiliaryQuery('SELECT pg_sleep(5)')).rejects.toThrow('statement timeout');
    const next = (await auxiliaryQuery('SELECT pg_backend_pid() AS pid')).rows[0].pid;
    expect(next).not.toBe(pid);
    expect((await pool.query('SELECT 1 FROM pg_stat_activity WHERE pid=$1', [pid])).rowCount).toBe(0);
  });
  it('caps checkout wait under auxiliary saturation', async () => {
    const work = [auxiliaryQuery('SELECT pg_sleep(0.45)'), auxiliaryQuery('SELECT pg_sleep(0.45)')];
    await new Promise(resolve => setTimeout(resolve, 30));
    await expect(auxiliaryQuery('SELECT 1')).rejects.toThrow();
    await Promise.all(work);
    expect((await auxiliaryQuery('SELECT 1 AS ok')).rows[0].ok).toBe(1);
  });
});
