import Fastify from 'fastify';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { getConfig } from '../config';
import { registerCurationRoutes } from './curation';

const { query } = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock('../db/client', () => ({ query, withTransaction: vi.fn() }));
vi.mock('../middleware/auth', () => ({
  getAuthAccountId: () => '6ed6b673-5d4a-437a-88af-80b004e35200',
  requireAdminScope: () => async () => {},
  requireVaultStatsAuth: async (request: any) => { request.vault = { id: '1e9dfb51-5ca0-472a-8856-32c29f3e2a49' }; }
}));
afterEach(() => { getConfig().API_REQUEST_BILLING = false; vi.resetAllMocks(); });

const routes = ['/v1/curation', '/admin/vaults/1e9dfb51-5ca0-472a-8856-32c29f3e2a49/curation'];
describe.each(routes)('curation response settlement: %s', route => {
  it.each([
    ['capacity', 'resolve'], ['capacity', 'reject'],
    ['recent', 'resolve'], ['recent', 'reject']
  ])('waits when %s fails and its sibling will %s, preserving the first error', async (failedQuery, siblingOutcome) => {
    getConfig().API_REQUEST_BILLING = true;
    const app = Fastify(); await registerCurationRoutes(app);
    let release!: () => void; let entered!: () => void; let sent = false;
    const pendingStarted = new Promise<void>(resolve => { entered = resolve; });
    const original = Object.assign(new Error('Original SQL failure'), { statusCode: 503 });
    query.mockImplementation(async (sql: string) => {
      const kind = sql.includes('v.plan_id') ? 'capacity' : sql.includes('FROM curation_review_runs') ? 'recent' : 'access';
      if (kind === 'access') return { rowCount: 1, rows: [{ id: '1e9dfb51-5ca0-472a-8856-32c29f3e2a49' }] };
      if (kind === failedQuery) throw original;
      return new Promise((resolve, reject) => {
        release = () => siblingOutcome === 'resolve' ? resolve({ rowCount: 0, rows: [] }) : reject(new Error('Later sibling failure'));
        entered();
      });
    });
    const response = app.inject(route).then(result => { sent = true; return result; });
    try {
      await pendingStarted; await new Promise(resolve => setImmediate(resolve));
      expect(sent).toBe(false);
      release();
      expect((await response).statusCode).toBe(503);
      expect(query).toHaveBeenCalledTimes(route.startsWith('/admin') ? 3 : 2);
    } finally { release(); await response; await app.close(); }
  });
});
