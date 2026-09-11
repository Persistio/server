import Fastify, { type FastifyRequest } from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { quota, embed, query, delivery } = vi.hoisted(() => ({
  quota: vi.fn(), embed: vi.fn(), query: vi.fn(), delivery: vi.fn()
}));
vi.mock('../middleware/auth', () => ({ requireVaultReadAuth: async (request: FastifyRequest) => {
  request.vault = { id: '11111111-1111-4111-8111-111111111111', vault_encryption_enabled: false } as FastifyRequest['vault'];
} }));
vi.mock('../config', () => ({ getConfig: () => ({ DEFAULT_RECALL_TOP_K: 5, MIN_RECALL_SIMILARITY: 0.3, GLOBAL_RULE_POLICY: 'approved_only' }) }));
vi.mock('../services/usage', () => ({ consumeApiQuota: quota, applyRateLimitHeaders: vi.fn() }));
vi.mock('../services/embedder', () => ({ getEmbedder: () => ({ embed }) }));
vi.mock('../db/client', () => ({ query }));
vi.mock('../services/raw-chunk-storage', () => ({ getRawChunkStorage: () => ({ get: vi.fn() }) }));
vi.mock('../services/memory-observability', async (original) => ({
  ...await original<typeof import('../services/memory-observability')>(), recordRecallDelivery: delivery
}));
import { registerRecallRoutes } from './recall';

describe('recall applicability request boundary', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    quota.mockResolvedValue({}); embed.mockResolvedValue([0.1, 0.2]);
    query.mockResolvedValue({ rows: [], rowCount: 0 });
    delivery.mockResolvedValue('22222222-2222-4222-8222-222222222222');
  });
  it.each(['', '?format=bundle', '?format=bundle_v2'])('validates raw/session combinations before any work for %s', async (suffix) => {
    const app = Fastify();
    await registerRecallRoutes(app);
    try {
      for (const context of [{}, { session_id: '' }, { session_id: ' ' }, { session_id: '\ns' }, { session_id: 's\t' }, { session_id: 's'.repeat(513) }]) {
        const response = await app.inject({ method: 'POST', url: `/v1/recall${suffix}`, payload: { query: 'fact', include_raw: true, context } });
        expect(response.statusCode).toBe(400);
      }
      if (suffix) {
        const response = await app.inject({ method: 'POST', url: `/v1/recall${suffix}`, payload: { query: 'fact', include_raw: true, context: { session_id: 'valid' } } });
        expect(response.statusCode).toBe(400);
      }
      for (const effect of [quota, embed, query, delivery]) expect(effect).not.toHaveBeenCalled();
    } finally { await app.close(); }
  });
  it.each(['', '?format=bundle', '?format=bundle_v2'])('still serves non-raw requests with and without session context for %s', async (suffix) => {
    const app = Fastify();
    await registerRecallRoutes(app);
    try {
      for (const context of [{}, { session_id: ' session-a ' }]) {
        const response = await app.inject({ method: 'POST', url: `/v1/recall${suffix}`, payload: { query: 'fact', include_raw: false, context } });
        expect(response.statusCode).toBe(200);
      }
      expect(quota).toHaveBeenCalledTimes(2);
      expect(delivery.mock.calls[1][0].context.session_id).toBe('session-a');
    } finally { await app.close(); }
  });
  it('binds valid raw recall to the normalized session and vault', async () => {
    const app = Fastify();
    await registerRecallRoutes(app);
    try {
      const response = await app.inject({ method: 'POST', url: '/v1/recall', payload: { query: 'fact', include_raw: true, context: { session_id: ' session-a ' } } });
      expect(response.statusCode).toBe(200);
      const raw = query.mock.calls.find(([sql]) => sql.includes('FROM raw_chunks'))!;
      expect(raw[0]).toContain('vault_id = $1');
      expect(raw[0]).toContain('session_id = $4');
      expect(raw[1][0]).toBe('11111111-1111-4111-8111-111111111111');
      expect(raw[1][3]).toBe('session-a');
    } finally { await app.close(); }
  });
  it.each([
    ['scheduled', false], ['unknown', false], ['backfill', false],
    ['direct', true], ['delegated', true]
  ] as const)('keeps the effective global-rule gate for %s context', async (trigger_type, effective) => {
    const app = Fastify();
    await registerRecallRoutes(app);
    try {
      const response = await app.inject({ method: 'POST', url: '/v1/recall?format=bundle_v2', payload: {
        query: 'fact', include_global_rules: true, context: { session_id: 'session-a', agent_id: 'main', trigger_type }
      } });
      expect(response.statusCode).toBe(200);
      expect(delivery.mock.calls[0][0].includeGlobalRulesEffective).toBe(effective);
    } finally { await app.close(); }
  });
});
