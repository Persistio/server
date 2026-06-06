import Fastify from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { AppConfig } from '../config';

const { poolQueryMock } = vi.hoisted(() => ({
  poolQueryMock: vi.fn()
}));

vi.mock('../db/client', () => ({
  pool: {
    query: poolQueryMock
  }
}));

import { registerHealthRoutes } from './health';

describe('health route', () => {
  beforeEach(() => {
    poolQueryMock.mockReset();
  });

  async function buildApp() {
    const app = Fastify();
    await registerHealthRoutes(app, { HEALTH_API_KEY: '' } as AppConfig);
    return app;
  }

  it('returns separate queue depths without the legacy aggregate queue_depth field', async () => {
    poolQueryMock
      .mockResolvedValueOnce({ rows: [{ ok: 1 }] })
      .mockResolvedValueOnce({ rows: [{ extraction_depth: 2, extraction_inflight_depth: 3, curation_depth: 1 }] });

    const app = await buildApp();
    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      status: 'ok',
      db: 'ok',
      extraction_queue_depth: 2,
      extraction_inflight_depth: 3,
      curation_queue_depth: 1
    });
    expect(response.json()).not.toHaveProperty('queue_depth');

    await app.close();
  });

  it('omits queue_depth from degraded responses', async () => {
    poolQueryMock
      .mockRejectedValueOnce(new Error('database unavailable'))
      .mockResolvedValueOnce({ rows: [{ extraction_depth: 2, extraction_inflight_depth: 3, curation_depth: 1 }] });

    const app = await buildApp();
    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({
      status: 'degraded',
      db: 'degraded',
      extraction_queue_depth: null,
      extraction_inflight_depth: null,
      curation_queue_depth: null
    });
    expect(response.json()).not.toHaveProperty('queue_depth');

    await app.close();
  });
});
