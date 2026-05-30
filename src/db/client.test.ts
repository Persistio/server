import type { PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';

import { createPgvectorVerifyHook, getConfiguredPoolMax } from './client';

describe('pgvector pool verification', () => {
  it('skips registration before migrations enable pgvector type loading', () => {
    const registerType = vi.fn<(client: PoolClient) => Promise<void>>().mockResolvedValue();
    const verify = createPgvectorVerifyHook(registerType, () => false);
    const callback = vi.fn();

    verify({} as PoolClient, callback);

    expect(registerType).not.toHaveBeenCalled();
    expect(callback).toHaveBeenCalledOnce();
    expect(callback).toHaveBeenCalledWith();
  });

  it('waits for pgvector registration before releasing a fresh client to pg-pool', async () => {
    let resolveRegistration!: () => void;
    const registration = new Promise<void>((resolve) => {
      resolveRegistration = resolve;
    });
    const registerType = vi.fn<(client: PoolClient) => Promise<void>>().mockReturnValue(registration);
    const verify = createPgvectorVerifyHook(registerType, () => true);
    const callback = vi.fn();
    const client = {} as PoolClient;

    verify(client, callback);

    expect(registerType).toHaveBeenCalledWith(client);
    expect(callback).not.toHaveBeenCalled();

    resolveRegistration();
    await registration;
    await new Promise((resolve) => setImmediate(resolve));

    expect(callback).toHaveBeenCalledOnce();
    expect(callback).toHaveBeenCalledWith();
  });

  it('passes registration failures back to pg-pool', async () => {
    const error = new Error('vector type not found');
    const registerType = vi.fn<(client: PoolClient) => Promise<void>>().mockRejectedValue(error);
    const verify = createPgvectorVerifyHook(registerType, () => true);
    const callback = vi.fn();

    verify({} as PoolClient, callback);

    await new Promise((resolve) => setImmediate(resolve));

    expect(callback).toHaveBeenCalledOnce();
    expect(callback).toHaveBeenCalledWith(error);
  });
});

describe('pool sizing', () => {
  it('caps the worker-derived pool size at DB_POOL_MAX', () => {
    expect(getConfiguredPoolMax({
      DB_POOL_MAX: 20,
      EXTRACTION_WORKER_CONCURRENCY: 5
    } as Parameters<typeof getConfiguredPoolMax>[0])).toBe(12);

    expect(getConfiguredPoolMax({
      DB_POOL_MAX: 8,
      EXTRACTION_WORKER_CONCURRENCY: 5
    } as Parameters<typeof getConfiguredPoolMax>[0])).toBe(8);
  });
});
