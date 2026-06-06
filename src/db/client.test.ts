import type { PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';

import { createPgvectorVerifyHook, getConfiguredPoolMax, validateStorageEmbeddingDimensions } from './client';

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
  it('uses DB_POOL_MAX directly as the configured pool size', () => {
    expect(getConfiguredPoolMax({
      DB_POOL_MAX: 20,
      EXTRACTION_WORKER_CONCURRENCY: 5
    } as Parameters<typeof getConfiguredPoolMax>[0])).toBe(20);

    expect(getConfiguredPoolMax({
      DB_POOL_MAX: 50,
      EXTRACTION_WORKER_CONCURRENCY: 5
    } as Parameters<typeof getConfiguredPoolMax>[0])).toBe(50);
  });
});

describe('embedding dimension validation', () => {
  it('accepts pgvector columns that match the configured storage dimension', async () => {
    const client = {
      query: vi.fn().mockResolvedValue({
        rows: [
          { table_name: 'entity_aliases', column_type: 'vector(1024)' },
          { table_name: 'memories', column_type: 'vector(1024)' },
          { table_name: 'memory_embeddings', column_type: 'vector(1024)' },
          { table_name: 'raw_chunks', column_type: 'vector(1024)' }
        ]
      })
    };

    await expect(validateStorageEmbeddingDimensions(client as never, 1024)).resolves.toBeUndefined();
    expect(client.query.mock.calls[0]?.[0]).toContain('format_type');
  });

  it('rejects pgvector columns that do not match the configured storage dimension', async () => {
    const client = {
      query: vi.fn().mockResolvedValue({
        rows: [
          { table_name: 'memories', column_type: 'vector(1536)' },
          { table_name: 'raw_chunks', column_type: 'vector(1536)' }
        ]
      })
    };

    await expect(validateStorageEmbeddingDimensions(client as never, 1024))
      .rejects.toThrow('Configured STORAGE_EMBEDDING_DIMENSIONS=1024 does not match pgvector columns');
  });
});
