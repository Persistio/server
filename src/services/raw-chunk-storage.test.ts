import { describe, expect, it, vi } from 'vitest';

const gcsMock = vi.hoisted(() => {
  const objects = new Map<string, string>();
  return {
    objects,
    bucketNames: [] as string[],
    save: vi.fn(async (key: string, content: string) => {
      objects.set(key, content);
    }),
    download: vi.fn(async (key: string) => [Buffer.from(objects.get(key) ?? '', 'utf8')]),
    delete: vi.fn(async (key: string) => {
      objects.delete(key);
    })
  };
});

vi.mock('../config', () => ({
  getConfig: () => ({
    RAW_CHUNK_STORAGE_PROVIDER: 'gcs',
    RAW_CHUNK_GCS_BUCKET: 'persistio-raw-chunks',
    RAW_CHUNK_LOCAL_DIR: './data/raw-chunks',
    RAW_CHUNK_BLOB_CONTAINER: 'raw-chunks',
    AZURE_STORAGE_CONNECTION_STRING: '',
    AZURE_STORAGE_ACCOUNT_NAME: ''
  })
}));

vi.mock('@google-cloud/storage', () => ({
  Storage: class {
    bucket(name: string) {
      gcsMock.bucketNames.push(name);
      return {
        file: (key: string) => ({
          save: (content: string) => gcsMock.save(key, content),
          download: () => gcsMock.download(key),
          delete: () => gcsMock.delete(key)
        })
      };
    }
  }
}));

import { getRawChunkStorage } from './raw-chunk-storage';

describe('GCS raw chunk storage', () => {
  it('stores, reads, and deletes raw chunks in the configured bucket', async () => {
    const storage = getRawChunkStorage();

    const ref = await storage.put('vaults/v1/sessions/s1/chunks/c1.txt', 'hello world');
    const content = await storage.get(ref.blobKey);
    await storage.delete(ref.blobKey);

    expect(ref).toEqual({
      blobStore: 'gcs',
      blobKey: 'vaults/v1/sessions/s1/chunks/c1.txt'
    });
    expect(content).toBe('hello world');
    expect(gcsMock.bucketNames).toContain('persistio-raw-chunks');
    expect(gcsMock.save).toHaveBeenCalledWith('vaults/v1/sessions/s1/chunks/c1.txt', 'hello world');
    expect(gcsMock.delete).toHaveBeenCalledWith('vaults/v1/sessions/s1/chunks/c1.txt');
    expect(gcsMock.objects.has(ref.blobKey)).toBe(false);
  });
});
