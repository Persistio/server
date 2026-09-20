import { describe, expect, it, vi } from 'vitest';

const gcsMock = vi.hoisted(() => {
  const objects = new Map<string, string>();
  return {
    objects,
    bucketNames: [] as string[],
    save: vi.fn(async (key: string, content: string, options:any) => {
      if(objects.has(key) && options.preconditionOpts?.ifGenerationMatch===0)throw new Error('Object already exists');
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
          save: (content: string,options:any) => gcsMock.save(key, content,options),
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
    await expect(storage.put(ref.blobKey,'overwrite')).rejects.toThrow('already exists');
    const content = await storage.get(ref.blobKey);
    await storage.delete(ref.blobKey);

    expect(ref).toEqual({
      blobStore: 'gcs',
      blobKey: 'vaults/v1/sessions/s1/chunks/c1.txt'
    });
    expect(content).toBe('hello world');
    expect(gcsMock.bucketNames).toContain('persistio-raw-chunks');
    expect(gcsMock.save).toHaveBeenCalledWith('vaults/v1/sessions/s1/chunks/c1.txt', 'hello world',expect.objectContaining({preconditionOpts:{ifGenerationMatch:0}}));
    expect(gcsMock.delete).toHaveBeenCalledWith('vaults/v1/sessions/s1/chunks/c1.txt');
    expect(gcsMock.objects.has(ref.blobKey)).toBe(false);
  });
});
