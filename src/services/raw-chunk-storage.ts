import fs from 'node:fs/promises';
import path from 'node:path';

import { DefaultAzureCredential } from '@azure/identity';
import { BlobServiceClient } from '@azure/storage-blob';
import { Storage, type Bucket } from '@google-cloud/storage';

import { getConfig } from '../config';
import { resolveRawChunkLocalRoot } from './raw-chunk-storage-config';

export type RawChunkBlobStore = 'local' | 'azure_blob' | 'gcs';

export interface RawChunkReference {
  blobStore: RawChunkBlobStore;
  blobKey: string;
}

export interface RawChunkStorage {
  readonly store: RawChunkBlobStore;
  put(key: string, content: string): Promise<RawChunkReference>;
  get(key: string): Promise<string>;
  delete(key: string): Promise<void>;
}

export function createRawChunkBlobKey(vaultId: string, sessionId: string, chunkId: string): string {
  return [
    'vaults',
    encodeURIComponent(vaultId),
    'sessions',
    encodeURIComponent(sessionId),
    'chunks',
    `${encodeURIComponent(chunkId)}.txt`
  ].join('/');
}

class LocalRawChunkStorage implements RawChunkStorage {
  readonly store = 'local' as const;

  private readonly rootDir: string;

  constructor(rootDir: string) {
    this.rootDir = resolveRawChunkLocalRoot(rootDir);
  }

  async put(key: string, content: string): Promise<RawChunkReference> {
    const filePath = this.resolvePath(key);
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, content, 'utf8');
    return { blobStore: this.store, blobKey: key };
  }

  async get(key: string): Promise<string> {
    return fs.readFile(this.resolvePath(key), 'utf8');
  }

  async delete(key: string): Promise<void> {
    await fs.rm(this.resolvePath(key), { force: true });
  }

  private resolvePath(key: string): string {
    const root = path.resolve(this.rootDir);
    const target = path.resolve(root, key);
    if (target !== root && !target.startsWith(`${root}${path.sep}`)) {
      throw new Error(`Raw chunk key resolves outside storage root: ${key}`);
    }
    return target;
  }
}

class AzureBlobRawChunkStorage implements RawChunkStorage {
  readonly store = 'azure_blob' as const;

  private readonly containerClient;
  private containerReady: Promise<void> | undefined;

  constructor() {
    const config = getConfig();
    const serviceClient = config.AZURE_STORAGE_CONNECTION_STRING
      ? BlobServiceClient.fromConnectionString(config.AZURE_STORAGE_CONNECTION_STRING)
      : new BlobServiceClient(
        `https://${config.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
        new DefaultAzureCredential()
      );
    this.containerClient = serviceClient.getContainerClient(config.RAW_CHUNK_BLOB_CONTAINER);
  }

  async put(key: string, content: string): Promise<RawChunkReference> {
    await this.ensureContainer();
    const blockBlobClient = this.containerClient.getBlockBlobClient(key);
    await blockBlobClient.upload(content, Buffer.byteLength(content, 'utf8'), {
      blobHTTPHeaders: {
        blobContentType: 'text/plain; charset=utf-8'
      }
    });
    return { blobStore: this.store, blobKey: key };
  }

  async get(key: string): Promise<string> {
    const blockBlobClient = this.containerClient.getBlockBlobClient(key);
    const response = await blockBlobClient.download();
    return streamToString(response.readableStreamBody);
  }

  async delete(key: string): Promise<void> {
    await this.containerClient.getBlockBlobClient(key).deleteIfExists();
  }

  private async ensureContainer(): Promise<void> {
    this.containerReady ??= this.containerClient.createIfNotExists().then(() => undefined);
    return this.containerReady;
  }
}

class GcsRawChunkStorage implements RawChunkStorage {
  readonly store = 'gcs' as const;

  private readonly bucket: Bucket;

  constructor() {
    const config = getConfig();
    const storage = new Storage();
    this.bucket = storage.bucket(config.RAW_CHUNK_GCS_BUCKET);
  }

  async put(key: string, content: string): Promise<RawChunkReference> {
    const file = this.bucket.file(key);
    await file.save(content, {
      contentType: 'text/plain; charset=utf-8',
      resumable: false
    });
    return { blobStore: this.store, blobKey: key };
  }

  async get(key: string): Promise<string> {
    const [content] = await this.bucket.file(key).download();
    return content.toString('utf8');
  }

  async delete(key: string): Promise<void> {
    await this.bucket.file(key).delete({ ignoreNotFound: true });
  }
}

async function streamToString(stream: NodeJS.ReadableStream | undefined): Promise<string> {
  if (!stream) {
    return '';
  }

  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString('utf8');
}

let cachedStorage: RawChunkStorage | undefined;

export function getRawChunkStorage(): RawChunkStorage {
  if (cachedStorage) {
    return cachedStorage;
  }

  const config = getConfig();
  if (config.RAW_CHUNK_STORAGE_PROVIDER === 'azure_blob') {
    cachedStorage = new AzureBlobRawChunkStorage();
  } else if (config.RAW_CHUNK_STORAGE_PROVIDER === 'gcs') {
    cachedStorage = new GcsRawChunkStorage();
  } else {
    cachedStorage = new LocalRawChunkStorage(config.RAW_CHUNK_LOCAL_DIR);
  }
  return cachedStorage;
}
