import fs from 'node:fs/promises';
import { createReadStream } from 'node:fs';
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
  /** Full object or rejection, never a prefix. Limits bytes before accumulation. */
  getBounded?(key: string, maxBytes: number): Promise<string>;
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
    await fs.writeFile(filePath, content, {encoding:'utf8',flag:'wx'});
    return { blobStore: this.store, blobKey: key };
  }

  async get(key: string): Promise<string> {
    return fs.readFile(this.resolvePath(key), 'utf8');
  }

  async getBounded(key: string, maxBytes: number): Promise<string> {
    return readBoundedRawStream(createReadStream(this.resolvePath(key)), maxBytes);
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
  private readonly uploadContainerClient;
  private containerReady: Promise<void> | undefined;

  constructor() {
    const config = getConfig();
    const createClient = (singleAttempt: boolean) => config.AZURE_STORAGE_CONNECTION_STRING
      ? BlobServiceClient.fromConnectionString(config.AZURE_STORAGE_CONNECTION_STRING,
        singleAttempt ? { retryOptions: { maxTries: 1 } } : {})
      : new BlobServiceClient(
        `https://${config.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
        new DefaultAzureCredential(),
        singleAttempt ? { retryOptions: { maxTries: 1 } } : {}
      );
    this.containerClient = createClient(false).getContainerClient(config.RAW_CHUNK_BLOB_CONTAINER);
    this.uploadContainerClient = createClient(true).getContainerClient(config.RAW_CHUNK_BLOB_CONTAINER);
  }

  async put(key: string, content: string): Promise<RawChunkReference> {
    await this.ensureContainer();
    const blockBlobClient = this.uploadContainerClient.getBlockBlobClient(key);
    await blockBlobClient.upload(content, Buffer.byteLength(content, 'utf8'), {
      conditions: { ifNoneMatch: '*' },
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

  async getBounded(key: string, maxBytes: number): Promise<string> {
    const response = await this.containerClient.getBlockBlobClient(key).download(0, maxBytes + 1);
    return readBoundedRawStream(response.readableStreamBody, maxBytes);
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
  private readonly uploadBucket: Bucket;

  constructor() {
    const config = getConfig();
    // A successful retry must not conceal an earlier still-indeterminate PUT.
    // Each application attempt has its own durable intent and unique object key.
    this.bucket = new Storage().bucket(config.RAW_CHUNK_GCS_BUCKET);
    this.uploadBucket = new Storage({ retryOptions: { autoRetry: false } }).bucket(config.RAW_CHUNK_GCS_BUCKET);
  }

  async put(key: string, content: string): Promise<RawChunkReference> {
    const file = this.uploadBucket.file(key);
    await file.save(content, {
      contentType: 'text/plain; charset=utf-8',
      resumable: false,
      preconditionOpts: { ifGenerationMatch: 0 }
    });
    return { blobStore: this.store, blobKey: key };
  }

  async get(key: string): Promise<string> {
    const [content] = await this.bucket.file(key).download();
    return content.toString('utf8');
  }

  async getBounded(key: string, maxBytes: number): Promise<string> {
    return readBoundedRawStream(this.bucket.file(key).createReadStream({ start: 0, end: maxBytes }), maxBytes);
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

/** A single overflow byte rejects the entire object; invalid UTF-8 is corruption. */
export async function readBoundedRawStream(stream: NodeJS.ReadableStream | undefined, maxBytes: number): Promise<string> {
  if (!stream || !Number.isSafeInteger(maxBytes) || maxBytes < 0) throw new Error('Invalid bounded source read');
  const chunks: Buffer[] = []; let bytes = 0;
  for await (const chunk of stream) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > maxBytes) { (stream as NodeJS.ReadableStream & {destroy?:()=>void}).destroy?.(); throw new Error('Source exceeds byte bound'); }
    chunks.push(buffer);
  }
  return new TextDecoder('utf-8', {fatal:true,ignoreBOM:true}).decode(Buffer.concat(chunks, bytes));
}
