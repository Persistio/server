#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { DefaultAzureCredential } from '@azure/identity';
import { BlobServiceClient } from '@azure/storage-blob';
import { Storage } from '@google-cloud/storage';
import pg from 'pg';

const args = new Map();
for (let index = 2; index < process.argv.length; index += 1) {
  const arg = process.argv[index];
  if (!arg.startsWith('--')) continue;
  const [key, inlineValue] = arg.slice(2).split('=', 2);
  const value = inlineValue ?? (process.argv[index + 1]?.startsWith('--') ? 'true' : process.argv[++index] ?? 'true');
  args.set(key, value);
}

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

const provider = process.env.RAW_CHUNK_STORAGE_PROVIDER ?? 'local';
const batchSize = Number(args.get('batch-size') ?? 250);
const dryRun = args.has('dry-run');
const vaultId = args.get('vault-id');
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const serverPackageRoot = path.resolve(scriptDir, '..');

if (!['local', 'azure_blob', 'gcs'].includes(provider)) {
  throw new Error(`Unsupported RAW_CHUNK_STORAGE_PROVIDER: ${provider}`);
}

if (!Number.isInteger(batchSize) || batchSize <= 0) {
  throw new Error('--batch-size must be a positive integer');
}

function createRawChunkBlobKey(vaultId, sessionId, chunkId) {
  return [
    'vaults',
    encodeURIComponent(vaultId),
    'sessions',
    encodeURIComponent(sessionId),
    'chunks',
    `${encodeURIComponent(chunkId)}.txt`
  ].join('/');
}

function checksum(value) {
  return crypto.createHash('sha256').update(value, 'utf8').digest('hex');
}

function resolveRawChunkLocalRoot(rootDir) {
  return path.isAbsolute(rootDir)
    ? rootDir
    : path.resolve(serverPackageRoot, rootDir);
}

function getRequiredLocalRootDir() {
  if (!process.env.RAW_CHUNK_LOCAL_DIR) {
    throw new Error('RAW_CHUNK_LOCAL_DIR is required when migrating with RAW_CHUNK_STORAGE_PROVIDER=local');
  }
  return resolveRawChunkLocalRoot(process.env.RAW_CHUNK_LOCAL_DIR);
}

class LocalStorage {
  store = 'local';

  constructor() {
    this.rootDir = getRequiredLocalRootDir();
  }

  async put(key, content) {
    const filePath = this.resolvePath(key);
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, content, 'utf8');
  }

  async get(key) {
    return fs.readFile(this.resolvePath(key), 'utf8');
  }

  async delete(key) {
    await fs.rm(this.resolvePath(key), { force: true });
  }

  resolvePath(key) {
    const root = path.resolve(this.rootDir);
    const target = path.resolve(root, key);
    if (target !== root && !target.startsWith(`${root}${path.sep}`)) {
      throw new Error(`Raw chunk key resolves outside storage root: ${key}`);
    }
    return target;
  }
}

class AzureBlobStorage {
  store = 'azure_blob';

  constructor() {
    const containerName = process.env.RAW_CHUNK_BLOB_CONTAINER ?? 'raw-chunks';
    const serviceClient = process.env.AZURE_STORAGE_CONNECTION_STRING
      ? BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING)
      : new BlobServiceClient(
        `https://${process.env.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
        new DefaultAzureCredential()
      );
    if (!process.env.AZURE_STORAGE_CONNECTION_STRING && !process.env.AZURE_STORAGE_ACCOUNT_NAME) {
      throw new Error('AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING is required for azure_blob migration');
    }
    this.containerClient = serviceClient.getContainerClient(containerName);
    this.containerReady = undefined;
  }

  async put(key, content) {
    this.containerReady ??= this.containerClient.createIfNotExists();
    await this.containerReady;
    await this.containerClient.getBlockBlobClient(key).upload(content, Buffer.byteLength(content, 'utf8'), {
      blobHTTPHeaders: {
        blobContentType: 'text/plain; charset=utf-8'
      }
    });
  }

  async get(key) {
    const response = await this.containerClient.getBlockBlobClient(key).download();
    const chunks = [];
    for await (const chunk of response.readableStreamBody ?? []) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString('utf8');
  }

  async delete(key) {
    await this.containerClient.getBlockBlobClient(key).deleteIfExists();
  }
}

class GcsStorage {
  store = 'gcs';

  constructor() {
    const bucketName = process.env.RAW_CHUNK_GCS_BUCKET;
    if (!bucketName) {
      throw new Error('RAW_CHUNK_GCS_BUCKET is required for gcs migration');
    }
    this.bucket = new Storage().bucket(bucketName);
  }

  async put(key, content) {
    await this.bucket.file(key).save(content, {
      contentType: 'text/plain; charset=utf-8',
      resumable: false
    });
  }

  async get(key) {
    const [content] = await this.bucket.file(key).download();
    return content.toString('utf8');
  }

  async delete(key) {
    await this.bucket.file(key).delete({ ignoreNotFound: true });
  }
}

function createStorage() {
  if (provider === 'azure_blob') {
    return new AzureBlobStorage();
  }
  if (provider === 'gcs') {
    return new GcsStorage();
  }
  return new LocalStorage();
}

const storage = dryRun ? undefined : createStorage();
const pool = new pg.Pool({ connectionString: databaseUrl });

let migrated = 0;
let lastId;

try {
  while (true) {
    const params = [];
    const predicates = [
      'blob_key IS NULL',
      'content IS NOT NULL'
    ];

    if (vaultId) {
      predicates.push(`vault_id = $${params.push(vaultId)}`);
    }

    if (lastId) {
      predicates.push(`id > $${params.push(lastId)}`);
    }

    const limitParam = params.push(batchSize);
    const result = await pool.query(
      `SELECT id, vault_id, session_id, content
       FROM raw_chunks
       WHERE ${predicates.join('\n         AND ')}
       ORDER BY id
       LIMIT $${limitParam}`,
      params
    );

    if (result.rows.length === 0) {
      break;
    }

    for (const row of result.rows) {
      const blobKey = createRawChunkBlobKey(row.vault_id, row.session_id, row.id);
      const contentSha256 = checksum(row.content);
      if (!dryRun) {
        if (!storage) {
          throw new Error('Raw chunk storage was not initialized');
        }
        let linkedInDatabase = false;
        try {
          await storage.put(blobKey, row.content);
          const uploaded = await storage.get(blobKey);
          if (checksum(uploaded) !== contentSha256) {
            throw new Error(`Checksum verification failed for raw chunk ${row.id}`);
          }
          const updateResult = await pool.query(
            `UPDATE raw_chunks
             SET blob_store = $2,
                 blob_key = $3,
                 content_sha256 = $4,
                 blob_migrated_at = now()
             WHERE id = $1`,
            [row.id, storage.store, blobKey, contentSha256]
          );
          if (updateResult.rowCount !== 1) {
            throw new Error(`Failed to mark raw chunk ${row.id} as migrated`);
          }
          linkedInDatabase = true;
        } catch (error) {
          if (!linkedInDatabase) {
            try {
              await storage.delete(blobKey);
            } catch (deleteError) {
              throw new AggregateError([error, deleteError], `Failed to migrate raw chunk ${row.id} and clean up uploaded blob`);
            }
          }
          throw error;
        }
      }
      migrated += 1;
      lastId = row.id;
      console.log(`${dryRun ? 'would migrate' : 'migrated'} ${row.id} -> ${provider}:${blobKey}`);
    }
  }

  console.log(`${dryRun ? 'Dry run complete' : 'Migration complete'}: ${migrated} raw chunks ${dryRun ? 'would be migrated' : 'migrated'}.`);
} finally {
  await pool.end();
}
