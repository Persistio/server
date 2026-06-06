#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import crypto from 'node:crypto';

import { DefaultAzureCredential, ManagedIdentityCredential } from '@azure/identity';
import { BlobServiceClient } from '@azure/storage-blob';
import { CryptographyClient, KeyClient } from '@azure/keyvault-keys';
import { KeyManagementServiceClient } from '@google-cloud/kms';
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
const ollamaBaseUrl = (process.env.OLLAMA_BASE_URL ?? 'http://localhost:11434').replace(/\/$/, '');
const ollamaModel = process.env.OLLAMA_EMBEDDING_MODEL ?? 'qwen3-embedding:0.6b';
const targetDimensions = Number(args.get('target-dimensions') ?? process.env.STORAGE_EMBEDDING_DIMENSIONS ?? 1024);
const batchSize = Number(args.get('batch-size') ?? 100);
const dryRun = args.has('dry-run');
const confirmed = args.has('confirm-maintenance-window');
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, '..');
const serverPackageRoot = repoRoot;
const rawChunkStorageReaders = new Map();

// ---------------------------------------------------------------------------
// Inline crypto helpers (mirrors src/services/crypto.ts)
// The script is plain ESM and cannot import TypeScript source directly.
// ---------------------------------------------------------------------------
const keyProvider = process.env.KEY_PROVIDER ?? 'azure_key_vault';
const keyVaultUri = process.env.KEY_VAULT_URI ?? '';
const kekKeyName = process.env.KEK_KEY_NAME ?? '';
const gcpKmsKeyName = process.env.GCP_KMS_KEY_NAME ?? '';
let azureCryptoClient = null;
let gcpKmsClient = null;
const dekCache = new Map();
const DEK_CACHE_TTL_MS = 5 * 60 * 1000;

async function initCryptoClient() {
  if (!isEncryptionProviderConfigured()) return; // no-op when encryption is not configured
  if (keyProvider === 'gcp_kms') {
    gcpKmsClient = new KeyManagementServiceClient();
    console.log('[reembed] GCP Cloud KMS crypto client initialised');
    return;
  }
  const credential = new ManagedIdentityCredential();
  const keyClient = new KeyClient(keyVaultUri, credential);
  const key = await keyClient.getKey(kekKeyName);
  azureCryptoClient = new CryptographyClient(key, credential);
  console.log('[reembed] Azure Key Vault crypto client initialised');
}

async function unwrapDek(encryptedDek) {
  if (keyProvider === 'gcp_kms') {
    if (!gcpKmsClient || !gcpKmsKeyName) {
      throw new Error('GCP Cloud KMS crypto client has not been initialised');
    }
    const [result] = await gcpKmsClient.decrypt({
      name: gcpKmsKeyName,
      ciphertext: Buffer.from(encryptedDek, 'base64')
    });
    return Buffer.from(result.plaintext ?? new Uint8Array());
  }

  if (!azureCryptoClient) throw new Error('Azure Key Vault crypto client has not been initialised');
  const result = await azureCryptoClient.unwrapKey('RSA-OAEP-256', Buffer.from(encryptedDek, 'base64'));
  return Buffer.from(result.result);
}

function isEncryptionProviderConfigured() {
  if (keyProvider === 'azure_key_vault') {
    return Boolean(keyVaultUri && kekKeyName);
  }
  if (keyProvider === 'gcp_kms') {
    return Boolean(gcpKmsKeyName);
  }
  throw new Error(`Unsupported KEY_PROVIDER: ${keyProvider}`);
}

function getEncryptionProviderConfigurationHint() {
  return keyProvider === 'gcp_kms'
    ? 'Set GCP_KMS_KEY_NAME for KEY_PROVIDER=gcp_kms.'
    : 'Set KEY_VAULT_URI and KEK_KEY_NAME for KEY_PROVIDER=azure_key_vault.';
}

function decryptField(ciphertext, dek) {
  const buf = Buffer.from(ciphertext, 'base64');
  const iv = buf.subarray(0, 12);
  const tag = buf.subarray(12, 28);
  const encrypted = buf.subarray(28);
  const decipher = crypto.createDecipheriv('aes-256-gcm', dek, iv, { authTagLength: 16 });
  decipher.setAuthTag(tag);
  return decipher.update(encrypted, undefined, 'utf8') + decipher.final('utf8');
}

async function getVaultDek(vault) {
  if (!vault.encrypted_dek) throw new Error(`Vault ${vault.id} is missing encrypted_dek`);
  const cached = dekCache.get(vault.id);
  if (cached && cached.expiresAt > Date.now()) return cached.dek;
  const dek = await unwrapDek(vault.encrypted_dek);
  dekCache.set(vault.id, { dek, expiresAt: Date.now() + DEK_CACHE_TTL_MS });
  return dek;
}

async function decryptForVault(vault, ciphertext) {
  if (!vault.vault_encryption_enabled) return ciphertext;
  const dek = await getVaultDek(vault);
  return decryptField(ciphertext, dek);
}

if (!databaseUrl) throw new Error('DATABASE_URL is required');
const encryptionEnabled = isEncryptionProviderConfigured();
if (!Number.isInteger(targetDimensions) || targetDimensions <= 0) throw new Error('--target-dimensions must be a positive integer');
if (!Number.isInteger(batchSize) || batchSize <= 0) throw new Error('--batch-size must be a positive integer');
if (!confirmed && !dryRun) {
  throw new Error('Refusing to run without --confirm-maintenance-window. Stop API/worker writes, take a backup/snapshot, then rerun.');
}

const pool = new pg.Pool({ connectionString: databaseUrl, max: 2 });

try {
  await preflight();
  if (encryptionEnabled) {
    await initCryptoClient();
  }
  if (dryRun) {
    console.log('Dry run complete. No embeddings were written.');
    process.exit(0);
  }

  await withAdvisoryLock(async () => {
    await createStagingTables();
    await stageRawChunkEmbeddings();
    await stageMemoryEmbeddings();
    await stageEntityAliasEmbeddings();
    await validateStagingCounts();
    await swapEmbeddings();
  });

  console.log('Qwen3 re-embedding migration complete.');
} finally {
  await pool.end();
}

async function preflight() {
  console.log('Preflight checklist:');
  console.log('- Take a PostgreSQL backup or provider snapshot before running.');
  console.log('- Stop API ingress, extraction workers, curation workers, and recall traffic.');
  console.log('- Verify Ollama is private/internal and has the Qwen model available.');
  console.log('- Keep the old database snapshot until sample recall validation passes.');

  await assertOllamaModelAvailable();

  const encryptedCount = await pool.query(
    `SELECT COUNT(*)::int AS count FROM vaults WHERE vault_encryption_enabled = true`
  );
  if (Number(encryptedCount.rows[0]?.count ?? 0) > 0 && !encryptionEnabled) {
    throw new Error(
      `Encrypted vault data is present but KEY_PROVIDER=${keyProvider} is not fully configured. ` +
      `${getEncryptionProviderConfigurationHint()} Configure the matching key provider before running the migration.`
    );
  }

  const activeWork = await pool.query(
    `SELECT
       (SELECT COUNT(*) FROM extraction_queue)::int AS extraction_queue,
       (SELECT COUNT(*) FROM curation_queue)::int AS curation_queue,
       (SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running'))::int AS active_jobs`
  );
  const work = activeWork.rows[0];
  if (work.extraction_queue > 0 || work.curation_queue > 0 || work.active_jobs > 0) {
    throw new Error(`Active pipeline work exists: extraction_queue=${work.extraction_queue}, curation_queue=${work.curation_queue}, active_jobs=${work.active_jobs}`);
  }

  const dimensions = await pool.query(
    `SELECT table_name, column_name, atttypmod
     FROM information_schema.columns
     JOIN pg_attribute
       ON attname = column_name
      AND attrelid = format('%I.%I', table_schema, table_name)::regclass
     WHERE table_schema = 'public'
       AND table_name IN ('raw_chunks', 'memories', 'memory_embeddings', 'entity_aliases')
       AND column_name = 'embedding'
     ORDER BY table_name`
  );
  console.log(`Target dimensions: ${targetDimensions}`);
  console.log(`Current vector columns: ${JSON.stringify(dimensions.rows)}`);
}

async function assertOllamaModelAvailable() {
  const response = await fetch(`${ollamaBaseUrl}/api/embeddings`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: ollamaModel, prompt: 'Persistio embedding migration preflight.' })
  });
  if (!response.ok) {
    throw new Error(`Ollama embedding preflight failed with HTTP ${response.status}. Pull ${ollamaModel} before migration.`);
  }
  const payload = await response.json();
  if (!Array.isArray(payload.embedding) || payload.embedding.length === 0) {
    throw new Error('Ollama embedding preflight returned no embedding.');
  }
  if (payload.embedding.length !== targetDimensions) {
    throw new Error(`Ollama returned ${payload.embedding.length} dimensions, expected ${targetDimensions}.`);
  }
}

async function withAdvisoryLock(callback) {
  const client = await pool.connect();
  try {
    const lock = await client.query(`SELECT pg_try_advisory_lock(hashtext('persistio:qwen3-reembed')) AS locked`);
    if (!lock.rows[0]?.locked) {
      throw new Error('Another re-embedding migration appears to hold the advisory lock.');
    }
    await callback();
  } finally {
    await client.query(`SELECT pg_advisory_unlock(hashtext('persistio:qwen3-reembed'))`).catch(() => undefined);
    client.release();
  }
}

async function createStagingTables() {
  await pool.query(`DROP TABLE IF EXISTS reembed_raw_chunks_stage`);
  await pool.query(`DROP TABLE IF EXISTS reembed_memories_stage`);
  await pool.query(`DROP TABLE IF EXISTS reembed_entity_aliases_stage`);
  await pool.query(`CREATE TABLE reembed_raw_chunks_stage (id UUID PRIMARY KEY, embedding TEXT NOT NULL)`);
  await pool.query(`CREATE TABLE reembed_memories_stage (id UUID PRIMARY KEY, embedding TEXT NOT NULL)`);
  await pool.query(`CREATE TABLE reembed_entity_aliases_stage (id UUID PRIMARY KEY, embedding TEXT NOT NULL)`);
}

async function stageRawChunkEmbeddings() {
  const contentSelect = await hasRawChunksContentColumn() ? 'rc.content' : 'NULL::text AS content';
  let lastId = null;
  let staged = 0;
  while (true) {
    const params = lastId ? [lastId, batchSize] : [batchSize];
    const result = await pool.query(
      `SELECT rc.id, rc.vault_id, rc.session_id, ${contentSelect}, rc.blob_store, rc.blob_key,
              v.vault_encryption_enabled, v.encrypted_dek
       FROM raw_chunks rc
       JOIN vaults v ON v.id = rc.vault_id
       ${lastId ? 'WHERE rc.id > $1' : ''}
       ORDER BY rc.id
       LIMIT $${lastId ? 2 : 1}`,
      params
    );
    if (result.rows.length === 0) break;
    for (const row of result.rows) {
      const rawText = await readRawChunkText(row);
      const vaultCtx = {
        id: row.vault_id,
        encrypted_dek: row.encrypted_dek ?? null,
        vault_encryption_enabled: Boolean(row.vault_encryption_enabled)
      };
      const text = await decryptForVault(vaultCtx, rawText);
      await insertStage('reembed_raw_chunks_stage', row.id, await embed(text));
      staged += 1;
      lastId = row.id;
    }
    console.log(`Staged raw chunk embeddings: ${staged}`);
  }
}

async function hasRawChunksContentColumn() {
  const result = await pool.query(
    `SELECT EXISTS (
       SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'raw_chunks'
         AND column_name = 'content'
     ) AS exists`
  );
  return Boolean(result.rows[0]?.exists);
}

async function stageMemoryEmbeddings() {
  let lastId = null;
  let staged = 0;
  while (true) {
    const params = lastId ? [lastId, batchSize] : [batchSize];
    const result = await pool.query(
      `SELECT m.id, m.data, m.vault_id,
              v.vault_encryption_enabled, v.encrypted_dek
       FROM memories m
       JOIN vaults v ON v.id = m.vault_id
       ${lastId ? 'WHERE m.id > $1' : ''}
       ORDER BY m.id
       LIMIT $${lastId ? 2 : 1}`,
      params
    );
    if (result.rows.length === 0) break;
    for (const row of result.rows) {
      const vaultCtx = {
        id: row.vault_id,
        encrypted_dek: row.encrypted_dek ?? null,
        vault_encryption_enabled: Boolean(row.vault_encryption_enabled)
      };
      const plaintext = await decryptForVault(vaultCtx, row.data ?? '');
      await insertStage('reembed_memories_stage', row.id, await embed(plaintext));
      staged += 1;
      lastId = row.id;
    }
    console.log(`Staged memory embeddings: ${staged}`);
  }
}

async function stageEntityAliasEmbeddings() {
  let lastId = null;
  let staged = 0;
  while (true) {
    const params = lastId ? [lastId, batchSize] : [batchSize];
    const result = await pool.query(
      `SELECT id, canonical
       FROM entity_aliases
       ${lastId ? 'WHERE id > $1' : ''}
       ORDER BY id
       LIMIT $${lastId ? 2 : 1}`,
      params
    );
    if (result.rows.length === 0) break;
    for (const row of result.rows) {
      await insertStage('reembed_entity_aliases_stage', row.id, await embed(row.canonical ?? ''));
      staged += 1;
      lastId = row.id;
    }
    console.log(`Staged entity alias embeddings: ${staged}`);
  }
}

async function readRawChunkText(row) {
  if (row.content !== null && row.content !== undefined) {
    return row.content;
  }
  if (row.blob_store && row.blob_key) {
    return getRawChunkStorageReader(row.blob_store).get(row.blob_key);
  }
  throw new Error(`Raw chunk ${row.id} has no readable content. Restore content, mount local blob storage, or use a fresh-database migration.`);
}

function getRawChunkStorageReader(store) {
  if (rawChunkStorageReaders.has(store)) {
    return rawChunkStorageReaders.get(store);
  }

  const reader = store === 'local'
    ? new LocalRawChunkReader()
    : store === 'azure_blob'
      ? new AzureBlobRawChunkReader()
      : store === 'gcs'
        ? new GcsRawChunkReader()
        : undefined;
  if (!reader) {
    throw new Error(`Unsupported raw chunk blob store: ${store}`);
  }

  rawChunkStorageReaders.set(store, reader);
  return reader;
}

class LocalRawChunkReader {
  constructor() {
    this.rootDir = resolveRawChunkLocalRoot(process.env.RAW_CHUNK_LOCAL_DIR ?? './data/raw-chunks');
  }

  async get(key) {
    return fs.readFile(this.resolvePath(key), 'utf8');
  }

  resolvePath(key) {
    const root = path.resolve(this.rootDir);
    const target = path.resolve(root, key);
    if (target !== root && !target.startsWith(`${root}${path.sep}`)) {
      throw new Error(`Raw chunk blob path escapes storage root: ${key}`);
    }
    return target;
  }
}

class AzureBlobRawChunkReader {
  constructor() {
    const containerName = process.env.RAW_CHUNK_BLOB_CONTAINER ?? 'raw-chunks';
    const serviceClient = process.env.AZURE_STORAGE_CONNECTION_STRING
      ? BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING)
      : new BlobServiceClient(
        `https://${process.env.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
        new DefaultAzureCredential()
      );
    if (!process.env.AZURE_STORAGE_CONNECTION_STRING && !process.env.AZURE_STORAGE_ACCOUNT_NAME) {
      throw new Error('AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING is required to read azure_blob raw chunks');
    }
    this.containerClient = serviceClient.getContainerClient(containerName);
  }

  async get(key) {
    const response = await this.containerClient.getBlockBlobClient(key).download();
    const chunks = [];
    for await (const chunk of response.readableStreamBody ?? []) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString('utf8');
  }
}

class GcsRawChunkReader {
  constructor() {
    const bucketName = process.env.RAW_CHUNK_GCS_BUCKET;
    if (!bucketName) {
      throw new Error('RAW_CHUNK_GCS_BUCKET is required to read gcs raw chunks');
    }
    this.bucket = new Storage().bucket(bucketName);
  }

  async get(key) {
    const [content] = await this.bucket.file(key).download();
    return content.toString('utf8');
  }
}

function resolveRawChunkLocalRoot(rootDir) {
  return path.isAbsolute(rootDir) ? rootDir : path.resolve(serverPackageRoot, rootDir);
}

async function embed(text) {
  const response = await fetch(`${ollamaBaseUrl}/api/embeddings`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: ollamaModel, prompt: text })
  });
  if (!response.ok) throw new Error(`Ollama embedding request failed with HTTP ${response.status}`);
  const payload = await response.json();
  if (!Array.isArray(payload.embedding) || payload.embedding.length !== targetDimensions) {
    throw new Error(`Embedding dimension mismatch for ${ollamaModel}`);
  }
  return JSON.stringify(payload.embedding);
}

async function insertStage(table, id, embedding) {
  await pool.query(
    `INSERT INTO ${table} (id, embedding)
     VALUES ($1, $2)
     ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding`,
    [id, embedding]
  );
}

async function validateStagingCounts() {
  const result = await pool.query(
    `SELECT
       (SELECT COUNT(*) FROM raw_chunks)::int AS raw_chunks,
       (SELECT COUNT(*) FROM reembed_raw_chunks_stage)::int AS raw_stage,
       (SELECT COUNT(*) FROM memories)::int AS memories,
       (SELECT COUNT(*) FROM reembed_memories_stage)::int AS memories_stage,
       (SELECT COUNT(*) FROM entity_aliases)::int AS entity_aliases,
       (SELECT COUNT(*) FROM reembed_entity_aliases_stage)::int AS aliases_stage`
  );
  const row = result.rows[0];
  if (row.raw_chunks !== row.raw_stage || row.memories !== row.memories_stage || row.entity_aliases !== row.aliases_stage) {
    throw new Error(`Staging count mismatch: ${JSON.stringify(row)}`);
  }
}

async function swapEmbeddings() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL lock_timeout = '30s'`);
    await client.query(`DROP INDEX IF EXISTS idx_memory_embeddings_embedding_hnsw`);
    await client.query(`DROP INDEX IF EXISTS idx_raw_chunks_embedding_hnsw`);
    await client.query(`DROP INDEX IF EXISTS idx_entity_aliases_embedding`);
    await client.query(`ALTER TABLE raw_chunks ALTER COLUMN embedding TYPE vector(${targetDimensions}) USING NULL`);
    await client.query(`ALTER TABLE memories ALTER COLUMN embedding TYPE vector(${targetDimensions}) USING NULL`);
    await client.query(`ALTER TABLE memory_embeddings ALTER COLUMN embedding TYPE vector(${targetDimensions}) USING NULL`);
    await client.query(`ALTER TABLE entity_aliases ALTER COLUMN embedding TYPE vector(${targetDimensions}) USING NULL`);
    await client.query(`UPDATE raw_chunks rc SET embedding = s.embedding::vector FROM reembed_raw_chunks_stage s WHERE s.id = rc.id`);
    await client.query(`UPDATE memories m SET embedding = s.embedding::vector FROM reembed_memories_stage s WHERE s.id = m.id`);
    await client.query(
      `INSERT INTO memory_embeddings (memory_id, embedding, embedding_model, embedded_at)
       SELECT id, embedding::vector, $1, now()
       FROM reembed_memories_stage
       ON CONFLICT (memory_id)
       DO UPDATE SET embedding = EXCLUDED.embedding, embedding_model = EXCLUDED.embedding_model, embedded_at = now()`,
      [ollamaModel]
    );
    await client.query(`UPDATE entity_aliases ea SET embedding = s.embedding::vector FROM reembed_entity_aliases_stage s WHERE s.id = ea.id`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_entity_aliases_embedding ON entity_aliases USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_memory_embeddings_embedding_hnsw ON memory_embeddings USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_raw_chunks_embedding_hnsw ON raw_chunks USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL`);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
