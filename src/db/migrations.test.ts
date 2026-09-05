import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

function migration(filename: string): string {
  return readFileSync(resolve(__dirname, 'migrations', filename), 'utf8');
}

function repoFile(...segments: string[]): string {
  return readFileSync(resolve(__dirname, '..', '..', '..', '..', ...segments), 'utf8');
}

describe('database migration guardrails', () => {
  it('migrates legacy pro vaults before deleting the pro plan', () => {
    const sql = migration('038_unlimited_curator_frequency_and_remove_pro.sql');

    expect(sql).toContain("SET plan_id = 'unlimited'");
    expect(sql).toContain("WHERE plan_id = 'pro'");
    expect(sql).toContain("DELETE FROM plans");
    expect(sql).toContain("WHERE id = 'pro'");
  });

  it('lets already re-embedded databases pass configurable dimension migration', () => {
    const sql = migration('034_configurable_embedding_dimensions.sql');

    expect(sql).toContain("target_type := format('vector(%s)', target_dimensions)");
    expect(sql).toContain('format_type(a.atttypid, a.atttypmod) <> target_type');
    expect(sql).toContain('target_dimensions > 2000');
  });

  it('leaves default-dimension databases eligible for a future configurable dimension migration', () => {
    const sql = migration('034_configurable_embedding_dimensions.sql');

    expect(sql).toContain("IF target_dimensions = 1536 THEN");
    expect(sql).toContain("set_config('persistio.skip_migration_record', 'true', true)");
  });

  it('uses bigint counters for durable model usage rollups', () => {
    const sql = migration('037_model_role_usage.sql');

    expect(sql).toContain('request_count BIGINT');
    expect(sql).toContain('embedding_input_chars BIGINT');
    expect(sql).toContain('total_tokens BIGINT');
  });

  it('keeps the Qwen re-embedding script compatible with finalized raw chunk blob schemas', () => {
    const script = repoFile('scripts', 'reembed-qwen3.mjs');

    expect(script).toContain('hasRawChunksContentColumn');
    expect(script).toContain('NULL::text AS content');
    expect(script).toContain('getRawChunkStorageReader(row.blob_store).get(row.blob_key)');
    expect(script).toContain('class AzureBlobRawChunkReader');
    expect(script).toContain('class GcsRawChunkReader');
  });

  it('persists raw chunk storage bytes for customer metric accounting', () => {
    const sql = migration('039_customer_metric_storage_bytes.sql');
    const rawChunkMigration = repoFile('scripts', 'migrate-raw-chunks-to-blob.mjs');

    expect(sql).toContain('ADD COLUMN IF NOT EXISTS storage_bytes BIGINT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS workspace_id UUID');
    expect(sql).toContain('information_schema.columns');
    expect(sql).toContain('octet_length(content::text)');
    expect(rawChunkMigration).toContain('storage_bytes = $5');
    expect(rawChunkMigration).toContain("Buffer.byteLength(row.content, 'utf8')");
  });

  it('seeds the memory graph entitlement for unlimited plans', () => {
    const sql = migration('041_memory_graph_plan_entitlement.sql');

    expect(sql).toContain('"graphEnabled": true');
    expect(sql).toContain("WHERE id = 'unlimited'");
  });

  it('keeps operator migrations provider-portable for GCP deployments', () => {
    const rawChunkMigration = repoFile('scripts', 'migrate-raw-chunks-to-blob.mjs');
    const reembedMigration = repoFile('scripts', 'reembed-qwen3.mjs');

    expect(rawChunkMigration).toContain("'gcs'");
    expect(rawChunkMigration).toContain('class GcsStorage');
    expect(rawChunkMigration).toContain('RAW_CHUNK_GCS_BUCKET');
    expect(reembedMigration).toContain('KEY_PROVIDER');
    expect(reembedMigration).toContain('KeyManagementServiceClient');
    expect(reembedMigration).toContain('GCP_KMS_KEY_NAME');
  });
});
