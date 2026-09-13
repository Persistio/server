import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

function migration(filename: string): string {
  return readFileSync(resolve(__dirname, 'migrations', filename), 'utf8');
}

function repoFile(...segments: string[]): string {
  return readFileSync(resolve(__dirname, '..', '..', '..', '..', ...segments), 'utf8');
}

function repositorySources(...roots: string[]): Array<{ path: string; source: string }> {
  const sources: Array<{ path: string; source: string }> = [];
  const visit = (path: string) => {
    for (const entry of readdirSync(path, { withFileTypes: true })) {
      if (entry.name === 'dist' || entry.name === 'node_modules') continue;
      const child = resolve(path, entry.name);
      if (entry.isDirectory()) {
        visit(child);
      } else if (/\.(?:c|m)?(?:j|t)sx?$/.test(entry.name)) {
        sources.push({ path: child, source: readFileSync(child, 'utf8') });
      }
    }
  };

  for (const root of roots) {
    visit(resolve(__dirname, '..', '..', '..', '..', root));
  }
  return sources;
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

  it('creates an append-only audit trail for memory scope transitions', () => {
    const sql = migration('043_memory_scope_change_log.sql');

    expect(sql).toContain('CREATE TABLE IF NOT EXISTS memory_scope_change_log');
    expect(sql).toContain("old_scope TEXT NOT NULL CHECK");
    expect(sql).toContain("new_scope TEXT NOT NULL CHECK");
    expect(sql).toContain('BEFORE UPDATE OR DELETE ON memory_scope_change_log');
    expect(sql).toContain("TG_OP = 'DELETE'");
    expect(sql).toContain('SELECT 1 FROM vaults WHERE id = OLD.vault_id');
    expect(sql).toContain("RAISE EXCEPTION 'memory_scope_change_log is append-only'");
  });

  it('upgrades the released scope audit trigger to permit parent-vault cascades', () => {
    const sql = migration('044_memory_scope_change_log_cascade.sql');

    expect(sql).toContain('CREATE OR REPLACE FUNCTION reject_memory_scope_change_log_mutation()');
    expect(sql).toContain("TG_OP = 'DELETE'");
    expect(sql).toContain('SELECT 1 FROM vaults WHERE id = OLD.vault_id');
    expect(sql).toContain("RAISE EXCEPTION 'memory_scope_change_log is append-only'");
  });

  it('migrates global rules to proposed authority with an immutable review snapshot', () => {
    const sql = migration('045_memory_authority.sql');

    expect(sql).toContain("authority_state TEXT NOT NULL DEFAULT 'proposed'");
    expect(sql).toContain('authority_required BOOLEAN NOT NULL DEFAULT false');
    expect(sql).toContain('authority_version INTEGER NOT NULL DEFAULT 1');
    expect(sql).toContain('preserve_behavioral_memory_authority_requirement');
    expect(sql).toContain("IF TG_OP = 'INSERT'");
    expect(sql).toContain('NEW.authority_required := true');
    expect(sql).toContain("NEW.authority_state := 'proposed'");
    expect(sql).toContain('NEW.data IS DISTINCT FROM OLD.data');
    expect(sql).toContain('NEW.subject IS DISTINCT FROM OLD.subject');
    expect(sql).toContain('NEW.categories IS DISTINCT FROM OLD.categories');
    expect(sql).toContain('NEW.evidence IS DISTINCT FROM OLD.evidence');
    expect(sql).toContain('NEW.source_chunks IS DISTINCT FROM OLD.source_chunks');
    expect(sql).toContain('NEW.source_segment_id IS DISTINCT FROM OLD.source_segment_id');
    expect(sql).toContain('NEW.authority_version := OLD.authority_version + 1');
    expect(sql).toContain("COALESCE(NEW.type IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint'), false)");
    expect(sql).toContain('UPDATE OF data, subject, subject_encrypted, subject_hmac, categories, type, scope, evidence, source_chunks, source_segment_id, authority_required');
    expect(sql).toContain('ALTER COLUMN authority_required SET DEFAULT true');
    expect(sql).toContain('memory_authority_events_actor_type_check');
    expect(sql).toContain('memory_scope_change_log_actor_type_check');
    expect(sql).toContain("actor_type IN ('api_key', 'service', 'system', 'user', 'worker')");
    expect(sql).toContain("OR status IS DISTINCT FROM 'active'");
    expect(sql).toContain('OR archived_at IS NOT NULL');
    expect(sql).toContain('WHERE authority_required');
    expect(sql).toContain("WHEN type = 'user_rule'");
    expect(sql).toContain("AND scope = 'global'");
    expect(sql).toContain("AND status = 'active'");
    expect(sql).toContain('AND archived_at IS NULL');
    expect(sql).toContain('approval was not grandfathered');
    expect(sql).toContain('non-recallable memory requires explicit review before future recall');
    expect(sql).toContain("'data', data");
    expect(sql).toContain("'subject_encrypted', subject_encrypted");
    expect(sql).toContain("'evidence', evidence");
    expect(sql).toContain("'source_chunks', source_chunks");
    expect(sql).toContain("'source_timestamp', source_timestamp");
    expect(sql).toContain("'archived_at', archived_at");
    expect(sql).toContain('BEFORE UPDATE OR DELETE ON memory_authority_events');
    expect(sql).toContain('SELECT 1 FROM vaults WHERE id = OLD.vault_id');
  });

  it('requires every memory writer to choose an explicit valid scope', () => {
    const sql = migration('046_explicit_memory_scope.sql');
    const scopeSchema = migration('016_behavioral_memory_graph.sql');

    expect(sql).toContain('ALTER COLUMN scope DROP DEFAULT');
    expect(sql).toContain('ALTER COLUMN scope SET NOT NULL');
    expect(scopeSchema).toContain("CHECK (scope IN ('global', 'project', 'task', 'session'))");
    expect(sql).toContain("conname = 'memories_scope_check'");
    expect(sql).toContain("AND contype = 'c'");
    expect(sql).toContain('AND convalidated');
    expect(sql).toContain("pg_get_constraintdef(oid, true) = $definition$CHECK (scope = ANY (ARRAY['global'::text, 'project'::text, 'task'::text, 'session'::text]))$definition$");
    expect(sql).not.toContain('DROP CONSTRAINT');
    expect(sql).not.toContain('ADD CONSTRAINT');
    expect(sql).toContain('CREATE OR REPLACE FUNCTION public.least_privileged_memory_scope');
    expect(sql).toContain('WHEN current_scope IS NULL');
    expect(sql).toContain('OR incoming_scope IS NULL');
    expect(sql).toContain("OR incoming_scope NOT IN ('global', 'project', 'task', 'session') THEN NULL");
  });

  it('binds non-global memory scope without rewriting historical evidence', () => {
    const sql = migration('047_memory_applicability.sql');

    expect(sql).toContain('ADD COLUMN IF NOT EXISTS scope_key TEXT');
    expect(sql).toContain("scope = 'global' AND scope_key IS NULL");
    expect(sql).toContain("scope <> 'global'");
    expect(sql).toContain("scope_key IS NULL");
    expect(sql).not.toContain('UPDATE memories SET scope_key');
    expect(sql).toContain("ELSIF NEW.scope_key IS NULL AND NEW.status <> 'needs_review'");
    expect(sql).toContain("NEW.status = 'active' AND OLD.status IS DISTINCT FROM 'active'");
    expect(sql).toContain('CREATE INDEX IF NOT EXISTS idx_memories_vault_scope_binding');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS scope TEXT');
    expect(sql).toContain('UNIQUE NULLS NOT DISTINCT (vault_id, scope, scope_key, alias)');
    expect(sql).toContain('CREATE INDEX idx_entity_aliases_vault_scope_canonical');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS project_id TEXT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS task_id TEXT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS agent_id TEXT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS trigger_type TEXT');
    expect(sql).toContain('OR NEW.scope_key IS DISTINCT FROM OLD.scope_key');
  });

  it('stores replay transport identity and enforces tenant-scoped source-event idempotency', () => {
    const sql = migration('048_transport_provenance.sql');

    expect(sql).toContain('ADD COLUMN IF NOT EXISTS source_event_namespace TEXT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS source_event_id TEXT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS source_message_id TEXT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS ingest_job_id UUID');
    expect(sql).toContain('raw_chunks_source_event_identity_check');
    expect(sql).toContain('CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_chunks_vault_source_event');
    expect(sql).toContain('ON raw_chunks (vault_id, source_event_key)');
    expect(sql).toContain("source_event_key ~ '^[0-9a-f]{64}$'");
  });

  it('persists chunk ordinals, stable payload identities, and recoverable blob write intents', () => {
    const sql = migration('050_ingest_idempotency.sql');

    expect(sql).toContain('ADD COLUMN IF NOT EXISTS source_event_ordinal BIGINT');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS source_event_payload_sha256 TEXT');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS raw_chunk_blob_write_intents');
    expect(sql).toContain('vault_id UUID NOT NULL,');
    expect(sql).toContain('UNIQUE (blob_store, blob_key)');
    expect(sql).toContain('ADD COLUMN IF NOT EXISTS idempotency_key TEXT');
    expect(sql).toContain('idx_jobs_vault_kind_idempotency');
  });

  it('persists versioned curator reviews and enforces activation policy in the database', () => {
    const sql = migration('049_curation_review_runs.sql');

    expect(sql).toContain('CREATE TABLE IF NOT EXISTS curation_review_runs');
    expect(sql).toContain('schema_version TEXT NOT NULL');
    expect(sql).toContain('prompt_version TEXT NOT NULL');
    expect(sql).toContain('prompt_hash TEXT NOT NULL');
    expect(sql).toContain('validation_errors JSONB NOT NULL');
    expect(sql).toContain('raw_response JSONB NOT NULL');
    expect(sql).toContain('before_state JSONB');
    expect(sql).toContain('after_state JSONB');
    expect(sql).toContain('ADD CONSTRAINT memories_validity_window_order');
    expect(sql).toContain('valid_from <= valid_until');
    expect(sql).toContain('NOT VALID');
    expect(sql).toContain('CREATE OR REPLACE FUNCTION enforce_memory_activation_policy()');
    expect(sql).toContain("policy-quarantined memory cannot be activated");
    expect(sql).toContain('BEFORE INSERT OR UPDATE OF status, archived_at, sensitivity, confidence, source_timestamp, evidence');
  });

  it('gates every platform role behind serialized schema migrations', () => {
    const client = repoFile('packages', 'server', 'src', 'db', 'client.ts');
    const entrypoint = repoFile('packages', 'server', 'src', 'index.ts');

    expect(client).toContain('SELECT pg_advisory_lock($1::bigint)');
    expect(client).toContain('SELECT pg_advisory_unlock($1::bigint)');
    expect(entrypoint).toContain('if (!isAnalyticsApi)');
    expect(entrypoint.indexOf('await runMigrations()')).toBeLessThan(entrypoint.indexOf('new Worker('));
  });

  it('supplies scope explicitly from every production and test-fixture memory insert', () => {
    const inserts = repositorySources('packages', 'app', 'scripts', 'eval').flatMap(({ path, source }) => (
      [...source.matchAll(/INSERT\s+INTO\s+memories\s*\(([^)]+)\)/gi)]
        .map((match) => ({
          columns: match[1].split(',').map((column) => column.trim()),
          intentionalOmission: match[1].includes('EXPECT_SCOPE_CONSTRAINT_FAILURE'),
          path
        }))
    ));

    expect(inserts.length).toBeGreaterThan(0);
    const intentionalOmissions = inserts.filter(({ intentionalOmission }) => intentionalOmission);
    expect(intentionalOmissions).toHaveLength(1);
    expect(intentionalOmissions[0].path).toMatch(/dedup\.scope-concurrency\.integration\.test\.ts$/);
    expect(intentionalOmissions[0].columns).not.toContain('scope');
    for (const { columns, intentionalOmission } of inserts) {
      if (!intentionalOmission) expect(columns).toContain('scope');
    }
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

  it('fences every worker transition and constrains vault-owned relationships', () => {
    const sql = migration('051_worker_fencing_and_vault_integrity.sql');
    const extractionWorker = repoFile('packages', 'server', 'src', 'daemon', 'extraction-worker.ts');
    const curationWorker = repoFile('packages', 'server', 'src', 'daemon', 'curation-worker.ts');
    const leaseService = repoFile('packages', 'server', 'src', 'services', 'worker-lease.ts');

    expect(sql).toContain('claim_token UUID');
    expect(sql).toContain('lease_expires_at TIMESTAMPTZ');
    expect(sql).toContain('worker_action_receipts');
    expect(sql).toContain('PRIMARY KEY (queue_kind, queue_id, action_key)');
    expect(sql).toContain('memories_source_chunk_vault_guard');
    expect(sql).toContain('memory_edges_from_vault_fkey');
    expect(sql).toContain('curation_action_memory_vault_fkey');
    expect(extractionWorker).not.toContain("claimed_at < now() - interval '10 minutes'");
    expect(curationWorker).not.toContain("claimed_at < now() - interval '10 minutes'");
    expect(extractionWorker).toContain('withWorkerLeaseTransaction(lease,');
    expect(curationWorker).toContain('withWorkerLeaseTransaction(lease,');
    expect(leaseService).toContain('AND claim_token = $2');
    expect(leaseService).toContain('AND lease_expires_at > clock_timestamp()');
    expect(leaseService).toContain('await fence.assertUnexpired()');
    expect(leaseService).toContain('curator_claim_token = $2');
  });
  it('removes live approval and ACK contracts while retaining append-only mutation/recovery evidence',()=>{
    const sql=migration('057_restore_platform_pipeline.sql');
    expect(sql).toContain('DROP COLUMN authority_state');
    expect(sql).toContain('DROP TABLE memory_delivery_pending');
    expect(sql).toContain('memory_job_recoveries_append_only');
    expect(sql).toContain('CREATE FUNCTION persistio_memory_observability_state');
    expect(sql).not.toMatch(/DROP TABLE (?:memory_mutation_events|memory_scope_change_log|worker_action_receipts|platform_event_outbox)/);
  });
  it('enforces active baseline data and a single revision-fenced improvement/scheduling model',()=>{
    const sql=migration('057_restore_platform_pipeline.sql');
    expect(sql).toContain("status IN ('active', 'superseded', 'contradicted')");
    expect(sql).toContain('CREATE TABLE curation_queue_items');
    expect(sql).toContain('work_key');
    expect(sql).toContain('NEW.revision = OLD.revision');
    expect(sql).toContain('generation = gen_random_uuid()');
    expect(sql).toContain('approved empty memory domain');
  });
  it('has no managed approval-policy configuration after restoration',()=>{
    for(const segments of [['infra','gcp','locals.tf'],['infra','gcp','variables.tf'],
      ['deployments','gcp','deploy-cloud-run.sh'],['.env.example']]){
      expect(repoFile(...segments)).not.toMatch(/GLOBAL_RULE_POLICY|global_rule_policy/);
    }
  });

});
