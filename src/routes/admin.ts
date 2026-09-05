import crypto from 'node:crypto';
import type { FastifyInstance, FastifyReply } from 'fastify';
import { z } from 'zod';

import { getConfig, getConfiguredEmbeddingDimensions } from '../config';
import { query, withTransaction } from '../db/client';
import { createApiKey, ensureRequestedAccountAccess, getAuthAccountId, requireAdminScope } from '../middleware/auth';
import { decryptForVault, encryptForVault, generateAndWrapDek, type VaultEncryptionContext } from '../services/crypto';
import { setCustomerMetricVaultId } from '../services/customer-api-request-metrics';
import { recordCustomerMetric } from '../services/customer-metrics';
import { getRawChunkStorage, type RawChunkStorage } from '../services/raw-chunk-storage';
import { getVaultStats } from '../services/vault-stats';
import {
  MAX_CUSTOM_EXTRACTION_PROMPT_BYTES,
  VAULT_TYPES,
  normalizeVaultType,
  validateVaultPromptSettings
} from '../services/vault-prompts';

const createVaultSchema = z.object({
  name: z.string().min(1),
  purpose: z.string().min(1).max(500).optional(),
  account_id: z.string().uuid().nullable().optional(),
  plan: z.string().min(1).optional(),
  type: z.enum(VAULT_TYPES).nullable().optional(),
  custom_extraction_prompt: z.string().min(1).max(MAX_CUSTOM_EXTRACTION_PROMPT_BYTES).nullable().optional(),
  custom_curation_prompt: z.string().min(1).max(MAX_CUSTOM_EXTRACTION_PROMPT_BYTES).nullable().optional()
}).strict();

const updateVaultSchema = z.object({
  name: z.string().min(1).optional(),
  purpose: z.string().min(1).max(500).nullable().optional(),
  plan: z.string().min(1).optional(),
  status: z.enum(['active', 'disabled']).optional(),
  type: z.enum(VAULT_TYPES).nullable().optional(),
  custom_extraction_prompt: z.string().min(1).max(MAX_CUSTOM_EXTRACTION_PROMPT_BYTES).nullable().optional(),
  custom_curation_prompt: z.string().min(1).max(MAX_CUSTOM_EXTRACTION_PROMPT_BYTES).nullable().optional()
}).strict().refine((value) =>
  value.name !== undefined
  || value.purpose !== undefined
  || value.plan !== undefined
  || value.status !== undefined
  || value.type !== undefined
  || value.custom_extraction_prompt !== undefined
  || value.custom_curation_prompt !== undefined, {
  message: 'At least one field must be provided'
});

const createPlanSchema = z.object({
  id: z.string().min(1),
  limits: z.record(z.unknown()).default({})
}).strict();

const updatePlanSchema = z.object({
  limits: z.record(z.unknown())
}).strict();

interface RawChunkBlobRow {
  blob_store: string | null;
  blob_key: string;
  storage_bytes: string | null;
}

interface RawChunkBlobDeletionRow {
  id: string;
  storage_bytes: string | null;
  vault_id: string;
  blob_store: string;
  blob_key: string;
  workspace_id: string | null;
}

interface VaultPromptStateRow extends VaultEncryptionContext {
  id: string;
  plan_id: string;
  type: string | null;
  custom_extraction_prompt: string | null;
  custom_curation_prompt: string | null;
}

async function upsertPlan(planId: string, limits: Record<string, unknown>): Promise<{ id: string; limits: Record<string, unknown> }> {
  const result = await query<{ id: string; limits: Record<string, unknown> }>(
    `INSERT INTO plans (id, limits)
     VALUES ($1, $2::jsonb)
     ON CONFLICT (id) DO UPDATE SET
       limits = EXCLUDED.limits
     RETURNING id, limits`,
    [planId, JSON.stringify(limits)]
  );

  return result.rows[0];
}

async function planExists(planId: string): Promise<boolean> {
  const result = await query<{ id: string }>(
    `SELECT id
     FROM plans
     WHERE id = $1
     LIMIT 1`,
    [planId]
  );

  return Boolean(result.rowCount);
}

async function getVaultPromptState(vaultId: string, accountId: string | null = null): Promise<VaultPromptStateRow | null> {
  const accountFilter = accountId ? 'AND account_id = $2::uuid' : '';
  const params = accountId ? [vaultId, accountId] : [vaultId];
  const result = await query<VaultPromptStateRow>(
    `SELECT id, plan_id, type, custom_extraction_prompt, custom_curation_prompt,
            encrypted_dek, vault_encryption_enabled
     FROM vaults
     WHERE id = $1
       ${accountFilter}
     LIMIT 1`,
    params
  );

  return result.rows[0] ?? null;
}

function sendPromptValidationError(
  reply: FastifyReply,
  validation: Exclude<ReturnType<typeof validateVaultPromptSettings>, { ok: true }>
) {
  return reply.code(validation.statusCode).send({
    error: validation.error,
    feedback: validation.feedback ?? []
  });
}

export async function registerAdminRoutes(app: FastifyInstance) {
  const planReadAuth = requireAdminScope(['platform:plans:read', 'platform:plans:write']);
  const planWriteAuth = requireAdminScope('platform:plans:write');
  const vaultReadAuth = requireAdminScope('platform:vaults:read');
  const vaultCreateAuth = requireAdminScope('platform:vaults:create');
  const vaultUpdateAuth = requireAdminScope('platform:vaults:update');
  const vaultDeleteAuth = requireAdminScope('platform:vaults:delete');
  const vaultRotateAuth = requireAdminScope('platform:vault_keys:rotate');
  const vaultStatsAuth = requireAdminScope('platform:vaults:stats:read');

  app.get('/admin/plans', { preHandler: planReadAuth }, async () => {
    const result = await query<{ id: string; limits: Record<string, unknown> }>(
      `SELECT id, limits
       FROM plans
       ORDER BY id ASC`
    );

    return { items: result.rows };
  });

  app.get('/admin/plans/:id', { preHandler: planReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().min(1) }).parse(request.params);
    const result = await query<{ id: string; limits: Record<string, unknown> }>(
      `SELECT id, limits
       FROM plans
       WHERE id = $1
       LIMIT 1`,
      [id]
    );

    if (!result.rowCount) return reply.code(404).send({ error: 'Plan not found' });
    return result.rows[0];
  });

  app.post('/admin/plans', { preHandler: planWriteAuth }, async (request, reply) => {
    const parsedBody = createPlanSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    const plan = await upsertPlan(body.id, body.limits);
    return reply.code(200).send(plan);
  });

  app.patch('/admin/plans/:id', { preHandler: planWriteAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().min(1) }).parse(request.params);
    const parsedBody = updatePlanSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    const result = await query<{ id: string; limits: Record<string, unknown> }>(
      `UPDATE plans
       SET limits = $2::jsonb
       WHERE id = $1
       RETURNING id, limits`,
      [id, JSON.stringify(body.limits)]
    );

    if (!result.rowCount) return reply.code(404).send({ error: 'Plan not found' });
    return result.rows[0];
  });

  app.delete('/admin/plans/:id', { preHandler: planWriteAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().min(1) }).parse(request.params);
    const usage = await query<{ count: string }>(
      `SELECT COUNT(*)::text AS count
       FROM vaults
       WHERE plan_id = $1`,
      [id]
    );

    if (Number(usage.rows[0]?.count ?? 0) > 0) {
      return reply.code(409).send({ error: 'Plan is in use' });
    }

    const result = await query<{ id: string }>(
      `DELETE FROM plans
       WHERE id = $1
       RETURNING id`,
      [id]
    );

    if (!result.rowCount) return reply.code(404).send({ error: 'Plan not found' });
    return { id: result.rows[0].id, deleted: true };
  });

  app.post('/admin/vaults', { preHandler: vaultCreateAuth }, async (request, reply) => {
    const parsedBody = createVaultSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    if (!ensureRequestedAccountAccess(request, body.account_id ?? null)) {
      return reply.code(403).send({ error: 'Forbidden' });
    }
    if (getAuthAccountId(request) && !body.account_id) {
      return reply.code(403).send({ error: 'Forbidden' });
    }
    const apiKey = createApiKey();
    const planId = body.plan ?? 'unlimited';
    if (!(await planExists(planId))) {
      return reply.code(404).send({ error: 'Plan not found' });
    }
    const vaultType = body.type ?? null;
    const promptValidation = validateVaultPromptSettings({
      type: vaultType,
      planId,
      customExtractionPrompt: body.custom_extraction_prompt ?? null,
      customCurationPrompt: body.custom_curation_prompt ?? null
    });
    if (!promptValidation.ok) {
      return sendPromptValidationError(reply, promptValidation);
    }
    const encryptedDek = getConfig().ENCRYPTION_ENABLED
      ? (await generateAndWrapDek()).encryptedDek
      : null;
    const vaultId = crypto.randomUUID();
    const newVaultEncryptionContext: VaultEncryptionContext = {
      id: vaultId,
      encrypted_dek: encryptedDek,
      vault_encryption_enabled: encryptedDek !== null
    };
    const storedCustomExtractionPrompt = vaultType === 'custom'
      ? await encryptForVault(newVaultEncryptionContext, body.custom_extraction_prompt!.trim())
      : null;
    const storedCustomCurationPrompt = vaultType === 'custom'
      ? await encryptForVault(newVaultEncryptionContext, body.custom_curation_prompt!.trim())
      : null;

    const result = await query<{ id: string; name: string; purpose: string | null; plan_id: string; status: string; created_at: string; type: string | null }>(
      `INSERT INTO vaults (
         id, name, purpose, account_id, plan_id, api_key_hash, settings, encrypted_dek,
         vault_encryption_enabled, type, custom_extraction_prompt, custom_curation_prompt
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12)
       RETURNING id, name, purpose, plan_id, status, created_at, type`,
      [
        vaultId,
        body.name,
        body.purpose ?? null,
        body.account_id ?? null,
        planId,
        apiKey.hash,
        JSON.stringify({
          embedding_dimensions: getConfiguredEmbeddingDimensions()
        }),
        encryptedDek,
        encryptedDek !== null,
        vaultType,
        storedCustomExtractionPrompt,
        storedCustomCurationPrompt
      ]
    );

    setCustomerMetricVaultId(request, result.rows[0].id);
    return reply.code(201).send({
      id: result.rows[0].id,
      name: result.rows[0].name,
      purpose: result.rows[0].purpose,
      plan: result.rows[0].plan_id,
      type: result.rows[0].type,
      status: result.rows[0].status,
      api_key: apiKey.rawKey
    });
  });

  app.post('/admin/vaults/:id/rotate-key', { preHandler: vaultRotateAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const accountId = getAuthAccountId(request);
    const apiKey = createApiKey();
    const accountFilter = accountId ? 'AND account_id = $3::uuid' : '';
    const params = accountId ? [apiKey.hash, id, accountId] : [apiKey.hash, id];
    const result = await query<{ id: string }>(
      `UPDATE vaults
       SET api_key_hash = $1
       WHERE id = $2
         ${accountFilter}
       RETURNING id`,
      params
    );
    if (!result.rowCount) return reply.code(404).send({ error: 'Vault not found' });
    setCustomerMetricVaultId(request, result.rows[0].id);
    return reply.code(200).send({ id: result.rows[0].id, api_key: apiKey.rawKey });
  });

  app.get('/admin/vaults/:id/stats', { preHandler: vaultStatsAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const stats = await getVaultStats(id, getAuthAccountId(request));
    if (!stats) return reply.code(404).send({ error: 'Vault not found' });
    setCustomerMetricVaultId(request, id);
    return stats;
  });

  app.patch('/admin/vaults/:id', { preHandler: vaultUpdateAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const parsedBody = updateVaultSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    const accountId = getAuthAccountId(request);
    if (body.plan && !(await planExists(body.plan))) {
      return reply.code(404).send({ error: 'Plan not found' });
    }
    const needsPromptState =
      body.plan !== undefined
      || body.type !== undefined
      || body.custom_extraction_prompt !== undefined
      || body.custom_curation_prompt !== undefined;
    const currentPromptState = needsPromptState ? await getVaultPromptState(id, accountId) : null;
    if (needsPromptState && !currentPromptState) {
      return reply.code(404).send({ error: 'Vault not found' });
    }
    const effectivePlanId = body.plan ?? currentPromptState?.plan_id ?? '';
    const effectiveType = body.type !== undefined
      ? body.type
      : normalizeVaultType(currentPromptState?.type);
    const effectiveCustomExtractionPrompt = effectiveType === 'custom'
      ? body.custom_extraction_prompt !== undefined
        ? body.custom_extraction_prompt
        : currentPromptState?.custom_extraction_prompt
          ? await decryptForVault(currentPromptState, currentPromptState.custom_extraction_prompt)
          : null
      : body.custom_extraction_prompt ?? null;
    const effectiveCustomCurationPrompt = effectiveType === 'custom'
      ? body.custom_curation_prompt !== undefined
        ? body.custom_curation_prompt
        : currentPromptState?.custom_curation_prompt
          ? await decryptForVault(currentPromptState, currentPromptState.custom_curation_prompt)
          : null
      : body.custom_curation_prompt ?? null;
    if (needsPromptState) {
      const promptValidation = validateVaultPromptSettings({
        type: effectiveType,
        planId: effectivePlanId,
        customExtractionPrompt: effectiveCustomExtractionPrompt,
        customCurationPrompt: effectiveCustomCurationPrompt
      });
      if (!promptValidation.ok) {
        return sendPromptValidationError(reply, promptValidation);
      }
    }
    const storedCustomExtractionPrompt = currentPromptState && effectiveType === 'custom'
      ? await encryptForVault(currentPromptState, effectiveCustomExtractionPrompt!.trim())
      : null;
    const storedCustomCurationPrompt = currentPromptState && effectiveType === 'custom'
      ? await encryptForVault(currentPromptState, effectiveCustomCurationPrompt!.trim())
      : null;
    const accountFilter = accountId ? 'AND account_id = $11::uuid' : '';
    const updateParams = [
      id,
      body.name ?? null,
      body.purpose !== undefined,
      body.purpose ?? null,
      body.plan ?? null,
      body.status ?? null,
      needsPromptState,
      effectiveType,
      storedCustomExtractionPrompt,
      storedCustomCurationPrompt
    ];
    if (accountId) updateParams.push(accountId);

    const result = await query(
      `UPDATE vaults
       SET name = COALESCE($2, name),
           purpose = CASE
             WHEN $3::boolean THEN $4
             ELSE purpose
           END,
           plan_id = COALESCE($5, plan_id),
           status = COALESCE($6, status),
           type = CASE
             WHEN $7::boolean THEN $8
             ELSE type
           END,
           custom_extraction_prompt = CASE
             WHEN $7::boolean THEN $9
             ELSE custom_extraction_prompt
           END,
           custom_curation_prompt = CASE
             WHEN $7::boolean THEN $10
             ELSE custom_curation_prompt
           END
       WHERE id = $1
         ${accountFilter}
       RETURNING id, name, purpose, created_at, settings, plan_id, status, account_id, vault_encryption_enabled,
                 type,
                 custom_extraction_prompt IS NOT NULL AS has_custom_extraction_prompt,
                 custom_curation_prompt IS NOT NULL AS has_custom_curation_prompt`,
      updateParams
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Vault not found' });
    }

    setCustomerMetricVaultId(request, result.rows[0].id);
    return result.rows[0];
  });

  app.delete('/admin/vaults/:id', { preHandler: vaultDeleteAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const accountId = getAuthAccountId(request);
    const storage = getRawChunkStorage();
    const result = await withTransaction(async (client) => {
      if (accountId) {
        const vaultAccess = await client.query<{ id: string }>(
          `SELECT id
           FROM vaults
           WHERE id = $1
             AND account_id = $2::uuid
           LIMIT 1`,
          [params.id, accountId]
        );
        if (!vaultAccess.rowCount) {
          return {
            deletedVault: { rowCount: 0, rows: [] as Array<{ account_id: string | null; id: string }> },
            queuedBlobDeletes: [] as RawChunkBlobDeletionRow[]
          };
        }
      }

      const rawChunks = await client.query<RawChunkBlobRow>(
        `SELECT blob_store, blob_key, storage_bytes::text AS storage_bytes
         FROM raw_chunks
         WHERE vault_id = $1
           AND blob_key IS NOT NULL`,
        [params.id]
      );
      const mismatchedStores = rawChunks.rows.filter((row) => row.blob_store && row.blob_store !== storage.store);
      if (mismatchedStores.length > 0) {
        throw new Error(`Cannot delete ${mismatchedStores.length} raw chunk blobs stored in a different backend`);
      }
      const queuedBlobDeletes = rawChunks.rows.length
        ? await client.query<RawChunkBlobDeletionRow>(
          `INSERT INTO raw_chunk_blob_deletion_queue (vault_id, workspace_id, blob_store, blob_key, storage_bytes)
           SELECT rc.vault_id, v.account_id, COALESCE(rc.blob_store, $2), rc.blob_key, rc.storage_bytes
           FROM raw_chunks rc
           JOIN vaults v
             ON v.id = rc.vault_id
           WHERE rc.vault_id = $1
             AND rc.blob_key IS NOT NULL
           ON CONFLICT (blob_store, blob_key) DO UPDATE SET
             vault_id = EXCLUDED.vault_id,
             workspace_id = EXCLUDED.workspace_id,
             storage_bytes = EXCLUDED.storage_bytes,
             deleted_at = NULL,
             last_error = NULL
           RETURNING id, vault_id, workspace_id::text AS workspace_id, blob_store, blob_key, storage_bytes::text AS storage_bytes`,
          [params.id, storage.store]
        )
        : { rows: [] as RawChunkBlobDeletionRow[] };
      const deletedVault = await client.query<{ account_id: string | null; id: string }>(
        `DELETE FROM vaults
         WHERE id = $1
         RETURNING id, account_id::text AS account_id`,
        [params.id]
      );

      return {
        deletedVault,
        queuedBlobDeletes: queuedBlobDeletes.rows
      };
    });

    if (!result.deletedVault.rowCount) {
      const retryResult = await cleanupQueuedRawChunkBlobDeletes(storage, params.id);
      if (retryResult.attempted > 0) {
        return { id: params.id, deleted: true };
      }
      return reply.code(404).send({ error: 'Vault not found' });
    }

    await cleanupQueuedRawChunkBlobDeletes(
      storage,
      params.id,
      result.queuedBlobDeletes
    );

    setCustomerMetricVaultId(request, result.deletedVault.rows[0].id);
    return { id: result.deletedVault.rows[0].id, deleted: true };
  });

  app.get('/admin/vaults/:id', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const accountId = getAuthAccountId(request);
    const accountFilter = accountId ? 'AND account_id = $2::uuid' : '';
    const params = accountId ? [id, accountId] : [id];
    const result = await query(
      `SELECT id, name, purpose, created_at, settings, plan_id, status, account_id, vault_encryption_enabled,
              type,
              custom_extraction_prompt IS NOT NULL AS has_custom_extraction_prompt,
              custom_curation_prompt IS NOT NULL AS has_custom_curation_prompt
       FROM vaults
       WHERE id = $1
         ${accountFilter}`,
      params
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Vault not found' });
    }

    setCustomerMetricVaultId(request, result.rows[0].id);
    return result.rows[0];
  });

  app.get('/admin/vaults', { preHandler: vaultReadAuth }, async (request) => {
    const accountId = getAuthAccountId(request);
    const accountFilter = accountId ? 'WHERE account_id = $1::uuid' : '';
    const params = accountId ? [accountId] : [];
    const result = await query(
      `SELECT id, name, purpose, created_at, settings, plan_id, status, account_id, vault_encryption_enabled,
              type,
              custom_extraction_prompt IS NOT NULL AS has_custom_extraction_prompt,
              custom_curation_prompt IS NOT NULL AS has_custom_curation_prompt
       FROM vaults
       ${accountFilter}
       ORDER BY created_at DESC`
      ,
      params
    );
    return { items: result.rows };
  });
}

async function cleanupQueuedRawChunkBlobDeletes(
  storage: RawChunkStorage,
  vaultId: string,
  queuedRows?: RawChunkBlobDeletionRow[]
): Promise<{ attempted: number }> {
  const rows = queuedRows ?? (await query<RawChunkBlobDeletionRow>(
    `SELECT id, vault_id, workspace_id::text AS workspace_id, blob_store, blob_key, storage_bytes::text AS storage_bytes
     FROM raw_chunk_blob_deletion_queue
     WHERE vault_id = $1
       AND deleted_at IS NULL
     ORDER BY queued_at, id`,
    [vaultId]
  )).rows;

  const activeRows = rows.filter((row) => row.blob_store === storage.store);
  const mismatchedRows = rows.filter((row) => row.blob_store !== storage.store);
  const errors: unknown[] = mismatchedRows.map((row) =>
    new Error(`Cannot delete raw chunk blob ${row.blob_key} stored in ${row.blob_store} while configured storage is ${storage.store}`)
  );

  await Promise.all(mismatchedRows.map((row, index) =>
    recordRawChunkBlobDeleteFailure(row.id, errors[index])
  ));

  const settled = await Promise.allSettled(activeRows.map(async (row) => {
    try {
      await storage.delete(row.blob_key);
      await query(
        `UPDATE raw_chunk_blob_deletion_queue
         SET deleted_at = now(),
             last_error = NULL
         WHERE id = $1`,
        [row.id]
      );
      recordRawChunkBlobDeleteDelta(row);
    } catch (error) {
      await recordRawChunkBlobDeleteFailure(row.id, error);
      throw error;
    }
  }));
  errors.push(...settled.flatMap((result) => result.status === 'rejected' ? [result.reason] : []));
  if (errors.length > 0) {
    throw new AggregateError(errors, 'Raw chunk blob cleanup failed; retry DELETE /admin/vaults/:id to resume cleanup');
  }

  return { attempted: rows.length };
}

function recordRawChunkBlobDeleteDelta(
  row: RawChunkBlobDeletionRow
): void {
  const accountId = row.workspace_id;
  const storageBytes = toPositiveNumber(row.storage_bytes);
  if (!accountId || storageBytes === null || storageBytes === 0) return;
  recordCustomerMetric({
    event_type: 'storage_delta',
    operation: 'raw_chunk_blob_delete',
    source: 'api',
    storage_bytes_delta: -storageBytes,
    vault_id: row.vault_id,
    workspace_id: accountId
  });
}

function toPositiveNumber(value: string | number | null | undefined): number | null {
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

async function recordRawChunkBlobDeleteFailure(id: string, error: unknown): Promise<void> {
  await query(
    `UPDATE raw_chunk_blob_deletion_queue
     SET last_error = $2
     WHERE id = $1`,
    [id, error instanceof Error ? error.message : String(error)]
  );
}
