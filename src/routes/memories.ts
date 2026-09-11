import crypto from 'node:crypto';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';
import type { QueryResultRow } from 'pg';

import { query } from '../db/client';
import { memoryPolicyEventCounter } from '../services/observability-effects';
import {
  memoryArchivedEventType,
  memoryCreatedEventType,
  type MemoryArchivedPayload,
  type MemoryCreatedPayload,
  type PlatformActor
} from '../events/platform-event';
import { getAuthAccountId, requireAdminScope, requireVaultReadAuth, requireVaultWriteAuth, type PlatformAuthContext, type VaultContext } from '../middleware/auth';
import { setCustomerMetricVaultId } from '../services/customer-api-request-metrics';
import { computeSubjectHmac, decryptForVault, encryptForVault, encryptSubjectForVault, isVaultEncryptionActive, unwrapDek } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { pendingRecallCutoff } from '../services/pending-memory';
import { enforceMemoryCreationLimit, recordMemoryCountDelta } from '../services/usage';
import type { MemoryAuthorityState } from '../services/memory-authority';
import { isScopeWidening, parseMemoryScope, type MemoryScope } from '../services/memory-scope';
import { contextIdentitySchema } from '../services/memory-applicability';

const booleanQueryParam = z.preprocess((value) => {
  if (value === undefined) return false;
  if (Array.isArray(value)) return value[0];
  if (value === 'true' || value === true) return true;
  if (value === 'false' || value === false) return false;
  return value;
}, z.boolean());

export function platformActorForAudit(auth: PlatformAuthContext): PlatformActor {
  if (auth.method === 'oauth' && auth.actor) {
    return auth.actor;
  }
  return {
    id: auth.client_id ?? auth.subject ?? null,
    type: auth.method === 'api_key' ? 'api_key' : 'service'
  };
}

const listQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  category: z.string().optional(),
  filter: z.enum(['all', 'high-confidence', 'low-confidence', 'positive', 'negative']).optional().default('all'),
  include_children: booleanQueryParam,
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0),
  q: z.string().trim().min(1).max(200).optional(),
  sort: z.enum(['recent', 'oldest', 'confidence', 'salience']).optional().default('recent'),
  subject: z.string().trim().min(1).max(500).optional()
});
const adminListQuerySchema = listQuerySchema.extend({
  include_pending: booleanQueryParam
});

const subjectListQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  limit: z.coerce.number().int().min(1).max(500).default(200),
  offset: z.coerce.number().int().min(0).default(0),
  q: z.string().trim().min(1).max(200).optional(),
  sort: z.enum(['count', 'recent', 'name']).optional().default('count')
});

const readMemoryQuerySchema = z.object({
  include_pending: booleanQueryParam
});

export const createMemoryShape = {
  data: z.string().min(1),
  subject: z.string().min(1),
  categories: z.array(z.string().min(1)).optional().default([]),
  parent_id: z.string().uuid().nullable().optional(),
  type: z.enum(['user_preference', 'user_rule', 'task_pattern', 'workflow', 'project', 'constraint', 'decision', 'system_fact', 'domain_knowledge']).optional().default('system_fact'),
  scope: z.enum(['global', 'project', 'task', 'session']),
  scope_key: contextIdentitySchema.nullable().optional(),
  evidence: z.string().optional(),
  volatility: z.enum(['very_low', 'low', 'medium', 'high']).optional().default('low')
};
const createMemorySchema = z.object(createMemoryShape).superRefine((body, context) => {
  if (body.scope === 'global' && body.scope_key != null) {
    context.addIssue({ code: z.ZodIssueCode.custom, path: ['scope_key'], message: 'Global memories must not have a scope_key' });
  } else if (body.scope !== 'global' && !body.scope_key) {
    context.addIssue({ code: z.ZodIssueCode.custom, path: ['scope_key'], message: 'Non-global memories require a scope_key' });
  }
});

export const updateMemoryShape = {
  data: z.string().min(1).optional(),
  subject: z.string().min(1).optional(),
  categories: z.array(z.string().min(1)).optional(),
  confidence: z.number().positive().max(1).optional(),
  type: z.enum(['user_preference', 'user_rule', 'task_pattern', 'workflow', 'project', 'constraint', 'decision', 'system_fact', 'domain_knowledge']).optional(),
  scope: z.enum(['global', 'project', 'task', 'session']).optional(),
  scope_key: contextIdentitySchema.nullable().optional(),
  scope_change_reason: z.string().trim().min(1).max(500).optional(),
  evidence: z.string().nullable().optional(),
  archived: z.boolean().optional()
};
const updateMemorySchema = z.object(updateMemoryShape).refine((body) => (
  Object.keys(body).some((field) => field !== 'scope_change_reason')
), {
  message: 'At least one memory field is required'
}).refine((body) => body.scope !== 'global' || body.scope_key == null, {
  path: ['scope_key'], message: 'Global memories must not have a scope_key'
});

const authorityTransitionSchema = z.object({
  expected_version: z.number().int().positive(),
  reason: z.string().trim().min(1).max(500)
}).strict();

const authorityControlFields = new Set([
  'authority_state',
  'authority_required',
  'authority_version',
  'approved_by',
  'approved_at',
  'approval_source',
  'revoked_by',
  'revoked_at'
]);

function containsAuthorityControlFields(value: unknown): boolean {
  return Boolean(value && typeof value === 'object' && Object.keys(value).some((key) => authorityControlFields.has(key)));
}

const graphEdgeTypes = [
  'applies_to',
  'part_of',
  'depends_on',
  'supports',
  'contradicts',
  'supersedes',
  'refines',
  'relevant_when'
] as const;

const graphQuerySchema = z.object({
  seed_memory_id: z.string().uuid().optional(),
  depth: z.coerce.number().int().min(0).max(4).optional().default(1),
  limit: z.coerce.number().int().min(1).max(100).optional().default(50),
  direction: z.enum(['out', 'in', 'both']).optional().default('both'),
  edge_types: z.preprocess((value) => {
    if (value === undefined) return undefined;
    const values = Array.isArray(value) ? value : [value];
    return values
      .flatMap((item) => String(item).split(','))
      .map((item) => item.trim())
      .filter(Boolean);
  }, z.array(z.enum(graphEdgeTypes)).min(1).max(graphEdgeTypes.length).optional())
});

function memoryResponseSelect(source: string, edgeSource = source): string {
  return `${source}.id, ${source}.vault_id, ${source}.data, ${source}.subject, ${source}.subject_encrypted, ${source}.hash, ${source}.source_chunks,
       ${source}.categories, ${source}.confidence, ${source}.score, ${source}.salience, ${source}.sensitivity, ${source}.type, ${source}.scope, ${source}.scope_key, ${source}.evidence, ${source}.polarity, ${source}.status,
       ${source}.valid_from, ${source}.valid_until, ${source}.source_timestamp, ${source}.archived_at, ${source}.created_at, ${source}.updated_at, ${source}.parent_id, ${source}.volatility,
       ${source}.authority_state, ${source}.authority_required, ${source}.authority_version, ${source}.approved_by, ${source}.approved_at, ${source}.approval_source, ${source}.revoked_by, ${source}.revoked_at,
       COALESCE((SELECT COUNT(*)::int FROM memory_edges edge_counts WHERE edge_counts.from_memory_id = ${edgeSource}.id OR edge_counts.to_memory_id = ${edgeSource}.id), 0) AS edge_count`;
}

type AdminVaultContext = {
  id: string;
  name: string;
  purpose: string | null;
  settings: Record<string, unknown>;
  plan_id: string;
  status: string;
  account_id: string | null;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
};

export async function registerMemoryRoutes(app: FastifyInstance) {
  const vaultReadAuth = requireAdminScope('platform:vaults:read');
  const vaultWriteAuth = requireAdminScope('platform:vaults:update');

  app.get('/admin/vaults/:id/memories', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const qs = adminListQuerySchema.parse(request.query);
    const vault = await getAdminVaultContext(request, id);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });

    setCustomerMetricVaultId(request, vault.id);
    const result = await listMemories(vault, qs, qs.include_pending);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.get('/admin/vaults/:id/memories/subjects', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const qs = subjectListQuerySchema.parse(request.query);
    const vault = await getAdminVaultContext(request, id);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });

    setCustomerMetricVaultId(request, vault.id);
    const result = await listMemorySubjects(vault, qs);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.get('/admin/vaults/:id/memories/graph', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const vault = await getAdminVaultContext(request, id);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });

    setCustomerMetricVaultId(request, vault.id);
    if (!(await vaultCanViewMemoryGraph(vault))) {
      return reply.code(403).send({ error: 'Memory graph requires a graph-capable plan' });
    }

    const parsedQuery = graphQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) {
      return reply.code(400).send({ error: 'Invalid graph query' });
    }

    return memoryGraphResponse(vault, parsedQuery.data, reply);
  });

  app.get('/v1/memories', { preHandler: requireVaultReadAuth }, async (request) => {
    const qs = listQuerySchema.parse(request.query);
    const result = await listMemories(request.vault, qs);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.get('/v1/memories/subjects', { preHandler: requireVaultReadAuth }, async (request) => {
    const qs = subjectListQuerySchema.parse(request.query);
    const result = await listMemorySubjects(request.vault, qs);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.post('/v1/memories', { preHandler: requireVaultWriteAuth }, async (request, reply) => {
    if (containsAuthorityControlFields(request.body)) {
      return reply.code(400).send({ error: 'Memory authority can only be changed through authority endpoints' });
    }
    const parsedBody = createMemorySchema.safeParse(request.body);
    if (!parsedBody.success) {
      return reply.code(400).send({ error: 'Invalid memory payload' });
    }
    const body = parsedBody.data;
    await enforceMemoryCreationLimit(request.vault.id);

    const embedder = getEmbedder();
    const embedding = await embedder.embed(body.data, { vaultId: request.vault.id, modelRole: 'embedding', source: 'api', inputType: 'document' });
    const hash = crypto.createHash('md5').update(body.data).digest('hex');
    const storedData = await encryptForVault(request.vault, body.data);
    const encryptedSubject = await encryptSubjectForVault(request.vault, body.subject);
    const storedSubject = isVaultEncryptionActive(request.vault) ? '' : body.subject;

    if (body.parent_id) {
      const parentCheck = await query(
        'SELECT id FROM memories WHERE id = $1 AND vault_id = $2',
        [body.parent_id, request.vault.id]
      );
      if (parentCheck.rowCount === 0) {
        return reply.status(400).send({ error: 'parent_id does not belong to this vault' });
      }
    }

    const result = await query<Record<string, unknown>>(
      `INSERT INTO memories (
         vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding, categories, parent_id, type, scope, scope_key, evidence, volatility
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::text[], $9, $10, $11, $12, $13::jsonb, $14::memory_volatility)
       RETURNING ${memoryResponseSelect('memories')}`,
      [
        request.vault.id,
        storedData,
        storedSubject,
        encryptedSubject?.encrypted ?? null,
        encryptedSubject?.hmac ?? null,
        hash,
        JSON.stringify(embedding),
        body.categories,
        body.parent_id ?? null,
        body.type,
        body.scope,
        body.scope_key ?? null,
        body.evidence ? JSON.stringify({ summary: body.evidence }) : null,
        body.volatility
      ]
    );

    recordMemoryCountDelta(request.vault.id, request.vault.account_id, 1, 'api');
    await recordMemoryCreatedActivity(request, String(result.rows[0].id));
    await query(
      `INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
       VALUES ($1, $2::vector, now())
       ON CONFLICT (memory_id)
       DO UPDATE SET embedding = EXCLUDED.embedding, embedded_at = now()`,
      [result.rows[0].id, JSON.stringify(embedding)]
    );

    return reply.code(201).send(await decryptMemoryRow(request.vault, result.rows[0]));
  });

  app.get('/v1/memories/graph', { preHandler: requireVaultReadAuth }, async (request, reply) => {
    if (!(await vaultCanViewMemoryGraph(request.vault))) {
      return reply.code(403).send({ error: 'Memory graph requires a graph-capable plan' });
    }

    const parsedQuery = graphQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) {
      return reply.code(400).send({ error: 'Invalid graph query' });
    }

    return memoryGraphResponse(request.vault, parsedQuery.data, reply);
  });

  app.get('/v1/memories/:id', { preHandler: requireVaultReadAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const qs = readMemoryQuerySchema.parse(request.query);
    const values: unknown[] = [request.vault.id, params.id];
    const visibility = qs.include_pending
      ? `(
           status IS NULL
           OR status <> 'candidate'
           OR (
             status = 'candidate'
             AND archived_at IS NULL
             AND COALESCE(source_timestamp, created_at) >= $3::timestamptz
           )
         )`
      : `(status IS NULL OR status <> 'candidate')`;

    if (qs.include_pending) {
      values.push(pendingRecallCutoff().toISOString());
    }

    const result = await query<Record<string, unknown>>(
      `SELECT ${memoryResponseSelect('memories')}
       FROM memories
       WHERE vault_id = $1
         AND id = $2
         AND ${visibility}
       LIMIT 1`,
      values
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
    }

    return decryptMemoryRow(request.vault, result.rows[0]);
  });

  app.delete('/v1/memories/:id', { preHandler: requireVaultWriteAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{ id: string; archived_at: string | null; previous_archived_at: string | null }>(
      `WITH target AS (
         SELECT id, archived_at
         FROM memories
         WHERE vault_id = $1
           AND id = $2
           AND (status IS NULL OR status <> 'candidate')
         LIMIT 1
         FOR UPDATE
       ), updated AS (
         UPDATE memories
         SET archived_at = COALESCE(memories.archived_at, now()),
             updated_at = now()
         FROM target
         WHERE memories.id = target.id
         RETURNING memories.id, memories.archived_at, target.archived_at AS previous_archived_at
       )
       SELECT id, archived_at, previous_archived_at
       FROM updated`,
      [request.vault.id, params.id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
    }

    if (result.rows[0].previous_archived_at === null) {
      recordMemoryCountDelta(request.vault.id, request.vault.account_id, -1, 'api');
      await recordMemoryArchivedActivity(request, String(result.rows[0].id));
    }
    const { previous_archived_at: _previousArchivedAt, ...response } = result.rows[0];
    return response;
  });

  app.patch('/v1/memories/:id', { preHandler: requireVaultWriteAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    if (containsAuthorityControlFields(request.body)) {
      return reply.code(400).send({ error: 'Memory authority can only be changed through authority endpoints' });
    }
    const parsedBody = updateMemorySchema.safeParse(request.body);
    if (!parsedBody.success) {
      return reply.code(400).send({ error: 'Invalid memory payload' });
    }
    const body = parsedBody.data;

    const existing = await query<{ id: string; scope: MemoryScope; scope_key: string | null }>(
      `SELECT id, scope, scope_key
       FROM memories
       WHERE vault_id = $1
         AND id = $2
         AND (status IS NULL OR status <> 'candidate')
       LIMIT 1`,
      [request.vault.id, params.id]
    );

    if (!existing.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
    }

    const current = existing.rows[0];
    const currentScope = parseMemoryScope(current.scope);
    if (!currentScope) {
      request.log.error({ memory_id: params.id, scope: current.scope }, 'memory has invalid persisted scope');
      return reply.code(409).send({ error: 'Memory has an invalid persisted scope' });
    }
    const nextScope = body.scope ?? currentScope;
    if (isScopeWidening(currentScope, nextScope)) {
      memoryPolicyEventCounter.add(1, { event: 'scope_widening_attempt', source: 'api', outcome: body.scope_change_reason ? 'authorized' : 'rejected' });
      request.log.warn({
        vault_id: request.vault.id,
        memory_id: params.id,
        old_scope: currentScope,
        requested_scope: nextScope,
        authorized_reason_present: Boolean(body.scope_change_reason)
      }, 'memory scope widening attempted');
      if (!body.scope_change_reason) {
        return reply.code(400).send({ error: 'scope_change_reason is required when widening memory scope' });
      }
    }
    if (body.scope !== undefined && body.scope !== 'global' && body.scope_key === undefined) {
      return reply.code(400).send({ error: 'scope_key must be explicit when changing memory scope' });
    }
    const currentScopeKey = current.scope_key ?? null;
    const nextScopeKey = body.scope === 'global'
      ? null
      : body.scope_key !== undefined ? body.scope_key : currentScopeKey;
    if ((body.scope !== undefined || body.scope_key !== undefined) && (nextScope === 'global' ? nextScopeKey !== null : nextScopeKey === null)) {
      return reply.code(400).send({ error: nextScope === 'global' ? 'Global memories must not have a scope_key' : 'Non-global memories require a scope_key' });
    }
    if (currentScopeKey !== nextScopeKey && !body.scope_change_reason) {
      return reply.code(400).send({ error: 'scope_change_reason is required when changing memory scope binding' });
    }
    const scopeActor = platformActorForAudit(request.auth);
    const suppliedStoredData = body.data
      ? await encryptForVault(request.vault, body.data)
      : null;
    const suppliedStoredSubject = body.subject
      ? isVaultEncryptionActive(request.vault) ? '' : body.subject
      : null;
    const suppliedEncryptedSubject = body.subject
      ? await encryptSubjectForVault(request.vault, body.subject)
      : null;
    let embedding: string | undefined;
    let hash: string | undefined;

    if (body.data) {
      const embedder = getEmbedder();
      embedding = JSON.stringify(await embedder.embed(body.data, { vaultId: request.vault.id, modelRole: 'embedding', source: 'api', inputType: 'document' }));
      hash = crypto.createHash('md5').update(body.data).digest('hex');
    }

    const result = await query<Record<string, unknown> & {
      archived_at: string | null;
      previous_archived_at: string | null;
      previous_scope: MemoryScope;
      previous_scope_key: string | null;
      scope_change_id: string | null;
      previous_authority_state: MemoryAuthorityState;
      previous_authority_version: string | number;
      invalidates_authority: boolean;
      authority_event_id: string | null;
    }>(
      `WITH target AS (
         SELECT id, archived_at, scope, type, authority_required, authority_state, authority_version,
                scope_key,
                (
                  $11::text IS NOT NULL
                  OR $4::text IS NOT NULL
                  OR ($7::text[] IS NOT NULL AND $7::text[] IS DISTINCT FROM categories)
                  OR ($9::text IS NOT NULL AND $9::text IS DISTINCT FROM type)
                  OR ($10::text IS NOT NULL AND $10::text IS DISTINCT FROM scope)
                  OR ($21::boolean AND $22::text IS DISTINCT FROM scope_key)
                  OR $19::boolean
                ) AS invalidates_authority
         FROM memories
         WHERE vault_id = $1
           AND id = $2
           AND scope = $23
           AND scope_key IS NOT DISTINCT FROM $24::text
           AND (status IS NULL OR status <> 'candidate')
         LIMIT 1
         FOR UPDATE
       ), updated AS (
         UPDATE memories
         SET data = COALESCE($3, memories.data),
             subject = COALESCE($4, memories.subject),
             subject_encrypted = CASE WHEN $4::text IS NOT NULL THEN $5 ELSE memories.subject_encrypted END,
             subject_hmac = CASE WHEN $4::text IS NOT NULL THEN $6 ELSE memories.subject_hmac END,
             categories = COALESCE($7::text[], memories.categories),
             confidence = COALESCE($8, memories.confidence),
             type = COALESCE($9, memories.type),
             scope = COALESCE($10::text, memories.scope),
             scope_key = CASE WHEN $21::boolean THEN $22::text ELSE memories.scope_key END,
             updated_at = now(),
             hash = COALESCE($11, hash),
             embedding = COALESCE($12::vector, embedding),
             evidence = CASE
               WHEN $19::boolean IS FALSE THEN memories.evidence
               WHEN $13::text IS NULL
                 AND (
                   jsonb_typeof(memories.evidence) IS DISTINCT FROM 'object'
                   OR (memories.evidence - 'summary') = '{}'::jsonb
                 )
                 THEN NULL
               ELSE
                 CASE
                   WHEN jsonb_typeof(memories.evidence) = 'object' THEN memories.evidence
                   ELSE '{}'::jsonb
                 END || jsonb_build_object('summary', $13::text)
             END,
             authority_required = CASE WHEN target.invalidates_authority THEN true ELSE memories.authority_required END,
             authority_state = CASE WHEN target.invalidates_authority THEN 'proposed' ELSE memories.authority_state END,
             approved_by = CASE WHEN target.invalidates_authority THEN NULL ELSE memories.approved_by END,
             approved_at = CASE WHEN target.invalidates_authority THEN NULL ELSE memories.approved_at END,
             approval_source = CASE WHEN target.invalidates_authority THEN NULL ELSE memories.approval_source END,
             revoked_by = CASE WHEN target.invalidates_authority THEN NULL ELSE memories.revoked_by END,
             revoked_at = CASE WHEN target.invalidates_authority THEN NULL ELSE memories.revoked_at END,
             authority_version = CASE WHEN target.invalidates_authority THEN memories.authority_version + 1 ELSE memories.authority_version END,
             archived_at = CASE
               WHEN $14::boolean IS FALSE THEN memories.archived_at
               WHEN $15::boolean THEN COALESCE(memories.archived_at, now())
               ELSE NULL
             END
         FROM target
         WHERE memories.id = target.id
           AND target.scope IN ('global', 'project', 'task', 'session')
           AND (
             $10::text IS NULL
             OR $18::text IS NOT NULL
             OR CASE COALESCE($10::text, target.scope)
                  WHEN 'session' THEN 0
                  WHEN 'task' THEN 1
                  WHEN 'project' THEN 2
                  WHEN 'global' THEN 3
                END
                <= CASE target.scope
                     WHEN 'session' THEN 0
                     WHEN 'task' THEN 1
                     WHEN 'project' THEN 2
                     WHEN 'global' THEN 3
                   END
           )
         RETURNING ${memoryResponseSelect('memories')},
                   target.archived_at AS previous_archived_at,
                   target.scope AS previous_scope,
                   target.scope_key AS previous_scope_key,
                   target.authority_state AS previous_authority_state,
                   target.authority_version AS previous_authority_version,
                   target.invalidates_authority
       ), scope_audit AS (
         INSERT INTO memory_scope_change_log (
           vault_id, memory_id, old_scope, new_scope, old_scope_key, new_scope_key, actor_type, actor_id, source, reason
         )
         SELECT $1, updated.id, updated.previous_scope, updated.scope, updated.previous_scope_key, updated.scope_key, $16, $17, 'api',
                COALESCE($18, 'Scope narrowed through the memory API')
         FROM updated
         WHERE updated.scope IS DISTINCT FROM updated.previous_scope
            OR updated.scope_key IS DISTINCT FROM updated.previous_scope_key
         RETURNING id
       ), authority_audit AS (
         INSERT INTO memory_authority_events (
           vault_id, memory_id, event_type, old_state, new_state, old_version, new_version,
           actor_type, actor_id, source, reason
         )
         SELECT $1, updated.id, 'invalidate', updated.previous_authority_state, updated.authority_state,
                updated.previous_authority_version, updated.authority_version,
                $16, $17, 'api', $20
         FROM updated
         WHERE updated.invalidates_authority
         RETURNING id
       )
       SELECT updated.*,
              (SELECT id FROM scope_audit LIMIT 1) AS scope_change_id,
              (SELECT id FROM authority_audit LIMIT 1) AS authority_event_id
       FROM updated`,
      [
        request.vault.id,
        params.id,
        suppliedStoredData,
        suppliedStoredSubject,
        suppliedEncryptedSubject?.encrypted ?? null,
        suppliedEncryptedSubject?.hmac ?? null,
        body.categories ?? null,
        body.confidence ?? null,
        body.type ?? null,
        body.scope ?? null,
        hash,
        embedding,
        body.evidence ?? null,
        body.archived !== undefined,
        body.archived ?? false,
        scopeActor.type,
        scopeActor.id,
        body.scope_change_reason ?? null,
        body.evidence !== undefined,
        'Memory prompt-bearing content, type, scope, or scope binding changed through the API; approval requires review.',
        body.scope_key !== undefined || body.scope === 'global',
        nextScopeKey,
        currentScope,
        currentScopeKey
      ]
    );

    if (!result.rowCount) {
      return reply.code(409).send({ error: 'Memory changed concurrently; retry with the latest scope' });
    }

    const memoryCountDelta = result.rows[0].previous_archived_at === null && result.rows[0].archived_at !== null
      ? -1
      : result.rows[0].previous_archived_at !== null && result.rows[0].archived_at === null
        ? 1
        : 0;
    recordMemoryCountDelta(request.vault.id, request.vault.account_id, memoryCountDelta, 'api');
    if (memoryCountDelta < 0) {
      await recordMemoryArchivedActivity(request, String(result.rows[0].id));
    }

    if (embedding) {
      await query(
        `INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
         VALUES ($1, $2::vector, now())
         ON CONFLICT (memory_id)
         DO UPDATE SET embedding = EXCLUDED.embedding, embedded_at = now()`,
        [params.id, embedding]
      );
    }

    const {
      previous_archived_at: _previousArchivedAt,
      previous_scope: _previousScope,
      previous_scope_key: _previousScopeKey,
      scope_change_id: _scopeChangeId,
      previous_authority_state: _previousAuthorityState,
      previous_authority_version: _previousAuthorityVersion,
      invalidates_authority: _invalidatesAuthority,
      authority_event_id: _authorityEventId,
      ...responseRow
    } = result.rows[0];
    return decryptMemoryRow(request.vault, responseRow);
  });

  app.post('/admin/vaults/:vaultId/memories/:id/authority/approve', { preHandler: vaultWriteAuth }, async (request, reply) => {
    const params = z.object({ vaultId: z.string().uuid(), id: z.string().uuid() }).safeParse(request.params);
    if (!params.success) return reply.code(400).send({ error: 'Invalid authority transition path' });
    const vault = await getAdminVaultContext(request, params.data.vaultId);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });
    return transitionMemoryAuthority(request, reply, vault, params.data.id, 'approved');
  });

  app.post('/admin/vaults/:vaultId/memories/:id/authority/revoke', { preHandler: vaultWriteAuth }, async (request, reply) => {
    const params = z.object({ vaultId: z.string().uuid(), id: z.string().uuid() }).safeParse(request.params);
    if (!params.success) return reply.code(400).send({ error: 'Invalid authority transition path' });
    const vault = await getAdminVaultContext(request, params.data.vaultId);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });
    return transitionMemoryAuthority(request, reply, vault, params.data.id, 'revoked');
  });
}

async function transitionMemoryAuthority(
  request: FastifyRequest,
  reply: FastifyReply,
  vault: AdminVaultContext,
  memoryId: string,
  nextState: Extract<MemoryAuthorityState, 'approved' | 'revoked'>
) {
  const parsedBody = authorityTransitionSchema.safeParse(request.body);
  if (!parsedBody.success) {
    return reply.code(400).send({ error: 'Invalid authority transition payload' });
  }

  const currentResult = await query<{
    id: string;
    type: string | null;
    authority_required: boolean;
    authority_state: MemoryAuthorityState;
    authority_version: string | number;
  }>(
    `SELECT id, type, authority_required, authority_state, authority_version
     FROM memories
     WHERE vault_id = $1
       AND id = $2
       AND ($3::boolean OR archived_at IS NULL)
     LIMIT 1`,
    [vault.id, memoryId, nextState === 'revoked']
  );
  if (!currentResult.rowCount) {
    return reply.code(404).send({ error: 'Memory not found' });
  }

  const current = currentResult.rows[0];
  if (!current.authority_required) {
    return reply.code(400).send({ error: 'Authority transitions apply only to authority-controlled memories' });
  }
  if (Number(current.authority_version) !== parsedBody.data.expected_version) {
    return reply.code(409).send({ error: 'Memory authority version conflict' });
  }
  if (current.authority_state === nextState) {
    return reply.code(409).send({ error: `Memory is already ${nextState}` });
  }

  const actor = platformActorForAudit(request.auth);
  const eventType = nextState === 'approved' ? 'approve' : 'revoke';
  const result = await query<Record<string, unknown> & {
    previous_authority_state: MemoryAuthorityState;
    previous_authority_version: string | number;
    authority_event_id: string;
  }>(
    `WITH target AS (
       SELECT id, type, authority_required, authority_state, authority_version
       FROM memories
       WHERE vault_id = $1
         AND id = $2
         AND ($3::text = 'revoked' OR archived_at IS NULL)
       LIMIT 1
       FOR UPDATE
     ), updated AS (
       UPDATE memories
       SET authority_state = $3,
           approved_by = CASE WHEN $3 = 'approved' THEN $5 ELSE memories.approved_by END,
           approved_at = CASE WHEN $3 = 'approved' THEN now() ELSE memories.approved_at END,
           approval_source = CASE WHEN $3 = 'approved' THEN $6 ELSE memories.approval_source END,
           revoked_by = CASE WHEN $3 = 'revoked' THEN $5 ELSE NULL END,
           revoked_at = CASE WHEN $3 = 'revoked' THEN now() ELSE NULL END,
           authority_version = memories.authority_version + 1,
           updated_at = now()
       FROM target
       WHERE memories.id = target.id
         AND target.authority_required
         AND target.authority_version = $8
         AND target.authority_state <> $3
       RETURNING ${memoryResponseSelect('memories')},
                 target.authority_state AS previous_authority_state,
                 target.authority_version AS previous_authority_version
     ), authority_audit AS (
       INSERT INTO memory_authority_events (
         vault_id, memory_id, event_type, old_state, new_state, old_version, new_version,
         actor_type, actor_id, source, reason
       )
       SELECT $1, updated.id, $4, updated.previous_authority_state, updated.authority_state,
              updated.previous_authority_version, updated.authority_version,
              $7, $5, 'api', $9
       FROM updated
       RETURNING id
     )
     SELECT updated.*, (SELECT id FROM authority_audit LIMIT 1) AS authority_event_id
     FROM updated`,
    [
      vault.id,
      memoryId,
      nextState,
      eventType,
      actor.id,
      request.auth?.method ?? 'api_key',
      actor.type,
      parsedBody.data.expected_version,
      parsedBody.data.reason
    ]
  );
  if (!result.rowCount) {
    return reply.code(409).send({ error: 'Memory authority changed concurrently' });
  }

  const {
    previous_authority_state: _previousState,
    previous_authority_version: _previousVersion,
    authority_event_id: _authorityEventId,
    ...responseRow
  } = result.rows[0];
  return decryptMemoryRow(vault, responseRow);
}

async function recordMemoryCreatedActivity(
  request: FastifyRequest,
  memoryId: string
): Promise<void> {
  const workspaceId = request.vault.account_id;
  if (!workspaceId) return;
  const actor = platformActorForAudit(request.auth);
  const payload: MemoryCreatedPayload = {
    actor,
    counts: { memories_added: 1 },
    memory_id: memoryId,
    platform_vault_id: request.vault.id,
    sensitivity: 'metadata_only',
    source: 'api',
    summary: 'New memory added',
    vault_id: request.vault.id,
    workspace_id: workspaceId
  };
  await writeActivityOutboxEvent(request, {
    eventType: memoryCreatedEventType,
    payload,
    subject: `vault:${request.vault.id}/memory:${memoryId}`
  });
}

async function recordMemoryArchivedActivity(
  request: FastifyRequest,
  memoryId: string
): Promise<void> {
  const workspaceId = request.vault.account_id;
  if (!workspaceId) return;
  const actor = platformActorForAudit(request.auth);
  const payload: MemoryArchivedPayload = {
    actor,
    counts: { memories_archived: 1 },
    memory_id: memoryId,
    platform_vault_id: request.vault.id,
    sensitivity: 'metadata_only',
    source: 'api',
    summary: 'Memory archived',
    vault_id: request.vault.id,
    workspace_id: workspaceId
  };
  await writeActivityOutboxEvent(request, {
    eventType: memoryArchivedEventType,
    payload,
    subject: `vault:${request.vault.id}/memory:${memoryId}`
  });
}

async function writeActivityOutboxEvent(
  request: FastifyRequest,
  input: {
    eventType: string;
    payload: MemoryArchivedPayload | MemoryCreatedPayload;
    subject: string;
  }
): Promise<void> {
  try {
    await query(
      `INSERT INTO platform_event_outbox (
         event_id, event_type, schema_version, occurred_at, subject, payload
       )
       VALUES (gen_random_uuid(), $1, 1, now(), $2, $3::jsonb)`,
      [input.eventType, input.subject, JSON.stringify(input.payload)]
    );
  } catch (error) {
    request.log.warn({
      err: error,
      event_type: input.eventType,
      vault_id: request.vault.id
    }, 'failed to write activity event outbox row');
  }
}

type GraphDirection = 'out' | 'in' | 'both';
type GraphEdgeType = typeof graphEdgeTypes[number];

interface GraphNodeRow extends MemoryResponseRow {
  id: string;
  depth: number;
}

interface GraphEdgeRow extends QueryResultRow {
  id: string;
  from_memory_id: string;
  to_memory_id: string;
  type: GraphEdgeType;
  confidence: number;
  reason: string | null;
  created_at: string;
  updated_at: string;
}

async function fetchSeededGraphNodes(
  vaultId: string,
  seedMemoryId: string,
  depth: number,
  limit: number,
  direction: GraphDirection,
  edgeTypes: GraphEdgeType[] | null
): Promise<GraphNodeRow[]> {
  const seed = await fetchVisibleGraphSeed(vaultId, seedMemoryId);
  if (!seed) return [];

  const nodes: GraphNodeRow[] = [seed];
  const seen = new Set<string>([seed.id]);
  let frontier: GraphNodeRow[] = [seed];

  for (let nextDepth = 1; nextDepth <= depth && nodes.length < limit && frontier.length > 0; nextDepth += 1) {
    const remaining = limit - nodes.length;
    const neighbors = await fetchGraphNeighborNodes(
      vaultId,
      frontier.map((node) => node.id),
      [...seen],
      nextDepth,
      remaining,
      direction,
      edgeTypes
    );
    const uniqueNeighbors = neighbors.filter((node) => {
      if (seen.has(node.id)) return false;
      seen.add(node.id);
      return true;
    });

    nodes.push(...uniqueNeighbors);
    frontier = uniqueNeighbors;
  }

  return nodes;
}

async function fetchVisibleGraphSeed(vaultId: string, seedMemoryId: string): Promise<GraphNodeRow | null> {
  const result = await query<GraphNodeRow>(
    `SELECT ${memoryResponseSelect('memories')}, 0 AS depth
     FROM memories
     WHERE vault_id = $1
       AND id = $2
       AND archived_at IS NULL
       AND (status IS NULL OR status <> 'candidate')
     LIMIT 1`,
    [vaultId, seedMemoryId]
  );

  return result.rows[0] ?? null;
}

function getNeighborEdgeSelect(direction: GraphDirection): string {
  const outbound = `SELECT e.to_memory_id AS neighbor_id, e.confidence, e.updated_at AS edge_updated_at
                   FROM memory_edges e
                   WHERE e.vault_id = $1
                     AND e.from_memory_id = frontier.id
                     AND NOT (e.to_memory_id = ANY($3::uuid[]))
                     AND ($4::text[] IS NULL OR e.type = ANY($4::text[]))`;
  const inbound = `SELECT e.from_memory_id AS neighbor_id, e.confidence, e.updated_at AS edge_updated_at
                  FROM memory_edges e
                  WHERE e.vault_id = $1
                    AND e.to_memory_id = frontier.id
                    AND NOT (e.from_memory_id = ANY($3::uuid[]))
                    AND ($4::text[] IS NULL OR e.type = ANY($4::text[]))`;

  if (direction === 'out') return outbound;
  if (direction === 'in') return inbound;
  return `${outbound} UNION ALL ${inbound}`;
}

async function fetchGraphNeighborNodes(
  vaultId: string,
  frontierIds: string[],
  excludedIds: string[],
  nextDepth: number,
  limit: number,
  direction: GraphDirection,
  edgeTypes: GraphEdgeType[] | null
): Promise<GraphNodeRow[]> {
  const perFrontierNodeLimit = Math.min(Math.max(limit * 2, 10), 50);
  const neighborEdgeSelect = getNeighborEdgeSelect(direction);
  const result = await query<GraphNodeRow>(
    `WITH frontier AS (
       SELECT unnest($2::uuid[]) AS id
     ), candidate_edges AS (
       SELECT candidate.neighbor_id, candidate.confidence, candidate.edge_updated_at
       FROM frontier
       CROSS JOIN LATERAL (
         ${neighborEdgeSelect}
         ORDER BY confidence DESC, edge_updated_at DESC, neighbor_id
         LIMIT $7
       ) candidate
     ), ranked_edges AS (
       SELECT neighbor_id,
              MAX(confidence) AS confidence,
              MAX(edge_updated_at) AS edge_updated_at
       FROM candidate_edges
       GROUP BY neighbor_id
     )
     SELECT ${memoryResponseSelect('memories')}, $5::int AS depth
     FROM ranked_edges
     JOIN memories ON memories.id = ranked_edges.neighbor_id
     WHERE memories.vault_id = $1
       AND memories.archived_at IS NULL
       AND (memories.status IS NULL OR memories.status <> 'candidate')
     ORDER BY ranked_edges.confidence DESC, memories.salience DESC, ranked_edges.edge_updated_at DESC, memories.updated_at DESC, memories.id
     LIMIT $6`,
    [vaultId, frontierIds, excludedIds, edgeTypes, nextDepth, limit, perFrontierNodeLimit]
  );

  return result.rows;
}

async function fetchGraphOverviewNodes(vaultId: string, limit: number): Promise<GraphNodeRow[]> {
  const result = await query<GraphNodeRow>(
    `SELECT ${memoryResponseSelect('memories')}, 0 AS depth
     FROM memories
     WHERE vault_id = $1
       AND archived_at IS NULL
       AND (status IS NULL OR status <> 'candidate')
     ORDER BY salience DESC, updated_at DESC, created_at DESC, id
     LIMIT $2`,
    [vaultId, limit]
  );

  return result.rows;
}

async function fetchGraphEdges(
  vaultId: string,
  nodeIds: string[],
  edgeTypes: GraphEdgeType[] | null
): Promise<GraphEdgeRow[]> {
  const result = await query<GraphEdgeRow>(
    `SELECT id, from_memory_id, to_memory_id, type, confidence, reason, created_at, updated_at
     FROM memory_edges
     WHERE vault_id = $1
       AND from_memory_id = ANY($2::uuid[])
       AND to_memory_id = ANY($2::uuid[])
       AND ($3::text[] IS NULL OR type = ANY($3::text[]))
     ORDER BY type ASC, confidence DESC, updated_at DESC, id
     LIMIT 500`,
    [vaultId, nodeIds, edgeTypes]
  );

  return result.rows;
}

interface MemoryResponseRow extends QueryResultRow {
  categories?: unknown;
  confidence?: number | null;
  created_at?: unknown;
  data?: unknown;
  polarity?: unknown;
  salience?: unknown;
  scope?: unknown;
  source_timestamp?: unknown;
  subject?: unknown;
  subject_encrypted?: unknown;
  type?: unknown;
  updated_at?: unknown;
}

interface MemorySubjectRow extends QueryResultRow {
  subject: unknown;
  subject_encrypted?: unknown;
  subject_hmac?: unknown;
  count: unknown;
  latest_at: unknown;
}

interface MemorySubjectSummary {
  count: number;
  latest_at: string | null;
  subject: string;
}

async function getAdminVaultContext(request: FastifyRequest, id: string): Promise<AdminVaultContext | null> {
  const accountId = getAuthAccountId(request);
  const accountFilter = accountId ? 'AND account_id = $2::uuid' : '';
  const vaultParams: unknown[] = accountId ? [id, accountId] : [id];
  const vaultResult = await query<AdminVaultContext>(
    `SELECT id, name, purpose, settings, plan_id, status, account_id, encrypted_dek, vault_encryption_enabled
     FROM vaults
     WHERE id = $1
       ${accountFilter}
     LIMIT 1`,
    vaultParams
  );

  return vaultResult.rows[0] ?? null;
}

async function vaultCanViewMemoryGraph(vault: Pick<VaultContext, 'plan_id'>): Promise<boolean> {
  const planResult = await query<{ limits: Record<string, unknown> | null }>(
    `SELECT limits
     FROM plans
     WHERE id = $1
     LIMIT 1`,
    [vault.plan_id]
  );
  const limits = planResult.rows[0]?.limits ?? {};
  return readBoolean(limits.graphEnabled ?? limits.graph_enabled) === true;
}

async function memoryGraphResponse(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof graphQuerySchema>,
  reply: { code: (statusCode: number) => { send: (payload: unknown) => unknown } }
) {
  const edgeTypes = qs.edge_types ?? null;
  const nodes = qs.seed_memory_id
    ? await fetchSeededGraphNodes(vault.id, qs.seed_memory_id, qs.depth, qs.limit, qs.direction, edgeTypes)
    : await fetchGraphOverviewNodes(vault.id, qs.limit);

  if (qs.seed_memory_id && nodes.length === 0) {
    return reply.code(404).send({ error: 'Seed memory not found' });
  }

  const nodeIds = nodes.map((node) => String(node.id));
  const edges = nodeIds.length
    ? await fetchGraphEdges(vault.id, nodeIds, edgeTypes)
    : [];
  const decryptedNodes = await Promise.all(nodes.map((row) => decryptMemoryRow(vault, row)));

  return {
    seed_memory_id: qs.seed_memory_id ?? null,
    depth: qs.depth,
    limit: qs.limit,
    direction: qs.direction,
    edge_types: qs.edge_types ?? null,
    nodes: decryptedNodes,
    edges
  };
}

function readBoolean(value: unknown): boolean | null {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') return true;
    if (normalized === 'false') return false;
  }
  return null;
}

async function listMemories(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof listQuerySchema>,
  includePending = false
): Promise<{ items: Awaited<ReturnType<typeof decryptMemoryRow>>[]; total: number }> {
  if (isVaultEncryptionActive(vault) && qs.q) {
    return listEncryptedSearchMemories(vault, qs, includePending);
  }

  const rows = await listMemoryRows(vault, qs, includePending);
  const items = await Promise.all(rows.items.map((row) => decryptMemoryRow(vault, row)));
  return { items, total: rows.total };
}

async function listMemoryRows(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof listQuerySchema>,
  includePending = false
): Promise<{ items: MemoryResponseRow[]; total: number }> {
  const values: unknown[] = [vault.id];
  const conditions = [`vault_id = $1`];
  if (!includePending) conditions.push(`status <> 'candidate'`);

  if (qs.archived === 'false') {
    conditions.push('archived_at IS NULL');
  } else {
    conditions.push('archived_at IS NOT NULL');
  }

  if (qs.category) {
    values.push(qs.category);
    conditions.push(`$${values.length} = ANY(categories)`);
  }

  if (qs.filter === 'high-confidence') {
    conditions.push('confidence >= 0.8');
  } else if (qs.filter === 'low-confidence') {
    conditions.push('(confidence IS NULL OR confidence < 0.5)');
  } else if (qs.filter === 'positive') {
    conditions.push(`polarity = 'positive'`);
  } else if (qs.filter === 'negative') {
    conditions.push(`polarity = 'negative'`);
  }

  if (qs.subject) {
    if (isVaultEncryptionActive(vault)) {
      values.push(await subjectHmacForVault(vault, qs.subject));
      conditions.push(`subject_hmac = $${values.length}`);
    } else {
      values.push(qs.subject);
      conditions.push(`subject = $${values.length}`);
    }
  }

  if (qs.q && !isVaultEncryptionActive(vault)) {
    values.push(`%${escapeLikePattern(qs.q)}%`);
    const index = values.length;
    conditions.push(`(subject ILIKE $${index} ESCAPE '\\' OR data ILIKE $${index} ESCAPE '\\' OR type ILIKE $${index} ESCAPE '\\' OR scope ILIKE $${index} ESCAPE '\\' OR EXISTS (SELECT 1 FROM unnest(categories) category WHERE category ILIKE $${index} ESCAPE '\\'))`);
  }

  let sql: string;
  let totalSql: string;
  if (qs.include_children) {
    const treeSql = `WITH RECURSIVE tree AS (
             SELECT *, 0 AS depth FROM memories WHERE ${conditions.join(' AND ')}
             UNION ALL
             SELECT m.*, t.depth + 1 FROM memories m
             JOIN tree t ON m.parent_id = t.id
             WHERE m.vault_id = $1
               AND ${qs.archived === 'false'
                 ? `m.archived_at IS NULL${includePending ? '' : ` AND m.status <> 'candidate'`}`
                 : `m.archived_at IS NOT NULL${includePending ? '' : ` AND m.status <> 'candidate'`}`}
               AND t.depth < 10
           )`;
    const finalArchivedClause = qs.archived === 'false'
      ? `archived_at IS NULL${includePending ? '' : ` AND status <> 'candidate'`}`
      : `archived_at IS NOT NULL${includePending ? '' : ` AND status <> 'candidate'`}`;
    sql = `${treeSql}
           SELECT ${memoryResponseSelect('tree')}
           FROM tree
           WHERE ${finalArchivedClause}
           ORDER BY created_at DESC
           LIMIT 1000`;
    totalSql = `${treeSql}
           SELECT COUNT(*)::int AS total
           FROM tree
           WHERE ${finalArchivedClause}`;
  } else {
    values.push(qs.limit, qs.offset);
    totalSql = `SELECT COUNT(*)::int AS total FROM memories WHERE ${conditions.join(' AND ')}`;
    sql = `SELECT ${memoryResponseSelect('memories')}
           FROM memories
           WHERE ${conditions.join(' AND ')}
           ORDER BY ${memoryOrderBy(qs.sort)}
           LIMIT $${values.length - 1}
           OFFSET $${values.length}`;
  }

  const countValues = qs.include_children ? values : values.slice(0, -2);
  const [result, countResult] = await Promise.all([
    query<MemoryResponseRow>(sql, values),
    query<{ total: number }>(totalSql, countValues)
  ]);
  const offset = parseInt(String(qs.offset)) || 0;
  const limit = parseInt(String(qs.limit)) || 50;
  const items = qs.include_children
    ? result.rows.slice(offset, offset + limit)
    : result.rows;
  return { items, total: Number(countResult.rows[0]?.total ?? items.length) };
}

async function listEncryptedSearchMemories(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof listQuerySchema>,
  includePending = false
): Promise<{ items: Awaited<ReturnType<typeof decryptMemoryRow>>[]; total: number }> {
  const pageSize = 200;
  const rows: MemoryResponseRow[] = [];
  let offset = 0;
  let total = 0;

  do {
    const page = await listMemoryRows(vault, { ...qs, limit: pageSize, offset, q: undefined }, includePending);
    rows.push(...page.items);
    total = page.total;
    if (page.items.length === 0) break;
    offset += page.items.length;
  } while (offset < total && rows.length < total);

  const decrypted = await Promise.all(rows.map((row) => decryptMemoryRow(vault, row)));
  const normalized = normalizeSearch(qs.q ?? '');
  const filtered = decrypted
    .filter((memory) => memoryMatchesQuery(memory, normalized))
    .sort((a, b) => compareMemories(a, b, qs.sort));

  return {
    items: filtered.slice(qs.offset, qs.offset + qs.limit),
    total: filtered.length
  };
}

async function listMemorySubjects(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof subjectListQuerySchema>
): Promise<{ items: MemorySubjectSummary[]; total: number }> {
  const values: unknown[] = [vault.id];
  const conditions = [`vault_id = $1`, `status <> 'candidate'`];
  if (qs.archived === 'false') {
    conditions.push('archived_at IS NULL');
  } else {
    conditions.push('archived_at IS NOT NULL');
  }

  const result = await query<MemorySubjectRow>(
    `SELECT
       COALESCE(subject_hmac, subject, 'Unknown subject') AS subject_hmac,
       MIN(subject) AS subject,
       MIN(subject_encrypted) AS subject_encrypted,
       COUNT(*)::int AS count,
       MAX(COALESCE(source_timestamp, updated_at, created_at)) AS latest_at
     FROM memories
     WHERE ${conditions.join(' AND ')}
     GROUP BY COALESCE(subject_hmac, subject, 'Unknown subject')`,
    values
  );

  const normalized = normalizeSearch(qs.q ?? '');
  const subjects = (await Promise.all(result.rows.map((row) => deserializeSubjectSummary(vault, row))))
    .filter((subject) => !normalized || normalizeSearch(subject.subject).includes(normalized))
    .sort((a, b) => compareSubjects(a, b, qs.sort));

  return {
    items: subjects.slice(qs.offset, qs.offset + qs.limit),
    total: subjects.length
  };
}

async function deserializeSubjectSummary(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  row: MemorySubjectRow
): Promise<MemorySubjectSummary> {
  const encryptedSubject = typeof row.subject_encrypted === 'string' ? row.subject_encrypted : null;
  const subject = encryptedSubject && isVaultEncryptionActive(vault)
    ? await decryptForVault(vault, encryptedSubject)
    : typeof row.subject === 'string' && row.subject.trim() ? row.subject : 'Unknown subject';

  return {
    count: Number(row.count ?? 0),
    latest_at: typeof row.latest_at === 'string' ? row.latest_at : row.latest_at instanceof Date ? row.latest_at.toISOString() : null,
    subject
  };
}

function memoryOrderBy(sort: z.infer<typeof listQuerySchema>['sort']): string {
  if (sort === 'oldest') return 'created_at ASC, updated_at ASC';
  if (sort === 'confidence') return 'confidence DESC NULLS LAST, updated_at DESC, created_at DESC';
  if (sort === 'salience') return 'salience DESC NULLS LAST, updated_at DESC, created_at DESC';
  return 'updated_at DESC, created_at DESC';
}

function compareSubjects(a: MemorySubjectSummary, b: MemorySubjectSummary, sort: z.infer<typeof subjectListQuerySchema>['sort']): number {
  if (sort === 'name') return a.subject.localeCompare(b.subject);
  if (sort === 'recent') return Date.parse(b.latest_at ?? '') - Date.parse(a.latest_at ?? '') || b.count - a.count || a.subject.localeCompare(b.subject);
  return b.count - a.count || Date.parse(b.latest_at ?? '') - Date.parse(a.latest_at ?? '') || a.subject.localeCompare(b.subject);
}

function compareMemories(
  a: Awaited<ReturnType<typeof decryptMemoryRow>>,
  b: Awaited<ReturnType<typeof decryptMemoryRow>>,
  sort: z.infer<typeof listQuerySchema>['sort']
): number {
  if (sort === 'oldest') return memoryTimestampMs(a) - memoryTimestampMs(b);
  if (sort === 'confidence') return (b.confidence ?? 0) - (a.confidence ?? 0) || memoryTimestampMs(b) - memoryTimestampMs(a);
  if (sort === 'salience') return Number(b.salience ?? 0) - Number(a.salience ?? 0) || memoryTimestampMs(b) - memoryTimestampMs(a);
  return memoryTimestampMs(b) - memoryTimestampMs(a);
}

function memoryTimestampMs(memory: { source_timestamp?: unknown; updated_at?: unknown; created_at?: unknown }): number {
  const value = memory.source_timestamp ?? memory.updated_at ?? memory.created_at;
  const parsed = typeof value === 'string' ? Date.parse(value) : 0;
  return Number.isFinite(parsed) ? parsed : 0;
}

function memoryMatchesQuery(memory: Awaited<ReturnType<typeof decryptMemoryRow>>, normalizedQuery: string): boolean {
  if (!normalizedQuery) return true;
  return [
    memory.subject,
    memory.data,
    memory.type,
    memory.scope,
    memory.polarity,
    ...(Array.isArray(memory.categories) ? memory.categories : [])
  ].some((value) => normalizeSearch(String(value ?? '')).includes(normalizedQuery));
}

function normalizeSearch(value: string): string {
  return value.trim().toLowerCase();
}

function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (match) => `\\${match}`);
}

async function subjectHmacForVault(
  vault: { encrypted_dek: string | null; vault_encryption_enabled: boolean },
  subject: string
): Promise<string> {
  if (!vault.encrypted_dek) throw new Error('Encrypted vault is missing encrypted_dek');
  return computeSubjectHmac(subject, await unwrapDek(vault.encrypted_dek));
}

async function decryptMemoryRow(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  row: MemoryResponseRow
) {
  const decryptedSubject = typeof row.subject_encrypted === 'string' && isVaultEncryptionActive(vault)
    ? await decryptForVault(vault, row.subject_encrypted)
    : row.subject;

  const { subject_encrypted, ...safeRow } = row as typeof row & { subject_encrypted?: unknown };

  return {
    ...safeRow,
    data: typeof row.data === 'string' ? await decryptForVault(vault, row.data) : row.data,
    subject: decryptedSubject
  };
}
