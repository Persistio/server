import crypto from 'node:crypto';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import { z } from 'zod';
import type { QueryResultRow } from 'pg';

import { query } from '../db/client';
import {
  memoryArchivedEventType,
  memoryCreatedEventType,
  type MemoryArchivedPayload,
  type MemoryCreatedPayload,
  type PlatformActor
} from '../events/platform-event';
import { getAuthAccountId, requireAdminScope, requireVaultReadAuth, requireVaultWriteAuth, type VaultContext } from '../middleware/auth';
import { setCustomerMetricVaultId } from '../services/customer-api-request-metrics';
import { computeSubjectHmac, decryptForVault, encryptForVault, encryptSubjectForVault, isVaultEncryptionActive, unwrapDek } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { pendingRecallCutoff } from '../services/pending-memory';
import { enforceMemoryCreationLimit, recordMemoryCountDelta } from '../services/usage';

const booleanQueryParam = z.preprocess((value) => {
  if (value === undefined) return false;
  if (Array.isArray(value)) return value[0];
  if (value === 'true' || value === true) return true;
  if (value === 'false' || value === false) return false;
  return value;
}, z.boolean());

const listQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  category: z.string().optional(),
  filter: z.enum(['all', 'high-confidence', 'low-confidence', 'positive', 'negative']).optional().default('all'),
  include_children: z.coerce.boolean().optional().default(false),
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0),
  q: z.string().trim().min(1).max(200).optional(),
  sort: z.enum(['recent', 'oldest', 'confidence', 'salience']).optional().default('recent'),
  subject: z.string().trim().min(1).max(500).optional()
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

const createMemorySchema = z.object({
  data: z.string().min(1),
  subject: z.string().min(1),
  categories: z.array(z.string().min(1)).optional().default([]),
  parent_id: z.string().uuid().nullable().optional(),
  type: z.enum(['user_preference', 'user_rule', 'task_pattern', 'workflow', 'project', 'constraint', 'decision', 'system_fact', 'domain_knowledge']).optional().default('system_fact'),
  scope: z.enum(['global', 'project', 'task', 'session']).optional().default('global'),
  evidence: z.string().optional(),
  volatility: z.enum(['very_low', 'low', 'medium', 'high']).optional().default('low')
});

const updateMemorySchema = z.object({
  data: z.string().min(1).optional(),
  subject: z.string().min(1).optional(),
  categories: z.array(z.string().min(1)).optional(),
  confidence: z.number().positive().optional(),
  type: z.enum(['user_preference', 'user_rule', 'task_pattern', 'workflow', 'project', 'constraint', 'decision', 'system_fact', 'domain_knowledge']).optional(),
  scope: z.enum(['global', 'project', 'task', 'session']).optional(),
  evidence: z.string().nullable().optional(),
  archived: z.boolean().optional()
});

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
       ${source}.categories, ${source}.confidence, ${source}.score, ${source}.salience, ${source}.sensitivity, ${source}.type, ${source}.scope, ${source}.evidence, ${source}.polarity, ${source}.status,
       ${source}.valid_from, ${source}.valid_until, ${source}.source_timestamp, ${source}.archived_at, ${source}.created_at, ${source}.updated_at, ${source}.parent_id, ${source}.volatility,
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

  app.get('/admin/vaults/:id/memories', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const qs = listQuerySchema.parse(request.query);
    const vault = await getAdminVaultContext(request, id);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });

    setCustomerMetricVaultId(request, vault.id);
    const result = await listMemories(vault, qs);
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
    const body = createMemorySchema.parse(request.body);
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
         vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding, categories, parent_id, type, scope, evidence, volatility
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::text[], $9, $10, $11, $12::jsonb, $13::memory_volatility)
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
    const body = updateMemorySchema.parse(request.body);

    const existing = await query<{
      id: string;
      data: string;
      subject: string;
      subject_encrypted: string | null;
      subject_hmac: string | null;
      categories: string[];
      confidence: number;
      type: string | null;
      scope: string;
      evidence: unknown;
      archived_at: string | null;
    }>(
      `SELECT id, data, subject, subject_encrypted, subject_hmac, categories, confidence, type, scope, evidence, archived_at
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
    const nextStoredData = body.data
      ? await encryptForVault(request.vault, body.data)
      : current.data;
    const nextSubject = body.subject ?? current.subject;
    const nextEncryptedSubject = body.subject
      ? await encryptSubjectForVault(request.vault, body.subject)
      : current.subject_encrypted
        ? { encrypted: current.subject_encrypted, hmac: current.subject_hmac ?? null }
        : null;
    const nextCategories = body.categories ?? current.categories;
    const nextConfidence = body.confidence ?? current.confidence;
    let embedding: string | undefined;
    let hash: string | undefined;

    if (body.data) {
      const embedder = getEmbedder();
      embedding = JSON.stringify(await embedder.embed(body.data, { vaultId: request.vault.id, modelRole: 'embedding', source: 'api', inputType: 'document' }));
      hash = crypto.createHash('md5').update(body.data).digest('hex');
    }

    const result = await query<Record<string, unknown> & { archived_at: string | null; previous_archived_at: string | null }>(
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
         SET data = $3,
             subject = $4,
             subject_encrypted = $5,
             subject_hmac = $6,
             categories = $7::text[],
             confidence = $8,
             type = COALESCE($9, type),
             scope = COALESCE($10, scope),
             updated_at = now(),
             hash = COALESCE($11, hash),
             embedding = COALESCE($12::vector, embedding),
             evidence = COALESCE($13::jsonb, evidence),
             archived_at = CASE
               WHEN $14::boolean IS FALSE THEN memories.archived_at
               WHEN $15::boolean THEN COALESCE(memories.archived_at, now())
               ELSE NULL
             END
         FROM target
         WHERE memories.id = target.id
         RETURNING ${memoryResponseSelect('memories')}, target.archived_at AS previous_archived_at
       )
       SELECT *
       FROM updated`,
      [
        request.vault.id,
        params.id,
        nextStoredData,
        isVaultEncryptionActive(request.vault) ? '' : nextSubject,
        nextEncryptedSubject?.encrypted ?? null,
        nextEncryptedSubject?.hmac ?? null,
        nextCategories,
        nextConfidence,
        body.type ?? null,
        body.scope ?? null,
        hash,
        embedding,
        body.evidence === undefined
          ? (typeof current.evidence === 'object' ? JSON.stringify(current.evidence) : null)
          : body.evidence
            ? JSON.stringify({ summary: body.evidence })
            : null,
        body.archived !== undefined,
        body.archived ?? false
      ]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
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

    const { previous_archived_at: _previousArchivedAt, ...responseRow } = result.rows[0];
    return decryptMemoryRow(request.vault, responseRow);
  });
}

async function recordMemoryCreatedActivity(
  request: FastifyRequest,
  memoryId: string
): Promise<void> {
  const workspaceId = request.vault.account_id;
  if (!workspaceId) return;
  const actor: PlatformActor = {
    id: request.auth?.actor?.id ?? request.auth?.client_id ?? null,
    type: request.auth?.actor?.type === 'user' ? 'user' : request.auth?.method === 'api_key' ? 'api_key' : 'system'
  };
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
  const actor: PlatformActor = {
    id: request.auth?.actor?.id ?? request.auth?.client_id ?? null,
    type: request.auth?.actor?.type === 'user' ? 'user' : request.auth?.method === 'api_key' ? 'api_key' : 'system'
  };
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
  qs: z.infer<typeof listQuerySchema>
): Promise<{ items: Awaited<ReturnType<typeof decryptMemoryRow>>[]; total: number }> {
  if (isVaultEncryptionActive(vault) && qs.q) {
    return listEncryptedSearchMemories(vault, qs);
  }

  const rows = await listMemoryRows(vault, qs);
  const items = await Promise.all(rows.items.map((row) => decryptMemoryRow(vault, row)));
  return { items, total: rows.total };
}

async function listMemoryRows(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof listQuerySchema>
): Promise<{ items: MemoryResponseRow[]; total: number }> {
  const values: unknown[] = [vault.id];
  const conditions = [`vault_id = $1`, `status <> 'candidate'`];

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
                 ? `m.archived_at IS NULL AND m.status <> 'candidate'`
                 : `m.archived_at IS NOT NULL AND m.status <> 'candidate'`}
               AND t.depth < 10
           )`;
    const finalArchivedClause = qs.archived === 'false'
      ? `archived_at IS NULL AND status <> 'candidate'`
      : `archived_at IS NOT NULL AND status <> 'candidate'`;
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
  qs: z.infer<typeof listQuerySchema>
): Promise<{ items: Awaited<ReturnType<typeof decryptMemoryRow>>[]; total: number }> {
  const pageSize = 200;
  const rows: MemoryResponseRow[] = [];
  let offset = 0;
  let total = 0;

  do {
    const page = await listMemoryRows(vault, { ...qs, limit: pageSize, offset, q: undefined });
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
