import crypto from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import type { QueryResultRow } from 'pg';

import { query } from '../db/client';
import { requireVaultAuth } from '../middleware/auth';
import { decryptForVault, encryptForVault, encryptSubjectForVault, isVaultEncryptionActive } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { pendingRecallCutoff } from '../services/pending-memory';
import { enforceMemoryCreationLimit } from '../services/usage';

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
  include_children: z.coerce.boolean().optional().default(false),
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0)
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
  depth: z.coerce.number().int().min(0).max(3).optional().default(1),
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

export async function registerMemoryRoutes(app: FastifyInstance) {
  app.get('/v1/memories', { preHandler: requireVaultAuth }, async (request) => {
    const qs = listQuerySchema.parse(request.query);
    const values: unknown[] = [request.vault.id];
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

    let sql: string;
    if (qs.include_children) {
      const finalArchivedClause = qs.archived === 'false'
        ? `archived_at IS NULL AND status <> 'candidate'`
        : `archived_at IS NOT NULL AND status <> 'candidate'`;
      const recursiveArchivedClause = qs.archived === 'false'
        ? `m.archived_at IS NULL AND m.status <> 'candidate'`
        : `m.archived_at IS NOT NULL AND m.status <> 'candidate'`;
      sql = `WITH RECURSIVE tree AS (
               SELECT *, 0 AS depth FROM memories WHERE ${conditions.join(' AND ')}
               UNION ALL
               SELECT m.*, t.depth + 1 FROM memories m
               JOIN tree t ON m.parent_id = t.id
               WHERE m.vault_id = $1
                 AND ${recursiveArchivedClause}
                 AND t.depth < 10
             )
             SELECT ${memoryResponseSelect('tree')}
             FROM tree
             WHERE ${finalArchivedClause}
             ORDER BY created_at DESC
             LIMIT 1000`;
    } else {
      values.push(qs.limit, qs.offset);
      sql = `SELECT ${memoryResponseSelect('memories')}
             FROM memories
             WHERE ${conditions.join(' AND ')}
             ORDER BY updated_at DESC, created_at DESC
             LIMIT $${values.length - 1}
             OFFSET $${values.length}`;
    }

    const result = await query<Record<string, unknown>>(sql, values);
    const offset = parseInt(String(qs.offset)) || 0;
    const limit = parseInt(String(qs.limit)) || 50;
    const rows = qs.include_children
      ? result.rows.slice(offset, offset + limit)
      : result.rows;
    const items = await Promise.all(rows.map((row) => decryptMemoryRow(request.vault, row)));
    return {
      items,
      limit: qs.limit,
      offset: qs.offset
    };
  });

  app.post('/v1/memories', { preHandler: requireVaultAuth }, async (request, reply) => {
    const body = createMemorySchema.parse(request.body);
    await enforceMemoryCreationLimit(request.vault.id);

    const embedder = getEmbedder();
    const embedding = await embedder.embed(body.data);
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

    await query(
      `INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
       VALUES ($1, $2::vector, now())
       ON CONFLICT (memory_id)
       DO UPDATE SET embedding = EXCLUDED.embedding, embedded_at = now()`,
      [result.rows[0].id, JSON.stringify(embedding)]
    );

    return reply.code(201).send(await decryptMemoryRow(request.vault, result.rows[0]));
  });

  app.get('/v1/memories/graph', { preHandler: requireVaultAuth }, async (request, reply) => {
    const parsedQuery = graphQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) {
      return reply.code(400).send({ error: 'Invalid graph query' });
    }

    const qs = parsedQuery.data;
    const edgeTypes = qs.edge_types ?? null;
    const nodes = qs.seed_memory_id
      ? await fetchSeededGraphNodes(request.vault.id, qs.seed_memory_id, qs.depth, qs.limit, qs.direction, edgeTypes)
      : await fetchGraphOverviewNodes(request.vault.id, qs.limit);

    if (qs.seed_memory_id && nodes.length === 0) {
      return reply.code(404).send({ error: 'Seed memory not found' });
    }

    const nodeIds = nodes.map((node) => String(node.id));
    const edges = nodeIds.length
      ? await fetchGraphEdges(request.vault.id, nodeIds, edgeTypes)
      : [];
    const decryptedNodes = await Promise.all(nodes.map((row) => decryptMemoryRow(request.vault, row)));

    return {
      seed_memory_id: qs.seed_memory_id ?? null,
      depth: qs.depth,
      limit: qs.limit,
      direction: qs.direction,
      edge_types: qs.edge_types ?? null,
      nodes: decryptedNodes,
      edges
    };
  });

  app.get('/v1/memories/:id', { preHandler: requireVaultAuth }, async (request, reply) => {
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

  app.delete('/v1/memories/:id', { preHandler: requireVaultAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query(
      `UPDATE memories
       SET archived_at = now(),
           updated_at = now()
       WHERE vault_id = $1
         AND id = $2
         AND (status IS NULL OR status <> 'candidate')
       RETURNING id, archived_at`,
      [request.vault.id, params.id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Memory not found' });
    }

    return result.rows[0];
  });

  app.patch('/v1/memories/:id', { preHandler: requireVaultAuth }, async (request, reply) => {
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
    const nextArchivedAt = body.archived === undefined
      ? current.archived_at
      : body.archived
        ? new Date().toISOString()
        : null;

    let embedding: string | undefined;
    let hash: string | undefined;

    if (body.data) {
      const embedder = getEmbedder();
      embedding = JSON.stringify(await embedder.embed(body.data));
      hash = crypto.createHash('md5').update(body.data).digest('hex');
    }

    const result = await query<Record<string, unknown>>(
      `UPDATE memories
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
           archived_at = $14
       WHERE vault_id = $1
         AND id = $2
         AND (status IS NULL OR status <> 'candidate')
       RETURNING ${memoryResponseSelect('memories')}`,
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
        nextArchivedAt
      ]
    );

    if (embedding) {
      await query(
        `INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
         VALUES ($1, $2::vector, now())
         ON CONFLICT (memory_id)
         DO UPDATE SET embedding = EXCLUDED.embedding, embedded_at = now()`,
        [params.id, embedding]
      );
    }

    return decryptMemoryRow(request.vault, result.rows[0]);
  });
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
  data?: unknown;
  subject?: unknown;
  subject_encrypted?: unknown;
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
