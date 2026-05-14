import crypto from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import type { QueryResultRow } from 'pg';

import { query } from '../db/client';
import { requireVaultAuth } from '../middleware/auth';
import { decryptForVault, encryptForVault, encryptSubjectForVault, isVaultEncryptionActive } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { enforceMemoryCreationLimit } from '../services/usage';

const listQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  category: z.string().optional(),
  include_children: z.coerce.boolean().optional().default(false),
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0)
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
             SELECT id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
                    categories, confidence, score, salience, sensitivity, type, scope, evidence, polarity, status,
                    valid_from, valid_until, archived_at, created_at, updated_at, parent_id, volatility,
                    COALESCE((SELECT COUNT(*) FROM memory_edges e WHERE e.from_memory_id = tree.id OR e.to_memory_id = tree.id), 0) AS edge_count
             FROM tree
             WHERE ${finalArchivedClause}
             ORDER BY created_at DESC
             LIMIT 1000`;
    } else {
      values.push(qs.limit, qs.offset);
      sql = `SELECT id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
                    categories, confidence, score, salience, sensitivity, type, scope, evidence, polarity, status,
                    valid_from, valid_until, archived_at, created_at, updated_at, parent_id, volatility,
                    COALESCE((SELECT COUNT(*) FROM memory_edges e WHERE e.from_memory_id = memories.id OR e.to_memory_id = memories.id), 0) AS edge_count
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
       RETURNING id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
                categories, confidence, score, salience, sensitivity, type, scope, evidence, polarity, status,
                valid_from, valid_until, archived_at, created_at, updated_at, parent_id, volatility,
                COALESCE((SELECT COUNT(*) FROM memory_edges e WHERE e.from_memory_id = memories.id OR e.to_memory_id = memories.id), 0) AS edge_count`,
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

  app.get('/v1/memories/:id', { preHandler: requireVaultAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<Record<string, unknown>>(
      `SELECT id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
              categories, confidence, score, salience, sensitivity, type, scope, evidence, polarity, status,
              valid_from, valid_until, archived_at, created_at, updated_at, parent_id, volatility,
              COALESCE((SELECT COUNT(*) FROM memory_edges e WHERE e.from_memory_id = memories.id OR e.to_memory_id = memories.id), 0) AS edge_count
       FROM memories
       WHERE vault_id = $1
         AND id = $2
         AND (status IS NULL OR status <> 'candidate')
       LIMIT 1`,
      [request.vault.id, params.id]
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
       RETURNING id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
                 categories, confidence, score, salience, sensitivity, type, scope, evidence, polarity, status,
                 valid_from, valid_until, archived_at, created_at, updated_at, parent_id, volatility,
                 COALESCE((SELECT COUNT(*) FROM memory_edges e WHERE e.from_memory_id = memories.id OR e.to_memory_id = memories.id), 0) AS edge_count`,
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
