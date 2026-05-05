import crypto from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import type { QueryResultRow } from 'pg';

import { query } from '../db/client';
import { requireVaultAuth } from '../middleware/auth';
import { decryptForVault, encryptForVault, encryptSubjectForVault, isVaultEncryptionActive } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { canCreateMemory, incrementUsage } from '../services/usage';

const listQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  category: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0)
});

const createMemorySchema = z.object({
  data: z.string().min(1),
  subject: z.string().min(1),
  categories: z.array(z.string().min(1)).optional().default([])
});

const updateMemorySchema = z.object({
  data: z.string().min(1).optional(),
  subject: z.string().min(1).optional(),
  categories: z.array(z.string().min(1)).optional(),
  confidence: z.number().positive().optional()
});

export async function registerMemoryRoutes(app: FastifyInstance) {
  app.get('/v1/memories', { preHandler: requireVaultAuth }, async (request) => {
    const qs = listQuerySchema.parse(request.query);
    const values: unknown[] = [request.vault.id];
    const conditions = ['vault_id = $1'];

    if (qs.archived === 'false') {
      conditions.push('archived_at IS NULL');
    } else {
      conditions.push('archived_at IS NOT NULL');
    }

    if (qs.category) {
      values.push(qs.category);
      conditions.push(`$${values.length} = ANY(categories)`);
    }

    values.push(qs.limit, qs.offset);

    const result = await query<Record<string, unknown>>(
      `SELECT id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
              categories, confidence, score, archived_at, created_at, updated_at
       FROM memories
       WHERE ${conditions.join(' AND ')}
       ORDER BY updated_at DESC, created_at DESC
       LIMIT $${values.length - 1}
       OFFSET $${values.length}`,
      values
    );

    const items = await Promise.all(result.rows.map((row) => decryptMemoryRow(request.vault, row)));
    return {
      items,
      limit: qs.limit,
      offset: qs.offset
    };
  });

  app.post('/v1/memories', { preHandler: requireVaultAuth }, async (request, reply) => {
    const body = createMemorySchema.parse(request.body);
    const canWrite = await canCreateMemory(request.vault.id);
    if (!canWrite) {
      return reply.code(429).send({ error: 'memories_max quota exceeded' });
    }

    const embedder = getEmbedder();
    const embedding = await embedder.embed(body.data);
    const hash = crypto.createHash('md5').update(body.data).digest('hex');
    const storedData = await encryptForVault(request.vault, body.data);
    const encryptedSubject = await encryptSubjectForVault(request.vault, body.subject);
    const storedSubject = isVaultEncryptionActive(request.vault) ? '' : body.subject;

    const result = await query<Record<string, unknown>>(
      `INSERT INTO memories (
         vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding, categories
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::text[])
       RETURNING id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
                 categories, confidence, score, archived_at, created_at, updated_at`,
      [
        request.vault.id,
        storedData,
        storedSubject,
        encryptedSubject?.encrypted ?? null,
        encryptedSubject?.hmac ?? null,
        hash,
        JSON.stringify(embedding),
        body.categories
      ]
    );

    await incrementUsage(request.vault.id, 'memory_adds');

    return reply.code(201).send(await decryptMemoryRow(request.vault, result.rows[0]));
  });

  app.get('/v1/memories/:id', { preHandler: requireVaultAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<Record<string, unknown>>(
      `SELECT id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
              categories, confidence, score, archived_at, created_at, updated_at
       FROM memories
       WHERE vault_id = $1 AND id = $2
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
       WHERE vault_id = $1 AND id = $2
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
    }>(
      `SELECT id, data, subject, subject_encrypted, subject_hmac, categories, confidence
       FROM memories
       WHERE vault_id = $1 AND id = $2
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
           updated_at = now(),
           hash = COALESCE($9, hash),
           embedding = COALESCE($10::vector, embedding)
       WHERE vault_id = $1 AND id = $2
       RETURNING id, vault_id, data, subject, subject_encrypted, hash, source_chunks,
                 categories, confidence, score, archived_at, created_at, updated_at`,
      [
        request.vault.id,
        params.id,
        nextStoredData,
        isVaultEncryptionActive(request.vault) ? '' : nextSubject,
        nextEncryptedSubject?.encrypted ?? null,
        nextEncryptedSubject?.hmac ?? null,
        nextCategories,
        nextConfidence,
        hash,
        embedding
      ]
    );

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
