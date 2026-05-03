import crypto from 'node:crypto';
import type { QueryResult, QueryResultRow } from 'pg';

import { query } from '../db/client';
import { ExtractorService } from './extractor';
import { withSpan } from '../telemetry';

export interface DedupInput {
  tenantId: string;
  fact: string;
  subject: string;
  embedding: number[];
  sourceChunks: string[];
}

interface MemoryRow {
  id: string;
  data: string;
  confidence: number;
}

export type DedupResult =
  | { action: 'skipped'; memoryId?: string }
  | { action: 'updated'; memoryId: string }
  | { action: 'inserted'; memoryId: string }
  | { action: 'conflict'; memoryId: string };

interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<T>>;
}

export async function deduplicateMemory(
  input: DedupInput,
  db: Queryable = { query },
  extractor?: ExtractorService
): Promise<DedupResult> {
  return withSpan('memory.deduplicate', {
    'tenant.id': input.tenantId,
    'memory.subject': input.subject,
    'memory.source_chunks_count': input.sourceChunks.length
  }, async (span) => {
    const hash = crypto.createHash('md5').update(input.fact).digest('hex');

    const exactMatch = await db.query<{ id: string }>(
      `SELECT id
       FROM memories
       WHERE tenant_id = $1 AND hash = $2 AND archived_at IS NULL
       LIMIT 1`,
      [input.tenantId, hash]
    );

    if (exactMatch.rowCount) {
      span.setAttribute('dedup.result', 'skipped');
      return {
        action: 'skipped',
        memoryId: exactMatch.rows[0].id
      };
    }

    const subjectMatches = await db.query<(MemoryRow & { similarity: number })>(
      `SELECT id, data, confidence, 1 - (embedding <=> $3::vector) AS similarity
       FROM memories
       WHERE tenant_id = $1
         AND subject = $2
         AND archived_at IS NULL
         AND embedding IS NOT NULL
       ORDER BY similarity DESC`,
      [input.tenantId, input.subject, JSON.stringify(input.embedding)]
    );

    const bestMatch = subjectMatches.rows[0];
    if (bestMatch) {
      span.setAttribute('dedup.best_similarity', bestMatch.similarity);
    }

    if (bestMatch && bestMatch.similarity > 0.90) {
      await db.query(
        `UPDATE memories
         SET data = $2, hash = $3, embedding = $4::vector,
             source_chunks = $5::uuid[], updated_at = now(), confidence = confidence + 1
         WHERE id = $1`,
        [bestMatch.id, input.fact, hash, JSON.stringify(input.embedding), input.sourceChunks]
      );
      span.setAttribute('dedup.result', 'updated');
      return { action: 'updated', memoryId: bestMatch.id };
    }

    if (bestMatch && bestMatch.similarity >= 0.80) {
      const decision = extractor
        ? await extractor.arbitrateConflict(bestMatch.data, input.fact)
        : 'keep_both';
      span.setAttribute('dedup.conflict_decision', decision);

      if (decision === 'update') {
        await db.query(
          `UPDATE memories
           SET data = $2, hash = $3, embedding = $4::vector,
               source_chunks = $5::uuid[], updated_at = now(), confidence = confidence + 1
           WHERE id = $1`,
          [bestMatch.id, input.fact, hash, JSON.stringify(input.embedding), input.sourceChunks]
        );
        span.setAttribute('dedup.result', 'updated');
        return { action: 'updated', memoryId: bestMatch.id };
      }

      if (decision === 'discard_new') {
        span.setAttribute('dedup.result', 'skipped');
        return { action: 'skipped', memoryId: bestMatch.id };
      }

      const inserted = await db.query<{ id: string }>(
        `INSERT INTO memories (tenant_id, data, subject, hash, embedding, source_chunks)
         VALUES ($1, $2, $3, $4, $5::vector, $6::uuid[])
         RETURNING id`,
        [input.tenantId, input.fact, input.subject, hash, JSON.stringify(input.embedding), input.sourceChunks]
      );
      span.setAttribute('dedup.result', 'inserted');
      return { action: 'inserted', memoryId: inserted.rows[0].id };
    }

    const inserted = await db.query<{ id: string }>(
      `INSERT INTO memories (
         tenant_id, data, subject, hash, embedding, source_chunks
       )
       VALUES ($1, $2, $3, $4, $5::vector, $6::uuid[])
       RETURNING id`,
      [
        input.tenantId,
        input.fact,
        input.subject,
        hash,
        JSON.stringify(input.embedding),
        input.sourceChunks
      ]
    );

    span.setAttribute('dedup.result', 'inserted');
    return {
      action: 'inserted',
      memoryId: inserted.rows[0].id
    };
  });
}
