import crypto from 'node:crypto';
import type { QueryResult, QueryResultRow } from 'pg';

import { query } from '../db/client';
import {
  computeSubjectHmac,
  decryptForVault,
  encryptForVault,
  encryptSubjectForVault,
  isVaultEncryptionActive,
  type VaultEncryptionContext
} from './crypto';
import { normaliseSubject, resolveCanonical } from './entity-resolver';
import { ExtractorService } from './extractor';
import { canCreateMemory, checkQuota, incrementUsage } from './usage';
import { withSpan } from '../telemetry';

export interface DedupInput {
  vaultId: string;
  fact: string;
  score: number;
  subject: string;
  embedding: number[];
  sourceChunks: string[];
  salience: number;
  sensitivity: 'low' | 'medium' | 'high';
  type: 'user_preference' | 'user_rule' | 'task_pattern' | 'workflow' | 'project' | 'constraint' | 'decision' | 'system_fact' | 'domain_knowledge' | null;
  scope: 'global' | 'project' | 'task' | 'session';
  polarity: 'positive' | 'negative' | 'neutral';
  status: 'active' | 'candidate' | 'superseded' | 'contradicted' | 'needs_review';
  volatility: 'very_low' | 'low' | 'medium' | 'high';
  evidence?: string | null;
  validFrom: string | null;
  validUntil: string | null;
  sourceSegmentId?: string | null;
}

interface MemoryRow {
  id: string;
  data: string;
  confidence: number;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
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
    'vault.id': input.vaultId,
    'memory.subject': input.subject,
    'memory.source_chunks_count': input.sourceChunks.length
  }, async (span) => {
    const hash = crypto.createHash('md5').update(input.fact).digest('hex');
    const normalisedSubject = normaliseSubject(input.subject);
    const canonicalSubject = await resolveCanonical(input.vaultId, normalisedSubject) ?? normalisedSubject;
    const vaultResult = await db.query<VaultEncryptionContext>(
      `SELECT id, encrypted_dek, vault_encryption_enabled
       FROM vaults
       WHERE id = $1
       LIMIT 1`,
      [input.vaultId]
    );
    const vault = vaultResult.rows[0];

    if (input.status === 'candidate') {
      if (!(await canCreateMemory(input.vaultId))) {
        span.setAttribute('dedup.result', 'skipped');
        span.setAttribute('dedup.reason', 'memories_max');
        return { action: 'skipped' };
      }

      await checkQuota(input.vaultId, 'memory_adds');
      const inserted = await insertMemory(db, vault, input, hash, canonicalSubject);
      await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
      await incrementUsage(input.vaultId, 'memory_adds');
      span.setAttribute('dedup.result', 'inserted');
      return {
        action: 'inserted',
        memoryId: inserted.rows[0].id
      };
    }

    const exactMatch = await db.query<{ id: string }>(
      `SELECT id
       FROM memories
       WHERE vault_id = $1 AND hash = $2 AND archived_at IS NULL
       LIMIT 1`,
      [input.vaultId, hash]
    );

    if (exactMatch.rowCount) {
      await db.query(
        `UPDATE memories
         SET source_chunks = (
               SELECT array_agg(DISTINCT u)
               FROM unnest(array_cat(source_chunks, $2::uuid[])) AS u
             ),
             score = GREATEST(score, $3),
             salience = GREATEST(salience, $4),
             sensitivity = CASE
               WHEN $5 = 'restricted' OR sensitivity = 'restricted' THEN 'restricted'
               WHEN $5 = 'high'       OR sensitivity = 'high'       THEN 'high'
               WHEN $5 = 'medium'     OR sensitivity = 'medium'     THEN 'medium'
               ELSE 'low'
             END,
             type = COALESCE($6, type),
             scope = COALESCE($7, scope),
             polarity = $8,
             status = 'active',
             volatility = COALESCE($9::memory_volatility, volatility),
             evidence = COALESCE($10::jsonb, evidence),
             valid_from = COALESCE($11::date, valid_from),
             valid_until = COALESCE($12::date, valid_until),
             updated_at = now()
         WHERE id = $1`,
        [
          exactMatch.rows[0].id,
          input.sourceChunks,
          input.score,
          input.salience,
          input.sensitivity,
          input.type,
          input.scope,
          input.polarity,
          input.volatility,
          input.evidence ? JSON.stringify({ summary: input.evidence }) : null,
          input.validFrom,
          input.validUntil
        ]
      );
      await syncEmbeddingRecord(db, exactMatch.rows[0].id, input.embedding);
      span.setAttribute('dedup.result', 'updated');
      return {
        action: 'updated',
        memoryId: exactMatch.rows[0].id
      };
    }

    const subjectMatchTarget = isVaultEncryptionActive(vault) && vault.encrypted_dek
      ? computeSubjectHmac(canonicalSubject, await unwrapVaultDek(vault))
      : canonicalSubject;
    const subjectMatchColumn = isVaultEncryptionActive(vault) ? 'm.subject_hmac' : 'm.subject';
    const subjectMatches = await db.query<(MemoryRow & { similarity: number })>(
      `SELECT m.id, m.data, m.confidence, v.encrypted_dek, v.vault_encryption_enabled,
              1 - (m.embedding <=> $3::vector) AS similarity
       FROM memories AS m
       JOIN vaults AS v
         ON v.id = m.vault_id
       WHERE m.vault_id = $1
         AND ${subjectMatchColumn} = $2
         AND m.archived_at IS NULL
         AND m.embedding IS NOT NULL
       ORDER BY similarity DESC`,
      [input.vaultId, subjectMatchTarget, JSON.stringify(input.embedding)]
    );

    const bestMatch = subjectMatches.rows[0];
    if (bestMatch) {
      span.setAttribute('dedup.best_similarity', bestMatch.similarity);
    }

    if (bestMatch && bestMatch.similarity > 0.90) {
      const storedFact = await encryptForVault(getVaultContext(bestMatch, input.vaultId), input.fact);
      await db.query(
        `UPDATE memories
         SET data = $2, hash = $3, embedding = $4::vector,
             source_chunks = $5::uuid[], score = GREATEST(score, $6),
             salience = GREATEST(salience, $7),
             sensitivity = CASE
               WHEN $8 = 'restricted' OR sensitivity = 'restricted' THEN 'restricted'
               WHEN $8 = 'high'       OR sensitivity = 'high'       THEN 'high'
               WHEN $8 = 'medium'     OR sensitivity = 'medium'     THEN 'medium'
               ELSE 'low'
             END,
             type = COALESCE($9, type),
             scope = COALESCE($10, scope),
             polarity = $11, status = 'active',
             volatility = COALESCE($12::memory_volatility, volatility),
             evidence = COALESCE($13::jsonb, evidence),
             valid_from = COALESCE($14::date, valid_from),
             valid_until = COALESCE($15::date, valid_until),
             updated_at = now()
         WHERE id = $1`,
        [
          bestMatch.id,
          storedFact,
          hash,
          JSON.stringify(input.embedding),
          input.sourceChunks,
          input.score,
          input.salience,
          input.sensitivity,
          input.type,
          input.scope,
          input.polarity,
          input.volatility,
          input.evidence ? JSON.stringify({ summary: input.evidence }) : null,
          input.validFrom,
          input.validUntil
        ]
      );
      await syncEmbeddingRecord(db, bestMatch.id, input.embedding);
      span.setAttribute('dedup.result', 'updated');
      return { action: 'updated', memoryId: bestMatch.id };
    }

    if (bestMatch && bestMatch.similarity >= 0.80) {
      const bestMatchVault = getVaultContext(bestMatch, input.vaultId);
      const existingFact = await decryptForVault(bestMatchVault, bestMatch.data);
      const decision = extractor
        ? await extractor.arbitrateConflict(existingFact, input.fact)
        : 'needs_review';
      span.setAttribute('dedup.conflict_decision', decision);

      if (decision === 'merge') {
        const storedFact = await encryptForVault(bestMatchVault, input.fact);
        await db.query(
          `UPDATE memories
           SET data = $2, hash = $3, embedding = $4::vector,
               source_chunks = $5::uuid[], score = GREATEST(score, $6),
               salience = GREATEST(salience, $7),
               sensitivity = CASE
                 WHEN $8 = 'restricted' OR sensitivity = 'restricted' THEN 'restricted'
                 WHEN $8 = 'high'       OR sensitivity = 'high'       THEN 'high'
                 WHEN $8 = 'medium'     OR sensitivity = 'medium'     THEN 'medium'
                 ELSE 'low'
               END,
               type = COALESCE($9, type),
               scope = COALESCE($10, scope),
               polarity = $11, status = 'active',
               volatility = COALESCE($12::memory_volatility, volatility),
               evidence = COALESCE($13::jsonb, evidence),
               valid_from = COALESCE($14::date, valid_from),
               valid_until = COALESCE($15::date, valid_until),
               updated_at = now()
           WHERE id = $1`,
          [
            bestMatch.id,
            storedFact,
            hash,
            JSON.stringify(input.embedding),
            input.sourceChunks,
            input.score,
            input.salience,
            input.sensitivity,
            input.type,
            input.scope,
            input.polarity,
            input.volatility,
            input.evidence ? JSON.stringify({ summary: input.evidence }) : null,
            input.validFrom,
            input.validUntil
          ]
        );
        await syncEmbeddingRecord(db, bestMatch.id, input.embedding);
        span.setAttribute('dedup.result', 'updated');
        return { action: 'updated', memoryId: bestMatch.id };
      }

      if (decision === 'discard_new') {
        span.setAttribute('dedup.result', 'skipped');
        return { action: 'skipped', memoryId: bestMatch.id };
      }

      if (!(await canCreateMemory(input.vaultId))) {
        span.setAttribute('dedup.result', 'skipped');
        span.setAttribute('dedup.reason', 'memories_max');
        return { action: 'skipped', memoryId: bestMatch.id };
      }

      if (decision === 'supersede_old') {
        await db.query(
          `UPDATE memories
           SET status = 'superseded',
               updated_at = now()
           WHERE id = $1`,
          [bestMatch.id]
        );
      } else if (decision === 'needs_review') {
        // This intentionally replaces the old keep_both path: callers now get
        // action: 'inserted' after marking the prior memory as needs_review,
        // rather than action: 'skipped'.
        await db.query(
          `UPDATE memories
           SET status = 'needs_review',
               updated_at = now()
           WHERE id = $1`,
          [bestMatch.id]
        );
      }

      await checkQuota(input.vaultId, 'memory_adds');
      const inserted = await insertMemory(db, bestMatchVault, input, hash, canonicalSubject);
      await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
      await incrementUsage(input.vaultId, 'memory_adds');
      span.setAttribute('dedup.result', 'inserted');
      return { action: 'inserted', memoryId: inserted.rows[0].id };
    }

    if (!(await canCreateMemory(input.vaultId))) {
      span.setAttribute('dedup.result', 'skipped');
      span.setAttribute('dedup.reason', 'memories_max');
      return { action: 'skipped' };
    }

    await checkQuota(input.vaultId, 'memory_adds');
    const inserted = await insertMemory(db, vault, input, hash, canonicalSubject);
    await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
    await incrementUsage(input.vaultId, 'memory_adds');

    span.setAttribute('dedup.result', 'inserted');
    return {
      action: 'inserted',
      memoryId: inserted.rows[0].id
    };
  });
}

function getVaultContext(row: MemoryRow, vaultId: string): VaultEncryptionContext {
  return {
    id: vaultId,
    encrypted_dek: row.encrypted_dek,
    vault_encryption_enabled: row.vault_encryption_enabled
  };
}

async function unwrapVaultDek(vault: VaultEncryptionContext): Promise<Buffer> {
  if (!vault.encrypted_dek) {
    throw new Error(`Vault ${vault.id} is missing encrypted_dek`);
  }

  const { unwrapDek } = await import('./crypto');
  return unwrapDek(vault.encrypted_dek);
}

async function insertMemory(
  db: Queryable,
  vault: VaultEncryptionContext,
  input: DedupInput,
  hash: string,
  canonicalSubject: string
) {
  const storedFact = await encryptForVault(vault, input.fact);
  const encryptedSubject = await encryptSubjectForVault(vault, canonicalSubject);
  return db.query<{ id: string }>(
     `INSERT INTO memories (
       vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding,
       source_chunks, score, salience, sensitivity, type, scope, polarity, status, volatility, evidence, valid_from, valid_until, source_segment_id
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::uuid[], $9, $10, $11, $12, $13, $14, $15, $16::memory_volatility, $17::jsonb, $18::date, $19::date, $20)
     RETURNING id`,
    [
      input.vaultId,
      storedFact,
      isVaultEncryptionActive(vault) ? '' : canonicalSubject,
      encryptedSubject?.encrypted ?? null,
      encryptedSubject?.hmac ?? null,
      hash,
      JSON.stringify(input.embedding),
      input.sourceChunks,
      input.score,
      input.salience,
      input.sensitivity,
      input.type,
      input.scope,
      input.polarity,
      input.status,
      input.volatility,
      input.evidence ? JSON.stringify({ summary: input.evidence }) : null,
      input.validFrom,
      input.validUntil,
      input.sourceSegmentId ?? null
    ]
  );
}

async function syncEmbeddingRecord(db: Queryable, memoryId: string, embedding: number[]) {
  await db.query(
    `INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
     VALUES ($1, $2::vector, now())
     ON CONFLICT (memory_id)
     DO UPDATE SET embedding = EXCLUDED.embedding, embedded_at = now()`,
    [memoryId, JSON.stringify(embedding)]
  );
}
