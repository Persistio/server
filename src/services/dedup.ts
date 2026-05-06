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
  sourceTimestamp?: string | null;
  salience: number;
  sensitivity: 'low' | 'medium' | 'high';
  predicate: 'preference' | 'fact' | 'plan' | 'relationship' | 'constraint' | 'event' | null;
  polarity: 'positive' | 'negative' | 'neutral';
  status: 'active' | 'superseded' | 'contradicted' | 'needs_review';
  validFrom: string | null;
  validUntil: string | null;
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
    const vaultResult = await db.query<VaultEncryptionContext>(
      `SELECT id, encrypted_dek, vault_encryption_enabled
       FROM vaults
       WHERE id = $1
       LIMIT 1`,
      [input.vaultId]
    );
    const vault = vaultResult.rows[0];

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
             predicate = COALESCE($6, predicate),
             polarity = $7,
             status = 'active',
             valid_from = COALESCE($8::date, valid_from),
             valid_until = COALESCE($9::date, valid_until),
             source_timestamp = COALESCE($10::timestamptz, source_timestamp),
             updated_at = now(),
             confidence = confidence + 1
         WHERE id = $1`,
        [
          exactMatch.rows[0].id,
          input.sourceChunks,
          input.score,
          input.salience,
          input.sensitivity,
          input.predicate,
          input.polarity,
          input.validFrom,
          input.validUntil,
          input.sourceTimestamp
        ]
      );
      span.setAttribute('dedup.result', 'updated');
      return {
        action: 'updated',
        memoryId: exactMatch.rows[0].id
      };
    }

    const subjectMatchTarget = isVaultEncryptionActive(vault) && vault.encrypted_dek
      ? computeSubjectHmac(input.subject, await unwrapVaultDek(vault))
      : input.subject;
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
             predicate = COALESCE($9, predicate),
             polarity = $10, status = 'active', valid_from = COALESCE($11::date, valid_from),
             valid_until = COALESCE($12::date, valid_until), source_timestamp = COALESCE($13::timestamptz, source_timestamp),
             updated_at = now(), confidence = confidence + 1
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
          input.predicate,
          input.polarity,
          input.validFrom,
          input.validUntil,
          input.sourceTimestamp
        ]
      );
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
               predicate = COALESCE($9, predicate),
               polarity = $10, status = 'active', valid_from = COALESCE($11::date, valid_from),
               valid_until = COALESCE($12::date, valid_until), source_timestamp = COALESCE($13::timestamptz, source_timestamp),
               updated_at = now(), confidence = confidence + 1
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
            input.predicate,
            input.polarity,
            input.validFrom,
            input.validUntil,
            input.sourceTimestamp
          ]
        );
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
      const inserted = await insertMemory(db, bestMatchVault, input, hash);
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
    const inserted = await insertMemory(db, vault, input, hash);
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
  hash: string
) {
  const storedFact = await encryptForVault(vault, input.fact);
  const encryptedSubject = await encryptSubjectForVault(vault, input.subject);
  return db.query<{ id: string }>(
    `INSERT INTO memories (
       vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding,
       source_chunks, score, salience, sensitivity, predicate, polarity, status, valid_from, valid_until, source_timestamp
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::uuid[], $9, $10, $11, $12, $13, $14, $15::date, $16::date, $17::date)
     RETURNING id`,
    [
      input.vaultId,
      storedFact,
      isVaultEncryptionActive(vault) ? '' : input.subject,
      encryptedSubject?.encrypted ?? null,
      encryptedSubject?.hmac ?? null,
      hash,
      JSON.stringify(input.embedding),
      input.sourceChunks,
      input.score,
      input.salience,
      input.sensitivity,
      input.predicate,
      input.polarity,
      input.status,
      input.validFrom,
      input.validUntil,
      input.sourceTimestamp
    ]
  );
}
