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
import { decideEscalation, defaultDecisionWithoutEscalator } from './escalation-routing';
import { ExtractorService, type ConflictResolution } from './extractor';
import { enforceMemoryCreationLimit } from './usage';
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
  sourceTimestamp?: string | null;
}

interface MemoryRow {
  id: string;
  data: string;
  confidence: number;
  score: number;
  salience: number;
  type: DedupInput['type'];
  polarity: DedupInput['polarity'];
  status: DedupInput['status'];
  volatility: DedupInput['volatility'];
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

export interface DedupOptions {
  precomputedConflictDecision?: ConflictResolution;
  precomputedConflictMemoryId?: string;
}

export interface DedupEscalationRequest {
  id: string;
  existingFact: string;
  newFact: string;
  existingMemoryId: string;
  reasons: string[];
}

interface DedupMatchResolution {
  hash: string;
  vault: VaultEncryptionContext;
  canonicalSubject: string;
  exactMatchId?: string;
  bestMatch?: MemoryRow & { similarity: number };
}

export async function deduplicateMemory(
  input: DedupInput,
  db: Queryable = { query },
  extractor?: ExtractorService,
  options: DedupOptions = {}
): Promise<DedupResult> {
  return withSpan('memory.deduplicate', {
    'vault.id': input.vaultId,
    'memory.subject': input.subject,
    'memory.source_chunks_count': input.sourceChunks.length
  }, async (span) => {
    const { hash, vault, canonicalSubject, exactMatchId, bestMatch } = await resolveDedupMatch(input, db);

    if (input.status === 'candidate') {
      await enforceMemoryCreationLimit(input.vaultId);
      const inserted = await insertMemory(db, vault, input, hash, canonicalSubject);
      await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
      span.setAttribute('dedup.result', 'inserted');
      return {
        action: 'inserted',
        memoryId: inserted.rows[0].id
      };
    }

    if (exactMatchId) {
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
             source_timestamp = CASE
               WHEN $13::timestamptz IS NULL THEN source_timestamp
               WHEN source_timestamp IS NULL OR source_timestamp < $13::timestamptz THEN $13::timestamptz
               ELSE source_timestamp
             END,
             updated_at = now()
         WHERE id = $1`,
        [
          exactMatchId,
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
          input.validUntil,
          input.sourceTimestamp ?? null
        ]
      );
      await syncEmbeddingRecord(db, exactMatchId, input.embedding);
      span.setAttribute('dedup.result', 'updated');
      return {
        action: 'updated',
        memoryId: exactMatchId
      };
    }
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
             source_timestamp = CASE
               WHEN $16::timestamptz IS NULL THEN source_timestamp
               WHEN source_timestamp IS NULL OR source_timestamp < $16::timestamptz THEN $16::timestamptz
               ELSE source_timestamp
             END,
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
          input.validUntil,
          input.sourceTimestamp ?? null
        ]
      );
      await syncEmbeddingRecord(db, bestMatch.id, input.embedding);
      span.setAttribute('dedup.result', 'updated');
      return { action: 'updated', memoryId: bestMatch.id };
    }

    if (bestMatch && bestMatch.similarity >= 0.80) {
      const bestMatchVault = getVaultContext(bestMatch, input.vaultId);
      const escalation = decideEscalation(input, {
        similarity: bestMatch.similarity,
        confidence: bestMatch.confidence,
        status: bestMatch.status,
        type: bestMatch.type,
        polarity: bestMatch.polarity,
        volatility: bestMatch.volatility,
        score: bestMatch.score,
        salience: bestMatch.salience
      });
      span.setAttribute('dedup.escalated', escalation.escalate);
      span.setAttribute('dedup.escalation_reasons', escalation.reasons.join(','));

      const existingFact = escalation.escalate
        ? await decryptForVault(bestMatchVault, bestMatch.data)
        : '';
      const canUsePrecomputedDecision = Boolean(
        escalation.escalate &&
        options.precomputedConflictDecision &&
        options.precomputedConflictMemoryId === bestMatch.id
      );
      const decision: ConflictResolution | 'keep_both' = canUsePrecomputedDecision
        ? options.precomputedConflictDecision!
        : escalation.escalate && extractor
          ? await extractor.arbitrateConflict(existingFact, input.fact, input.vaultId)
          : defaultDecisionWithoutEscalator(escalation.escalate);
      span.setAttribute('dedup.conflict_decision', decision);

      if (decision === 'keep_both') {
        await enforceMemoryCreationLimit(input.vaultId);
        const inserted = await insertMemory(db, bestMatchVault, input, hash, canonicalSubject);
        await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
        span.setAttribute('dedup.result', 'inserted');
        return { action: 'inserted', memoryId: inserted.rows[0].id };
      }

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
               source_timestamp = CASE
                 WHEN $16::timestamptz IS NULL THEN source_timestamp
                 WHEN source_timestamp IS NULL OR source_timestamp < $16::timestamptz THEN $16::timestamptz
                 ELSE source_timestamp
               END,
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
            input.validUntil,
            input.sourceTimestamp ?? null
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

      await enforceMemoryCreationLimit(input.vaultId);
      const inserted = await insertMemory(db, bestMatchVault, input, hash, canonicalSubject);
      await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
      span.setAttribute('dedup.result', 'inserted');
      return { action: 'inserted', memoryId: inserted.rows[0].id };
    }

    await enforceMemoryCreationLimit(input.vaultId);
    const inserted = await insertMemory(db, vault, input, hash, canonicalSubject);
    await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);

    span.setAttribute('dedup.result', 'inserted');
    return {
      action: 'inserted',
      memoryId: inserted.rows[0].id
    };
  });
}

export async function getDedupEscalationRequest(
  input: DedupInput,
  id: string,
  db: Queryable = { query }
): Promise<DedupEscalationRequest | null> {
  if (input.status === 'candidate') {
    return null;
  }

  const { bestMatch, exactMatchId } = await resolveDedupMatch(input, db);
  if (exactMatchId) {
    return null;
  }
  if (!bestMatch || bestMatch.similarity > 0.90 || bestMatch.similarity < 0.80) {
    return null;
  }

  const escalation = decideEscalation(input, {
    similarity: bestMatch.similarity,
    confidence: bestMatch.confidence,
    status: bestMatch.status,
    type: bestMatch.type,
    polarity: bestMatch.polarity,
    volatility: bestMatch.volatility,
    score: bestMatch.score,
    salience: bestMatch.salience
  });
  if (!escalation.escalate) {
    return null;
  }

  return {
    id,
    existingFact: await decryptForVault(getVaultContext(bestMatch, input.vaultId), bestMatch.data),
    newFact: input.fact,
    existingMemoryId: bestMatch.id,
    reasons: escalation.reasons
  };
}

async function resolveDedupMatch(
  input: DedupInput,
  db: Queryable
): Promise<DedupMatchResolution> {
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
  if (!vault) {
    throw new Error(`Vault ${input.vaultId} not found`);
  }

  const exactMatch = await db.query<{ id: string }>(
    `SELECT id
     FROM memories
     WHERE vault_id = $1 AND hash = $2 AND archived_at IS NULL
     LIMIT 1`,
    [input.vaultId, hash]
  );

  if (exactMatch.rowCount) {
    return {
      hash,
      vault,
      canonicalSubject,
      exactMatchId: exactMatch.rows[0].id
    };
  }

  const subjectMatchTarget = isVaultEncryptionActive(vault) && vault.encrypted_dek
    ? computeSubjectHmac(canonicalSubject, await unwrapVaultDek(vault))
    : canonicalSubject;
  const subjectMatchColumn = isVaultEncryptionActive(vault) ? 'm.subject_hmac' : 'm.subject';
  const subjectMatches = await db.query<(MemoryRow & { similarity: number })>(
    `SELECT m.id, m.data, m.confidence, m.score, m.salience, m.type, m.polarity, m.status, m.volatility,
            v.encrypted_dek, v.vault_encryption_enabled,
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

  return {
    hash,
    vault,
    canonicalSubject,
    bestMatch: subjectMatches.rows[0]
  };
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
       source_chunks, score, salience, sensitivity, type, scope, polarity, status, volatility, evidence, valid_from, valid_until, source_segment_id, source_timestamp
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::uuid[], $9, $10, $11, $12, $13, $14, $15, $16::memory_volatility, $17::jsonb, $18::date, $19::date, $20, $21::timestamptz)
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
      input.sourceSegmentId ?? null,
      input.sourceTimestamp ?? null
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
