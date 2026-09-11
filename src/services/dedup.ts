import crypto from 'node:crypto';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';

import { query, withTransaction } from '../db/client';
import {
  prepareVaultCrypto,
  type PreparedVaultCrypto,
  isVaultEncryptionActive,
  type VaultEncryptionContext
} from './crypto';
import { normaliseSubject, resolveCanonical } from './entity-resolver';
import { decideEscalation, defaultDecisionWithoutEscalator } from './escalation-routing';
import { ExtractorService, type ConflictResolution } from './extractor';
import { reserveMemoryCreationInTransaction, type ApiQuotaReservation } from './usage';
import { publishCommittedWorkerEffects, type WorkerEffect } from './worker-effects';
import { withSpan } from '../telemetry';
import { memoryPolicyEventCounter } from './observability-effects';
import type { MemoryScope } from './memory-scope';
import { mergeMemoryEvidence } from './memory-evidence';
import { isSecretLikeMemoryContent } from './deterministic-filter';
import {
  intersectValidityBoundSql,
  memoryValidityPredicateSql,
  toDateOnly,
  validityWindowsOverlapPredicateSql
} from './memory-validity';

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
  scope: MemoryScope;
  scopeKey: string | null;
  polarity: 'positive' | 'negative' | 'neutral';
  status: 'active' | 'candidate' | 'superseded' | 'contradicted' | 'needs_review';
  volatility: 'very_low' | 'low' | 'medium' | 'high';
  evidence?: string | null;
  validFrom: string | null;
  validUntil: string | null;
  sourceSegmentId?: string | null;
  sourceTimestamp?: string | null;
  policyRejections?: Array<{
    code: string;
    field: string;
    reason: string;
  }>;
}

interface MemoryRow {
  account_id: string | null;
  id: string;
  data: string;
  confidence: number;
  score: number;
  salience: number;
  type: DedupInput['type'];
  scope: MemoryScope;
  scope_key: string | null;
  polarity: DedupInput['polarity'];
  status: DedupInput['status'];
  volatility: DedupInput['volatility'];
  evidence: unknown;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
  row_version: string;
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
  precomputedConflictMemoryRevision?: string;
  /** Internal one-shot retry after a scope binding changes between match and lock. */
  staleRetryAttempted?: boolean;
  /** Binds a prepared decision to the complete candidate, not just its target. */
  precomputedConflictInput?: string;
}

export interface DedupEscalationRequest {
  id: string;
  inputFingerprint: string;
  existingFact: string;
  newFact: string;
  existingMemoryId: string;
  existingMemoryRevision: string;
  reasons: string[];
}

interface DedupMatchResolution {
  dedupDate: string;
  hash: string;
  vault: DedupVaultContext;
  canonicalSubject: string;
  exactMatch?: Pick<MemoryRow, 'id' | 'scope' | 'scope_key' | 'status' | 'evidence' | 'row_version'>;
  bestMatch?: MemoryRow & { similarity: number };
}

interface DedupVaultContext extends VaultEncryptionContext {
  account_id: string | null;
}

export async function deduplicateMemory(
  input: DedupInput,
  extractor?: ExtractorService,
  options: DedupOptions = {}
): Promise<DedupResult> {
  if (isSecretLikeMemoryContent(`${input.subject}\n${input.fact}`)) {
    throw new Error('Memory content rejected by secret policy');
  }
  const preparedCrypto = await prepareDedupCrypto(input.vaultId, { query });
  let transactionOptions = options;
  if (extractor && !options.precomputedConflictDecision) {
    const request = await getDedupEscalationRequest(input, 'transaction-preflight', { query }, preparedCrypto);
    if (request) {
      transactionOptions = {
        ...options,
        precomputedConflictDecision: await extractor.arbitrateConflict(
          request.existingFact,
          request.newFact,
          input.vaultId
        ),
        precomputedConflictMemoryId: request.existingMemoryId,
        precomputedConflictMemoryRevision: request.existingMemoryRevision,
        precomputedConflictInput: request.inputFingerprint
      };
    }
  }
  // Never hold a database transaction open across a model call. If the
  // preflight match changes before the transactional recheck, the inner call
  // has no live escalator and takes the safe needs_review path.
  const effects: WorkerEffect[] = [];
  const result = await withTransaction((client) => deduplicateMemoryInTransaction(
    input, client, preparedCrypto, effects, transactionOptions
  ));
  publishCommittedWorkerEffects(effects);
  return result;
}

export function fingerprintDedupInput(input: DedupInput): string {
  return crypto.createHash('sha256').update(JSON.stringify(input)).digest('hex');
}

/** SQL/local-crypto only. The outer commit owner alone may publish effects. */
export async function deduplicateMemoryInTransaction(
  input: DedupInput,
  db: PoolClient,
  preparedCrypto: PreparedVaultCrypto,
  effects: WorkerEffect[],
  options: DedupOptions = {}
): Promise<DedupResult> {
  if (isSecretLikeMemoryContent(`${input.subject}\n${input.fact}`)) {
    throw new Error('Memory content rejected by secret policy');
  }
  await preparedCrypto.assertCurrent(db);
  if (input.policyRejections?.length) {
    input = { ...input, status: 'needs_review' };
  }
  if (input.scope === 'global' && input.scopeKey !== null) {
    throw new Error('Global memories must not have a scope key');
  }
  if (input.scope !== 'global' && input.scopeKey === null && input.status !== 'needs_review') {
    throw new Error('Non-global memories require a scope key');
  }
  return withSpan('memory.deduplicate', {
    'vault.id': input.vaultId,
    'memory.subject': input.subject,
    'memory.source_chunks_count': input.sourceChunks.length
  }, async (span) => {
    const { dedupDate, hash, vault, canonicalSubject, exactMatch, bestMatch } = await resolveDedupMatch(input, db, preparedCrypto);

    if (input.status === 'candidate' || input.status === 'needs_review') {
      const reservation = await reserveMemoryCreationInTransaction(db, input.vaultId);
      const inserted = await insertMemory(db, vault, input, hash, canonicalSubject, preparedCrypto);
      await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
      recordSuccessfulInsert(effects, reservation, input.vaultId, vault.account_id);
      span.setAttribute('dedup.result', 'inserted');
      return {
        action: 'inserted',
        memoryId: inserted.rows[0].id
      };
    }

    if (exactMatch) {
      const mergedScopeSql = 'target.previous_scope';
      const mergedValidFromSql = intersectValidityBoundSql('target.previous_valid_from', '$11', 'lower');
      const mergedValidUntilSql = intersectValidityBoundSql('target.previous_valid_until', '$12', 'upper');
      const updateResult = await db.query(
        `WITH target AS (
           SELECT id,
                  scope AS previous_scope,
                  valid_from AS previous_valid_from,
                  valid_until AS previous_valid_until,
                  authority_state AS previous_authority_state,
                  authority_version AS previous_authority_version
           FROM memories
           WHERE id = $1
             AND vault_id = $14
             AND scope = $7
             AND scope_key IS NOT DISTINCT FROM $16::text
             AND status = 'active'
             AND archived_at IS NULL
             AND sensitivity <> 'restricted'
             AND confidence > 0 AND confidence <= 1
             AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
             AND ${memoryValidityPredicateSql('memories', '$17')}
             AND ${validityWindowsOverlapPredicateSql('memories', '$11', '$12')}
             AND CASE WHEN evidence ? 'policy_rejections' THEN
               CASE WHEN jsonb_typeof(evidence -> 'policy_rejections') = 'array'
                 THEN jsonb_array_length(evidence -> 'policy_rejections') = 0
                 ELSE false END
               ELSE true END
             AND xmin::text = $18
           FOR UPDATE
         ), updated AS (
           UPDATE memories
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
             scope = ${mergedScopeSql},
             polarity = $8,
             authority_state = CASE
               WHEN (
                 authority_required
                 OR $6 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               ) AND (
                 ($6 IS NOT NULL AND $6 IS DISTINCT FROM type)
                 OR (${mergedScopeSql}) IS DISTINCT FROM target.previous_scope
               )
               THEN 'proposed' ELSE authority_state END,
             approved_by = CASE WHEN (
               authority_required OR $6 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
             ) AND (($6 IS NOT NULL AND $6 IS DISTINCT FROM type) OR (${mergedScopeSql}) IS DISTINCT FROM target.previous_scope) THEN NULL ELSE approved_by END,
             approved_at = CASE WHEN (
               authority_required OR $6 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
             ) AND (($6 IS NOT NULL AND $6 IS DISTINCT FROM type) OR (${mergedScopeSql}) IS DISTINCT FROM target.previous_scope) THEN NULL ELSE approved_at END,
             approval_source = CASE WHEN (
               authority_required OR $6 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
             ) AND (($6 IS NOT NULL AND $6 IS DISTINCT FROM type) OR (${mergedScopeSql}) IS DISTINCT FROM target.previous_scope) THEN NULL ELSE approval_source END,
             revoked_by = CASE WHEN (
               authority_required OR $6 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
             ) AND (($6 IS NOT NULL AND $6 IS DISTINCT FROM type) OR (${mergedScopeSql}) IS DISTINCT FROM target.previous_scope) THEN NULL ELSE revoked_by END,
             revoked_at = CASE WHEN (
               authority_required OR $6 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
             ) AND (($6 IS NOT NULL AND $6 IS DISTINCT FROM type) OR (${mergedScopeSql}) IS DISTINCT FROM target.previous_scope) THEN NULL ELSE revoked_at END,
             authority_version = CASE
               WHEN (
                 authority_required
                 OR $6 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               ) AND (
                 ($6 IS NOT NULL AND $6 IS DISTINCT FROM type)
                 OR (${mergedScopeSql}) IS DISTINCT FROM target.previous_scope
               )
               THEN authority_version + 1 ELSE authority_version END,
             volatility = COALESCE($9::memory_volatility, volatility),
             evidence = COALESCE($10::jsonb, evidence),
             valid_from = ${mergedValidFromSql},
             valid_until = ${mergedValidUntilSql},
             source_timestamp = CASE
               WHEN $13::timestamptz IS NULL THEN source_timestamp
               WHEN source_timestamp IS NULL OR source_timestamp < $13::timestamptz THEN $13::timestamptz
               ELSE source_timestamp
             END,
             updated_at = now()
           FROM target
           WHERE memories.id = target.id
           RETURNING memories.id, memories.scope, memories.authority_state, memories.authority_version,
                     target.previous_scope,
                     target.previous_authority_state, target.previous_authority_version
         ), scope_audit AS (
           INSERT INTO memory_scope_change_log (
             vault_id, memory_id, old_scope, new_scope, actor_type, actor_id, source, reason
           )
           SELECT $14, updated.id, updated.previous_scope, updated.scope,
                  'worker', NULL, 'extraction_worker',
                  'Exact-match extraction retained the least-privileged scope under row lock.'
           FROM updated
           WHERE updated.scope <> updated.previous_scope
           RETURNING id
         ), authority_audit AS (
           INSERT INTO memory_authority_events (
             vault_id, memory_id, event_type, old_state, new_state, old_version, new_version,
             actor_type, source, reason
           )
           SELECT $14, updated.id, 'invalidate', updated.previous_authority_state, updated.authority_state,
                  updated.previous_authority_version, updated.authority_version,
                  'worker', 'extraction_worker', $15
           FROM updated
           WHERE updated.authority_version <> updated.previous_authority_version
           RETURNING id
         )
         SELECT id FROM updated`,
        [
          exactMatch.id,
          input.sourceChunks,
          input.score,
          input.salience,
          input.sensitivity,
          input.type,
          input.scope,
          input.polarity,
          input.volatility,
          serializeEvidence(input, exactMatch.evidence),
          input.validFrom,
          input.validUntil,
          input.sourceTimestamp ?? null,
          input.vaultId,
          'Exact-match extraction changed prompt-bearing metadata; approval requires review.',
          input.scopeKey,
          dedupDate,
          exactMatch.row_version
        ]
      );
      if (updateResult.rowCount === 0) {
        if (options.staleRetryAttempted) return { action: 'skipped' };
        return deduplicateMemoryInTransaction(input, db, preparedCrypto, effects, { ...options, staleRetryAttempted: true });
      }
      await syncEmbeddingRecord(db, exactMatch.id, input.embedding);
      span.setAttribute('dedup.result', 'updated');
      return {
        action: 'updated',
        memoryId: exactMatch.id
      };
    }
    if (bestMatch) {
      span.setAttribute('dedup.best_similarity', bestMatch.similarity);
    }

    if (bestMatch && bestMatch.similarity > 0.90) {
      const storedFact = preparedCrypto.encrypt(getVaultContext(bestMatch, input.vaultId), input.fact);
      const mergedScopeSql = 'target.previous_scope';
      const mergedValidFromSql = intersectValidityBoundSql('target.previous_valid_from', '$14', 'lower');
      const mergedValidUntilSql = intersectValidityBoundSql('target.previous_valid_until', '$15', 'upper');
      const updateResult = await db.query(
        `WITH target AS (
           SELECT id,
                  scope AS previous_scope,
                  valid_from AS previous_valid_from,
                  valid_until AS previous_valid_until,
                  authority_state AS previous_authority_state,
                  authority_version AS previous_authority_version
           FROM memories
           WHERE id = $1
             AND vault_id = $17
             AND scope = $10
             AND scope_key IS NOT DISTINCT FROM $19::text
             AND status = 'active'
             AND archived_at IS NULL
             AND sensitivity <> 'restricted'
             AND confidence > 0 AND confidence <= 1
             AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
             AND ${memoryValidityPredicateSql('memories', '$20')}
             AND ${validityWindowsOverlapPredicateSql('memories', '$14', '$15')}
             AND CASE WHEN evidence ? 'policy_rejections' THEN
               CASE WHEN jsonb_typeof(evidence -> 'policy_rejections') = 'array'
                 THEN jsonb_array_length(evidence -> 'policy_rejections') = 0
                 ELSE false END
               ELSE true END
             AND xmin::text = $21
           FOR UPDATE
         ), updated AS (
           UPDATE memories
           SET data = $2, hash = $3, embedding = $4::vector,
             source_chunks = (
               SELECT array_agg(DISTINCT chunk_id)
               FROM unnest(array_cat(COALESCE(memories.source_chunks, '{}'::uuid[]), $5::uuid[])) AS chunk_id
             ), score = GREATEST(score, $6),
             salience = GREATEST(salience, $7),
             sensitivity = CASE
               WHEN $8 = 'restricted' OR sensitivity = 'restricted' THEN 'restricted'
               WHEN $8 = 'high'       OR sensitivity = 'high'       THEN 'high'
               WHEN $8 = 'medium'     OR sensitivity = 'medium'     THEN 'medium'
               ELSE 'low'
             END,
             type = COALESCE($9, type),
             scope = ${mergedScopeSql},
             polarity = $11,
             authority_state = CASE
               WHEN authority_required
                 OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               THEN 'proposed' ELSE authority_state END,
             approved_by = CASE
               WHEN authority_required
                 OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               THEN NULL ELSE approved_by END,
             approved_at = CASE
               WHEN authority_required
                 OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               THEN NULL ELSE approved_at END,
             approval_source = CASE
               WHEN authority_required
                 OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               THEN NULL ELSE approval_source END,
             revoked_by = CASE
               WHEN authority_required
                 OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               THEN NULL ELSE revoked_by END,
             revoked_at = CASE
               WHEN authority_required
                 OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               THEN NULL ELSE revoked_at END,
             authority_version = CASE
               WHEN authority_required
                 OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
               THEN authority_version + 1 ELSE authority_version END,
             volatility = COALESCE($12::memory_volatility, volatility),
             evidence = COALESCE($13::jsonb, evidence),
             valid_from = ${mergedValidFromSql},
             valid_until = ${mergedValidUntilSql},
             source_timestamp = CASE
               WHEN $16::timestamptz IS NULL THEN source_timestamp
               WHEN source_timestamp IS NULL OR source_timestamp < $16::timestamptz THEN $16::timestamptz
               ELSE source_timestamp
             END,
             updated_at = now()
           FROM target
           WHERE memories.id = target.id
           RETURNING memories.id, memories.scope, memories.authority_state, memories.authority_version,
                     target.previous_scope,
                     target.previous_authority_state, target.previous_authority_version
         ), scope_audit AS (
           INSERT INTO memory_scope_change_log (
             vault_id, memory_id, old_scope, new_scope, actor_type, actor_id, source, reason
           )
           SELECT $17, updated.id, updated.previous_scope, updated.scope,
                  'worker', NULL, 'extraction_worker',
                  'Automatic similarity merge retained the least-privileged scope under row lock.'
           FROM updated
           WHERE updated.scope <> updated.previous_scope
           RETURNING id
         ), authority_audit AS (
           INSERT INTO memory_authority_events (
             vault_id, memory_id, event_type, old_state, new_state, old_version, new_version,
             actor_type, source, reason
           )
           SELECT $17, updated.id, 'invalidate', updated.previous_authority_state, updated.authority_state,
                  updated.previous_authority_version, updated.authority_version,
                  'worker', 'extraction_worker', $18
           FROM updated
           WHERE updated.authority_version <> updated.previous_authority_version
           RETURNING id
         )
         SELECT id FROM updated`,
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
          serializeEvidence(input, bestMatch.evidence),
          input.validFrom,
          input.validUntil,
          input.sourceTimestamp ?? null,
          input.vaultId,
          'Automatic similarity merge rewrote prompt-bearing memory content; approval requires review.',
          input.scopeKey,
          dedupDate,
          bestMatch.row_version
        ]
      );
      if (updateResult.rowCount === 0) {
        if (options.staleRetryAttempted) return { action: 'skipped' };
        return deduplicateMemoryInTransaction(input, db, preparedCrypto, effects, { ...options, staleRetryAttempted: true });
      }
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

      const canUsePrecomputedDecision = Boolean(
        escalation.escalate &&
        options.precomputedConflictDecision &&
        options.precomputedConflictInput === fingerprintDedupInput(input) &&
        options.precomputedConflictMemoryId === bestMatch.id &&
        options.precomputedConflictMemoryRevision === bestMatch.row_version
      );
      const decision: ConflictResolution | 'keep_both' = canUsePrecomputedDecision
        ? options.precomputedConflictDecision!
        : defaultDecisionWithoutEscalator(escalation.escalate);
      span.setAttribute('dedup.conflict_decision', decision);

      if (decision === 'keep_both') {
        const reservation = await reserveMemoryCreationInTransaction(db, input.vaultId);
        const inserted = await insertMemory(db, bestMatchVault, input, hash, canonicalSubject, preparedCrypto);
        await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
        recordSuccessfulInsert(effects, reservation, input.vaultId, bestMatchVault.account_id);
        span.setAttribute('dedup.result', 'inserted');
        return { action: 'inserted', memoryId: inserted.rows[0].id };
      }

      if (decision === 'merge') {
        const storedFact = preparedCrypto.encrypt(bestMatchVault, input.fact);
        const mergedScopeSql = 'target.previous_scope';
        const mergedValidFromSql = intersectValidityBoundSql('target.previous_valid_from', '$14', 'lower');
        const mergedValidUntilSql = intersectValidityBoundSql('target.previous_valid_until', '$15', 'upper');
        const updateResult = await db.query(
          `WITH target AS (
             SELECT id,
                    scope AS previous_scope,
                    valid_from AS previous_valid_from,
                    valid_until AS previous_valid_until,
                    authority_state AS previous_authority_state,
                    authority_version AS previous_authority_version
             FROM memories
             WHERE id = $1
               AND vault_id = $17
               AND scope = $10
               AND scope_key IS NOT DISTINCT FROM $19::text
               AND status = 'active'
               AND archived_at IS NULL
               AND sensitivity <> 'restricted'
               AND confidence > 0 AND confidence <= 1
               AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
               AND ${memoryValidityPredicateSql('memories', '$20')}
               AND ${validityWindowsOverlapPredicateSql('memories', '$14', '$15')}
               AND CASE WHEN evidence ? 'policy_rejections' THEN
                 CASE WHEN jsonb_typeof(evidence -> 'policy_rejections') = 'array'
                   THEN jsonb_array_length(evidence -> 'policy_rejections') = 0
                   ELSE false END
                 ELSE true END
               AND xmin::text = $21
             FOR UPDATE
           ), updated AS (
             UPDATE memories
             SET data = $2, hash = $3, embedding = $4::vector,
               source_chunks = (
                 SELECT array_agg(DISTINCT chunk_id)
                 FROM unnest(array_cat(COALESCE(memories.source_chunks, '{}'::uuid[]), $5::uuid[])) AS chunk_id
               ), score = GREATEST(score, $6),
               salience = GREATEST(salience, $7),
               sensitivity = CASE
                 WHEN $8 = 'restricted' OR sensitivity = 'restricted' THEN 'restricted'
                 WHEN $8 = 'high'       OR sensitivity = 'high'       THEN 'high'
                 WHEN $8 = 'medium'     OR sensitivity = 'medium'     THEN 'medium'
                 ELSE 'low'
               END,
               type = COALESCE($9, type),
               scope = ${mergedScopeSql},
               polarity = $11,
               authority_state = CASE
                 WHEN authority_required
                   OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
                 THEN 'proposed' ELSE authority_state END,
               approved_by = CASE
                 WHEN authority_required
                   OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
                 THEN NULL ELSE approved_by END,
               approved_at = CASE
                 WHEN authority_required
                   OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
                 THEN NULL ELSE approved_at END,
               approval_source = CASE
                 WHEN authority_required
                   OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
                 THEN NULL ELSE approval_source END,
               revoked_by = CASE
                 WHEN authority_required
                   OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
                 THEN NULL ELSE revoked_by END,
               revoked_at = CASE
                 WHEN authority_required
                   OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
                 THEN NULL ELSE revoked_at END,
               authority_version = CASE
                 WHEN authority_required
                   OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
                 THEN authority_version + 1 ELSE authority_version END,
               volatility = COALESCE($12::memory_volatility, volatility),
               evidence = COALESCE($13::jsonb, evidence),
               valid_from = ${mergedValidFromSql},
               valid_until = ${mergedValidUntilSql},
               source_timestamp = CASE
                 WHEN $16::timestamptz IS NULL THEN source_timestamp
                 WHEN source_timestamp IS NULL OR source_timestamp < $16::timestamptz THEN $16::timestamptz
                 ELSE source_timestamp
               END,
               updated_at = now()
             FROM target
             WHERE memories.id = target.id
             RETURNING memories.id, memories.scope, memories.authority_state, memories.authority_version,
                       target.previous_scope,
                       target.previous_authority_state, target.previous_authority_version
           ), scope_audit AS (
             INSERT INTO memory_scope_change_log (
               vault_id, memory_id, old_scope, new_scope, actor_type, actor_id, source, reason
             )
             SELECT $17, updated.id, updated.previous_scope, updated.scope,
                    'worker', NULL, 'extraction_worker',
                    'Conflict-arbitrated merge retained the least-privileged scope under row lock.'
             FROM updated
             WHERE updated.scope <> updated.previous_scope
             RETURNING id
           ), authority_audit AS (
             INSERT INTO memory_authority_events (
               vault_id, memory_id, event_type, old_state, new_state, old_version, new_version,
               actor_type, source, reason
             )
             SELECT $17, updated.id, 'invalidate', updated.previous_authority_state, updated.authority_state,
                    updated.previous_authority_version, updated.authority_version,
                    'worker', 'extraction_worker', $18
             FROM updated
             WHERE updated.authority_version <> updated.previous_authority_version
             RETURNING id
           )
           SELECT id FROM updated`,
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
            serializeEvidence(input, bestMatch.evidence),
            input.validFrom,
            input.validUntil,
            input.sourceTimestamp ?? null,
            input.vaultId,
            'Conflict-arbitrated merge rewrote prompt-bearing memory content; approval requires review.',
            input.scopeKey,
            dedupDate,
            bestMatch.row_version
          ]
        );
        if (updateResult.rowCount === 0) {
          if (options.staleRetryAttempted) return { action: 'skipped' };
          return deduplicateMemoryInTransaction(input, db, preparedCrypto, effects, { ...options, staleRetryAttempted: true });
        }
        await syncEmbeddingRecord(db, bestMatch.id, input.embedding);
        span.setAttribute('dedup.result', 'updated');
        return { action: 'updated', memoryId: bestMatch.id };
      }

      if (decision === 'discard_new') {
        span.setAttribute('dedup.result', 'skipped');
        return { action: 'skipped', memoryId: bestMatch.id };
      }

      if (decision === 'supersede_old') {
        const superseded = await db.query(
          `UPDATE memories
           SET status = 'superseded',
               updated_at = now()
           WHERE id = $1
             AND vault_id = $2
             AND scope = $3
             AND scope_key IS NOT DISTINCT FROM $4::text
             AND status = 'active'
             AND archived_at IS NULL
             AND sensitivity <> 'restricted'
             AND confidence > 0 AND confidence <= 1
             AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
             AND CASE WHEN evidence ? 'policy_rejections' THEN
               CASE WHEN jsonb_typeof(evidence -> 'policy_rejections') = 'array'
                 THEN jsonb_array_length(evidence -> 'policy_rejections') = 0
                 ELSE false END
               ELSE true END
             AND xmin::text = $5`,
          [bestMatch.id, input.vaultId, input.scope, input.scopeKey, bestMatch.row_version]
        );
        if (superseded.rowCount !== 1) {
          throw new Error(`Dedup supersede target ${bestMatch.id} changed after arbitration`);
        }
      } else if (decision === 'needs_review') {
        // Without a usable decision neither side may remain active. Quarantine
        // the old target under CAS and insert the incoming side as review-only.
        const quarantined = await db.query(
          `UPDATE memories
           SET status = 'needs_review',
               updated_at = now()
           WHERE id = $1
             AND vault_id = $2
             AND scope = $3
             AND scope_key IS NOT DISTINCT FROM $4::text
             AND status = 'active'
             AND archived_at IS NULL
             AND sensitivity <> 'restricted'
             AND confidence > 0 AND confidence <= 1
             AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
             AND CASE WHEN evidence ? 'policy_rejections' THEN
               CASE WHEN jsonb_typeof(evidence -> 'policy_rejections') = 'array'
                 THEN jsonb_array_length(evidence -> 'policy_rejections') = 0
                 ELSE false END
               ELSE true END
             AND xmin::text = $5`,
          [bestMatch.id, input.vaultId, input.scope, input.scopeKey, bestMatch.row_version]
        );
        if (quarantined.rowCount !== 1) {
          memoryPolicyEventCounter.add(1, {
            event: 'quarantine_failure',
            source: 'dedup',
            reason: 'concurrent_change'
          });
          throw new Error(`Dedup review target ${bestMatch.id} changed after arbitration`);
        }
      }

      const reservation = await reserveMemoryCreationInTransaction(db, input.vaultId);
      const inserted = await insertMemory(db, bestMatchVault,
        decision === 'needs_review' ? { ...input, status: 'needs_review' } : input,
        hash, canonicalSubject, preparedCrypto);
      await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
      recordSuccessfulInsert(effects, reservation, input.vaultId, bestMatchVault.account_id);
      span.setAttribute('dedup.result', 'inserted');
      return { action: 'inserted', memoryId: inserted.rows[0].id };
    }

    const reservation = await reserveMemoryCreationInTransaction(db, input.vaultId);
    const inserted = await insertMemory(db, vault, input, hash, canonicalSubject, preparedCrypto);
    await syncEmbeddingRecord(db, inserted.rows[0].id, input.embedding);
    recordSuccessfulInsert(effects, reservation, input.vaultId, vault.account_id);

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
  db: Queryable = { query },
  preparedCrypto?: PreparedVaultCrypto
): Promise<DedupEscalationRequest | null> {
  if (isSecretLikeMemoryContent(`${input.subject}\n${input.fact}`)) {
    throw new Error('Memory content rejected by secret policy');
  }
  if (input.status === 'candidate' || input.status === 'needs_review') {
    return null;
  }

  const localCrypto = preparedCrypto ?? await prepareDedupCrypto(input.vaultId, db);
  const { bestMatch, exactMatch } = await resolveDedupMatch(input, db, localCrypto);
  if (exactMatch) {
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
    inputFingerprint: fingerprintDedupInput(input),
    existingFact: localCrypto.decrypt(getVaultContext(bestMatch, input.vaultId), bestMatch.data),
    newFact: input.fact,
    existingMemoryId: bestMatch.id,
    existingMemoryRevision: bestMatch.row_version,
    reasons: escalation.reasons
  };
}

async function resolveDedupMatch(
  input: DedupInput,
  db: Queryable,
  preparedCrypto: PreparedVaultCrypto
): Promise<DedupMatchResolution> {
  const dedupDate = toDateOnly(new Date());
  if (!dedupDate) {
    throw new Error('Unable to derive a valid deduplication date');
  }
  const hash = crypto.createHash('md5').update(input.fact).digest('hex');
  const normalisedSubject = normaliseSubject(input.subject);
  const canonicalSubject = await resolveCanonical(
    input.vaultId,
    normalisedSubject,
    input.scope,
    input.scopeKey,
    db
  ) ?? normalisedSubject;
  const vaultResult = await db.query<DedupVaultContext>(
    `SELECT id, account_id::text AS account_id, encrypted_dek, vault_encryption_enabled
     FROM vaults
     WHERE id = $1
     LIMIT 1`,
    [input.vaultId]
  );
  const vault = vaultResult.rows[0];
  if (!vault) {
    throw new Error(`Vault ${input.vaultId} not found`);
  }
  // Validate the input-vault binding even when an exact match skips HMAC lookup.
  const subjectMatchTarget = preparedCrypto.subjectMatch(vault, canonicalSubject);

  const exactMatch = await db.query<Pick<MemoryRow, 'id' | 'scope' | 'scope_key' | 'status' | 'evidence' | 'row_version'>>(
    `SELECT id, scope, scope_key, status, evidence, xmin::text AS row_version
     FROM memories
     WHERE vault_id = $1
       AND hash = $2
       AND scope = $6
       AND scope_key IS NOT DISTINCT FROM $7::text
       AND archived_at IS NULL
       AND status = 'active'
       AND sensitivity <> 'restricted'
       AND confidence > 0 AND confidence <= 1
       AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
       AND CASE WHEN evidence ? 'policy_rejections' THEN
         CASE WHEN jsonb_typeof(evidence -> 'policy_rejections') = 'array'
           THEN jsonb_array_length(evidence -> 'policy_rejections') = 0
           ELSE false END
         ELSE true END
       AND ${memoryValidityPredicateSql('memories', '$3')}
       AND ${validityWindowsOverlapPredicateSql('memories', '$4', '$5')}
     LIMIT 1`,
    [input.vaultId, hash, dedupDate, input.validFrom, input.validUntil, input.scope, input.scopeKey]
  );

  if (exactMatch.rowCount) {
    return {
      dedupDate,
      hash,
      vault,
      canonicalSubject,
      exactMatch: exactMatch.rows[0]
    };
  }

  const subjectMatchColumn = isVaultEncryptionActive(vault) ? 'm.subject_hmac' : 'm.subject';
  const subjectMatches = await db.query<(MemoryRow & { similarity: number })>(
    `SELECT m.id, m.data, m.confidence, m.score, m.salience, m.type, m.scope, m.scope_key, m.polarity, m.status, m.volatility, m.evidence,
            m.xmin::text AS row_version,
            v.account_id::text AS account_id,
            v.encrypted_dek, v.vault_encryption_enabled,
            1 - (m.embedding <=> $3::vector) AS similarity
     FROM memories AS m
     JOIN vaults AS v
       ON v.id = m.vault_id
     WHERE m.vault_id = $1
       AND ${subjectMatchColumn} = $2
       AND m.scope = $7
       AND m.scope_key IS NOT DISTINCT FROM $8::text
       AND m.archived_at IS NULL
       AND m.status = 'active'
       AND m.sensitivity <> 'restricted'
       AND m.confidence > 0 AND m.confidence <= 1
       AND (m.source_timestamp IS NULL OR m.source_timestamp <= now() + interval '5 minutes')
       AND CASE WHEN m.evidence ? 'policy_rejections' THEN
         CASE WHEN jsonb_typeof(m.evidence -> 'policy_rejections') = 'array'
           THEN jsonb_array_length(m.evidence -> 'policy_rejections') = 0
           ELSE false END
         ELSE true END
       AND ${memoryValidityPredicateSql('m', '$4')}
       AND ${validityWindowsOverlapPredicateSql('m', '$5', '$6')}
       AND m.embedding IS NOT NULL
     ORDER BY similarity DESC
     LIMIT 1`,
    [input.vaultId, subjectMatchTarget, JSON.stringify(input.embedding), dedupDate, input.validFrom, input.validUntil, input.scope, input.scopeKey]
  );

  return {
    dedupDate,
    hash,
    vault,
    canonicalSubject,
    bestMatch: subjectMatches.rows[0]
  };
}

function getVaultContext(row: MemoryRow, vaultId: string): DedupVaultContext {
  return {
    account_id: row.account_id,
    id: vaultId,
    encrypted_dek: row.encrypted_dek,
    vault_encryption_enabled: row.vault_encryption_enabled
  };
}

async function prepareDedupCrypto(vaultId: string, db: Queryable): Promise<PreparedVaultCrypto> {
  const result = await db.query<DedupVaultContext>(
    'SELECT id, encrypted_dek, vault_encryption_enabled FROM vaults WHERE id=$1', [vaultId]
  );
  if (!result.rows[0]) throw new Error('Dedup vault not found');
  return prepareVaultCrypto(result.rows[0]);
}

async function insertMemory(
  db: Queryable,
  vault: DedupVaultContext,
  input: DedupInput,
  hash: string,
  canonicalSubject: string,
  preparedCrypto: PreparedVaultCrypto
) {
  const storedFact = preparedCrypto.encrypt(vault, input.fact);
  const encryptedSubject = preparedCrypto.subject(vault, canonicalSubject);
  const result = await db.query<{ id: string }>(
     `INSERT INTO memories (
       vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding,
       source_chunks, score, salience, sensitivity, type, scope, scope_key, polarity, status, volatility, evidence, valid_from, valid_until, source_segment_id, source_timestamp
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::uuid[], $9, $10, $11, $12, $13, $14, $15, $16, $17::memory_volatility, $18::jsonb, $19::date, $20::date, $21, $22::timestamptz)
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
      input.scopeKey,
      input.polarity,
      input.status,
      input.volatility,
      serializeEvidence(input),
      input.validFrom,
      input.validUntil,
      input.sourceSegmentId ?? null,
      input.sourceTimestamp ?? null
    ]
  );
  return result;
}

function recordSuccessfulInsert(
  effects: WorkerEffect[],
  reservation: ApiQuotaReservation,
  vaultId: string,
  accountId: string | null
): void {
  effects.push(
    { kind: 'quota', reservation },
    { kind: 'memory-count', vaultId, accountId, delta: 1, source: 'extraction_worker' }
  );
}

function serializeEvidence(
  input: Pick<DedupInput, 'evidence' | 'policyRejections'>,
  existingEvidence?: unknown
): string | null {
  if (!input.evidence && (input.policyRejections?.length ?? 0) === 0) {
    return null;
  }
  return mergeMemoryEvidence(
    existingEvidence,
    input.evidence ?? undefined,
    input.policyRejections
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
