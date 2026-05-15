import crypto from 'node:crypto';
import { parentPort } from 'node:worker_threads';
import type { PoolClient } from 'pg';

import { getConfig } from '../config';
import { closePool, query, withTransaction } from '../db/client';
import { CircuitBreakerOpenError } from '../services/ai-resilience';
import {
  decryptForVault,
  encryptForVault,
  encryptSubjectForVault,
  initCryptoClient,
  isVaultEncryptionActive,
  type VaultEncryptionContext
} from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { CuratorService, type CuratorAliasMaps, type CuratorMemory, type CuratorResult, type EdgeType, type MemoryType } from '../services/curator';
import { getSpanAttributes } from '../telemetry';
import { aiBudgetThrottledJobsCounter, aiBudgetWaitHistogram } from '../metrics';
import { AiBudgetDeferredError } from '../services/usage';

interface CurationQueueRow {
  queue_id: string;
  vault_id: string;
  segment_id: string;
}

interface VaultRow extends VaultEncryptionContext {
  plan_id: string;
}

interface MemoryRow {
  id: string;
  vault_id: string;
  data: string;
  subject: string;
  subject_encrypted: string | null;
  subject_hmac: string | null;
  confidence: number;
  salience: number;
  sensitivity: 'low' | 'medium' | 'high' | 'restricted';
  type: MemoryType | null;
  scope: 'global' | 'project' | 'task' | 'session';
  polarity: 'positive' | 'negative' | 'neutral';
  volatility: 'very_low' | 'low' | 'medium' | 'high';
  evidence: unknown;
  parent_id: string | null;
  source_chunks: string[];
  archived_at: string | null;
  status: string;
}

interface LoadedCurationJob {
  queueId: string;
  segmentId: string;
  vault: VaultRow;
  conversation: string | null;
  candidates: CuratorMemory[];
  activeMemories: CuratorMemory[];
  candidateIds: Set<string>;
}

const config = getConfig();
const curator = new CuratorService();
const embedder = getEmbedder();
const workerId = crypto.randomUUID();
const MAX_CURATION_RETRIES = Number(process.env.MAX_CURATION_RETRIES ?? 5);
const AUTO_PROMOTE_DUPLICATE_SIMILARITY = 0.90;

async function processBatch() {
  await query(
    `UPDATE curation_queue
     SET claimed_at = NULL, claimed_by = NULL
     WHERE claimed_at < now() - interval '10 minutes'`
  );

  const claimed = await query<CurationQueueRow>(
    `WITH claimed AS (
       SELECT cq.id AS queue_id, cq.vault_id, cq.segment_id
       FROM curation_queue cq
       WHERE cq.claimed_at IS NULL
         AND cq.available_at <= now()
       ORDER BY cq.enqueued_at ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE curation_queue cq
     SET claimed_at = now(), claimed_by = $2
     FROM claimed
     WHERE cq.id = claimed.queue_id
     RETURNING claimed.queue_id, claimed.vault_id, claimed.segment_id`,
    [config.CURATION_BATCH_SIZE, workerId]
  );

  for (const row of claimed.rows) {
    try {
      const retryResult = await query<{ retry_count: number; last_error: string | null }>(
        `SELECT retry_count, last_error
         FROM curation_queue
         WHERE id = $1`,
        [row.queue_id]
      );

      if (!retryResult.rowCount) {
        continue;
      }

      if (Number(retryResult.rows[0].retry_count) >= MAX_CURATION_RETRIES) {
        console.warn(
          `Curation job exceeded retry limit and will be dead-lettered: queueId=${row.queue_id}, retryCount=${retryResult.rows[0].retry_count}, maxRetries=${MAX_CURATION_RETRIES}`
        );
        await withTransaction(async (client) => {
          await client.query(
            `INSERT INTO curation_dead_letter (vault_id, segment_id, retry_count, last_error)
             VALUES ($1, $2, $3, $4)`,
            [row.vault_id, row.segment_id, retryResult.rows[0].retry_count, retryResult.rows[0].last_error]
          );
          await client.query(`DELETE FROM curation_queue WHERE id = $1`, [row.queue_id]);
        });
        continue;
      }

      await query(
        `UPDATE curation_queue
         SET claimed_at = now()
         WHERE id = $1`,
        [row.queue_id]
      );

      const job = await loadJob(row);
      if (job.candidates.length === 0) {
        await query(`DELETE FROM curation_queue WHERE id = $1`, [job.queueId]);
        continue;
      }

      const { result, aliasMaps, rawResponse } = await curator.curate(job.candidates, job.activeMemories, job.conversation, job.vault.id);
      await applyActions(job, result, aliasMaps, rawResponse);
      await query(`DELETE FROM curation_queue WHERE id = $1`, [job.queueId]);
    } catch (error) {
      if (error instanceof AiBudgetDeferredError) {
        aiBudgetWaitHistogram.record(error.waitMs, { role: error.role, queue: 'curation', vault_id: row.vault_id });
        aiBudgetThrottledJobsCounter.add(1, { role: error.role, queue: 'curation', vault_id: row.vault_id });
        console.info(JSON.stringify({
          level: 30,
          msg: 'deferring curation job for ai budget',
          queue_id: row.queue_id,
          role: error.role,
          available_at: error.availableAt.toISOString(),
          wait_ms: error.waitMs
        }));
        await query(
          `UPDATE curation_queue
           SET available_at = $2,
               last_error = $3,
               claimed_at = NULL,
               claimed_by = NULL
           WHERE id = $1`,
          [row.queue_id, error.availableAt.toISOString(), error.message]
        );
        continue;
      }
      if (error instanceof CircuitBreakerOpenError) {
        console.warn(JSON.stringify({
          level: 40,
          msg: 'skipping curation job while circuit breaker is open',
          queue_id: row.queue_id,
          retry_after_ms: error.retryAfterMs
        }));
        await query(
          `UPDATE curation_queue
           SET last_error = $2,
               claimed_at = NULL,
               claimed_by = NULL
           WHERE id = $1`,
          [row.queue_id, error.message]
        );
        continue;
      }
      const lastError = error instanceof Error ? error.message : 'Unknown curation error';
      console.error(getSpanAttributes({ error, queueId: row.queue_id }), 'Curation job failed');
      await query(
        `UPDATE curation_queue
         SET retry_count = retry_count + 1,
             last_error = $2,
             claimed_at = NULL,
             claimed_by = NULL
         WHERE id = $1`,
        [row.queue_id, lastError]
      );
    }
  }
}

async function loadJob(row: CurationQueueRow): Promise<LoadedCurationJob> {
  const vaultResult = await query<VaultRow>(
    `SELECT id, encrypted_dek, vault_encryption_enabled, plan_id
     FROM vaults
     WHERE id = $1
     LIMIT 1`,
    [row.vault_id]
  );

  if (!vaultResult.rowCount) {
    throw new Error(`Vault ${row.vault_id} not found`);
  }

  const vault = vaultResult.rows[0];
  const segmentResult = await query<{ context: string | null }>(
    `SELECT context
     FROM segments
     WHERE id = $1 AND vault_id = $2
     LIMIT 1`,
    [row.segment_id, row.vault_id]
  );

  if (!segmentResult.rowCount) {
    throw new Error(`Segment ${row.segment_id} not found`);
  }

  const conversation = segmentResult.rows[0].context
    ? await decryptForVault(vault, segmentResult.rows[0].context)
    : null;

  const candidateRows = await query<MemoryRow>(
    `SELECT id, vault_id, data, subject, subject_encrypted, subject_hmac, confidence, salience, sensitivity, type,
            scope, polarity, volatility, evidence, parent_id, source_chunks, archived_at, status
     FROM memories
     WHERE vault_id = $1
       AND source_segment_id = $2
       AND archived_at IS NULL
       AND status = 'candidate'
     ORDER BY created_at ASC`,
    [row.vault_id, row.segment_id]
  );

  const candidates = await Promise.all(candidateRows.rows.map((memory) => decryptMemory(vault, memory)));
  const candidateIds = new Set(candidates.map((memory) => memory.id));
  const subjects = Array.from(new Set(candidates.map((memory) => memory.subject)));
  const activeMemories = await loadActiveMemoriesForSubjects(vault, subjects);

  return {
    queueId: row.queue_id,
    segmentId: row.segment_id,
    vault,
    conversation,
    candidates,
    activeMemories,
    candidateIds
  };
}

async function loadActiveMemoriesForSubjects(vault: VaultRow, subjects: string[]): Promise<CuratorMemory[]> {
  if (subjects.length === 0) {
    return [];
  }

  const plaintextSubjects = isVaultEncryptionActive(vault) ? [] : subjects;
  const subjectHmacs = isVaultEncryptionActive(vault)
    ? (await Promise.all(subjects.map(async (subject) => (await encryptSubjectForVault(vault, subject))?.hmac ?? null)))
        .filter((value): value is string => Boolean(value))
    : [];

  const result = await query<MemoryRow>(
    `SELECT id, vault_id, data, subject, subject_encrypted, subject_hmac, confidence, salience, sensitivity, type,
            scope, polarity, volatility, evidence, parent_id, source_chunks, archived_at, status
     FROM memories
     WHERE vault_id = $1
       AND archived_at IS NULL
       AND status = 'active'
       AND (
         subject = ANY($2::text[])
         OR subject_hmac = ANY($3::text[])
       )
     ORDER BY updated_at DESC, created_at DESC`,
    [vault.id, plaintextSubjects, subjectHmacs]
  );

  return Promise.all(result.rows.map((memory) => decryptMemory(vault, memory)));
}

async function decryptMemory(vault: VaultRow, memory: MemoryRow): Promise<CuratorMemory> {
  const subject = memory.subject_encrypted
    ? await decryptForVault(vault, memory.subject_encrypted)
    : memory.subject;

  return {
    id: memory.id,
    subject,
    data: await decryptForVault(vault, memory.data),
    type: memory.type,
    scope: memory.scope,
    salience: Number(memory.salience),
    confidence: Number(memory.confidence),
    sensitivity: memory.sensitivity,
    polarity: memory.polarity,
    volatility: memory.volatility,
    evidence: typeof memory.evidence === 'object' && memory.evidence && 'summary' in (memory.evidence as Record<string, unknown>)
      ? String((memory.evidence as { summary?: unknown }).summary ?? '')
      : null,
    parent_id: memory.parent_id
  };
}

async function applyActions(
  job: LoadedCurationJob,
  actions: CuratorResult,
  aliasMaps: CuratorAliasMaps,
  rawResponse: unknown
): Promise<void> {
  const capCuratorText = (value: string | null | undefined): string | undefined => value ? value.slice(0, 500) : undefined;
  const knownById = new Map<string, CuratorMemory>();
  for (const memory of [...job.candidates, ...job.activeMemories]) {
    knownById.set(memory.id, memory);
  }
  const knownSubjectIds = new Map<string, string>();
  for (const memory of [...job.activeMemories, ...job.candidates]) {
    if (!knownSubjectIds.has(memory.subject)) {
      knownSubjectIds.set(memory.subject, memory.id);
    }
  }

  const touchedCandidates = new Set<string>();

  await withTransaction(async (client) => {
    for (const action of actions.nodes_to_create) {
      try {
        const inserted = await insertActiveMemory(client, job.vault, {
          subject: action.subject,
          fact: action.statement,
          type: action.type,
          scope: action.scope ?? 'global',
          salience: action.salience ?? 0.6,
          confidence: action.confidence ?? 0.95,
          sensitivity: action.sensitivity ?? 'low',
          polarity: action.polarity ?? 'neutral',
          volatility: action.volatility ?? 'low',
          evidence: capCuratorText(action.evidence) ?? null,
          parentId: action.parent_subject ? knownSubjectIds.get(action.parent_subject) ?? null : null,
          sourceSegmentId: job.segmentId
        });
        knownSubjectIds.set(action.subject, inserted.id);
        knownById.set(inserted.id, {
          id: inserted.id,
          subject: action.subject,
          data: action.statement,
          type: action.type,
          scope: action.scope ?? 'global',
          salience: action.salience ?? 0.6,
          confidence: action.confidence ?? 0.95,
          sensitivity: action.sensitivity ?? 'low',
          polarity: action.polarity ?? 'neutral',
          volatility: action.volatility ?? 'low',
          evidence: capCuratorText(action.evidence) ?? null,
          parent_id: action.parent_subject ? knownSubjectIds.get(action.parent_subject) ?? null : null
        });
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'create',
          subject: action.subject,
          newMemoryId: inserted.id,
          newValue: action.statement,
          rawResponse,
          applied: true
        });
      } catch (error) {
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'create',
          subject: action.subject,
          newValue: action.statement,
          rawResponse,
          applied: false,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }

    for (const action of actions.nodes_to_update) {
      const resolvedId = resolveAlias(action.id, aliasMaps);
      const existing = knownById.get(resolvedId);
      if (!existing) {
        console.warn(`Skipping curator update with unknown alias or memory id: id=${action.id}`);
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'update',
          newValue: action.statement,
          rawResponse,
          applied: false,
          error: `Curator referenced memory outside curation context: ${action.id}`
        });
        continue;
      }

      try {
        await updateMemoryNode(client, job.vault, {
          id: resolvedId,
          fact: action.statement,
          subject: action.subject ?? existing.subject,
          type: action.type ?? existing.type ?? 'system_fact',
          scope: existing.scope,
          salience: action.salience ?? existing.salience,
          confidence: action.confidence ?? existing.confidence ?? 0.95,
          sensitivity: existing.sensitivity,
          polarity: existing.polarity,
          volatility: action.volatility ?? existing.volatility,
          evidence: capCuratorText(action.reason) ?? existing.evidence ?? null,
          parentId: existing.parent_id
        });
        knownById.set(resolvedId, {
          ...existing,
          data: action.statement,
          subject: action.subject ?? existing.subject,
          type: action.type ?? existing.type,
          salience: action.salience ?? existing.salience,
          confidence: action.confidence ?? existing.confidence,
          volatility: action.volatility ?? existing.volatility,
          evidence: capCuratorText(action.reason) ?? existing.evidence ?? null
        });
        if (job.candidateIds.has(resolvedId)) {
          touchedCandidates.add(resolvedId);
        }
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'update',
          memoryId: resolvedId,
          newMemoryId: resolvedId,
          subject: action.subject ?? existing.subject,
          oldValue: existing.data,
          newValue: action.statement,
          rawResponse,
          applied: true
        });
      } catch (error) {
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'update',
          memoryId: knownById.has(resolvedId) ? resolvedId : undefined,
          subject: knownById.has(resolvedId) ? knownById.get(resolvedId)?.subject : action.subject,
          oldValue: knownById.has(resolvedId) ? knownById.get(resolvedId)?.data : undefined,
          newValue: action.statement,
          rawResponse,
          applied: false,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }

    for (const action of actions.edges_to_create) {
      try {
        const fromId = knownSubjectIds.get(action.from_subject);
        const toId = knownSubjectIds.get(action.to_subject);
        if (!fromId || !toId) {
          throw new Error(`Unable to resolve edge subjects: ${action.from_subject} -> ${action.to_subject}`);
        }
        const cappedReason = capCuratorText(action.reason);
        await insertEdge(client, job.vault.id, fromId, toId, action.type, action.confidence ?? 0.8, cappedReason ?? null);
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'create',
          memoryId: fromId,
          newMemoryId: toId,
          subject: `${action.from_subject} -> ${action.to_subject}`,
          newValue: `${action.type}: ${cappedReason ?? ''}`.trim(),
          rawResponse,
          applied: true,
          error: undefined
        });
      } catch (error) {
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'create',
          subject: `${action.from_subject} -> ${action.to_subject}`,
          newValue: action.type,
          rawResponse,
          applied: false,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }

    for (const action of actions.nodes_to_archive) {
      const resolvedId = resolveAlias(action.id, aliasMaps);
      const existing = knownById.get(resolvedId);
      if (!existing) {
        console.warn(`Skipping curator archive with unknown alias or memory id: id=${action.id}`);
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'delete',
          rawResponse,
          applied: false,
          error: `Curator referenced memory outside curation context: ${action.id}`
        });
        continue;
      }

      try {
        await archiveMemory(client, resolvedId, job.vault.id);
        if (job.candidateIds.has(resolvedId)) {
          touchedCandidates.add(resolvedId);
        }
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'delete',
          memoryId: resolvedId,
          subject: existing.subject,
          oldValue: existing.data,
          newValue: capCuratorText(action.reason),
          rawResponse,
          applied: true
        });
      } catch (error) {
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'delete',
          memoryId: knownById.has(resolvedId) ? resolvedId : undefined,
          subject: knownById.has(resolvedId) ? knownById.get(resolvedId)?.subject : undefined,
          oldValue: knownById.has(resolvedId) ? knownById.get(resolvedId)?.data : undefined,
          newValue: capCuratorText(action.reason),
          rawResponse,
          applied: false,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }

    for (const action of actions.discarded_candidates) {
      const resolvedId = resolveAlias(action.id, aliasMaps);
      const existing = knownById.get(resolvedId);
      if (!existing) {
        console.warn(`Skipping curator discard with unknown alias or memory id: id=${action.id}`);
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'delete',
          rawResponse,
          applied: false,
          error: `Curator referenced memory outside curation context: ${action.id}`
        });
        continue;
      }

      try {
        await archiveMemory(client, resolvedId, job.vault.id);
        touchedCandidates.add(resolvedId);
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'delete',
          memoryId: resolvedId,
          subject: existing.subject,
          oldValue: existing.data,
          newValue: capCuratorText(action.reason),
          rawResponse,
          applied: true
        });
      } catch (error) {
        await insertActionLog(client, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'delete',
          memoryId: knownById.has(resolvedId) ? resolvedId : undefined,
          subject: knownById.has(resolvedId) ? knownById.get(resolvedId)?.subject : undefined,
          oldValue: knownById.has(resolvedId) ? knownById.get(resolvedId)?.data : undefined,
          newValue: capCuratorText(action.reason),
          rawResponse,
          applied: false,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }

    const duplicateResult = await archiveDuplicatePromotionCandidates(client, job, Array.from(touchedCandidates));
    for (const duplicate of duplicateResult) {
      touchedCandidates.add(duplicate.candidate_id);
      const memory = knownById.get(duplicate.candidate_id);
      await insertActionLog(client, {
        vaultId: job.vault.id,
        vault: job.vault,
        segmentId: job.segmentId,
        actionType: 'archive_duplicate',
        memoryId: duplicate.candidate_id,
        newMemoryId: duplicate.active_id,
        subject: memory?.subject,
        oldValue: memory?.data,
        newValue: `duplicate of active memory ${duplicate.active_id} (${duplicate.similarity.toFixed(3)} similarity)`,
        rawResponse,
        applied: true
      });
    }

    const promoteResult = await client.query<{ id: string }>(
      `UPDATE memories
       SET status = 'active',
           updated_at = now()
       WHERE vault_id = $1
         AND source_segment_id = $2
         AND archived_at IS NULL
         AND status = 'candidate'
         AND NOT (id = ANY($3::uuid[]))
       RETURNING id`,
      [job.vault.id, job.segmentId, Array.from(touchedCandidates)]
    );

    for (const row of promoteResult.rows) {
      const memory = knownById.get(row.id);
      await insertActionLog(client, {
        vaultId: job.vault.id,
        vault: job.vault,
        segmentId: job.segmentId,
        actionType: 'promote',
        memoryId: row.id,
        subject: memory?.subject,
        oldValue: memory?.data,
        newValue: memory?.data,
        rawResponse,
        applied: true
      });
    }
  });
}

async function archiveDuplicatePromotionCandidates(
  client: PoolClient,
  job: LoadedCurationJob,
  touchedCandidateIds: string[]
): Promise<Array<{ candidate_id: string; active_id: string; similarity: number }>> {
  const duplicates = await client.query<{ candidate_id: string; active_id: string; similarity: number }>(
    `WITH duplicate_candidates AS (
       SELECT DISTINCT ON (candidate.id)
              candidate.id AS candidate_id,
              active.id AS active_id,
              1 - (candidate_embedding.embedding <=> active_embedding.embedding) AS similarity
       FROM memories candidate
       JOIN memory_embeddings candidate_embedding
         ON candidate_embedding.memory_id = candidate.id
       JOIN memories active
         ON active.vault_id = candidate.vault_id
        AND active.id <> candidate.id
        AND active.status = 'active'
        AND active.archived_at IS NULL
        AND (
          (
            candidate.subject <> ''
            AND active.subject = candidate.subject
          )
          OR (
            candidate.subject_hmac IS NOT NULL
            AND active.subject_hmac = candidate.subject_hmac
          )
        )
       JOIN memory_embeddings active_embedding
         ON active_embedding.memory_id = active.id
       WHERE candidate.vault_id = $1
         AND candidate.source_segment_id = $2
         AND candidate.archived_at IS NULL
         AND candidate.status = 'candidate'
         AND NOT (candidate.id = ANY($3::uuid[]))
         AND 1 - (candidate_embedding.embedding <=> active_embedding.embedding) >= $4
       ORDER BY candidate.id, candidate_embedding.embedding <=> active_embedding.embedding
     ),
     archived AS (
       UPDATE memories candidate
       SET archived_at = now(),
           status = 'superseded',
           updated_at = now()
       FROM duplicate_candidates duplicate
       WHERE candidate.id = duplicate.candidate_id
       RETURNING candidate.id AS candidate_id, duplicate.active_id, candidate.source_chunks, duplicate.similarity
     ),
     merged_source_chunks AS (
       SELECT active_id, array_agg(DISTINCT chunk_id) AS source_chunks
       FROM archived, unnest(source_chunks) AS chunk_id
       GROUP BY active_id
     ),
     updated_active AS (
       UPDATE memories active
       SET source_chunks = (
             SELECT array_agg(DISTINCT chunk_id)
             FROM unnest(array_cat(active.source_chunks, merged.source_chunks)) AS chunk_id
           ),
           updated_at = now()
       FROM merged_source_chunks merged
       WHERE active.id = merged.active_id
       RETURNING active.id
     )
     SELECT archived.candidate_id, archived.active_id, archived.similarity
     FROM archived
     JOIN updated_active
       ON updated_active.id = archived.active_id`,
    [job.vault.id, job.segmentId, touchedCandidateIds, AUTO_PROMOTE_DUPLICATE_SIMILARITY]
  );

  return duplicates.rows;
}

function resolveAlias(id: string, aliasMaps: CuratorAliasMaps): string {
  return aliasMaps.aliasToId.get(id) ?? id;
}

async function insertActiveMemory(
  client: PoolClient,
  vault: VaultRow,
  input: {
    subject: string;
    fact: string;
    type: NonNullable<CuratorMemory['type']>;
    scope: CuratorMemory['scope'];
    salience: number;
    confidence: number;
    sensitivity: CuratorMemory['sensitivity'];
    polarity: CuratorMemory['polarity'];
    volatility: CuratorMemory['volatility'];
    evidence: string | null;
    parentId: string | null;
    sourceSegmentId: string;
  }
): Promise<{ id: string }> {
  const embedding = await embedder.embed(input.fact);
  const storedFact = await encryptForVault(vault, input.fact);
  const encryptedSubject = await encryptSubjectForVault(vault, input.subject);
  const result = await client.query<{ id: string }>(
     `INSERT INTO memories (
       vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding,
       salience, confidence, sensitivity, type, scope, polarity, status, parent_id, volatility, evidence, source_segment_id
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8, $9, $10, $11, $12, $13, 'active', $14, $15::memory_volatility, $16::jsonb, $17)
     RETURNING id`,
    [
      vault.id,
      storedFact,
      isVaultEncryptionActive(vault) ? '' : input.subject,
      encryptedSubject?.encrypted ?? null,
      encryptedSubject?.hmac ?? null,
      crypto.createHash('md5').update(input.fact).digest('hex'),
      JSON.stringify(embedding),
      input.salience,
      input.confidence,
      input.sensitivity,
      input.type,
      input.scope,
      input.polarity,
      input.parentId,
      input.volatility,
      input.evidence ? JSON.stringify({ summary: input.evidence }) : null,
      input.sourceSegmentId
    ]
  );
  await upsertMemoryEmbedding(client, result.rows[0].id, embedding);
  return result.rows[0];
}

async function updateMemoryNode(
  client: PoolClient,
  vault: VaultRow,
  input: {
    id: string;
    fact: string;
    subject: string;
    type: NonNullable<CuratorMemory['type']>;
    scope: CuratorMemory['scope'];
    salience: number;
    confidence: number;
    sensitivity: CuratorMemory['sensitivity'];
    polarity: CuratorMemory['polarity'];
    volatility: CuratorMemory['volatility'];
    evidence: string | null;
    parentId: string | null;
  }
) {
  const embedding = await embedder.embed(input.fact);
  const storedFact = await encryptForVault(vault, input.fact);
  const encryptedSubject = await encryptSubjectForVault(vault, input.subject);
  await client.query(
    `UPDATE memories
     SET data = $3,
         subject = $4,
         subject_encrypted = $5,
         subject_hmac = $6,
         hash = $7,
         embedding = $8::vector,
         type = $9,
         scope = $10,
         salience = $11,
         confidence = $12,
         sensitivity = $13,
         polarity = $14,
         -- Curator updates are authoritative curation decisions, including
         -- candidate edits, so updated nodes must become recallable.
         status = 'active',
         volatility = $15::memory_volatility,
         evidence = $16::jsonb,
         parent_id = $17,
         updated_at = now()
     WHERE vault_id = $1
       AND id = $2`,
    [
      vault.id,
      input.id,
      storedFact,
      isVaultEncryptionActive(vault) ? '' : input.subject,
      encryptedSubject?.encrypted ?? null,
      encryptedSubject?.hmac ?? null,
      crypto.createHash('md5').update(input.fact).digest('hex'),
      JSON.stringify(embedding),
      input.type,
      input.scope,
      input.salience,
      input.confidence,
      input.sensitivity,
      input.polarity,
      input.volatility,
      input.evidence ? JSON.stringify({ summary: input.evidence }) : null,
      input.parentId
    ]
  );
  await upsertMemoryEmbedding(client, input.id, embedding);
}

async function upsertMemoryEmbedding(client: PoolClient, memoryId: string, embedding: number[]) {
  await client.query(
    `INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
     VALUES ($1, $2::vector, now())
     ON CONFLICT (memory_id)
     DO UPDATE SET embedding = EXCLUDED.embedding, embedded_at = now()`,
    [memoryId, JSON.stringify(embedding)]
  );
}

async function insertEdge(
  client: PoolClient,
  vaultId: string,
  fromMemoryId: string,
  toMemoryId: string,
  type: EdgeType,
  confidence: number,
  reason: string | null
) {
  const result = await client.query<{ id: string }>(
    `INSERT INTO memory_edges (vault_id, from_memory_id, to_memory_id, type, confidence, reason)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (from_memory_id, to_memory_id, type)
     DO UPDATE SET confidence = EXCLUDED.confidence, reason = EXCLUDED.reason, updated_at = now()
     RETURNING id`,
    [vaultId, fromMemoryId, toMemoryId, type, confidence, reason]
  );

  if (!result.rowCount) {
    throw new Error(`Failed to persist edge ${fromMemoryId} -> ${toMemoryId} (${type})`);
  }
}

async function archiveMemory(client: PoolClient, memoryId: string, vaultId: string): Promise<void> {
  await client.query(
    `UPDATE memories
     SET archived_at = now(),
         updated_at = now()
     WHERE id = $1
       AND vault_id = $2`,
    [memoryId, vaultId]
  );
}

async function insertActionLog(
  client: PoolClient,
  input: {
    vaultId: string;
    vault: VaultEncryptionContext;
    segmentId: string;
    actionType: 'create' | 'update' | 'delete' | 'promote' | 'archive_duplicate';
    memoryId?: string;
    newMemoryId?: string;
    subject?: string;
    oldValue?: string;
    newValue?: string;
    rawResponse: unknown;
    applied: boolean;
    error?: string;
  }
) {
  const rawResponseJson = JSON.stringify(input.rawResponse);
  const [subject, oldValue, newValue, encryptedRawResponse] = isVaultEncryptionActive(input.vault)
    ? await Promise.all([
      input.subject ? encryptForVault(input.vault, input.subject) : Promise.resolve(null),
      input.oldValue ? encryptForVault(input.vault, input.oldValue) : Promise.resolve(null),
      input.newValue ? encryptForVault(input.vault, input.newValue) : Promise.resolve(null),
      encryptForVault(input.vault, rawResponseJson)
    ])
    : [input.subject ?? null, input.oldValue ?? null, input.newValue ?? null, null];
  const storedRawResponse = encryptedRawResponse
    ? JSON.stringify({ encrypted: encryptedRawResponse })
    : rawResponseJson;

  await client.query(
    `INSERT INTO curation_action_log (
       vault_id, segment_id, action_type, memory_id, new_memory_id, subject, old_value, new_value,
       raw_curator_response, applied_at, error
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)`,
    [
      input.vaultId,
      input.segmentId,
      input.actionType,
      input.memoryId ?? null,
      input.newMemoryId ?? null,
      subject,
      oldValue,
      newValue,
      storedRawResponse,
      input.applied ? new Date().toISOString() : null,
      input.error ?? null
    ]
  );
}

async function runLoop() {
  if (config.ENCRYPTION_ENABLED) {
    await initCryptoClient();
  }

  while (true) {
    try {
      await processBatch();
    } catch (error) {
      console.error('Curation loop iteration failed', error);
    }

    await new Promise((resolve) => setTimeout(resolve, config.CURATION_INTERVAL_MS));
  }
}

if (parentPort) {
  parentPort.on('message', () => {});
}

void runLoop().catch(async (error) => {
  console.error(getSpanAttributes({ error }), 'Curation worker terminated');
  await closePool();
  process.exit(1);
});
