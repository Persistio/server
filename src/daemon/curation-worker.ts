import crypto from 'node:crypto';
import { parentPort } from 'node:worker_threads';
import { shutdownTelemetry } from '../azure-monitor';
import { createShutdownDeadline, drainRuntimeOwner, parseShutdownDeadline } from '../runtime-shutdown';
import type { PoolClient } from 'pg';

import { getConfig } from '../config';
import { closePool, query } from '../db/client';
import { CircuitBreakerOpenError } from '../services/ai-resilience';
import {
  decryptForVault,
  encryptForVault,
  encryptSubjectForVault,
  initCryptoClient,
  isVaultEncryptionActive,
  prepareVaultCrypto,
  type PreparedVaultCrypto,
  type VaultEncryptionContext
} from '../services/crypto';
import {
  CuratorPlanValidationError,
  CuratorPreparationDeferredError,
  CuratorService,
  type CuratorAliasMaps,
  type CuratorMemory,
  type CuratorResult,
  type CuratorValidationAudit,
  type EdgeType,
  type MemoryType
} from '../services/curator';
import type { CompiledCuratorGraph, CuratorNodeRef } from '../services/curator-graph';
import { getSpanAttributes } from '../telemetry';
import { aiBudgetThrottledJobsCounter, aiBudgetWaitHistogram } from '../metrics';
import { memoryPolicyEventCounter } from '../services/observability-effects';
import { AiBudgetDeferredError } from '../services/usage';
import { publishCommittedWorkerEffects } from '../services/worker-effects';
import { prepareCuratorWrites, type PreparedCuratorWrites } from '../services/curator-write-preparation';
import { initCustomerMetrics, shutdownCustomerMetrics } from '../services/customer-metrics';
import {
  claimEligibleCurationJobs,
  getCuratorPlanBlockReason,
  getCuratorLimits,
  recordCuratorDeferralInTransaction,
  recordCuratorRunCompletedActivity,
  recordCuratorUsageInTransaction,
  releaseCuratorClaim,
  type CuratorPlanLimits
} from '../services/curation-capacity';
import type { VaultPromptContext } from '../services/vault-prompts';
import { isScopeWidening, type MemoryScope } from '../services/memory-scope';
import { intersectValidityWindows, memoryValidityPredicateSql, toDateOnly } from '../services/memory-validity';
import {
  withWorkerLeaseTransaction,
  releaseWorkerLeaseInTransaction,
  recordWorkerAction,
  releaseWorkerLease,
  startWorkerLeaseHeartbeat,
  StaleWorkerLeaseError,
  type WorkerLease
} from '../services/worker-lease';

interface CurationQueueRow {
  queue_id: string;
  vault_id: string;
  segment_id: string;
  claim_token: string;
  vault_claim_token: string;
}

interface VaultRow extends VaultEncryptionContext {
  account_id: string | null;
  plan_id: string;
  type: 'general' | 'custom' | null;
  custom_extraction_prompt: string | null;
  custom_curation_prompt: string | null;
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
  scope: MemoryScope;
  scope_key: string | null;
  polarity: 'positive' | 'negative' | 'neutral';
  volatility: 'very_low' | 'low' | 'medium' | 'high';
  evidence: unknown;
  parent_id: string | null;
  source_chunks: string[];
  archived_at: string | null;
  status: string;
  valid_from: string | null;
  valid_until: string | null;
  row_version: string;
  total_candidates?: string;
  relevant_candidate_ids?: string[];
}

interface LoadedCurationJob {
  queueId: string;
  segmentId: string;
  vault: VaultRow;
  conversation: string | null;
  candidates: CuratorMemory[];
  activeMemories: CuratorMemory[];
  candidateIds: Set<string>;
  limits: CuratorPlanLimits;
  hasMoreCandidates: boolean;
  deferredCandidates: number;
}

interface WorkerShutdownRequest {
  type: 'shutdown';
  deadline?: unknown;
}

interface CuratorRunActivityTotals {
  totalCandidatesProcessed: number;
  totalCompletionTokens: number;
  totalCuratorRequests: number;
  totalCuratorRuns: number;
  totalPromptTokens: number;
}

const config = getConfig();
const curator = new CuratorService();
const workerId = crypto.randomUUID();
const MAX_CURATION_RETRIES = Number(process.env.MAX_CURATION_RETRIES ?? 5);
const RELEVANT_ACTIVE_MEMORY_MIN_SIMILARITY = 0.72;
const WORKER_LEASE_MS = 10 * 60_000;
const activeWorkerTasks = new Set<Promise<unknown>>();
let isShuttingDown = false;
let resolveWorkerSleep: (() => void) | null = null;
let shutdownPromise: Promise<void> | null = null;
let workerLoop: Promise<void> | undefined;
let workerLoopFailed = false;

function trackWorkerTask<T>(task: Promise<T>): Promise<T> {
  const tracked = task.finally(() => {
    activeWorkerTasks.delete(tracked);
  });
  activeWorkerTasks.add(tracked);
  return tracked;
}

function sleepUntilNextBatch(ms: number): Promise<void> {
  if (isShuttingDown) return Promise.resolve();

  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      resolveWorkerSleep = null;
      resolve();
    }, ms);
    resolveWorkerSleep = () => {
      clearTimeout(timeout);
      resolveWorkerSleep = null;
      resolve();
    };
  });
}

async function shutdownWorker(deadlineValue?: unknown) {
  if (!shutdownPromise) {
    shutdownPromise = (async () => {
      isShuttingDown = true;
      resolveWorkerSleep?.();
      let complete = false;
      try {
        const deadline = parseShutdownDeadline(deadlineValue) ?? createShutdownDeadline(Boolean(process.env.K_SERVICE));
        await drainRuntimeOwner({
          // Task rejection is not drain failure: the loop/handler owns business
          // errors. All known tasks must settle before resource cleanup.
          drain: [async () => { await Promise.allSettled([workerLoop, ...Array.from(activeWorkerTasks)]); }],
          publishers: [shutdownCustomerMetrics],
          telemetry: shutdownTelemetry,
          pool: closePool,
          deadline
        });
        complete = !workerLoopFailed;
      } catch {
        console.warn('[persistio] Worker shutdown incomplete');
      } finally {
        try { parentPort?.postMessage({ type: complete ? 'shutdown-complete' : 'shutdown-failed' }); }
        finally { parentPort?.close(); }
      }
    })();
  }

  await shutdownPromise;
}

export async function processBatch() {
  const claimed = await claimEligibleCurationJobs(config.CURATION_BATCH_SIZE, workerId);
  const claimedVaults = new Map(claimed.map((row) => [row.vault_id, row.vault_claim_token]));
  const remainingCandidatesByVault = new Map<string, number>();
  const runRecordedVaults = new Set<string>();
  const runActivityTotalsByVault = new Map<string, CuratorRunActivityTotals>();
  const leases = new Map(claimed.map((row) => [row.queue_id, {
    queueKind: 'curation' as const,
    queueId: row.queue_id,
    claimToken: row.claim_token,
    workerId,
    vaultId: row.vault_id,
    vaultClaimToken: row.vault_claim_token
  }]));
  const heartbeats = new Map(Array.from(leases, ([queueId, lease]) => [
    queueId,
    startWorkerLeaseHeartbeat(lease, WORKER_LEASE_MS)
  ]));

  try {
    for (const row of claimed) {
      const lease = leases.get(row.queue_id)!;
      const heartbeat = heartbeats.get(row.queue_id)!;
      const assertNotLost = () => { if (heartbeat.lost) throw new StaleWorkerLeaseError(lease); };
      let loadedJob: LoadedCurationJob | null = null;
      let reviewRunId: string | null = null;
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
          await withWorkerLeaseTransaction(lease, async (client) => {
            if (!await recordWorkerAction(client, lease, 'dead-letter')) return;
            await client.query(
              `INSERT INTO curation_dead_letter (vault_id, segment_id, retry_count, last_error)
               VALUES ($1, $2, $3, $4)`,
              [row.vault_id, row.segment_id, retryResult.rows[0].retry_count, retryResult.rows[0].last_error]
            );
            const deleted = await client.query(
              `DELETE FROM curation_queue WHERE id = $1 AND claim_token = $2`,
              [row.queue_id, row.claim_token]
            );
            if (deleted.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
          });
          continue;
        }

        const limits = await getCuratorLimits(row.vault_id);
        const planBlockReason = getCuratorPlanBlockReason(limits);
        if (planBlockReason) {
          await deferCapacityBlockedJob(row, lease, limits, planBlockReason);
          continue;
        }

        const remainingCandidates = remainingCandidatesByVault.get(row.vault_id) ?? limits.curator_candidates_per_run;
        if (remainingCandidates <= 0) {
          await deferCapacityBlockedJob(row, lease, limits, 'curator candidate batch limit reached');
          continue;
        }

        const candidateLimit = Math.min(remainingCandidates, limits.curator_candidates_per_call);
        const job = await loadJob(row, limits, candidateLimit);
        loadedJob = job;
        if (job.candidates.length === 0) {
          await completeEmptyCurationJob(lease);
          continue;
        }
        assertNotLost();
        const prepared = curator.prepare(
          job.candidates,
          job.activeMemories,
          job.conversation,
          {
            maxInputTokens: limits.curator_input_tokens_per_call > 0 ? limits.curator_input_tokens_per_call : undefined,
            maxOutputTokens: limits.curator_output_tokens_per_call > 0 ? limits.curator_output_tokens_per_call : undefined,
            vaultPromptContext: await decryptVaultPromptContext(job.vault)
          }
        );
        job.candidates = prepared.candidates;
        job.activeMemories = prepared.activeMemories;
        job.candidateIds = new Set(prepared.candidates.map(memory => memory.id));
        job.hasMoreCandidates ||= prepared.deferredCandidateIds.length > 0;
        job.deferredCandidates += prepared.deferredCandidateIds.length;
        remainingCandidatesByVault.set(row.vault_id, Math.max(0, remainingCandidates - job.candidates.length));
        assertNotLost();
        const { result, graph, aliasMaps, rawResponse, usage, audit } = await curator.curatePrepared(prepared, job.vault.id);
        reviewRunId = await insertCurationReviewRun(job, audit, rawResponse, 'valid', []);
        const writes = await prepareCuratorWrites(result, job.vault.id, assertNotLost);
        assertNotLost();
        const preparedCrypto = await prepareVaultCrypto(job.vault);
        const countRun = !runRecordedVaults.has(job.vault.id);
        const actionKey = buildCurationActionKey(job);
        const usageRecorded = await applyActions(
          job,
          result,
          aliasMaps,
          graph,
          rawResponse,
          reviewRunId,
          preparedCrypto,
          writes,
          lease,
          actionKey,
          {
            countRun,
            promptTokens: usage?.promptTokens ?? 0,
            completionTokens: usage?.completionTokens ?? 0
          }
        );
        if (countRun && usageRecorded) runRecordedVaults.add(job.vault.id);
        if (usageRecorded) {
          addCuratorRunActivityTotals(runActivityTotalsByVault, job.vault.id, {
            totalCandidatesProcessed: job.candidates.length,
            totalCompletionTokens: usage?.completionTokens ?? 0,
            totalCuratorRequests: 1,
            totalCuratorRuns: countRun ? 1 : 0,
            totalPromptTokens: usage?.promptTokens ?? 0
          });
        }
      } catch (error) {
        if (error instanceof CuratorPreparationDeferredError && loadedJob) {
          await deferCapacityBlockedJob(row, lease, loadedJob.limits, error.message);
          continue;
        }
        if (error instanceof StaleWorkerLeaseError) {
          if (reviewRunId) {
            await query(
              `UPDATE curation_review_runs
               SET validation_status = 'application_failed',
                   validation_errors = '["worker lease was lost before application"]'::jsonb
               WHERE id = $1 AND validation_status = 'valid'`,
              [reviewRunId]
            );
          }
          console.warn(JSON.stringify({
            level: 40,
            msg: 'discarding stale curation worker result',
            queue_id: row.queue_id,
            claim_token: row.claim_token
          }));
          continue;
        }
        if (loadedJob && error instanceof CuratorPlanValidationError) {
          await insertCurationReviewRun(
            loadedJob,
            error.audit,
            error.audit.rawResponse,
            'invalid',
            error.audit.validationErrors
          );
        } else if (reviewRunId) {
          await query(
            `UPDATE curation_review_runs
             SET validation_status = 'application_failed',
                 validation_errors = $2::jsonb
             WHERE id = $1 AND validation_status = 'valid'`,
            [reviewRunId, JSON.stringify([error instanceof Error ? error.message : String(error)])]
          );
        }
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
          await deferCurationJob(lease, {
              vaultId: row.vault_id,
              reason: error.message,
              availableAt: error.availableAt
            });
          continue;
        }
        if (error instanceof CircuitBreakerOpenError) {
          console.warn(JSON.stringify({
            level: 40,
            msg: 'skipping curation job while circuit breaker is open',
            queue_id: row.queue_id,
            retry_after_ms: error.retryAfterMs
          }));
          await deferCurationJob(lease, {
              vaultId: row.vault_id,
              reason: error.message
            });
          continue;
        }
        const lastError = error instanceof Error ? error.message : 'Unknown curation error';
        console.error(getSpanAttributes({ error, queueId: row.queue_id }), 'Curation job failed');
        await releaseWorkerLease(lease, { incrementRetry: true, lastError });
      } finally {
        await heartbeat.stop();
      }
    }

    for (const [vaultId, totals] of runActivityTotalsByVault) {
      try { await recordCuratorRunCompletedActivity({
        vaultId,
        ...totals
      }); } catch { /* Committed work must never be retried for activity failure. */ }
    }
  } finally {
    await Promise.all(Array.from(heartbeats.values(), heartbeat => heartbeat.stop()));
    const releases = await Promise.allSettled(Array.from(claimedVaults, ([vaultId, claimToken]) =>
      releaseCuratorClaim(vaultId, workerId, claimToken)));
    const failed = releases.filter((result): result is PromiseRejectedResult => result.status === 'rejected');
    if (failed.length) throw new AggregateError(failed.map(result => result.reason), 'Curation claim cleanup failed');
  }
}

function addCuratorRunActivityTotals(
  totalsByVault: Map<string, CuratorRunActivityTotals>,
  vaultId: string,
  delta: CuratorRunActivityTotals
): void {
  const current = totalsByVault.get(vaultId) ?? {
    totalCandidatesProcessed: 0,
    totalCompletionTokens: 0,
    totalCuratorRequests: 0,
    totalCuratorRuns: 0,
    totalPromptTokens: 0
  };

  totalsByVault.set(vaultId, {
    totalCandidatesProcessed: current.totalCandidatesProcessed + delta.totalCandidatesProcessed,
    totalCompletionTokens: current.totalCompletionTokens + delta.totalCompletionTokens,
    totalCuratorRequests: current.totalCuratorRequests + delta.totalCuratorRequests,
    totalCuratorRuns: current.totalCuratorRuns + delta.totalCuratorRuns,
    totalPromptTokens: current.totalPromptTokens + delta.totalPromptTokens
  });
}

function getNextCapacityAvailableAt(limits: CuratorPlanLimits): Date {
  const delayMinutes = Math.max(1, limits.curator_schedule_interval_minutes);
  return new Date(Date.now() + delayMinutes * 60_000);
}

async function deferCapacityBlockedJob(
  row: CurationQueueRow,
  lease: WorkerLease,
  limits: CuratorPlanLimits,
  reason: string
): Promise<void> {
  const countResult = await query<{ total_candidates: string }>(
    `SELECT COUNT(*)::text AS total_candidates
     FROM memories
     WHERE vault_id = $1
       AND source_segment_id = $2
       AND archived_at IS NULL
       AND status = 'candidate'`,
    [row.vault_id, row.segment_id]
  );
  const totalCandidates = Number(countResult.rows[0]?.total_candidates ?? 0);

  if (totalCandidates === 0) {
    await completeEmptyCurationJob(lease);
    return;
  }

  const availableAt = getNextCapacityAvailableAt(limits);
  await deferCurationJob(lease, {
      vaultId: row.vault_id,
      reason,
      availableAt
    });
}

async function deferCurationJob(lease: WorkerLease, input: Parameters<typeof recordCuratorDeferralInTransaction>[1]): Promise<void> {
  try {
    await withWorkerLeaseTransaction(lease, async client => {
      await recordCuratorDeferralInTransaction(client, input);
      await releaseWorkerLeaseInTransaction(client, lease, { availableAt: input.availableAt, lastError: input.reason });
    });
  } catch (error) {
    if (!(error instanceof StaleWorkerLeaseError)) throw error;
  }
}

async function completeEmptyCurationJob(lease: WorkerLease): Promise<void> {
  await withWorkerLeaseTransaction(lease, async (client) => {
    if (!await recordWorkerAction(client, lease, 'empty-complete')) return;
    const deleted = await client.query(
      `DELETE FROM curation_queue WHERE id = $1 AND claim_token = $2 AND claimed_by = $3`,
      [lease.queueId, lease.claimToken, lease.workerId]
    );
    if (deleted.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
  });
}

async function finishCurationJobInTransaction(client: PoolClient, job: LoadedCurationJob, lease: WorkerLease): Promise<void> {
  const remaining = await client.query<{ count: string }>(
    `SELECT COUNT(*)::text AS count FROM memories
     WHERE vault_id = $1 AND source_segment_id = $2 AND status = 'candidate' AND archived_at IS NULL`,
    [job.vault.id, job.segmentId]
  );
  const candidatesDeferred = Number(remaining.rows[0].count);
  if (candidatesDeferred > 0) {
    const availableAt = getNextCapacityAvailableAt(job.limits);
    const reason = 'curator candidate batch limit reached';
    await recordCuratorDeferralInTransaction(client, { vaultId: job.vault.id, candidatesDeferred, availableAt, reason });
    await releaseWorkerLeaseInTransaction(client, lease, { availableAt, lastError: reason });
  } else {
    const deleted = await client.query(
      `DELETE FROM curation_queue WHERE id = $1 AND claim_token = $2 AND claimed_by = $3`,
      [lease.queueId, lease.claimToken, lease.workerId]
    );
    if (deleted.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
  }
}

async function decryptVaultPromptContext(vault: VaultRow): Promise<VaultPromptContext> {
  if (vault.type !== 'custom') {
    return { type: vault.type };
  }

  return {
    type: vault.type,
    custom_extraction_prompt: vault.custom_extraction_prompt
      ? await decryptForVault(vault, vault.custom_extraction_prompt)
      : null,
    custom_curation_prompt: vault.custom_curation_prompt
      ? await decryptForVault(vault, vault.custom_curation_prompt)
      : null
  };
}

async function loadJob(row: CurationQueueRow, limits: CuratorPlanLimits, candidateLimit: number): Promise<LoadedCurationJob> {
  const vaultResult = await query<VaultRow>(
    `SELECT id, account_id::text AS account_id, encrypted_dek, vault_encryption_enabled, plan_id,
            type, custom_extraction_prompt, custom_curation_prompt
     FROM vaults
     WHERE id = $1
     LIMIT 1`,
    [row.vault_id]
  );

  if (!vaultResult.rowCount) {
    throw new Error(`Vault ${row.vault_id} not found`);
  }

  const vault = vaultResult.rows[0];
  const segmentResult = await query<{
    context: string | null;
    session_id: string;
    project_id: string | null;
    task_id: string | null;
  }>(
    `SELECT context, session_id, project_id, task_id
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
  const candidateDate = toDateOnly(new Date());
  if (!candidateDate) {
    throw new Error('Unable to derive a valid candidate curation date');
  }

  const candidateRows = await query<MemoryRow>(
    `SELECT id, vault_id, data, subject, subject_encrypted, subject_hmac, confidence, salience, sensitivity, type,
            scope, scope_key, polarity, volatility, evidence, parent_id, source_chunks, archived_at, status,
            valid_from, valid_until, xmin::text AS row_version,
            COUNT(*) OVER()::text AS total_candidates
     FROM memories
     WHERE vault_id = $1
       AND source_segment_id = $2
       AND archived_at IS NULL
       AND status = 'candidate'
       AND sensitivity <> 'restricted'
       AND confidence > 0 AND confidence <= 1
       AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
       AND ${memoryValidityPredicateSql('memories', '$7')}
       AND (
         (scope = 'global' AND scope_key IS NULL)
         OR (scope = 'project' AND scope_key = $4::text AND $4::text IS NOT NULL)
         OR (scope = 'task' AND scope_key = $5::text AND $5::text IS NOT NULL)
         OR (scope = 'session' AND scope_key = $6::text)
       )
     ORDER BY confidence DESC, salience DESC, created_at ASC, id ASC
     LIMIT $3`,
    [
      row.vault_id,
      row.segment_id,
      candidateLimit + 1,
      segmentResult.rows[0].project_id,
      segmentResult.rows[0].task_id,
      segmentResult.rows[0].session_id,
      candidateDate
    ]
  );

  const hasMoreCandidates = candidateRows.rows.length > candidateLimit;
  const candidates = await Promise.all(candidateRows.rows.slice(0, candidateLimit).map((memory) => decryptMemory(vault, memory)));
  const totalCandidates = candidateRows.rows[0]?.total_candidates ? Number(candidateRows.rows[0].total_candidates) : candidates.length;
  const candidateIds = new Set(candidates.map((memory) => memory.id));
  const activeMemories = await loadRelevantActiveMemories(
    vault,
    candidates,
    Math.max(0, limits.curator_active_memories_per_call)
  );

  return {
    queueId: row.queue_id,
    segmentId: row.segment_id,
    vault,
    conversation,
    candidates,
    activeMemories,
    candidateIds,
    limits,
    hasMoreCandidates,
    deferredCandidates: Math.max(0, totalCandidates - candidates.length)
  };
}

async function loadRelevantActiveMemories(
  vault: VaultRow,
  candidates: CuratorMemory[],
  limit: number
): Promise<CuratorMemory[]> {
  if (candidates.length === 0 || limit <= 0) {
    return [];
  }

  const subjects = Array.from(new Set(candidates.map((memory) => memory.subject)));
  const plaintextSubjects = isVaultEncryptionActive(vault) ? [] : subjects;
  const subjectHmacs = isVaultEncryptionActive(vault)
    ? (await Promise.all(subjects.map(async (subject) => (await encryptSubjectForVault(vault, subject))?.hmac ?? null)))
        .filter((value): value is string => Boolean(value))
    : [];
  const candidateIds = candidates.map((candidate) => candidate.id);
  const semanticLimitPerCandidate = Math.max(1, Math.min(5, limit));
  const curationDate = toDateOnly(new Date());
  if (!curationDate) {
    throw new Error('Unable to derive a valid curation date');
  }

  const result = await query<MemoryRow>(
    `WITH candidate_context AS (
       SELECT candidate.id,
              candidate.subject,
              candidate.subject_hmac,
              candidate.scope,
              candidate.scope_key,
              candidate_embedding.embedding
       FROM memories candidate
       LEFT JOIN memory_embeddings candidate_embedding
         ON candidate_embedding.memory_id = candidate.id
       WHERE candidate.vault_id = $1
         AND candidate.id = ANY($2::uuid[])
         AND candidate.archived_at IS NULL
         AND candidate.status = 'candidate'
     ),
     exact_matches AS (
       SELECT active.id, active.vault_id, active.data, active.subject, active.subject_encrypted,
              active.subject_hmac, active.confidence, active.salience, active.sensitivity, active.type,
              active.scope, active.scope_key, active.polarity, active.volatility, active.evidence, active.parent_id,
              active.source_chunks, active.archived_at, active.status, active.valid_from, active.valid_until,
              active.xmin::text AS row_version, active.updated_at,
              1 AS exact_subject_rank,
              NULL::double precision AS similarity,
              ARRAY(SELECT binding.id FROM candidate_context binding
                WHERE active.scope = binding.scope
                  AND active.scope_key IS NOT DISTINCT FROM binding.scope_key
                  AND ((active.subject_hmac IS NULL AND binding.subject_hmac IS NULL AND active.subject = binding.subject)
                    OR active.subject_hmac = binding.subject_hmac)) AS relevant_candidate_ids
       FROM memories active
       WHERE active.vault_id = $1
         AND active.archived_at IS NULL
         AND active.status = 'active'
         AND active.sensitivity <> 'restricted'
         AND active.confidence > 0 AND active.confidence <= 1
         AND CASE WHEN active.evidence ? 'policy_rejections' THEN
           CASE WHEN jsonb_typeof(active.evidence -> 'policy_rejections') = 'array'
             THEN jsonb_array_length(active.evidence -> 'policy_rejections') = 0
             ELSE false END
           ELSE true END
         AND (active.source_timestamp IS NULL OR active.source_timestamp <= now() + interval '5 minutes')
         AND ${memoryValidityPredicateSql('active', '$9')}
         AND EXISTS (
           SELECT 1 FROM candidate_context binding
           WHERE active.scope = binding.scope
             AND active.scope_key IS NOT DISTINCT FROM binding.scope_key
             AND ((active.subject_hmac IS NULL AND binding.subject_hmac IS NULL AND active.subject = binding.subject)
               OR active.subject_hmac = binding.subject_hmac)
         )
         AND (
           active.subject = ANY($3::text[])
           OR active.subject_hmac = ANY($4::text[])
         )
       ORDER BY active.updated_at DESC, active.created_at DESC, active.id
       LIMIT $6
     ),
     semantic_matches AS (
       SELECT nearest.id, nearest.vault_id, nearest.data, nearest.subject, nearest.subject_encrypted,
              nearest.subject_hmac, nearest.confidence, nearest.salience, nearest.sensitivity, nearest.type,
              nearest.scope, nearest.scope_key, nearest.polarity, nearest.volatility, nearest.evidence, nearest.parent_id,
              nearest.source_chunks, nearest.archived_at, nearest.status, nearest.valid_from, nearest.valid_until,
              nearest.row_version, nearest.updated_at,
              0 AS exact_subject_rank,
              1 - (nearest.embedding <=> candidate_context.embedding) AS similarity,
              ARRAY[candidate_context.id] AS relevant_candidate_ids
       FROM candidate_context
       CROSS JOIN LATERAL (
         SELECT active.id, active.vault_id, active.data, active.subject, active.subject_encrypted,
                active.subject_hmac, active.confidence, active.salience, active.sensitivity, active.type,
                active.scope, active.scope_key, active.polarity, active.volatility, active.evidence, active.parent_id,
                active.source_chunks, active.archived_at, active.status, active.valid_from, active.valid_until,
                active.xmin::text AS row_version, active.updated_at,
                active_embedding.embedding
         FROM memory_embeddings active_embedding
         JOIN memories active
           ON active.id = active_embedding.memory_id
         WHERE candidate_context.embedding IS NOT NULL
           AND active.vault_id = $1
           AND active.archived_at IS NULL
           AND active.status = 'active'
           AND active.sensitivity <> 'restricted'
           AND active.confidence > 0 AND active.confidence <= 1
           AND CASE WHEN active.evidence ? 'policy_rejections' THEN
             CASE WHEN jsonb_typeof(active.evidence -> 'policy_rejections') = 'array'
               THEN jsonb_array_length(active.evidence -> 'policy_rejections') = 0
               ELSE false END
             ELSE true END
           AND ${memoryValidityPredicateSql('active', '$9')}
           AND active.scope = candidate_context.scope
           AND active.scope_key IS NOT DISTINCT FROM candidate_context.scope_key
           AND (active.source_timestamp IS NULL OR active.source_timestamp <= now() + interval '5 minutes')
         ORDER BY active_embedding.embedding <=> candidate_context.embedding, active.id
         LIMIT $7
       ) nearest
       WHERE candidate_context.embedding IS NOT NULL
         AND 1 - (nearest.embedding <=> candidate_context.embedding) >= $5
     ),
     all_matches AS (
       SELECT * FROM exact_matches UNION ALL SELECT * FROM semantic_matches
     ),
     ranked AS (
       SELECT DISTINCT ON (matches.id)
              matches.id, matches.vault_id, matches.data, matches.subject, matches.subject_encrypted,
              matches.subject_hmac, matches.confidence, matches.salience, matches.sensitivity, matches.type,
              matches.scope, matches.scope_key, matches.polarity, matches.volatility, matches.evidence, matches.parent_id,
              matches.source_chunks, matches.archived_at, matches.status, matches.valid_from, matches.valid_until,
              matches.row_version, matches.updated_at,
              matches.exact_subject_rank, matches.similarity
       FROM all_matches matches
       ORDER BY matches.id,
                matches.exact_subject_rank DESC,
                matches.similarity DESC NULLS LAST,
                matches.updated_at DESC
     )
     SELECT id, vault_id, data, subject, subject_encrypted, subject_hmac, confidence, salience,
            sensitivity, type, scope, scope_key, polarity, volatility, evidence, parent_id, source_chunks,
            archived_at, status, valid_from, valid_until, row_version,
            ARRAY(SELECT DISTINCT candidate_id FROM all_matches matches,
              unnest(matches.relevant_candidate_ids) candidate_id WHERE matches.id = ranked.id) AS relevant_candidate_ids
     FROM ranked
     ORDER BY exact_subject_rank DESC, similarity DESC NULLS LAST, updated_at DESC, id
     LIMIT $8`,
    [
      vault.id,
      candidateIds,
      plaintextSubjects,
      subjectHmacs,
      RELEVANT_ACTIVE_MEMORY_MIN_SIMILARITY,
      limit,
      semanticLimitPerCandidate,
      limit,
      curationDate
    ]
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
    scope_key: memory.scope_key,
    salience: Number(memory.salience),
    confidence: Number(memory.confidence),
    sensitivity: memory.sensitivity,
    polarity: memory.polarity,
    volatility: memory.volatility,
    evidence: typeof memory.evidence === 'object' && memory.evidence && 'summary' in (memory.evidence as Record<string, unknown>)
      ? String((memory.evidence as { summary?: unknown }).summary ?? '')
      : null,
    evidence_record: memory.evidence,
    source_chunks: memory.source_chunks,
    row_version: memory.row_version,
    relevant_candidate_ids: memory.relevant_candidate_ids,
    parent_id: memory.parent_id,
    valid_from: memory.valid_from ?? null,
    valid_until: memory.valid_until ?? null
  };
}

function mostRestrictiveSensitivity(
  sources: CuratorMemory[],
  requested: CuratorMemory['sensitivity']
): CuratorMemory['sensitivity'] {
  const rank: Record<CuratorMemory['sensitivity'], number> = { low: 0, medium: 1, high: 2, restricted: 3 };
  return [...sources.map((memory) => memory.sensitivity), requested]
    .reduce((current, value) => rank[value] > rank[current] ? value : current, 'low');
}

function buildCuratedEvidence(sources: CuratorMemory[], summary: string | null): Record<string, unknown> {
  const policyRejections = sources.flatMap((memory) => {
    const record = memory.evidence_record;
    return record && typeof record === 'object' && !Array.isArray(record)
      && Array.isArray((record as { policy_rejections?: unknown }).policy_rejections)
      ? (record as { policy_rejections: unknown[] }).policy_rejections
      : [];
  });
  return {
    summary,
    curation_source_candidate_ids: sources.map((memory) => memory.id),
    ...(policyRejections.length > 0 ? { policy_rejections: policyRejections } : {})
  };
}

async function serializeCurationAuditPayload(vault: VaultRow, value: unknown): Promise<string> {
  const plaintext = JSON.stringify(value);
  return isVaultEncryptionActive(vault)
    ? JSON.stringify({ encrypted: await encryptForVault(vault, plaintext) })
    : plaintext;
}

function serializePreparedCurationAuditPayload(vault: VaultRow, value: unknown, preparedCrypto: PreparedVaultCrypto): string {
  const plaintext = JSON.stringify(value);
  return isVaultEncryptionActive(vault) ? JSON.stringify({ encrypted: preparedCrypto.encrypt(vault, plaintext) }) : plaintext;
}

function jobMemorySnapshot(job: LoadedCurationJob): unknown[] {
  return [...job.candidates, ...job.activeMemories].map((memory) => ({
    id: memory.id,
    data: memory.data,
    subject: memory.subject,
    type: memory.type,
    scope: memory.scope,
    scope_key: memory.scope_key,
    sensitivity: memory.sensitivity,
    evidence: memory.evidence_record,
    source_chunks: memory.source_chunks,
    valid_from: memory.valid_from,
    valid_until: memory.valid_until
  }));
}

async function insertCurationReviewRun(
  job: LoadedCurationJob,
  audit: Omit<CuratorValidationAudit, 'validationErrors' | 'rawResponse'> | CuratorValidationAudit,
  rawResponse: unknown,
  status: 'valid' | 'invalid',
  validationErrors: string[]
): Promise<string> {
  const [storedRawResponse, beforeState] = await Promise.all([
    serializeCurationAuditPayload(job.vault, rawResponse),
    serializeCurationAuditPayload(job.vault, jobMemorySnapshot(job))
  ]);
  const result = await query<{ id: string }>(
    `INSERT INTO curation_review_runs (
       vault_id, segment_id, model, schema_version, prompt_version, prompt_hash,
       validation_status, validation_errors, raw_response, before_state
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb)
     RETURNING id`,
    [
      job.vault.id,
      job.segmentId,
      audit.model,
      audit.schemaVersion,
      audit.promptVersion,
      audit.promptHash,
      status,
      JSON.stringify(validationErrors),
      storedRawResponse,
      beforeState
    ]
  );
  return result.rows[0].id;
}

async function applyActions(
  job: LoadedCurationJob,
  actions: CuratorResult,
  aliasMaps: CuratorAliasMaps,
  graph: CompiledCuratorGraph,
  rawResponse: unknown,
  reviewRunId: string,
  preparedCrypto: PreparedVaultCrypto,
  writes: PreparedCuratorWrites,
  lease: WorkerLease,
  actionKey: string,
  usage: { countRun: boolean; promptTokens: number; completionTokens: number }
): Promise<boolean> {
  const capCuratorText = (value: string | null | undefined): string | undefined => value ? value.slice(0, 500) : undefined;
  const knownById = new Map<string, CuratorMemory>();
  for (const memory of [...job.candidates, ...job.activeMemories]) {
    knownById.set(memory.id, memory);
  }
  const createdIds = new Map<number, string>();
  const resolveRef = (ref: CuratorNodeRef): string => {
    const id = ref.kind === 'existing' ? ref.id : createdIds.get(ref.index);
    if (!id || !knownById.has(id)) throw new Error('Compiled graph target is not available');
    return id;
  };

  const touchedCandidates = new Set<string>();
  let memoryCountDelta = 0;

  const applied = await withWorkerLeaseTransaction(lease, async (client) => {
    await preparedCrypto.assertCurrent(client);
    if (!await recordWorkerAction(client, lease, actionKey)) {
      await client.query(
        `UPDATE curation_review_runs
         SET validation_status = 'application_failed',
             validation_errors = '["durable action already applied by an earlier claim"]'::jsonb
         WHERE id = $1 AND validation_status = 'valid'`,
        [reviewRunId]
      );
      await finishCurationJobInTransaction(client, job, lease);
      return false;
    }
    const reviewedIds = [...job.candidates, ...job.activeMemories].map((memory) => memory.id);
    const locked = await client.query<{
      id: string;
      scope: MemoryScope;
      scope_key: string | null;
      status: string;
      archived_at: string | null;
      row_version: string;
      [key: string]: unknown;
    }>(
      `SELECT id, data, subject, subject_encrypted, type, scope, scope_key, sensitivity,
              status, authority_state, authority_required, authority_version, evidence,
              source_chunks, valid_from, valid_until, archived_at, xmin::text AS row_version
       FROM memories
       WHERE vault_id = $1
         AND id = ANY($2::uuid[])
       ORDER BY id
       FOR UPDATE`,
      [job.vault.id, reviewedIds]
    );
    if (locked.rows.length !== reviewedIds.length) {
      throw new Error('Curation context changed after review: one or more reviewed memories are missing');
    }
    for (const row of locked.rows) {
      const expected = knownById.get(row.id);
      const expectedStatus = job.candidateIds.has(row.id) ? 'candidate' : 'active';
      if (!expected || row.archived_at !== null || row.status !== expectedStatus
        || row.scope !== expected.scope || row.scope_key !== expected.scope_key
        || row.row_version !== expected.row_version) {
        throw new Error(`Curation context changed after review for memory ${row.id}`);
      }
    }
    const lockedBeforeState = serializePreparedCurationAuditPayload(job.vault, locked.rows, preparedCrypto);
    const auditLock = await client.query(
      `UPDATE curation_review_runs
       SET before_state = $2::jsonb
       WHERE id = $1 AND validation_status = 'valid'
       RETURNING id`,
      [reviewRunId, lockedBeforeState]
    );
    if (auditLock.rowCount !== 1) {
      throw new Error(`Curation review run ${reviewRunId} was not in a valid state`);
    }

    const promotionDate = toDateOnly(new Date());
    if (!promotionDate) {
      throw new Error('Unable to derive a valid candidate promotion date');
    }
    for (const action of actions.promoted_candidates) {
      const memoryId = resolveAlias(action.id, aliasMaps);
      const memory = knownById.get(memoryId);
      if (!memory || !job.candidateIds.has(memoryId) || touchedCandidates.has(memoryId)) {
        throw new Error(`Curator promotion target is not an untouched reviewed candidate: ${action.id}`);
      }
      const promoteResult = await client.query<{ id: string }>(
        `UPDATE memories
         SET status = 'active',
             evidence = CASE
               WHEN jsonb_typeof(evidence) = 'object' THEN evidence
               ELSE '{}'::jsonb
             END || jsonb_build_object('curation_promotion_evidence', $7::text),
             updated_at = now()
         WHERE id = $1
           AND vault_id = $2
           AND source_segment_id = $3
           AND archived_at IS NULL
           AND status = 'candidate'
           AND scope = $4
           AND scope_key IS NOT DISTINCT FROM $5::text
           AND sensitivity <> 'restricted'
           AND confidence > 0 AND confidence <= 1
           AND (source_timestamp IS NULL OR source_timestamp <= now() + interval '5 minutes')
           AND ${memoryValidityPredicateSql('memories', '$6')}
           AND CASE WHEN evidence ? 'policy_rejections' THEN
             CASE WHEN jsonb_typeof(evidence -> 'policy_rejections') = 'array'
               THEN jsonb_array_length(evidence -> 'policy_rejections') = 0
               ELSE false END
             ELSE true END
         RETURNING id`,
        [memoryId, job.vault.id, job.segmentId, memory.scope, memory.scope_key, promotionDate, action.evidence]
      );
      if (promoteResult.rowCount !== 1) {
        throw new Error(`Candidate ${action.id} failed promotion policy revalidation`);
      }
      touchedCandidates.add(memoryId);
      await insertActionLog(client, preparedCrypto, {
        vaultId: job.vault.id,
        vault: job.vault,
        segmentId: job.segmentId,
        actionType: 'promote',
        memoryId,
        subject: memory?.subject,
        oldValue: memory?.data,
        newValue: action.evidence,
        rawResponse,
        applied: true
      });
    }

    for (const creationIndex of graph.creationOrder) {
      const action = actions.nodes_to_create[creationIndex];
      if (action.type === 'user_rule') {
        memoryPolicyEventCounter.add(1, {
          event: 'generated_rule_proposal',
          source: 'curation_worker',
          scope: action.scope,
          outcome: 'proposed'
        });
      }
      const consumedCandidateIds = resolveCandidateAliases(
        action.consumed_candidate_ids,
        aliasMaps,
        job.candidateIds
      );
      const sourceCandidates = consumedCandidateIds.length > 0
        ? consumedCandidateIds.flatMap((id) => {
          const memory = knownById.get(id);
          return memory ? [memory] : [];
        })
        : [];
      const sourceBinding = sourceCandidates[0];
      const hasTrustedBinding = Boolean(sourceBinding)
        && sourceCandidates.every((memory) => memory.scope === action.scope && memory.scope_key === sourceBinding.scope_key)
        && (action.scope === 'global' ? sourceBinding.scope_key === null : sourceBinding.scope_key !== null);
      if (!hasTrustedBinding) {
        throw new Error(`Curator applicability change denied: requested=${action.scope}, sources=${sourceCandidates.map((memory) => `${memory.scope}:${memory.scope_key ?? '<unbound>'}`).join(',')}`);
      }
      const parent = graph.parents[creationIndex];
      const parentId = parent ? resolveRef(parent) : null;
      if (parentId) {
        const checked = await client.query(
          `SELECT id FROM memories WHERE id = $1 AND vault_id = $2 AND scope = $3
             AND scope_key IS NOT DISTINCT FROM $4::text AND status = 'active' AND archived_at IS NULL FOR UPDATE`,
          [parentId, job.vault.id, action.scope, sourceBinding!.scope_key]
        );
        if (checked.rowCount !== 1) throw new Error('Compiled parent failed write-time revalidation');
      }
      try {
        const validity = intersectValidityWindows(sourceCandidates.map((memory) => ({
          valid_from: memory.valid_from ?? null,
          valid_until: memory.valid_until ?? null
        })));
        const inserted = await insertActiveMemory(client, job.vault, preparedCrypto,
          writes.vector('create', creationIndex, action.statement, action.subject), {
          subject: action.subject,
          fact: action.statement,
          type: action.type,
          scope: action.scope,
          scopeKey: sourceBinding!.scope_key,
          salience: action.salience ?? 0.6,
          confidence: action.confidence ?? 0.95,
          sensitivity: mostRestrictiveSensitivity(sourceCandidates, action.sensitivity ?? 'low'),
          polarity: action.polarity ?? 'neutral',
          volatility: action.volatility ?? 'low',
          evidence: buildCuratedEvidence(sourceCandidates, capCuratorText(action.evidence) ?? null),
          sourceChunks: Array.from(new Set(sourceCandidates.flatMap((memory) => memory.source_chunks ?? []))),
          parentId,
          sourceSegmentId: job.segmentId,
          validFrom: validity.valid_from,
          validUntil: validity.valid_until
        });
        memoryCountDelta += 1;
        createdIds.set(creationIndex, inserted.id);
        knownById.set(inserted.id, {
          id: inserted.id,
          subject: action.subject,
          data: action.statement,
          type: action.type,
          scope: action.scope,
          scope_key: sourceBinding!.scope_key,
          salience: action.salience ?? 0.6,
          confidence: action.confidence ?? 0.95,
          sensitivity: mostRestrictiveSensitivity(sourceCandidates, action.sensitivity ?? 'low'),
          polarity: action.polarity ?? 'neutral',
          volatility: action.volatility ?? 'low',
          evidence: capCuratorText(action.evidence) ?? null,
          evidence_record: buildCuratedEvidence(sourceCandidates, capCuratorText(action.evidence) ?? null),
          source_chunks: Array.from(new Set(sourceCandidates.flatMap((memory) => memory.source_chunks ?? []))),
          parent_id: parentId,
          valid_from: validity.valid_from,
          valid_until: validity.valid_until
        });
        await insertActionLog(client, preparedCrypto, {
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
        memoryCountDelta -= await archiveConsumedCandidates(client, preparedCrypto, {
          job,
          knownById,
          aliasMaps,
          touchedCandidates,
          consumedAliases: action.consumed_candidate_ids,
          targetMemoryId: inserted.id,
          rawResponse,
          reason: `absorbed into curator-created memory ${inserted.id}`
        });
      } catch (error) {
        await insertActionLog(client, preparedCrypto, {
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
        throw error;
      }
    }

    for (const [updateIndex, action] of actions.nodes_to_update.entries()) {
      const resolvedId = resolveAlias(action.id, aliasMaps);
      const existing = knownById.get(resolvedId);
      if (!existing) {
        throw new Error(`Curator referenced memory outside curation context: ${action.id}`);
      }

      if ((action.type ?? existing.type) === 'user_rule') {
        memoryPolicyEventCounter.add(1, {
          event: 'generated_rule_proposal',
          source: 'curation_worker',
          scope: action.scope ?? existing.scope,
          outcome: 'proposed_update'
        });
      }

      if (action.scope && action.scope !== existing.scope) {
        memoryPolicyEventCounter.add(1, {
          event: isScopeWidening(existing.scope, action.scope) ? 'scope_widening_attempt' : 'scope_change_attempt',
          source: 'curation_worker',
          outcome: 'rejected'
        });
        throw new Error(`Curator scope change denied: existing=${existing.scope}, requested=${action.scope}`);
      }

      try {
        const consumedCandidateIds = resolveCandidateAliases(
          action.consumed_candidate_ids,
          aliasMaps,
          job.candidateIds
        );
        const sourceCandidates = consumedCandidateIds.map((id) => knownById.get(id)!);
        const validity = intersectValidityWindows([existing, ...sourceCandidates].map((memory) => ({
          valid_from: memory.valid_from ?? null,
          valid_until: memory.valid_until ?? null
        })));
        const updated = await updateMemoryNode(client, job.vault, preparedCrypto,
          writes.vector('update', updateIndex, action.statement, action.subject ?? null), {
          id: resolvedId,
          fact: action.statement,
          subject: action.subject ?? null,
          type: action.type ?? null,
          scope: action.scope ?? null,
          salience: action.salience ?? null,
          confidence: action.confidence ?? null,
          volatility: action.volatility ?? null,
          evidence: buildCuratedEvidence(sourceCandidates, capCuratorText(action.reason) ?? null),
          sensitivity: mostRestrictiveSensitivity(sourceCandidates, existing.sensitivity),
          sourceChunks: Array.from(new Set([
            ...(existing.source_chunks ?? []),
            ...sourceCandidates.flatMap((memory) => memory.source_chunks ?? [])
          ])),
          validFrom: validity.valid_from,
          validUntil: validity.valid_until,
          expectedScope: existing.scope,
          expectedScopeKey: existing.scope_key
        });
        knownById.set(resolvedId, {
          ...existing,
          data: action.statement,
          subject: updated.subject,
          type: updated.type,
          scope: updated.scope,
          salience: updated.salience,
          confidence: updated.confidence,
          sensitivity: updated.sensitivity,
          polarity: updated.polarity,
          volatility: updated.volatility,
          evidence: updated.evidence,
          evidence_record: updated.evidence_record,
          source_chunks: updated.source_chunks,
          parent_id: updated.parent_id
        });
        if (job.candidateIds.has(resolvedId)) {
          touchedCandidates.add(resolvedId);
        }
        await insertActionLog(client, preparedCrypto, {
          vaultId: job.vault.id,
          vault: job.vault,
          segmentId: job.segmentId,
          actionType: 'update',
          memoryId: resolvedId,
          newMemoryId: resolvedId,
          subject: updated.subject,
          oldValue: updated.previousFact,
          newValue: action.statement,
          rawResponse,
          applied: true
        });
        memoryCountDelta -= await archiveConsumedCandidates(client, preparedCrypto, {
          job,
          knownById,
          aliasMaps,
          touchedCandidates,
          consumedAliases: action.consumed_candidate_ids,
          targetMemoryId: resolvedId,
          rawResponse,
          reason: `absorbed into curator-updated memory ${resolvedId}`
        });
      } catch (error) {
        await insertActionLog(client, preparedCrypto, {
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
        throw error;
      }
    }

    for (const [edgeIndex, action] of actions.edges_to_create.entries()) {
      try {
        const fromId = resolveRef(graph.edges[edgeIndex].from);
        const toId = resolveRef(graph.edges[edgeIndex].to);
        const fromMemory = knownById.get(fromId);
        const toMemory = knownById.get(toId);
        if (!fromMemory || !toMemory) {
          throw new Error(`Curator edge endpoint left the reviewed context: ${action.from_subject} -> ${action.to_subject}`);
        }
        const cappedReason = capCuratorText(action.reason);
        await insertEdge(
          client,
          job.vault.id,
          fromId,
          toId,
          action.type,
          action.confidence ?? 0.8,
          cappedReason ?? null,
          fromMemory,
          toMemory
        );
        await insertActionLog(client, preparedCrypto, {
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
        await insertActionLog(client, preparedCrypto, {
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
        throw error;
      }
    }

    for (const action of actions.nodes_to_archive) {
      const resolvedId = resolveAlias(action.id, aliasMaps);
      const existing = knownById.get(resolvedId);
      if (!existing) {
        throw new Error(`Curator referenced memory outside curation context: ${action.id}`);
      }

      try {
        const archived = await archiveMemory(client, resolvedId, job.vault.id, existing.scope, existing.scope_key, 'active');
        if (!archived) throw new Error(`Curator archive target ${action.id} changed after review`);
        memoryCountDelta -= 1;
        if (job.candidateIds.has(resolvedId)) {
          touchedCandidates.add(resolvedId);
        }
        await insertActionLog(client, preparedCrypto, {
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
        await insertActionLog(client, preparedCrypto, {
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
        throw error;
      }
    }

    for (const action of actions.discarded_candidates) {
      const resolvedId = resolveAlias(action.id, aliasMaps);
      const existing = knownById.get(resolvedId);
      if (!existing) {
        throw new Error(`Curator referenced memory outside curation context: ${action.id}`);
      }

      try {
        const archived = await archiveMemory(client, resolvedId, job.vault.id, existing.scope, existing.scope_key, 'candidate');
        if (!archived) throw new Error(`Curator discard target ${action.id} changed after review`);
        memoryCountDelta -= 1;
        touchedCandidates.add(resolvedId);
        await insertActionLog(client, preparedCrypto, {
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
        await insertActionLog(client, preparedCrypto, {
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
        throw error;
      }
    }

    const afterResult = await client.query(
      `SELECT id, data, subject, subject_encrypted, type, scope, scope_key, sensitivity,
              status, authority_state, authority_required, authority_version, evidence,
              source_chunks, valid_from, valid_until, archived_at
       FROM memories
       WHERE vault_id = $1
         AND (source_segment_id = $2 OR id = ANY($3::uuid[]))
       ORDER BY id`,
      [job.vault.id, job.segmentId, job.activeMemories.map((memory) => memory.id)]
    );
    const afterState = serializePreparedCurationAuditPayload(job.vault, afterResult.rows, preparedCrypto);
    const auditResult = await client.query(
      `UPDATE curation_review_runs
       SET validation_status = 'applied',
           after_state = $2::jsonb,
           applied_at = now()
       WHERE id = $1
         AND validation_status = 'valid'
       RETURNING id`,
      [reviewRunId, afterState]
    );
    if (auditResult.rowCount !== 1) {
      throw new Error(`Curation review run ${reviewRunId} was not in an applicable state`);
    }
    const accounted = await recordCuratorUsageInTransaction(client, {
      vaultId: job.vault.id,
      candidatesProcessed: job.candidates.length,
      countRun: usage.countRun,
      promptTokens: usage.promptTokens,
      completionTokens: usage.completionTokens,
      limits: job.limits,
      actionReceipt: { queueId: job.queueId, actionKey }
    });
    if (!accounted) throw new Error(`Curation action receipt ${actionKey} was not available for accounting`);
    await finishCurationJobInTransaction(client, job, lease);
    return true;
  });

  if (applied) publishCommittedWorkerEffects([{
    kind: 'memory-count', vaultId: job.vault.id, accountId: job.vault.account_id,
    delta: memoryCountDelta, source: 'curation_worker'
  }]);
  return applied;
}

function buildCurationActionKey(job: LoadedCurationJob): string {
  const reviewedCandidates = job.candidates
    .map((memory) => memory.id)
    .sort();
  return `apply-actions:${crypto.createHash('sha256').update(JSON.stringify({
    reviewedCandidates
  })).digest('hex')}`;
}

async function archiveConsumedCandidates(
  client: PoolClient,
  preparedCrypto: PreparedVaultCrypto,
  input: {
    job: LoadedCurationJob;
    knownById: Map<string, CuratorMemory>;
    aliasMaps: CuratorAliasMaps;
    touchedCandidates: Set<string>;
    consumedAliases: string[] | undefined;
    targetMemoryId: string;
    rawResponse: unknown;
    reason: string;
  }
): Promise<number> {
  const consumedCandidateIds = resolveCandidateAliases(input.consumedAliases, input.aliasMaps, input.job.candidateIds);
  const targetMemory = input.knownById.get(input.targetMemoryId);
  const hasSameBinding = (candidateId: string) => {
    const candidate = input.knownById.get(candidateId);
    return Boolean(targetMemory && candidate
      && candidate.scope === targetMemory.scope
      && candidate.scope_key === targetMemory.scope_key);
  };
  const candidatesToArchive = consumedCandidateIds.filter((candidateId) =>
    candidateId !== input.targetMemoryId
      && !input.touchedCandidates.has(candidateId)
      && hasSameBinding(candidateId)
  );
  let archivedCount = 0;

  for (const candidateId of consumedCandidateIds) {
    if (candidateId === input.targetMemoryId) {
      input.touchedCandidates.add(candidateId);
      continue;
    }
    if (input.touchedCandidates.has(candidateId)) {
      continue;
    }
    if (!hasSameBinding(candidateId)) {
      continue;
    }

    const memory = input.knownById.get(candidateId);
    const archived = await archiveMemory(
      client,
      candidateId,
      input.job.vault.id,
      memory!.scope,
      memory!.scope_key,
      'candidate'
    );
    if (!archived) throw new Error(`Consumed candidate ${candidateId} changed after review`);
    archivedCount += 1;
    input.touchedCandidates.add(candidateId);
    await insertActionLog(client, preparedCrypto, {
      vaultId: input.job.vault.id,
      vault: input.job.vault,
      segmentId: input.job.segmentId,
      actionType: 'archive_duplicate',
      memoryId: candidateId,
      newMemoryId: input.targetMemoryId,
      subject: memory?.subject,
      oldValue: memory?.data,
      newValue: input.reason,
      rawResponse: input.rawResponse,
      applied: true
    });
  }

  return archivedCount;
}

function resolveCandidateAliases(
  aliases: string[] | undefined,
  aliasMaps: CuratorAliasMaps,
  candidateIds: Set<string>
): string[] {
  if (!aliases?.length) {
    return [];
  }

  const resolved = new Set<string>();
  for (const alias of aliases) {
    const id = resolveAlias(alias, aliasMaps);
    if (candidateIds.has(id)) {
      resolved.add(id);
    }
  }
  return Array.from(resolved);
}

function resolveAlias(id: string, aliasMaps: CuratorAliasMaps): string {
  const resolved = aliasMaps.aliasToId.get(id);
  if (!resolved) throw new Error('Unknown curator alias');
  return resolved;
}

async function insertActiveMemory(
  client: PoolClient,
  vault: VaultRow,
  preparedCrypto: PreparedVaultCrypto,
  embedding: number[],
  input: {
    subject: string;
    fact: string;
    type: NonNullable<CuratorMemory['type']>;
    scope: CuratorMemory['scope'];
    scopeKey: string | null;
    salience: number;
    confidence: number;
    sensitivity: CuratorMemory['sensitivity'];
    polarity: CuratorMemory['polarity'];
    volatility: CuratorMemory['volatility'];
    evidence: Record<string, unknown>;
    sourceChunks: string[];
    parentId: string | null;
    sourceSegmentId: string;
    validFrom: string | null;
    validUntil: string | null;
  }
): Promise<{ id: string }> {
  const storedFact = preparedCrypto.encrypt(vault, input.fact);
  const encryptedSubject = preparedCrypto.subject(vault, input.subject);
  const result = await client.query<{ id: string }>(
     `INSERT INTO memories (
       vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding,
       salience, confidence, sensitivity, type, scope, scope_key, polarity, status, parent_id, volatility, evidence, source_chunks, source_segment_id,
       valid_from, valid_until
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8, $9, $10, $11, $12, $13, $14, 'active', $15, $16::memory_volatility, $17::jsonb, $18::uuid[], $19, $20::date, $21::date)
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
      input.scopeKey,
      input.polarity,
      input.parentId,
      input.volatility,
      JSON.stringify(input.evidence),
      input.sourceChunks,
      input.sourceSegmentId,
      input.validFrom,
      input.validUntil
    ]
  );
  await upsertMemoryEmbedding(client, result.rows[0].id, embedding);
  return result.rows[0];
}

async function updateMemoryNode(
  client: PoolClient,
  vault: VaultRow,
  preparedCrypto: PreparedVaultCrypto,
  embedding: number[],
  input: {
    id: string;
    fact: string;
    subject: string | null;
    type: CuratorMemory['type'];
    scope: CuratorMemory['scope'] | null;
    salience: number | null;
    confidence: number | null;
    volatility: CuratorMemory['volatility'] | null;
    evidence: Record<string, unknown>;
    sensitivity: CuratorMemory['sensitivity'];
    sourceChunks: string[];
    validFrom: string | null;
    validUntil: string | null;
    expectedScope: CuratorMemory['scope'];
    expectedScopeKey: string | null;
  }
) {
  const storedFact = preparedCrypto.encrypt(vault, input.fact);
  const encryptedSubject = input.subject === null
    ? null
    : preparedCrypto.subject(vault, input.subject);
  const result = await client.query<{
    id: string;
    confidence: number;
    evidence: string | null;
    evidence_record: unknown;
    parent_id: string | null;
    polarity: CuratorMemory['polarity'];
    salience: number;
    scope: MemoryScope;
    sensitivity: CuratorMemory['sensitivity'];
    source_chunks: string[];
    subject: string;
    subject_encrypted: string | null;
    type: CuratorMemory['type'];
    valid_from: string | null;
    valid_until: string | null;
    volatility: CuratorMemory['volatility'];
    previous_data: string;
  }>(
    `WITH target AS (
       SELECT id,
              data AS previous_data,
              scope AS previous_scope,
              authority_state AS previous_authority_state,
              authority_version AS previous_authority_version
       FROM memories
       WHERE vault_id = $1
         AND id = $2
         AND scope = $15
         AND scope_key IS NOT DISTINCT FROM $16::text
         AND ($10::text IS NULL OR $10::text = memories.scope)
         AND status = 'active'
         AND archived_at IS NULL
       FOR UPDATE
     ), updated AS (
       UPDATE memories
       SET data = $3,
         subject = COALESCE($4::text, memories.subject),
         subject_encrypted = CASE WHEN $4::text IS NULL THEN memories.subject_encrypted ELSE $5 END,
         subject_hmac = CASE WHEN $4::text IS NULL THEN memories.subject_hmac ELSE $6 END,
         hash = $7,
         embedding = $8::vector,
         type = COALESCE($9::text, memories.type),
         scope = target.previous_scope,
         salience = COALESCE($11::numeric, memories.salience),
         confidence = COALESCE($12::double precision, memories.confidence),
         authority_state = CASE
           WHEN authority_required
             OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
           THEN 'proposed'
           ELSE authority_state
         END,
         approved_by = CASE
           WHEN authority_required
             OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
           THEN NULL
           ELSE approved_by
         END,
         approved_at = CASE
           WHEN authority_required
             OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
           THEN NULL
           ELSE approved_at
         END,
         approval_source = CASE
           WHEN authority_required
             OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
           THEN NULL
           ELSE approval_source
         END,
         revoked_by = CASE
           WHEN authority_required
             OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
           THEN NULL
           ELSE revoked_by
         END,
         revoked_at = CASE
           WHEN authority_required
             OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
           THEN NULL
           ELSE revoked_at
         END,
         authority_version = CASE
           WHEN authority_required
             OR $9 IN ('user_preference', 'user_rule', 'task_pattern', 'workflow', 'constraint')
           THEN authority_version + 1
           ELSE authority_version
         END,
         volatility = COALESCE($13::memory_volatility, memories.volatility),
         evidence = CASE
           WHEN jsonb_typeof(memories.evidence) = 'object' THEN memories.evidence
           ELSE '{}'::jsonb
         END || $14::jsonb,
         sensitivity = $17,
         source_chunks = $18::uuid[],
         valid_from = $19::date,
         valid_until = $20::date,
         updated_at = now()
       FROM target
       WHERE memories.id = target.id
       RETURNING memories.id, memories.subject, memories.subject_encrypted,
                 memories.type, memories.scope, memories.salience,
                 memories.confidence, memories.sensitivity, memories.polarity,
                 memories.volatility, memories.evidence, memories.source_chunks,
                 memories.valid_from, memories.valid_until, memories.parent_id,
                 memories.authority_state, memories.authority_version,
                 target.previous_data,
                 target.previous_scope,
                 target.previous_authority_state, target.previous_authority_version
     ), scope_audit AS (
       INSERT INTO memory_scope_change_log (
         vault_id, memory_id, old_scope, new_scope, actor_type, actor_id, source, reason
       )
       SELECT $1, updated.id, updated.previous_scope, updated.scope,
              'worker', NULL, 'curation_worker',
              'Curator retained the least-privileged scope under row lock.'
       FROM updated
       WHERE updated.scope <> updated.previous_scope
       RETURNING id
     ), authority_audit AS (
       INSERT INTO memory_authority_events (
         vault_id, memory_id, event_type, old_state, new_state, old_version, new_version,
         actor_type, source, reason
       )
       SELECT $1, updated.id, 'invalidate', updated.previous_authority_state, updated.authority_state,
              updated.previous_authority_version, updated.authority_version,
              'worker', 'curation_worker', 'Curator rewrote prompt-bearing memory content; approval requires review.'
       FROM updated
       WHERE updated.authority_version <> updated.previous_authority_version
       RETURNING id
     )
     SELECT id, subject, subject_encrypted, type, scope,
            salience::double precision AS salience, confidence, sensitivity,
            polarity, volatility, evidence #>> '{summary}' AS evidence, evidence AS evidence_record,
            source_chunks, valid_from::text, valid_until::text, parent_id,
            previous_data
     FROM updated`,
    [
      vault.id,
      input.id,
      storedFact,
      input.subject === null
        ? null
        : isVaultEncryptionActive(vault) ? '' : input.subject,
      encryptedSubject?.encrypted ?? null,
      encryptedSubject?.hmac ?? null,
      crypto.createHash('md5').update(input.fact).digest('hex'),
      JSON.stringify(embedding),
      input.type,
      input.scope,
      input.salience,
      input.confidence,
      input.volatility,
      JSON.stringify(input.evidence),
      input.expectedScope,
      input.expectedScopeKey,
      input.sensitivity,
      input.sourceChunks,
      input.validFrom,
      input.validUntil
    ]
  );
  const updated = result.rows[0];
  if (!updated) {
    throw new Error(`Curator update target ${input.id} no longer has its reviewed scope binding in vault ${vault.id}`);
  }
  await upsertMemoryEmbedding(client, input.id, embedding);
  return {
    ...updated,
    subject: updated.subject_encrypted
      ? preparedCrypto.decrypt(vault, updated.subject_encrypted)
      : updated.subject,
    previousFact: preparedCrypto.decrypt(vault, updated.previous_data)
  };
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
  reason: string | null,
  fromMemory: Pick<CuratorMemory, 'scope' | 'scope_key'>,
  toMemory: Pick<CuratorMemory, 'scope' | 'scope_key'>
) {
  const result = await client.query<{ id: string }>(
    `INSERT INTO memory_edges (vault_id, from_memory_id, to_memory_id, type, confidence, reason)
     SELECT $1, source.id, destination.id, $4, $5, $6
     FROM memories source
     JOIN memories destination ON destination.vault_id = source.vault_id
     WHERE source.vault_id = $1
       AND source.id = $2
       AND destination.id = $3
       AND source.scope = $7
       AND source.scope_key IS NOT DISTINCT FROM $8::text
       AND destination.scope = $9
       AND destination.scope_key IS NOT DISTINCT FROM $10::text
       AND source.scope = destination.scope
       AND source.scope_key IS NOT DISTINCT FROM destination.scope_key
       AND source.archived_at IS NULL
       AND destination.archived_at IS NULL
       AND source.status = 'active'
       AND destination.status = 'active'
     ON CONFLICT (from_memory_id, to_memory_id, type)
     DO UPDATE SET confidence = EXCLUDED.confidence, reason = EXCLUDED.reason, updated_at = now()
     RETURNING id`,
    [
      vaultId,
      fromMemoryId,
      toMemoryId,
      type,
      confidence,
      reason,
      fromMemory.scope,
      fromMemory.scope_key,
      toMemory.scope,
      toMemory.scope_key
    ]
  );

  if (!result.rowCount) {
    throw new Error(`Failed to persist edge ${fromMemoryId} -> ${toMemoryId} (${type})`);
  }
}

async function archiveMemory(
  client: PoolClient,
  memoryId: string,
  vaultId: string,
  expectedScope: CuratorMemory['scope'],
  expectedScopeKey: string | null,
  expectedStatus: 'active' | 'candidate'
): Promise<boolean> {
  const result = await client.query<{ id: string }>(
    `UPDATE memories
     SET archived_at = now(),
         status = 'superseded',
         updated_at = now()
     WHERE id = $1
       AND vault_id = $2
       AND scope = $3
       AND scope_key IS NOT DISTINCT FROM $4::text
       AND status = $5
       AND archived_at IS NULL
     RETURNING id`,
    [memoryId, vaultId, expectedScope, expectedScopeKey, expectedStatus]
  );
  return (result.rowCount ?? 0) > 0;
}

async function insertActionLog(
  client: PoolClient,
  preparedCrypto: PreparedVaultCrypto,
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
      input.subject ? preparedCrypto.encrypt(input.vault, input.subject) : null,
      input.oldValue ? preparedCrypto.encrypt(input.vault, input.oldValue) : null,
      input.newValue ? preparedCrypto.encrypt(input.vault, input.newValue) : null,
      preparedCrypto.encrypt(input.vault, rawResponseJson)
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
  await initCustomerMetrics(config);

  while (!isShuttingDown) {
    try {
      await trackWorkerTask(processBatch());
    } catch (error) {
      if (!isShuttingDown) {
        console.error('Curation loop iteration failed', error);
      }
    }

    await sleepUntilNextBatch(config.CURATION_INTERVAL_MS);
  }
}

if (parentPort) {
  parentPort.on('message', (message: WorkerShutdownRequest) => {
    if (message.type === 'shutdown') {
      void shutdownWorker(message.deadline).catch(() => process.exit(1));
    }
  });
}

if (parentPort) {
  workerLoop = runLoop();
  // The failure handler is outside the promise shutdown waits for.
  void workerLoop.catch(async (error) => {
    workerLoopFailed = true;
    try { console.error(getSpanAttributes({ error }), 'Curation worker terminated'); }
    finally { try { await shutdownWorker(); } finally { process.exit(1); } }
  });
}
