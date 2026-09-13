import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('curation-worker');
import crypto from 'node:crypto';
import { parentPort } from 'node:worker_threads';
import { shutdownTelemetry } from '../azure-monitor';
import { createShutdownDeadline, drainRuntimeOwner, parseShutdownDeadline } from '../runtime-shutdown';
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
import { discardObsoleteCurationTargets } from '../services/curation-work';
import { lockMemoryWriteVault } from '../services/dedup';
import { getRawChunkStorage } from '../services/raw-chunk-storage';
import { validateCuratorContract, type CuratorRawSource } from '../services/curator-contract';
import { compileCuratorGraph, type CompiledCuratorGraph, type CuratorNodeRef } from '../services/curator-graph';
import { getSpanAttributes } from '../telemetry';
import { aiBudgetThrottledJobsCounter, aiBudgetWaitHistogram } from '../metrics';
import { AiBudgetDeferredError, getCurrentUsagePeriod } from '../services/usage';
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
  segment_id: string | null;
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
  source_timestamp: string | null;
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
  segmentId: string | null;
  vault: VaultRow;
  conversation: string | null;
  rawSources: CuratorRawSource[];
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
        operationalLog.warn('[persistio] Worker shutdown incomplete');
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
          operationalLog.warn(
            `Curation job exceeded retry limit and will be dead-lettered: queueId=${row.queue_id}, retryCount=${retryResult.rows[0].retry_count}, maxRetries=${MAX_CURATION_RETRIES}`
          );
          await withWorkerLeaseTransaction(lease, async (client) => {
            if (!await recordWorkerAction(client, lease, 'dead-letter')) return;
            await client.query(
              `INSERT INTO curation_dead_letter (vault_id, segment_id, retry_count, last_error,work_key,targets,source_queue_id)
               SELECT $1,$2,$3,$4,q.work_key,COALESCE((SELECT jsonb_agg(jsonb_build_object('memory_id',i.memory_id,'revision',i.revision::text)) FROM curation_queue_items i WHERE i.queue_id=q.id),'[]'::jsonb),q.id
               FROM curation_queue q WHERE q.id=$5`,
              [row.vault_id, row.segment_id, retryResult.rows[0].retry_count, retryResult.rows[0].last_error,row.queue_id]
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
        await withWorkerLeaseTransaction(lease, client => discardObsoleteCurationTargets(client,row.queue_id,row.vault_id));
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
            vaultPromptContext: await decryptVaultPromptContext(job.vault),
            rawSources: job.rawSources
          }
        );
        job.rawSources = prepared.rawSources;
        job.candidates = prepared.candidates;
        job.activeMemories = prepared.activeMemories;
        job.candidateIds = new Set(prepared.candidates.map(memory => memory.id));
        job.hasMoreCandidates ||= prepared.deferredCandidateIds.length > 0;
        job.deferredCandidates += prepared.deferredCandidateIds.length;
        remainingCandidatesByVault.set(row.vault_id, Math.max(0, remainingCandidates - job.candidates.length));
        assertNotLost();
        const attemptId = crypto.randomUUID();
        const requestActionKey = 'provider-request:'+attemptId;
        const tokenActionKey = 'provider-usage:'+attemptId;
        const countRun = !runRecordedVaults.has(job.vault.id);
        const { result: proposedResult, aliasMaps, rawResponse, usage, audit } = await curator.curatePrepared(prepared, job.vault.id,{
          beforeRequest:async () => {
            await withWorkerLeaseTransaction(lease,async client => {
              await lockMemoryWriteVault(client,job.vault.id);
              const currentLimits = await getCuratorLimits(job.vault.id,client);
              if (getCuratorPlanBlockReason(currentLimits)) throw new CuratorPreparationDeferredError('Curation entitlement changed');
              const usage = (await client.query<{curator_runs:number;curator_requests:number;tokens:number}>(`SELECT curator_runs,curator_requests,
                curator_input_tokens+curator_output_tokens AS tokens FROM vault_usage WHERE vault_id=$1 AND period=$2 FOR UPDATE`,
                [job.vault.id,getCurrentUsagePeriod()])).rows[0];
              if ((countRun && (usage?.curator_runs ?? 0)>=currentLimits.curator_runs_per_month)
                || (usage?.curator_requests ?? 0)>=currentLimits.curator_requests_per_month
                || (usage?.tokens ?? 0)>=currentLimits.curator_tokens_per_month) throw new CuratorPreparationDeferredError('Curation monthly capacity exhausted');
              if (!await recordWorkerAction(client,lease,requestActionKey)) throw new Error('Provider request already attempted');
              await recordCuratorUsageInTransaction(client,{vaultId:job.vault.id,candidatesProcessed:0,countRun,
                promptTokens:0,completionTokens:0,limits:currentLimits,requestCount:1,
                actionReceipt:{queueId:job.queueId,actionKey:requestActionKey}});
            });
            runRecordedVaults.add(job.vault.id);
          },
          returnedUsage:async usage => {
            // The provider already ran. Account its reported usage even if the
            // application lease expired; this grants no memory-write capability.
            await withTransaction(async client => {
              const attempted = await client.query(`SELECT queue_id FROM worker_action_receipts
                WHERE queue_kind='curation' AND queue_id=$1 AND action_key=$2 AND claim_token=$3 FOR UPDATE`,
                [job.queueId,requestActionKey,lease.claimToken]);
              if (attempted.rowCount!==1) throw new Error('Provider usage lacks attempt receipt');
              if (!await recordWorkerAction(client,lease,tokenActionKey)) return;
              await recordCuratorUsageInTransaction(client,{vaultId:job.vault.id,candidatesProcessed:0,countRun:false,
                promptTokens:usage.promptTokens,completionTokens:usage.completionTokens,requestCount:0,limits:job.limits,
                actionReceipt:{queueId:job.queueId,actionKey:tokenActionKey}});
            });
          }
        });
        const result=validateCuratorContract(proposedResult,job.candidates,job.activeMemories,aliasMaps,job.rawSources);
        const graph=compileCuratorGraph(result,job.candidates,job.activeMemories,aliasMaps);
        reviewRunId = await insertCurationReviewRun(job, audit, rawResponse, 'valid', []);
        const writes = await prepareCuratorWrites(result, job.vault.id, assertNotLost);
        assertNotLost();
        const preparedCrypto = await prepareVaultCrypto(job.vault);
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
          operationalLog.warn(JSON.stringify({
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
          operationalLog.info(JSON.stringify({
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
          operationalLog.warn(JSON.stringify({
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
        operationalLog.error(getSpanAttributes({ error, queueId: row.queue_id }), 'Curation job failed');
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
     FROM curation_queue_items WHERE vault_id=$1 AND queue_id=$2`,
    [row.vault_id, row.queue_id]
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
  await client.query(`DELETE FROM curation_queue_items i USING jsonb_to_recordset($3::jsonb) AS done(id uuid,revision bigint)
    WHERE i.queue_id=$1 AND i.vault_id=$2 AND i.memory_id=done.id AND i.revision=done.revision`,
    [job.queueId,job.vault.id,JSON.stringify(job.candidates.map(m => ({id:m.id,revision:m.row_version})))]);
  await discardObsoleteCurationTargets(client,job.queueId,job.vault.id);
  const remaining = await client.query<{count:string}>(
    'SELECT count(*)::text AS count FROM curation_queue_items WHERE queue_id=$1 AND vault_id=$2',[job.queueId,job.vault.id]);
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
  // Target and related-memory revisions come from one read snapshot. No provider
  // call occurs under a mutation lock; a later short apply transaction rechecks it.
  const loaded = await withTransaction(async client => {
    await client.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const vault = (await client.query<VaultRow>(`SELECT id,account_id::text,encrypted_dek,vault_encryption_enabled,plan_id,
      type,custom_extraction_prompt,custom_curation_prompt FROM vaults WHERE id=$1`,[row.vault_id])).rows[0];
    if (!vault) throw new Error('Curation vault missing');
    const targets = (await client.query<MemoryRow>(`SELECT m.*,m.revision::text AS row_version
      FROM curation_queue_items i JOIN memories m ON m.id=i.memory_id AND m.vault_id=i.vault_id AND m.revision=i.revision
      WHERE i.queue_id=$1 AND i.vault_id=$2 AND m.status='active' AND m.archived_at IS NULL
      ORDER BY m.confidence DESC,m.salience DESC,m.created_at,m.id LIMIT $3`,[row.queue_id,row.vault_id,candidateLimit+1])).rows;
    const selected = targets.slice(0,candidateLimit);
    const ids = selected.map(m => m.id);
    const related = (await client.query<MemoryRow>(`WITH targets AS (SELECT m.*,e.embedding AS vector FROM memories m
        LEFT JOIN memory_embeddings e ON e.memory_id=m.id WHERE m.vault_id=$1 AND m.id=ANY($2::uuid[])),
      matches AS (
        SELECT m.id,t.id AS target_id,CASE WHEN m.id=t.parent_id OR m.parent_id=t.id THEN 2
          WHEN (m.subject_hmac IS NOT NULL AND m.subject_hmac=t.subject_hmac)
            OR (m.subject_hmac IS NULL AND t.subject_hmac IS NULL AND m.subject=t.subject) THEN 1 ELSE 0 END AS rank
        FROM targets t CROSS JOIN LATERAL (
          SELECT a.* FROM memories a LEFT JOIN memory_embeddings e ON e.memory_id=a.id
          WHERE a.vault_id=$1 AND a.status='active' AND a.archived_at IS NULL AND NOT(a.id=ANY($2::uuid[]))
            AND a.scope=t.scope AND a.scope_key IS NOT DISTINCT FROM t.scope_key
            AND (a.id=t.parent_id OR a.parent_id=t.id
              OR (a.subject_hmac IS NOT NULL AND a.subject_hmac=t.subject_hmac)
              OR (a.subject_hmac IS NULL AND t.subject_hmac IS NULL AND a.subject=t.subject)
              OR (t.vector IS NOT NULL AND 1-(e.embedding <=> t.vector)>=$4))
          ORDER BY (a.id=t.parent_id OR a.parent_id=t.id) DESC,
            (e.embedding <=> t.vector) NULLS LAST,a.id LIMIT $3
        ) m
      ), ranked AS (SELECT id,max(rank) AS rank,array_agg(DISTINCT target_id) AS relevant_candidate_ids
        FROM matches GROUP BY id ORDER BY max(rank) DESC,id LIMIT $3)
      SELECT m.*,m.revision::text AS row_version,r.relevant_candidate_ids FROM ranked r JOIN memories m ON m.id=r.id
      ORDER BY r.rank DESC,m.id`,[row.vault_id,ids,Math.max(0,limits.curator_active_memories_per_call),RELEVANT_ACTIVE_MEMORY_MIN_SIMILARITY])).rows;
    const sourceIds = [...new Set([...selected,...related].flatMap(m => m.source_chunks ?? []))];
    const sourceStore = sourceIds.length ? getRawChunkStorage().store : null;
    const sourceRows = (await client.query<{id:string;role:string;created_at:string;provenance:unknown;session_id:string;project_id:string|null;task_id:string|null;blob_key:string;blob_store:string;storage_bytes:string}>(`
      SELECT r.id,r.role,r.created_at,r.provenance,r.session_id,r.blob_key,r.blob_store,r.storage_bytes,
        r.capture_context->>'project_id' AS project_id,r.capture_context->>'task_id' AS task_id FROM raw_chunks r
      WHERE r.vault_id=$1 AND r.id=ANY($2::uuid[]) AND r.role IN ('user','assistant')
        AND r.storage_bytes BETWEEN 0 AND 32768 AND r.blob_key IS NOT NULL AND r.blob_store=$3
      ORDER BY r.acceptance_ordinal DESC LIMIT 8`,[row.vault_id,sourceIds,sourceStore])).rows;
    return {vault,selected,related,sourceRows,hasMore:targets.length>candidateLimit};
  });
  const rawSources: CuratorRawSource[] = [];
  if (loaded.sourceRows.length) {
    const storage = getRawChunkStorage();
    let bytes = 0;
    for (const r of loaded.sourceRows) {
      if (r.blob_store !== storage.store || bytes + Number(r.storage_bytes)>32768) continue;
      const content = await decryptForVault(loaded.vault,await storage.get(r.blob_key));
      const size = Buffer.byteLength(content,'utf8');
      if (bytes+size>32768) continue;
      bytes+=size;
      rawSources.push({id:r.id,role:r.role,created_at:r.created_at,content,provenance:r.provenance,current:true,
        context:{session_id:r.session_id,...(r.project_id ? {project_id:r.project_id}:{}),...(r.task_id?{task_id:r.task_id}:{})}});
    }
  }
  const candidates = await Promise.all(loaded.selected.map(m => decryptMemory(loaded.vault,m)));
  const activeMemories = await Promise.all(loaded.related.map(m => decryptMemory(loaded.vault,m)));
  return {queueId:row.queue_id,segmentId:row.segment_id,vault:loaded.vault,conversation:null,rawSources,candidates,activeMemories,
    candidateIds:new Set(candidates.map(m=>m.id)),limits,hasMoreCandidates:loaded.hasMore,deferredCandidates:loaded.hasMore?1:0};
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
    source_timestamp: memory.source_timestamp ? new Date(memory.source_timestamp).toISOString() : null,
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
    curation_source_memory_ids: sources.map((memory) => memory.id),
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
  job: LoadedCurationJob, actions: CuratorResult, aliasMaps: CuratorAliasMaps, graph: CompiledCuratorGraph,
  rawResponse: unknown, reviewRunId: string, preparedCrypto: PreparedVaultCrypto, writes: PreparedCuratorWrites,
  lease: WorkerLease, actionKey: string, usage: {countRun:boolean;promptTokens:number;completionTokens:number}
): Promise<boolean> {
  validateCuratorContract(actions,job.candidates,job.activeMemories,aliasMaps,job.rawSources);
  const reviewed = new Map([...job.candidates,...job.activeMemories].map(m => [m.id,m]));
  const byAlias = (alias:string) => {
    const memory = reviewed.get(aliasMaps.aliasToId.get(alias) ?? '');
    if (!memory) throw new Error('Unknown reviewed memory alias');
    return memory;
  };
  const createdIds = new Map<number,string>();
  const resolveRef = (ref:CuratorNodeRef) => {
    const id = ref.kind==='existing' ? ref.id : createdIds.get(ref.index);
    if (!id) throw new Error('Replacement is unavailable');
    return id;
  };
  let delta = 0;
  const applied = await withWorkerLeaseTransaction(lease,async client => {
    await lockMemoryWriteVault(client,job.vault.id);
    await preparedCrypto.assertCurrent(client);
    const limits = await getCuratorLimits(job.vault.id,client);
    const blocked = getCuratorPlanBlockReason(limits);
    if (blocked) throw new CuratorPreparationDeferredError('Curation entitlement or capacity changed');
    if (!await recordWorkerAction(client,lease,actionKey)) return false;
    const ids = [...reviewed.keys()].sort();
    // The vault write lock serializes graph writers as well as memory writers.
    // Discover bounded incident rows before acquiring all memory locks in ID order.
    const incident = (await client.query<{id:string;from_memory_id:string;to_memory_id:string;type:EdgeType;confidence:number;reason:string|null}>(`
      SELECT id,from_memory_id,to_memory_id,type,confidence,reason FROM memory_edges
      WHERE vault_id=$1 AND (from_memory_id=ANY($2::uuid[]) OR to_memory_id=ANY($2::uuid[]))
      ORDER BY id LIMIT 501 FOR UPDATE`,[job.vault.id,ids])).rows;
    const children = (await client.query<{id:string;parent_id:string}>(`
      SELECT id,parent_id FROM memories WHERE vault_id=$1 AND parent_id=ANY($2::uuid[]) ORDER BY id LIMIT 501`,[job.vault.id,ids])).rows;
    if (incident.length>500 || children.length>500) throw new CuratorPreparationDeferredError('Curation incident graph exceeds bounded application capacity');
    const lockIds = [...new Set([...ids,...incident.flatMap(e => [e.from_memory_id,e.to_memory_id]),
      ...children.map(c => c.id),...[...reviewed.values()].flatMap(m => m.parent_id ? [m.parent_id]:[])])].sort();
    const locked = (await client.query<MemoryRow>(`SELECT m.*,m.revision::text AS row_version FROM memories m
      WHERE vault_id=$1 AND id=ANY($2::uuid[]) ORDER BY id FOR UPDATE`,[job.vault.id,lockIds])).rows;
    const current = new Map(locked.map(m => [m.id,m]));
    for (const [id,expected] of reviewed) {
      const actual = current.get(id);
      if (!actual || actual.status!=='active' || actual.archived_at!==null || actual.row_version!==expected.row_version) {
        throw new Error('Curation reviewed revision changed');
      }
    }
    const sourceIds = [...new Set([...reviewed.values()].flatMap(m => m.source_chunks ?? []))];
    if (sourceIds.length) {
      const sourceRows = await client.query('SELECT id FROM raw_chunks WHERE vault_id=$1 AND id=ANY($2::uuid[]) ORDER BY id FOR KEY SHARE',[job.vault.id,sourceIds]);
      if (sourceRows.rows.length!==sourceIds.length) throw new Error('Curation source lineage is unavailable');
    }
    const audit = await client.query(`UPDATE curation_review_runs SET before_state=$2::jsonb
      WHERE id=$1 AND vault_id=$3 AND validation_status='valid' RETURNING id`,
      [reviewRunId,serializePreparedCurationAuditPayload(job.vault,locked.filter(m=>reviewed.has(m.id)),preparedCrypto),job.vault.id]);
    if (audit.rowCount!==1) throw new Error('Curation review is not applicable');

    const replacementById = new Map<string,string>();
    const changes = new Map(actions.scope_changes.map(c => [c.id,c]));
    // Insert replacements with their complete source union before retiring anything;
    // all changes roll back together and no intermediate state is externally visible.
    for (const index of graph.creationOrder) {
      const action = actions.consolidate[index];
      const sources = action.sources.map(byAlias);
      const proposed = action.memory;
      const binding = changes.get(action.id) ?? sources[0];
      const sourceChunks = [...new Set(sources.flatMap(m => m.source_chunks ?? []))].sort();
      const inserted = await insertActiveMemory(client,job.vault,preparedCrypto,
        writes.vector('create',index,proposed.statement,proposed.subject),{
          subject:proposed.subject,fact:proposed.statement,type:proposed.type,scope:binding.scope,
          scopeKey:binding.scope_key,salience:proposed.salience,confidence:proposed.confidence,
          sensitivity:proposed.sensitivity,polarity:proposed.polarity,volatility:proposed.volatility,
          evidence:buildCuratedEvidence(sources,proposed.evidence),sourceChunks,
          parentId:graph.parents[index] ? resolveRef(graph.parents[index]!) : null,
          sourceSegmentId:job.segmentId,validFrom:proposed.valid_from,validUntil:proposed.valid_until
          ,sourceTimestamp:sources.map(m=>m.source_timestamp).filter((s):s is string=>Boolean(s)).sort().at(-1) ?? null
        });
      createdIds.set(index,inserted.id);
      sources.forEach(m => replacementById.set(m.id,inserted.id));
      await insertActionLog(client,preparedCrypto,{reviewRunId,vaultId:job.vault.id,vault:job.vault,segmentId:job.segmentId,
        actionType:'create',newMemoryId:inserted.id,subject:proposed.subject,newValue:proposed.statement,rawResponse,applied:true});
      delta+=1;
    }
    for (const [index,action] of actions.update.entries()) {
      const original = byAlias(action.id);
      const sources = action.source_refs.map(byAlias);
      const proposed = action.memory;
      const vector = writes.vector('update',index,proposed.statement,proposed.subject);
      const encryptedSubject = preparedCrypto.subject(job.vault,proposed.subject);
      await client.query(`UPDATE memories SET data=$3,subject=$4,subject_encrypted=$5,subject_hmac=$6,hash=$7,
        embedding=$8::vector,type=$9,confidence=$10,salience=$11,sensitivity=$12,polarity=$13,volatility=$14,
        evidence=$15::jsonb,source_chunks=$16::uuid[],source_timestamp=$17::timestamptz,updated_at=now()
        WHERE vault_id=$1 AND id=$2`,[job.vault.id,original.id,preparedCrypto.encrypt(job.vault,proposed.statement),
        isVaultEncryptionActive(job.vault)?'':proposed.subject,encryptedSubject?.encrypted ?? null,encryptedSubject?.hmac ?? null,
        crypto.createHash('sha256').update(proposed.statement).digest('hex'),JSON.stringify(vector),proposed.type,proposed.confidence,
        proposed.salience,proposed.sensitivity,proposed.polarity,proposed.volatility,
        JSON.stringify(buildCuratedEvidence(sources,proposed.evidence)),[...new Set(sources.flatMap(m=>m.source_chunks ?? []))].sort(),
        sources.map(m=>m.source_timestamp).filter((s):s is string=>Boolean(s)).sort().at(-1) ?? null]);
      await upsertMemoryEmbedding(client,original.id,vector);
      await insertActionLog(client,preparedCrypto,{reviewRunId,vaultId:job.vault.id,vault:job.vault,segmentId:job.segmentId,
        actionType:'update',memoryId:original.id,subject:proposed.subject,oldValue:original.data,newValue:proposed.statement,rawResponse,applied:true});
    }
    // Scope is a separate auditable action, never inferred from a merge/update.
    for (const change of actions.scope_changes) {
      const index = actions.consolidate.findIndex(c=>c.id===change.id);
      const id = index>=0 ? createdIds.get(index)! : byAlias(change.id).id;
      const previous = index>=0 ? byAlias(actions.consolidate[index].sources[0]) : byAlias(change.id);
      if (previous.scope===change.scope && previous.scope_key===change.scope_key) continue;
      await client.query('UPDATE memories SET scope=$3,scope_key=$4,updated_at=now() WHERE vault_id=$1 AND id=$2',
        [job.vault.id,id,change.scope,change.scope_key]);
      await client.query(`INSERT INTO memory_scope_change_log
        (vault_id,memory_id,old_scope,new_scope,old_scope_key,new_scope_key,actor_type,actor_id,source,reason)
        VALUES($1,$2,$3,$4,$5,$6,'worker',NULL,'curator',$7)`,
        [job.vault.id,id,previous.scope,change.scope,previous.scope_key,change.scope_key,change.reason]);
    }

    // Rewire all incident relationships, retaining their evidence and restrictive
    // confidence on duplicate edges. Internal consolidation edges become self edges
    // and have no remaining semantic relationship to represent.
    for (const edge of incident) {
      const from = replacementById.get(edge.from_memory_id) ?? edge.from_memory_id;
      const to = replacementById.get(edge.to_memory_id) ?? edge.to_memory_id;
      if (from===edge.from_memory_id && to===edge.to_memory_id) continue;
      if (from!==to) await client.query(`INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(from_memory_id,to_memory_id,type) DO UPDATE
        SET confidence=LEAST(memory_edges.confidence,EXCLUDED.confidence),
          reason=CASE WHEN memory_edges.reason IS NOT DISTINCT FROM EXCLUDED.reason THEN memory_edges.reason
            ELSE concat_ws(E'\\n',memory_edges.reason,EXCLUDED.reason) END,updated_at=now()`,
        [job.vault.id,from,to,edge.type,edge.confidence,edge.reason]);
      await client.query('DELETE FROM memory_edges WHERE vault_id=$1 AND id=$2',[job.vault.id,edge.id]);
    }
    for (const child of children) {
      if (replacementById.has(child.id)) continue;
      const parent = replacementById.get(child.parent_id);
      if (parent) await client.query('UPDATE memories SET parent_id=$3,updated_at=now() WHERE vault_id=$1 AND id=$2',[job.vault.id,child.id,parent]);
      else if (actions.archive.some(a=>byAlias(a.id).id===child.parent_id)
        && !actions.archive.some(a=>byAlias(a.id).id===child.id)) throw new Error('Archive would orphan a surviving child');
    }
    for (const [index,edge] of graph.edges.entries()) {
      const action = actions.edges[index];
      await client.query(`INSERT INTO memory_edges(vault_id,from_memory_id,to_memory_id,type,confidence,reason)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(from_memory_id,to_memory_id,type) DO UPDATE
        SET confidence=EXCLUDED.confidence,reason=EXCLUDED.reason,updated_at=now()`,
        [job.vault.id,resolveRef(edge.from),resolveRef(edge.to),action.type,action.confidence,action.reason]);
    }
    const retiredIds = [...replacementById.keys(),...actions.archive.map(a=>byAlias(a.id).id)];
    if (retiredIds.length) {
      await client.query(`UPDATE memories SET status='superseded',archived_at=now(),updated_at=now()
        WHERE vault_id=$1 AND id=ANY($2::uuid[])`,[job.vault.id,retiredIds]);
      delta-=retiredIds.length;
      for (const id of retiredIds) await insertActionLog(client,preparedCrypto,{
        reviewRunId,vaultId:job.vault.id,vault:job.vault,segmentId:job.segmentId,actionType:'delete',memoryId:id,
        newMemoryId:replacementById.get(id),oldValue:reviewed.get(id)!.data,rawResponse,applied:true});
    }
    const finalIds = [...new Set([...lockIds,...createdIds.values()])];
    const invalid = await client.query(`SELECT 1 FROM memory_edges e
      JOIN memories a ON a.id=e.from_memory_id JOIN memories b ON b.id=e.to_memory_id
      WHERE e.vault_id=$1 AND (a.id=ANY($2::uuid[]) OR b.id=ANY($2::uuid[]))
        AND a.status='active' AND b.status='active' AND a.archived_at IS NULL AND b.archived_at IS NULL
        AND (a.scope<>b.scope OR a.scope_key IS DISTINCT FROM b.scope_key OR a.id=b.id)
      UNION ALL SELECT 1 FROM memories a JOIN memories b ON b.id=a.parent_id
      WHERE a.vault_id=$1 AND (a.id=ANY($2::uuid[]) OR b.id=ANY($2::uuid[]))
        AND a.status='active' AND a.archived_at IS NULL
        AND (a.scope<>b.scope OR a.scope_key IS DISTINCT FROM b.scope_key)
      LIMIT 1`,[job.vault.id,finalIds]);
    if (invalid.rows.length) throw new Error('Curation final graph violates applicability');
    await validateFinalHierarchy(client,job.vault.id,finalIds);
    const after = await client.query('SELECT * FROM memories WHERE vault_id=$1 AND id=ANY($2::uuid[]) ORDER BY id',[job.vault.id,[...ids,...createdIds.values()]]);
    await client.query(`UPDATE curation_review_runs SET validation_status='applied',after_state=$2::jsonb,applied_at=now()
      WHERE id=$1 AND validation_status='valid'`,[reviewRunId,serializePreparedCurationAuditPayload(job.vault,after.rows,preparedCrypto)]);
    const accounted = await recordCuratorUsageInTransaction(client,{vaultId:job.vault.id,candidatesProcessed:job.candidates.length,
      countRun:false,promptTokens:0,completionTokens:0,requestCount:0,
      limits,actionReceipt:{queueId:job.queueId,actionKey}});
    if (!accounted) throw new Error('Curation receipt accounting unavailable');
    await finishCurationJobInTransaction(client,job,lease);
    return true;
  });
  if (applied) publishCommittedWorkerEffects([{kind:'memory-count',vaultId:job.vault.id,accountId:job.vault.account_id,delta,source:'curation_worker'}]);
  return applied;
}

async function validateFinalHierarchy(client:PoolClient,vaultId:string,ids:string[]):Promise<void> {
  const paths = (await client.query<{cycle:boolean;depth:number}>(`WITH RECURSIVE walk(id,path,cycle,depth) AS (
    SELECT id,ARRAY[id],false,0 FROM memories WHERE vault_id=$1 AND id=ANY($2::uuid[]) AND status='active' AND archived_at IS NULL
    UNION ALL
    SELECT parent.id,w.path||parent.id,parent.id=ANY(w.path),w.depth+1 FROM walk w
    CROSS JOIN LATERAL (
      SELECT parent_id AS id FROM memories WHERE vault_id=$1 AND id=w.id AND parent_id IS NOT NULL
      UNION SELECT to_memory_id FROM memory_edges WHERE vault_id=$1 AND from_memory_id=w.id AND type='part_of'
    ) link JOIN memories parent ON parent.id=link.id AND parent.vault_id=$1 AND parent.status='active' AND parent.archived_at IS NULL
    WHERE NOT w.cycle AND w.depth<128
  ) SELECT cycle,depth FROM walk LIMIT 1001`,[vaultId,ids])).rows;
  if (paths.length>1000 || paths.some(p=>p.depth>=128)) throw new CuratorPreparationDeferredError('Curation hierarchy exceeds bounded validation capacity');
  if (paths.some(p=>p.cycle)) throw new Error('Curation creates hierarchy cycle');
}

function buildCurationActionKey(job:LoadedCurationJob):string {
  const targets = job.candidates.map(m=>[m.id,m.row_version]).sort((a,b)=>String(a[0]).localeCompare(String(b[0])));
  return 'apply-actions:'+crypto.createHash('sha256').update(JSON.stringify(targets)).digest('hex');
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
    sourceSegmentId: string | null;
    validFrom: string | null;
    validUntil: string | null;
    sourceTimestamp: string | null;
  }
): Promise<{ id: string }> {
  const storedFact = preparedCrypto.encrypt(vault, input.fact);
  const encryptedSubject = preparedCrypto.subject(vault, input.subject);
  const result = await client.query<{ id: string }>(
     `INSERT INTO memories (
       vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding,
       salience, confidence, sensitivity, type, scope, scope_key, polarity, status, parent_id, volatility, evidence, source_chunks, source_segment_id,
       valid_from, valid_until,source_timestamp
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8, $9, $10, $11, $12, $13, $14, 'active', $15, $16::memory_volatility, $17::jsonb, $18::uuid[], $19, $20::date, $21::date,$22::timestamptz)
     RETURNING id`,
    [
      vault.id,
      storedFact,
      isVaultEncryptionActive(vault) ? '' : input.subject,
      encryptedSubject?.encrypted ?? null,
      encryptedSubject?.hmac ?? null,
      crypto.createHash('sha256').update(input.fact).digest('hex'),
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
      input.validUntil,input.sourceTimestamp
    ]
  );
  await upsertMemoryEmbedding(client, result.rows[0].id, embedding);
  return result.rows[0];
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

async function insertActionLog(
  client: PoolClient,
  preparedCrypto: PreparedVaultCrypto,
  input: {
    reviewRunId: string;
    vaultId: string;
    vault: VaultEncryptionContext;
    segmentId: string | null;
    actionType: 'create' | 'update' | 'delete';
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
       raw_curator_response, applied_at, error, review_run_id
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12)`,
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
      input.error ?? null,
      input.reviewRunId
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
        operationalLog.error('Curation loop iteration failed', error);
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
    try { operationalLog.error(getSpanAttributes({ error }), 'Curation worker terminated'); }
    finally { try { await shutdownWorker(); } finally { process.exit(1); } }
  });
}
