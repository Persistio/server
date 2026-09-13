import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('extraction-worker');
import crypto from 'node:crypto';
import { parentPort } from 'node:worker_threads';
import { shutdownTelemetry } from '../azure-monitor';
import { createShutdownDeadline, drainRuntimeOwner, parseShutdownDeadline } from '../runtime-shutdown';
import pLimit from 'p-limit';

import { getConfig } from '../config';
import { closePool, query } from '../db/client';
import { aiBudgetThrottledJobsCounter, aiBudgetWaitHistogram, extractionCandidatesCounter, extractionJobsCounter, extractionLagHistogram } from '../metrics';
import { CircuitBreakerOpenError, isRateLimitError } from '../services/ai-resilience';
import { drainDueContradictionActivations } from '../services/contradiction-activation';
import { prepareVaultCrypto, initCryptoClient, type PreparedVaultCrypto } from '../services/crypto';
import { deduplicateMemoryInTransaction, getDedupEscalationRequest, type DedupInput } from '../services/dedup';
import { publishCommittedWorkerEffects, type WorkerEffect } from '../services/worker-effects';
import { filterMemoryCandidates } from '../services/deterministic-filter';
import { getEmbedder } from '../services/embedder';
import { admitExtractionCandidates, formatExtractionSources, resolveExtractionSources, sourceHasHumanIntent, type ExtractionSource } from '../services/extraction-contract';
import { freezeExtractionContext, MAX_EXTRACTION_CONTEXT_BYTES } from '../services/extraction-context';
import { enqueueCurationWork } from '../services/curation-work';
import { buildPromptHeader } from '../services/extraction-prompt-header';
import { EXTRACTION_QUEUE_READY_PREDICATE } from '../services/extraction-queue-eligibility';
import { ExtractorService } from '../services/extractor';
import { getRawChunkStorage } from '../services/raw-chunk-storage';
import type { ConflictResolution } from '../services/extractor';
import { completePersistentJobIfReady, failPersistentJob, markPersistentJobRunning } from '../services/job-status';
import { archiveStaleMemories } from '../services/staleness';
import { AiBudgetDeferredError } from '../services/usage';
import { initCustomerMetrics, shutdownCustomerMetrics } from '../services/customer-metrics';
import {
  getVaultSubjectList,
  normaliseSubject,
  resolveSubjectTier1,
  resolveSubjectTier2,
  storeCanonicalEmbedding,
  storeSubjectAlias,
  type VaultSubject
} from '../services/entity-resolver';
import { getSpanAttributes, withSpan } from '../telemetry';
import { matchSecretPattern } from '../utils/secret-filter';
import type { VaultPromptContext } from '../services/vault-prompts';
import {
  FUTURE_SOURCE_TIMESTAMP_POLICY_CODE,
  isFutureSourceTimestamp,
  MISSING_SCOPE_BINDING_POLICY_CODE,
  scopeKeyForContext,
  recallContextSchema,
  type RecallContext
} from '../services/memory-applicability';
import {
  withWorkerLeaseTransaction,
  recordWorkerAction,
  releaseWorkerLease,
  startWorkerLeaseHeartbeat,
  StaleWorkerLeaseError,
  type WorkerLease
} from '../services/worker-lease';

interface QueuedWorkRow {
  queue_id: string;
  chunk_id: string | null;
  segment_id: string | null;
  vault_id: string;
  retry_count: number;
  job_id: string | null;
  claim_token: string;
}

interface VaultContextRow {
  id: string;
  plan_id: string;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
  purpose: string | null;
  type: 'general' | 'custom' | null;
  custom_extraction_prompt: string | null;
  custom_curation_prompt: string | null;
}

interface RawChunkRow {
  id: string;
  vault_id: string;
  session_id: string;
  role: string;
  blob_store: string | null;
  blob_key: string | null;
  created_at: string;
  provenance: unknown;
  capture_context?: unknown;
}

interface SegmentRow {
  id: string;
  vault_id: string;
  session_id: string;
  chunk_ids: string[];
  created_at: string;
  project_id: string | null;
  task_id: string | null;
  agent_id: string | null;
  trigger_type: RecallContext['trigger_type'] | null;
}

interface LoadedJob {
  queueId: string;
  jobId: string | null;
  segmentId: string | null;
  vault: VaultContextRow;
  sessionId: string;
  chunkIds: string[];
  chunks: RawChunkRow[];
  createdAt: string;
  context: RecallContext;
}

interface WorkerRunOnceRequest {
  type: 'run-once';
  jobId?: string;
  vaultId?: string;
}

interface WorkerShutdownRequest {
  type: 'shutdown';
  deadline?: unknown;
}

type WorkerRequest = WorkerRunOnceRequest | WorkerShutdownRequest;

const config = getConfig();
const embedder = getEmbedder();
const extractor = new ExtractorService();
const rawChunkStorage = getRawChunkStorage();
const workerId = crypto.randomUUID();
const subjectArbitrationLimit = pLimit(5);
const MAX_EXTRACTION_RATE_LIMIT_RETRIES = 5;
const EXTRACTION_RATE_LIMIT_BASE_DELAY_MS = 1_000;
const EXTRACTION_RATE_LIMIT_MAX_DELAY_MS = 32_000;
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

export async function processBatch(vaultId?: string) {
  return withSpan('extraction.process_batch', {
    'vault.id': vaultId,
    'extraction.batch_limit': config.EXTRACTION_BATCH_SIZE
  }, async (span) => {
    const values: unknown[] = [];
    const vaultClause = vaultId ? 'AND eq.vault_id = $1' : '';

    if (vaultId) {
      values.push(vaultId);
    }

    values.push(config.EXTRACTION_BATCH_SIZE);
    values.push(workerId);

    const claimedResult = await query<QueuedWorkRow>(
      `WITH claimed AS (
         SELECT eq.id AS queue_id, eq.chunk_id, eq.segment_id, eq.vault_id, eq.retry_count, eq.job_id
         FROM extraction_queue eq
         WHERE ${EXTRACTION_QUEUE_READY_PREDICATE}
           ${vaultClause}
         ORDER BY eq.priority DESC, eq.enqueued_at ASC
         LIMIT $${vaultId ? 2 : 1}
         FOR UPDATE SKIP LOCKED
       )
       UPDATE extraction_queue eq
       SET claimed_at = now(),
           claimed_by = $${vaultId ? 3 : 2},
           claim_token = gen_random_uuid(),
           lease_expires_at = now() + interval '10 minutes'
       FROM claimed
       WHERE eq.id = claimed.queue_id
       RETURNING claimed.queue_id, claimed.chunk_id, claimed.segment_id, claimed.vault_id,
                 claimed.retry_count, claimed.job_id, eq.claim_token`,
      values
    );

    span.setAttribute('extraction.batch_size', claimedResult.rows.length);
    if (!claimedResult.rowCount) {
      span.setAttribute('extraction.memories_created', 0);
      return 0;
    }

    const sessionContextCache = new Map<string, string>();
    const subjectListCache = new Map<string, Promise<VaultSubject[]>>();
    let memoriesCreated = 0;
    const leases = new Map(claimedResult.rows.map((row) => [row.queue_id, {
      queueKind: 'extraction' as const,
      queueId: row.queue_id,
      claimToken: row.claim_token,
      workerId
    }]));
    const heartbeats = new Map(Array.from(leases, ([queueId, lease]) => [
      queueId,
      startWorkerLeaseHeartbeat(lease, WORKER_LEASE_MS)
    ]));

    const processOneJob = async (queuedJob: QueuedWorkRow): Promise<void> => {
      const lease = leases.get(queuedJob.queue_id)!;
      const heartbeat = heartbeats.get(queuedJob.queue_id)!;
      const assertNotLost = () => { if (heartbeat.lost) throw new StaleWorkerLeaseError(lease); };
      try {
        try {
          await withRateLimitRetries(queuedJob, async () => {
          const job = await loadQueuedJob(queuedJob);
          assertNotLost();
          await withWorkerLeaseTransaction(lease, client => markPersistentJobRunning(job.jobId, client));
          const preparedCrypto = await prepareVaultCrypto(job.vault);
          const decryptedChunks = await Promise.all(job.chunks.map(async (chunk) => ({
            ...chunk,
            decryptedContent: preparedCrypto.decrypt(job.vault, await readRawChunkContent(chunk))
          })));

          const contextChunks = await withWorkerLeaseTransaction(lease, client => freezeExtractionContext(client, {
            queueId: job.queueId, vaultId: job.vault.id, chunkIds: job.chunkIds, context: job.context,
            blobStore: getRawChunkStorage().store
          }));
          const contextSources: ExtractionSource[] = [];
          let contextBytes = 0;
          for (const chunk of contextChunks) {
            assertNotLost();
            const content = preparedCrypto.decrypt(job.vault, await readRawChunkContent(chunk));
            const bytes = Buffer.byteLength(content, 'utf8');
            if (contextBytes + bytes > MAX_EXTRACTION_CONTEXT_BYTES) continue;
            contextBytes += bytes;
            contextSources.push({ ...chunk, content, current: false });
          }
          const sources: ExtractionSource[] = [...contextSources, ...decryptedChunks
            .filter(chunk => chunk.role === 'user' || chunk.role === 'assistant')
            .map(chunk => ({ ...chunk, content: chunk.decryptedContent, current: true }))];
          if (!sources.some(source => source.current)) {
            await completeExtractionJob(job, lease);
            return;
          }
          const conversation = formatExtractionSources(sources, job.context);
          const sessionContextCacheKey = `${job.vault.id}:${job.sessionId}`;
          assertNotLost();
          const sessionContext = sessionContextCache.get(sessionContextCacheKey)
            ?? await getOrCreateSessionContext(job.vault, job.sessionId, conversation, lease,
              preparedCrypto, scopeKeyForContext('session', job.context), assertNotLost);
          if (sessionContext !== null) sessionContextCache.set(sessionContextCacheKey, sessionContext);
          const promptHeader = [
            // Subject aliases are intentionally not injected before facts have a
            // model-assigned scope. Canonicalisation below loads only the exact
            // binding for each resulting fact.
            buildPromptHeader(job.vault.purpose, sessionContext, [])
          ].filter(Boolean).join('\n\n');
          assertNotLost();
          const facts = await extractor.extractFacts(
            conversation,
            promptHeader,
            job.vault.id,
            decryptVaultPromptContext(job.vault, preparedCrypto),
            { humanIntentAvailable: sources.some(sourceHasHumanIntent) }
          );
          // Full-response parsing and referential integrity precede proposal admission.
          // A context duplicate or unsupported human preference does not retry an
          // otherwise valid capture. Corrupt references/bindings still fail it all.
          const admission = admitExtractionCandidates(facts, sources, job.context);
          const filteredByScore = admission.accepted.filter((fact) => fact.score >= config.EXTRACTION_SCORE_THRESHOLD);
          const afterSecretFilter = filteredByScore.filter((fact) => {
            const match = matchSecretPattern(fact.fact);
            if (!match) {
              return true;
            }

            operationalLog.warn(JSON.stringify({
              level: 40,
              msg: 'secret pre-filter: discarding fact before sensitivity filter',
              subject: fact.subject,
              pattern: match
            }));
            return false;
          });

          // Filter restricted facts before embedding — no point embedding facts we'll discard
          type NonRestrictedFact = typeof afterSecretFilter[number] & { sensitivity: 'low' | 'medium' | 'high' };
          const nonRestrictedFacts = afterSecretFilter.filter((fact): fact is NonRestrictedFact => {
            if (fact.sensitivity !== 'restricted') {
              return true;
            }
            operationalLog.warn(JSON.stringify({
              level: 40,
              msg: 'sensitivity filter: discarding restricted memory before embed',
              subject: fact.subject
            }));
            return false;
          });

          const deterministicFilterResult = filterMemoryCandidates(nonRestrictedFacts);
          span.setAttribute('extraction.candidates.extracted', facts.length);
          span.setAttribute('extraction.candidates.accepted', deterministicFilterResult.accepted.length);
          span.setAttribute('extraction.candidates.dropped', facts.length - deterministicFilterResult.accepted.length);

          extractionCandidatesCounter.add(deterministicFilterResult.accepted.length, {
            status: 'accepted',
            vault_id: job.vault.id,
            session_id: job.sessionId
          });

          const droppedByReason = new Map<string, number>();
          for (const [reason, count] of Object.entries(admission.excluded)) {
            if (count > 0) droppedByReason.set(reason, count);
          }
          for (const dropped of deterministicFilterResult.dropped) {
            droppedByReason.set(dropped.reason, (droppedByReason.get(dropped.reason) ?? 0) + 1);
          }

          for (const [reason, count] of droppedByReason) {
            extractionCandidatesCounter.add(count, {
              status: 'dropped',
              reason,
              vault_id: job.vault.id,
              session_id: job.sessionId
            });
          }

          const factsToEmbed: NonRestrictedFact[] = deterministicFilterResult.accepted.map(({fact}) => fact);

          operationalLog.log(JSON.stringify({
            level: 30,
            msg: 'extraction pipeline attrition',
            raw_facts: facts.length,
            after_source_filter: admission.accepted.length,
            excluded_context_only: admission.excluded.context_only,
            excluded_unsupported_human_intent: admission.excluded.unsupported_human_intent,
            after_score_filter: filteredByScore.length,
            after_secret_filter: afterSecretFilter.length,
            after_sensitivity_filter: nonRestrictedFacts.length,
            after_deterministic_filter: factsToEmbed.length,
            threshold: config.EXTRACTION_SCORE_THRESHOLD
          }));

          if (factsToEmbed.length === 0) {
            assertNotLost();
            await completeExtractionJob(job, lease);
            return;
          }

          // Subject canonicalisation: resolve each fact's subject through tiers
          const resolvedFacts = new Array<NonRestrictedFact>(factsToEmbed.length);
          const subjectResolutionInputs: Array<{ fact: NonRestrictedFact; index: number }> = [];
          const subjectProposals: Array<{ fact: NonRestrictedFact; canonical?: string; embedding?: number[] }> = [];
          const subjectLists = await Promise.all(factsToEmbed.map(async (fact) => {
            const scopeKey = scopeKeyForContext(fact.scope, job.context);
            if (fact.scope !== 'global' && scopeKey === null) {
              return [];
            }
            const cacheKey = JSON.stringify([job.vault.id, fact.scope, scopeKey]);
            let cached = subjectListCache.get(cacheKey);
            if (!cached) {
              cached = getVaultSubjectList(
                job.vault.id,
                config.SUBJECT_INJECTION_TOP_N,
                config.SUBJECT_INJECTION_RECENT_N,
                fact.scope,
                scopeKey
              ).catch((err) => {
                operationalLog.warn(JSON.stringify({
                  level: 40,
                  msg: 'failed to load bound subject list',
                  vault_id: job.vault.id,
                  scope: fact.scope,
                  scope_key: scopeKey,
                  err: String(err)
                }));
                return [];
              });
              subjectListCache.set(cacheKey, cached);
            }
            return cached;
          }));

          for (let index = 0; index < factsToEmbed.length; index++) {
            const fact = factsToEmbed[index];
            const boundSubjects = subjectLists[index];
            // Tier 1: text normalisation + Levenshtein (free)
            const tier1 = resolveSubjectTier1(fact.subject, boundSubjects, config.SUBJECT_TEXT_MATCH_DISTANCE);
            if (tier1) {
              resolvedFacts[index] = { ...fact, subject: tier1 };
              continue;
            }
            subjectResolutionInputs.push({ fact, index });
          }

          assertNotLost();
          const subjectEmbeddings = await embedder.embedBatch(
            subjectResolutionInputs.map(({ fact }) => fact.subject),
            { vaultId: job.vault.id, modelRole: 'embedding', source: 'extraction_worker', inputType: 'document' }
          );
          await Promise.all(subjectResolutionInputs.map(async ({ fact, index }, inputIndex) => {
            const subjectEmbedding = subjectEmbeddings[inputIndex];

            // Tier 2: embedding similarity (embed cost only, no LLM)
            const tier2 = resolveSubjectTier2(
              subjectEmbedding,
              subjectLists[index],
              config.SUBJECT_EMBED_HIGH_THRESHOLD,
              config.SUBJECT_EMBED_LOW_THRESHOLD
            );
            const scopeKey = scopeKeyForContext(fact.scope, job.context);

            if (tier2) {
              if (tier2.confidence === 'high') {
                subjectProposals.push({ fact, canonical: tier2.canonical });
                resolvedFacts[index] = { ...fact, subject: tier2.canonical };
                return;
              }
              // Tier 3: LLM arbitration — only for genuinely ambiguous cases
              const decision = await subjectArbitrationLimit(() =>
                { assertNotLost(); return extractor.arbitrateSubject(tier2.canonical, fact.subject, job.vault.id); }
              );
              if (decision === 'use_existing') {
                subjectProposals.push({ fact, canonical: tier2.canonical });
                resolvedFacts[index] = { ...fact, subject: tier2.canonical };
                return;
              }
            }

            subjectProposals.push({ fact, embedding: subjectEmbedding });
            resolvedFacts[index] = fact;
          }));
          await withWorkerLeaseTransaction(lease, async client => {
            await preparedCrypto.assertCurrent(client);
            // Stable order avoids opposite alias-upsert lock ordering between jobs.
            for (const proposal of subjectProposals.sort((a, b) =>
              JSON.stringify([a.fact.scope, scopeKeyForContext(a.fact.scope, job.context), normaliseSubject(a.fact.subject)])
                .localeCompare(JSON.stringify([b.fact.scope, scopeKeyForContext(b.fact.scope, job.context), normaliseSubject(b.fact.subject)])))) {
              const key = scopeKeyForContext(proposal.fact.scope, job.context);
              if (proposal.fact.scope !== 'global' && key === null) continue;
              if (proposal.canonical !== undefined) {
                await storeSubjectAlias(job.vault.id, proposal.fact.subject, proposal.canonical, proposal.fact.scope, key, client);
              } else if (proposal.embedding) {
                await storeCanonicalEmbedding(job.vault.id, proposal.fact.subject, proposal.embedding, proposal.fact.scope, key, client);
              }
            }
          });
          assertNotLost();
          const factEmbeddings = await embedder.embedBatch(
            factsToEmbed.map((fact) => fact.fact),
            { vaultId: job.vault.id, modelRole: 'embedding', source: 'extraction_worker', inputType: 'document' }
          );

          const memoryInputs: DedupInput[] = [];

          for (let i = 0; i < factsToEmbed.length; i++) {
            const fact = resolvedFacts[i];
            if (!fact) {
              throw new Error(`Subject resolution did not complete for fact index ${i}`);
            }
            const embedding = factEmbeddings[i];
            const scopeKey = scopeKeyForContext(fact.scope, job.context);
            const supportingSources = resolveExtractionSources(fact, sources, job.context);
            const sourceTimestamp = getLatestChunkTimestamp(supportingSources);
            if (sourceTimestamp !== null && isFutureSourceTimestamp(sourceTimestamp)) {
              throw new Error('Source timestamp exceeds accepted clock skew');
            }
            const status = 'active' as const;
            memoryInputs.push({
              vaultId: job.vault.id,
              fact: fact.fact,
              score: fact.score,
              subject: fact.subject,
              embedding,
              sourceChunks: supportingSources.map(source => source.id),
              salience: fact.salience,
              sensitivity: fact.sensitivity,
              type: fact.type,
              scope: fact.scope,
              scopeKey,
              polarity: fact.polarity,
              status,
              volatility: fact.volatility,
              evidence: JSON.stringify({ summary: fact.evidence, scope_basis: fact.scope_basis }),
              validFrom: fact.valid_from,
              validUntil: fact.valid_until,
              sourceSegmentId: job.segmentId,
              sourceTimestamp
            });
          }

          // This preflight intentionally repeats dedup's read-side matching before
          // writes. The extra DB reads let us batch expensive escalation calls while
          // dedup remains the final write authority and rechecks the best match.
          const escalationRequests = (await Promise.all(
            memoryInputs.map((input, index) => getDedupEscalationRequest(input, String(index), { query }, preparedCrypto))
          )).filter((request): request is NonNullable<typeof request> => Boolean(request));
          assertNotLost();
          const precomputedDecisions = escalationRequests.length > 0
            ? await extractor.arbitrateConflictsBatch(escalationRequests, job.vault.id)
            : new Map<string, ConflictResolution>();
          const escalationRequestById = new Map(escalationRequests.map((request) => [request.id, request]));
          const validPrecomputedDecisionIds = new Set<string>();
          const seenEscalationTargets = new Set<string>();
          for (const request of escalationRequests) {
            if (seenEscalationTargets.has(request.existingMemoryId)) {
              continue;
            }
            seenEscalationTargets.add(request.existingMemoryId);
            validPrecomputedDecisionIds.add(request.id);
          }

          operationalLog.log(JSON.stringify({
            level: 30,
            msg: 'extraction escalation routing',
            candidates: memoryInputs.length,
            escalation_requests: escalationRequests.length,
            precomputed_decisions_usable: validPrecomputedDecisionIds.size,
            batch_arbitration: escalationRequests.length > 0
          }));

          // Keep dedup writes sequential within a segment. The read-side matching,
          // canonical subject resolution, and memory creation quota check are not
          // atomic with the write, so parallelizing here can race aliases, exact
          // duplicates, or plan capacity. EXTRACTION_WORKER_CONCURRENCY still
          // provides coarse-grained throughput across claimed queue rows.
          const effects: WorkerEffect[] = [];
          const committedResults = await withWorkerLeaseTransaction(lease, async (client) => {
            if (!await recordWorkerAction(client, lease, 'extract-and-complete')) return [];
            const results = [];
            for (let i = 0; i < memoryInputs.length; i++) {
              results.push(await deduplicateMemoryInTransaction(
                memoryInputs[i],
                client,
                preparedCrypto,
                effects,
                {
                  precomputedConflictInput: escalationRequestById.get(String(i))?.inputFingerprint,
                  precomputedConflictDecision: validPrecomputedDecisionIds.has(String(i))
                    ? precomputedDecisions.get(String(i))
                    : undefined,
                  precomputedConflictMemoryId: validPrecomputedDecisionIds.has(String(i))
                    ? escalationRequestById.get(String(i))?.existingMemoryId
                    : undefined,
                  precomputedConflictMemoryRevision: validPrecomputedDecisionIds.has(String(i))
                    ? escalationRequestById.get(String(i))?.existingMemoryRevision
                    : undefined
                }
              ));
            }
            await enqueueCurationWork(client, {
              vaultId: job.vault.id, segmentId: job.segmentId, workKey: 'extraction:' + job.queueId,
              memoryIds: results.flatMap(result => result.action !== 'skipped' && result.memoryId ? [result.memoryId] : [])
            });
            await finalizeExtractionJob(client, job, lease);
            return results;
          });
          publishCommittedWorkerEffects(effects);

          for (const result of committedResults) {
            if (result.action === 'inserted' || result.action === 'updated') {
              memoriesCreated += 1;
            }
            try { extractionLagHistogram.record(Date.now() - new Date(job.createdAt).getTime(), {
              vault_id: job.vault.id,
              session_id: job.sessionId,
              dedup_action: result.action
            }); } catch { /* Operational telemetry cannot retry committed work. */ }
          }
          });
        } catch (error) {
          if (error instanceof AiBudgetDeferredError) {
            aiBudgetWaitHistogram.record(error.waitMs, { role: error.role, queue: 'extraction', vault_id: queuedJob.vault_id });
            aiBudgetThrottledJobsCounter.add(1, { role: error.role, queue: 'extraction', vault_id: queuedJob.vault_id });
            operationalLog.info(JSON.stringify({
              level: 30,
              msg: 'deferring extraction job for ai budget',
              queue_id: queuedJob.queue_id,
              role: error.role,
              available_at: error.availableAt.toISOString(),
              wait_ms: error.waitMs
            }));
            await deferQueuedJob(lease, error);
            return;
          }
          if (error instanceof CircuitBreakerOpenError) {
            operationalLog.warn(JSON.stringify({
              level: 40,
              msg: 'skipping extraction job while circuit breaker is open',
              queue_id: queuedJob.queue_id,
              retry_after_ms: error.retryAfterMs
            }));
            await releaseQueuedJob(lease, error.message);
            return;
          }
          const lastError = error instanceof Error ? error.message : 'Unknown extraction error';
          operationalLog.error(getSpanAttributes({ error, queueId: queuedJob.queue_id }), 'Extraction job failed');
          if (!(error instanceof StaleWorkerLeaseError)) await failQueuedJob(queuedJob, lease, lastError);
        }
      } catch (error) {
        const lastError = error instanceof Error ? error.message : 'Unknown extraction error';
        operationalLog.error(getSpanAttributes({ error, queueId: queuedJob.queue_id }), 'Extraction job failed');
        if (!(error instanceof StaleWorkerLeaseError)) await failQueuedJob(queuedJob, lease, lastError);
      } finally {
        await heartbeat.stop();
      }
    };

    try {
      const limit = pLimit(config.EXTRACTION_WORKER_CONCURRENCY);
      const results = await Promise.allSettled(
        claimedResult.rows.map(queuedJob =>
          limit(() => processOneJob(queuedJob))
        )
      );

      for (const result of results) {
        if (result.status === 'rejected') {
          operationalLog.error(JSON.stringify({
            level: 50,
            msg: 'unexpected batch job rejection',
            error: result.reason instanceof Error ? result.reason.message : String(result.reason)
          }));
        }
      }
    } finally {
      await Promise.all(Array.from(heartbeats.values(), heartbeat => heartbeat.stop()));
      span.setAttribute('extraction.memories_created', memoriesCreated);
      await archiveStaleMemories();
    }

    return claimedResult.rows.length;
  });
}

async function completeExtractionJob(job: LoadedJob, lease: WorkerLease): Promise<void> {
  await withWorkerLeaseTransaction(lease, async (client) => {
    if (!await recordWorkerAction(client, lease, 'extract-and-complete')) return;
    await finalizeExtractionJob(client, job, lease);
  });
}

async function finalizeExtractionJob(client: import('pg').PoolClient, job: LoadedJob, lease: WorkerLease): Promise<void> {
  const deleted = await client.query(
    `DELETE FROM extraction_queue
     WHERE id = $1 AND claim_token = $2 AND claimed_by = $3`,
    [job.queueId, lease.claimToken, lease.workerId]
  );
  if (deleted.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
  await client.query(
    `UPDATE raw_chunks
     SET processed = true
     WHERE vault_id = $2 AND id = ANY($1::uuid[])`,
    [job.chunkIds, job.vault.id]
  );


  await completePersistentJobIfReady(client, job.jobId);
}

async function deadLetterQueuedJob(queuedJob: QueuedWorkRow, lease: WorkerLease, retryCount: number, lastError: string) {
  await withWorkerLeaseTransaction(lease, async (client) => {
    if (!await recordWorkerAction(client, lease, 'dead-letter')) return;
    await client.query(
      `INSERT INTO extraction_dead_letter (vault_id, chunk_id, segment_id, retry_count, last_error, job_id,source_queue_id,context_chunk_ids)
       SELECT $1,$2,$3,$4,$5,$6,id,context_chunk_ids FROM extraction_queue WHERE id=$7`,
      [queuedJob.vault_id, queuedJob.chunk_id, queuedJob.segment_id, retryCount, lastError, queuedJob.job_id,queuedJob.queue_id]
    );
    const deleted = await client.query(
      `DELETE FROM extraction_queue WHERE id = $1 AND claim_token = $2 AND claimed_by = $3`,
      [queuedJob.queue_id, lease.claimToken, lease.workerId]
    );
    if (deleted.rowCount !== 1) throw new StaleWorkerLeaseError(lease);
    await failPersistentJob(client, queuedJob.job_id, lastError);
  });
}

async function failQueuedJob(queuedJob: QueuedWorkRow, lease: WorkerLease, lastError: string) {
  // This limit is based on the persisted queue retry_count, unlike rate-limit
  // retries which happen in memory during a single job attempt.
  const nextRetryCount = queuedJob.retry_count + 1;

  if (nextRetryCount >= config.MAX_EXTRACTION_RETRIES) {
    operationalLog.warn(JSON.stringify({
      level: 40,
      msg: 'dead-lettering extraction job after retry limit',
      queue_id: queuedJob.queue_id,
      retries: nextRetryCount,
      max_retries: config.MAX_EXTRACTION_RETRIES,
      error: lastError
    }));
    await deadLetterQueuedJob(queuedJob, lease, nextRetryCount, lastError);
    return;
  }

  await releaseWorkerLease(lease, { incrementRetry: true, lastError });
}

async function releaseQueuedJob(lease: WorkerLease, lastError: string) {
  await releaseWorkerLease(lease, { lastError });
}

async function deferQueuedJob(lease: WorkerLease, error: AiBudgetDeferredError) {
  await releaseWorkerLease(lease, { availableAt: error.availableAt, lastError: error.message });
}

async function loadQueuedJob(queuedJob: QueuedWorkRow): Promise<LoadedJob> {
  if (queuedJob.segment_id) {
    const segmentResult = await query<SegmentRow & VaultContextRow>(
      `SELECT s.id, s.vault_id, s.session_id, s.chunk_ids, s.created_at,
              s.project_id, s.task_id, s.agent_id, s.trigger_type,
              v.encrypted_dek, v.vault_encryption_enabled, v.purpose, v.plan_id,
              v.type, v.custom_extraction_prompt, v.custom_curation_prompt
       FROM segments s
       JOIN vaults v ON v.id = s.vault_id
       WHERE s.id = $1
         AND s.vault_id = $2
       LIMIT 1`,
      [queuedJob.segment_id, queuedJob.vault_id]
    );

    if (!segmentResult.rowCount) {
      throw new Error(`Segment ${queuedJob.segment_id} not found`);
    }

    const segment = segmentResult.rows[0];
    const chunksResult = await query<RawChunkRow>(
      `SELECT id, vault_id, session_id, role, blob_store, blob_key, created_at, provenance
       FROM raw_chunks
       WHERE id = ANY($1::uuid[])
         AND vault_id = $2
         AND blob_key IS NOT NULL`,
      [segment.chunk_ids, segment.vault_id]
    );
    const chunkById = new Map(chunksResult.rows.map((row) => [row.id, row]));
    const orderedChunks = segment.chunk_ids
      .map((chunkId) => chunkById.get(chunkId))
      .filter((chunk): chunk is RawChunkRow => Boolean(chunk));
    if (orderedChunks.length !== segment.chunk_ids.length) {
      throw new Error(`Segment ${segment.id} has ${segment.chunk_ids.length - orderedChunks.length} raw chunks without blob storage`);
    }

    return {
      queueId: queuedJob.queue_id,
      jobId: queuedJob.job_id,
      segmentId: segment.id,
      vault: {
        id: segment.vault_id,
        plan_id: segment.plan_id,
        encrypted_dek: segment.encrypted_dek,
        vault_encryption_enabled: segment.vault_encryption_enabled,
        purpose: segment.purpose,
        type: segment.type,
        custom_extraction_prompt: segment.custom_extraction_prompt,
        custom_curation_prompt: segment.custom_curation_prompt
      },
      sessionId: segment.session_id,
      chunkIds: segment.chunk_ids,
      chunks: orderedChunks,
      createdAt: segment.created_at,
      context: {
        session_id: segment.session_id,
        project_id: segment.project_id ?? undefined,
        task_id: segment.task_id ?? undefined,
        agent_id: segment.agent_id ?? undefined,
        trigger_type: segment.trigger_type ?? undefined
      }
    };
  }

  if (!queuedJob.chunk_id) {
    throw new Error(`Queue row ${queuedJob.queue_id} has no chunk_id or segment_id`);
  }

  const chunkResult = await query<RawChunkRow & VaultContextRow>(
    `SELECT rc.id, rc.vault_id, rc.session_id, rc.role, rc.blob_store, rc.blob_key, rc.created_at, rc.provenance, rc.capture_context,
            v.encrypted_dek, v.vault_encryption_enabled, v.purpose, v.plan_id,
            v.type, v.custom_extraction_prompt, v.custom_curation_prompt
     FROM raw_chunks rc
     JOIN vaults v ON v.id = rc.vault_id
     WHERE rc.id = $1
       AND rc.vault_id = $2
       AND rc.blob_key IS NOT NULL
     LIMIT 1`,
    [queuedJob.chunk_id, queuedJob.vault_id]
  );

  if (!chunkResult.rowCount) {
    throw new Error(`Chunk ${queuedJob.chunk_id} not found or has no blob_key`);
  }

  const chunk = chunkResult.rows[0];
  const captureContext = recallContextSchema.parse(chunk.capture_context ?? {});
  return {
    queueId: queuedJob.queue_id,
    jobId: queuedJob.job_id,
    segmentId: null,
    vault: {
      id: chunk.vault_id,
      plan_id: chunk.plan_id,
      encrypted_dek: chunk.encrypted_dek,
      vault_encryption_enabled: chunk.vault_encryption_enabled,
      purpose: chunk.purpose,
      type: chunk.type,
      custom_extraction_prompt: chunk.custom_extraction_prompt,
      custom_curation_prompt: chunk.custom_curation_prompt
    },
    sessionId: chunk.session_id,
    chunkIds: [chunk.id],
    chunks: [chunk],
    createdAt: chunk.created_at,
    context: {
      ...captureContext,
      session_id: chunk.session_id,
      trigger_type: captureContext.trigger_type ?? getChunkTriggerType(chunk)
    }
  };
}

function getChunkTriggerType(chunk: RawChunkRow): RecallContext['trigger_type'] | undefined {
  if (!chunk.provenance || typeof chunk.provenance !== 'object') return undefined;
  const value = (chunk.provenance as { trigger_type?: unknown }).trigger_type;
  return typeof value === 'string' && ['direct', 'delegated', 'scheduled', 'event', 'backfill', 'api', 'unknown'].includes(value)
    ? value as RecallContext['trigger_type']
    : undefined;
}

function decryptVaultPromptContext(vault: VaultContextRow, preparedCrypto: PreparedVaultCrypto): VaultPromptContext {
  if (vault.type !== 'custom') {
    return { type: vault.type };
  }

  return {
    type: vault.type,
    custom_extraction_prompt: vault.custom_extraction_prompt
      ? preparedCrypto.decrypt(vault, vault.custom_extraction_prompt)
      : null,
    custom_curation_prompt: vault.custom_curation_prompt
      ? preparedCrypto.decrypt(vault, vault.custom_curation_prompt)
      : null
  };
}

async function readRawChunkContent(chunk: RawChunkRow): Promise<string> {
  if (!chunk.blob_key) {
    throw new Error(`Raw chunk ${chunk.id} has no blob_key`);
  }
  if (chunk.blob_store && chunk.blob_store !== rawChunkStorage.store) {
    throw new Error(`Raw chunk ${chunk.id} is stored in ${chunk.blob_store}, but configured storage is ${rawChunkStorage.store}`);
  }
  return rawChunkStorage.get(chunk.blob_key);
}

function getLatestChunkTimestamp(chunks: Array<{ created_at: string }>): string | null {
  const latest = chunks.reduce<number | null>((currentLatest, chunk) => {
    const value = new Date(chunk.created_at).getTime();
    if (!Number.isFinite(value)) {
      return currentLatest;
    }

    return currentLatest === null || value > currentLatest ? value : currentLatest;
  }, null);

  return latest === null ? null : new Date(latest).toISOString();
}

async function getOrCreateSessionContext(
  vault: VaultContextRow,
  sessionId: string,
  conversation: string,
  lease: WorkerLease,
  preparedCrypto: PreparedVaultCrypto,
  aliasScopeKey: string | null,
  assertNotLost: () => void
): Promise<string | null> {
  const existing = await query<{ context: string }>(
    `SELECT context
     FROM session_contexts
     WHERE vault_id = $1 AND session_id = $2
     LIMIT 1`,
    [vault.id, sessionId]
  );

  if (existing.rowCount) {
    return preparedCrypto.decrypt(vault, existing.rows[0].context);
  }

  const summary = await extractor.extractSessionContext(conversation, buildPromptHeader(vault.purpose, null), vault.id);
  if (!summary) {
    return null;
  }

  assertNotLost();
  const aliases = aliasScopeKey === null ? [] : await extractor.extractSessionAliases(conversation, vault.id);
  return withWorkerLeaseTransaction(lease, async client => {
    await preparedCrypto.assertCurrent(client);
    const storedContext = preparedCrypto.encrypt(vault, summary);
    const inserted = await client.query<{ context: string }>(
      `INSERT INTO session_contexts (vault_id, session_id, context)
       VALUES ($1, $2, $3)
       ON CONFLICT (vault_id, session_id) DO NOTHING
       RETURNING context`,
      [vault.id, sessionId, storedContext]
    );

    if (inserted.rowCount) {
      await upsertEntityAliases(client, vault.id, aliases, 'session', aliasScopeKey);
      return summary;
    }

    const conflictRead = await client.query<{ context: string }>(
      `SELECT context
       FROM session_contexts
       WHERE vault_id = $1 AND session_id = $2
       LIMIT 1`,
      [vault.id, sessionId]
    );
    if (!conflictRead.rows[0]) throw new Error('Committed session context disappeared');
    return preparedCrypto.decrypt(vault, conflictRead.rows[0].context);
  });
}

async function withRateLimitRetries(queuedJob: QueuedWorkRow, fn: () => Promise<void>) {
  let attempt = 0;

  while (true) {
    try {
      await fn();
      return;
    } catch (error) {
      if (!isRateLimitError(error)) {
        throw error;
      }

      if (attempt >= MAX_EXTRACTION_RATE_LIMIT_RETRIES) {
        const lastError = error instanceof Error ? error.message : 'Extraction rate limit exceeded';
        operationalLog.warn(JSON.stringify({
          level: 40,
          msg: 'dead-lettering extraction job after rate limit retries',
          queue_id: queuedJob.queue_id,
          retries: attempt,
          error: lastError
        }));
        const lease: WorkerLease = {
          queueKind: 'extraction', queueId: queuedJob.queue_id,
          claimToken: queuedJob.claim_token, workerId
        };
        await deadLetterQueuedJob(queuedJob, lease, attempt, lastError);
        return;
      }

      const delayMs = Math.min(
        EXTRACTION_RATE_LIMIT_MAX_DELAY_MS,
        EXTRACTION_RATE_LIMIT_BASE_DELAY_MS * (2 ** attempt)
      );
      attempt += 1;
      operationalLog.warn(JSON.stringify({
        level: 40,
        msg: 'retrying extraction job after rate limit',
        queue_id: queuedJob.queue_id,
        attempt,
        delay_ms: delayMs
      }));
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

async function upsertEntityAliases(
  client: import('pg').PoolClient,
  vaultId: string,
  aliases: Array<{ alias: string; canonical: string }>,
  scope: 'global' | 'project' | 'task' | 'session',
  scopeKey: string | null
): Promise<void> {
  const normalisedAliases = aliases.map(({ alias, canonical }) => ({
    alias: normaliseSubject(alias),
    canonical: normaliseSubject(canonical)
  }));

  if (normalisedAliases.length === 0) {
    return;
  }

  await client.query(
    `INSERT INTO entity_aliases (vault_id, alias, canonical, scope, scope_key)
     SELECT $1, alias, canonical, $4, $5
     FROM UNNEST($2::text[], $3::text[]) AS t(alias, canonical)
     ON CONFLICT (vault_id, scope, scope_key, alias) DO NOTHING`,
    [
      vaultId,
      normalisedAliases.map(({ alias }) => alias),
      normalisedAliases.map(({ canonical }) => canonical),
      scope,
      scopeKey
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
        operationalLog.error('Extraction loop iteration failed', error);
      }
    }

    if (!isShuttingDown) {
      try {
        await trackWorkerTask(drainDueContradictionActivations(extractor));
      } catch (error) {
        operationalLog.error('Contradiction activation iteration failed', error);
      }
    }

    await sleepUntilNextBatch(config.EXTRACTION_INTERVAL_MS);
  }
}

async function handleRunOnce(message: WorkerRunOnceRequest) {
  if (!parentPort) {
    return;
  }

  if (isShuttingDown) {
    if (message.jobId) {
      parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'failed', error: 'Extraction worker is shutting down' });
    }
    return;
  }

  if (message.jobId) {
    parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'running' });
  }

  try {
    await trackWorkerTask(processBatch(message.vaultId));
    extractionJobsCounter.add(1, {
      status: 'success',
      vault_id: message.vaultId ?? 'all'
    });
    if (message.jobId) {
      parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'completed' });
    }
  } catch (error) {
    extractionJobsCounter.add(1, {
      status: 'error',
      vault_id: message.vaultId ?? 'all'
    });
    if (message.jobId) {
      parentPort.postMessage({
        type: 'job-status',
        jobId: message.jobId,
        status: 'failed',
        error: error instanceof Error ? error.message : 'Unknown worker error'
      });
    }
  }
}

if (parentPort) {
  parentPort.on('message', (message: WorkerRequest) => {
    if (message.type === 'run-once') {
      // Include status/counter completion after processBatch in the drain barrier.
      if (!isShuttingDown) void trackWorkerTask(handleRunOnce(message)).catch(() => process.exit(1));
      else void handleRunOnce(message).catch(() => process.exit(1));
    } else if (message.type === 'shutdown') {
      void shutdownWorker(message.deadline).catch(() => process.exit(1));
    }
  });
}

if (parentPort) {
  workerLoop = runLoop();
  // The failure handler is outside the promise shutdown waits for.
  void workerLoop.catch(async (error) => {
    workerLoopFailed = true;
    try { operationalLog.error(getSpanAttributes({ error }), 'Extraction worker terminated'); }
    finally { try { await shutdownWorker(); } finally { process.exit(1); } }
  });
}
