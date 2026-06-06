import crypto from 'node:crypto';
import { parentPort } from 'node:worker_threads';
import pLimit from 'p-limit';

import { getConfig } from '../config';
import { closePool, query, withTransaction } from '../db/client';
import { aiBudgetThrottledJobsCounter, aiBudgetWaitHistogram, extractionCandidatesCounter, extractionJobsCounter, extractionLagHistogram } from '../metrics';
import { CircuitBreakerOpenError, isRateLimitError } from '../services/ai-resilience';
import { scanForContradictions } from '../services/contradiction-scanner';
import { decryptForVault, encryptForVault, initCryptoClient } from '../services/crypto';
import { deduplicateMemory, getDedupEscalationRequest, type DedupInput } from '../services/dedup';
import { filterMemoryCandidates } from '../services/deterministic-filter';
import { getEmbedder } from '../services/embedder';
import { formatConversationForExtraction } from '../services/extraction-formatting';
import { buildPromptHeader } from '../services/extraction-prompt-header';
import { EXTRACTION_QUEUE_READY_PREDICATE } from '../services/extraction-queue-eligibility';
import { ExtractorService } from '../services/extractor';
import { getRawChunkStorage } from '../services/raw-chunk-storage';
import type { ConflictResolution } from '../services/extractor';
import { completePersistentJobIfReady, failPersistentJob, markPersistentJobRunning } from '../services/job-status';
import { archiveStaleMemories } from '../services/staleness';
import { AiBudgetDeferredError } from '../services/usage';
import { isCuratorEnabled } from '../services/curation-capacity';
import { enqueueCurationIfSegmentReady } from '../services/segment-curation-readiness';
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

interface QueuedWorkRow {
  queue_id: string;
  chunk_id: string | null;
  segment_id: string | null;
  vault_id: string;
  retry_count: number;
  job_id: string | null;
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
}

interface SegmentRow {
  id: string;
  vault_id: string;
  session_id: string;
  chunk_ids: string[];
  created_at: string;
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
}

interface WorkerRequest {
  type: 'run-once';
  jobId?: string;
  vaultId?: string;
}

const config = getConfig();
const embedder = getEmbedder();
const extractor = new ExtractorService();
const rawChunkStorage = getRawChunkStorage();
const workerId = crypto.randomUUID();
const subjectArbitrationLimit = pLimit(5);
const MAX_EXTRACTION_RATE_LIMIT_RETRIES = 5;
const EXTRACTION_RATE_LIMIT_BASE_DELAY_MS = 1_000;
const EXTRACTION_RATE_LIMIT_MAX_DELAY_MS = 32_000;

async function processBatch(vaultId?: string) {
  return withSpan('extraction.process_batch', {
    'vault.id': vaultId,
    'extraction.batch_limit': config.EXTRACTION_BATCH_SIZE
  }, async (span) => {
    await query(
      `UPDATE extraction_queue
       SET claimed_at = NULL, claimed_by = NULL
       WHERE claimed_at < now() - interval '10 minutes'`
    );

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
       SET claimed_at = now(), claimed_by = $${vaultId ? 3 : 2}
       FROM claimed
       WHERE eq.id = claimed.queue_id
       RETURNING claimed.queue_id, claimed.chunk_id, claimed.segment_id, claimed.vault_id, claimed.retry_count, claimed.job_id`,
      values
    );

    span.setAttribute('extraction.batch_size', claimedResult.rows.length);
    if (!claimedResult.rowCount) {
      span.setAttribute('extraction.memories_created', 0);
      return 0;
    }

    const sessionContextCache = new Map<string, Promise<{ context: string | null; isNew: boolean }>>();
    let memoriesCreated = 0;
    const affectedMemoryIds = new Map<string, Set<string>>();

    // Build per-vault subject list cache — once per batch, never per fact
    const vaultSubjectCache = new Map<string, VaultSubject[]>();
    for (const queuedJob of claimedResult.rows) {
      if (!vaultSubjectCache.has(queuedJob.vault_id)) {
        try {
          const subjects = await getVaultSubjectList(
            queuedJob.vault_id,
            config.SUBJECT_INJECTION_TOP_N,
            config.SUBJECT_INJECTION_RECENT_N
          );
          vaultSubjectCache.set(queuedJob.vault_id, subjects);
        } catch (err) {
          console.warn(JSON.stringify({ level: 40, msg: 'failed to load vault subject list', vault_id: queuedJob.vault_id, err: String(err) }));
          vaultSubjectCache.set(queuedJob.vault_id, []);
        }
      }
    }

    const processOneJob = async (queuedJob: QueuedWorkRow, sessionContextCache: Map<string, Promise<{ context: string | null; isNew: boolean }>>): Promise<void> => {
      try {
        try {
          await withRateLimitRetries(queuedJob, async () => {
          const job = await loadQueuedJob(queuedJob);
          await markPersistentJobRunning(job.jobId);
          const decryptedChunks = await Promise.all(job.chunks.map(async (chunk) => ({
            ...chunk,
            decryptedContent: await decryptForVault(job.vault, await readRawChunkContent(chunk))
          })));

          const conversation = formatConversationForExtraction(decryptedChunks);
          const sessionContextCacheKey = `${job.vault.id}:${job.sessionId}`;
          if (!sessionContextCache.has(sessionContextCacheKey)) {
            sessionContextCache.set(
              sessionContextCacheKey,
              getOrCreateSessionContext(job.vault, job.sessionId, conversation)
            );
          }
          const { context: sessionContext, isNew: sessionIsNew } = await sessionContextCache.get(sessionContextCacheKey)!;
          if (sessionIsNew) {
            const sessionAliases = await extractor.extractSessionAliases(conversation, job.vault.id);
            await upsertEntityAliases(job.vault.id, sessionAliases);
          }
          const vaultSubjects = vaultSubjectCache.get(job.vault.id) ?? [];
          const promptHeader = buildPromptHeader(job.vault.purpose, sessionContext, vaultSubjects);
          const facts = await extractor.extractFacts(
            conversation,
            promptHeader,
            job.vault.id,
            await decryptVaultPromptContext(job.vault)
          );
          const filteredByScore = facts.filter((fact) => fact.score >= config.EXTRACTION_SCORE_THRESHOLD);
          const afterSecretFilter = filteredByScore.filter((fact) => {
            const match = matchSecretPattern(fact.fact);
            if (!match) {
              return true;
            }

            console.warn(JSON.stringify({
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
            console.warn(JSON.stringify({
              level: 40,
              msg: 'sensitivity filter: discarding restricted memory before embed',
              subject: fact.subject
            }));
            return false;
          });

          const deterministicFilterResult = filterMemoryCandidates(nonRestrictedFacts);
          span.setAttribute('extraction.candidates.extracted', facts.length);
          span.setAttribute('extraction.candidates.accepted', deterministicFilterResult.accepted.length);
          span.setAttribute('extraction.candidates.dropped', deterministicFilterResult.dropped.length);

          extractionCandidatesCounter.add(deterministicFilterResult.accepted.length, {
            status: 'accepted',
            vault_id: job.vault.id,
            session_id: job.sessionId
          });

          const droppedByReason = new Map<string, number>();
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

          const factsToEmbed = deterministicFilterResult.accepted.map((candidate) => candidate.fact);

          console.log(JSON.stringify({
            level: 30,
            msg: 'extraction pipeline attrition',
            raw_facts: facts.length,
            after_score_filter: filteredByScore.length,
            after_secret_filter: afterSecretFilter.length,
            after_sensitivity_filter: nonRestrictedFacts.length,
            after_deterministic_filter: factsToEmbed.length,
            threshold: config.EXTRACTION_SCORE_THRESHOLD
          }));

          // Subject canonicalisation: resolve each fact's subject through tiers
          const resolvedFacts = new Array<NonRestrictedFact>(factsToEmbed.length);
          const subjectResolutionInputs: Array<{ fact: NonRestrictedFact; index: number }> = [];

          for (let index = 0; index < factsToEmbed.length; index++) {
            const fact = factsToEmbed[index];
            // Tier 1: text normalisation + Levenshtein (free)
            const tier1 = resolveSubjectTier1(fact.subject, vaultSubjects, config.SUBJECT_TEXT_MATCH_DISTANCE);
            if (tier1) {
              resolvedFacts[index] = { ...fact, subject: tier1 };
              continue;
            }
            subjectResolutionInputs.push({ fact, index });
          }

          const subjectEmbeddings = await embedder.embedBatch(
            subjectResolutionInputs.map(({ fact }) => fact.subject),
            { vaultId: job.vault.id, modelRole: 'embedding', inputType: 'document' }
          );
          await Promise.all(subjectResolutionInputs.map(async ({ fact, index }, inputIndex) => {
            const subjectEmbedding = subjectEmbeddings[inputIndex];

            // Tier 2: embedding similarity (embed cost only, no LLM)
            const tier2 = resolveSubjectTier2(
              subjectEmbedding,
              vaultSubjects,
              config.SUBJECT_EMBED_HIGH_THRESHOLD,
              config.SUBJECT_EMBED_LOW_THRESHOLD
            );

            if (tier2) {
              if (tier2.confidence === 'high') {
                try {
                  await storeSubjectAlias(job.vault.id, fact.subject, tier2.canonical);
                } catch (error) {
                  console.warn(JSON.stringify({
                    level: 40,
                    msg: 'failed to store subject alias',
                    vault_id: job.vault.id,
                    alias: fact.subject,
                    canonical: tier2.canonical,
                    err: String(error)
                  }));
                }
                resolvedFacts[index] = { ...fact, subject: tier2.canonical };
                return;
              }
              // Tier 3: LLM arbitration — only for genuinely ambiguous cases
              const decision = await subjectArbitrationLimit(() =>
                extractor.arbitrateSubject(tier2.canonical, fact.subject, job.vault.id)
              );
              if (decision === 'use_existing') {
                try {
                  await storeSubjectAlias(job.vault.id, fact.subject, tier2.canonical);
                } catch (error) {
                  console.warn(JSON.stringify({
                    level: 40,
                    msg: 'failed to store subject alias',
                    vault_id: job.vault.id,
                    alias: fact.subject,
                    canonical: tier2.canonical,
                    err: String(error)
                  }));
                }
                resolvedFacts[index] = { ...fact, subject: tier2.canonical };
                return;
              }
            }

            // New subject — store canonical embedding for future matching
            try {
              await storeCanonicalEmbedding(job.vault.id, fact.subject, subjectEmbedding);
            } catch (error) {
              console.warn(JSON.stringify({
                level: 40,
                msg: 'failed to store canonical embedding',
                vault_id: job.vault.id,
                canonical: fact.subject,
                err: String(error)
              }));
            }
            resolvedFacts[index] = fact;
          }));
          const factEmbeddings = await embedder.embedBatch(
            factsToEmbed.map((fact) => fact.fact),
            { vaultId: job.vault.id, modelRole: 'embedding', inputType: 'document' }
          );

          const curatorEnabled = config.CURATOR_AUTO_RUN && await isCuratorEnabled(job.vault.id);
          const memoryInputs: DedupInput[] = [];
          const sourceTimestamp = getLatestChunkTimestamp(job.chunks);

          for (let i = 0; i < factsToEmbed.length; i++) {
            const fact = resolvedFacts[i];
            if (!fact) {
              throw new Error(`Subject resolution did not complete for fact index ${i}`);
            }
            const embedding = factEmbeddings[i];
            const status = curatorEnabled ? 'candidate' : fact.status;
            memoryInputs.push({
              vaultId: job.vault.id,
              fact: fact.fact,
              score: fact.score,
              subject: fact.subject,
              embedding,
              sourceChunks: job.chunkIds,
              salience: fact.salience,
              sensitivity: fact.sensitivity,
              type: fact.type,
              scope: fact.scope,
              polarity: fact.polarity,
              status,
              volatility: fact.volatility,
              evidence: fact.evidence,
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
            memoryInputs.map((input, index) => getDedupEscalationRequest(input, String(index)))
          )).filter((request): request is NonNullable<typeof request> => Boolean(request));
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

          console.log(JSON.stringify({
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
          for (let i = 0; i < memoryInputs.length; i++) {
            const result = await deduplicateMemory(
              memoryInputs[i],
              undefined,
              extractor,
              {
                precomputedConflictDecision: validPrecomputedDecisionIds.has(String(i))
                  ? precomputedDecisions.get(String(i))
                  : undefined,
                precomputedConflictMemoryId: validPrecomputedDecisionIds.has(String(i))
                  ? escalationRequestById.get(String(i))?.existingMemoryId
                  : undefined
              }
            );

            if (result.action === 'inserted' || result.action === 'updated') {
              memoriesCreated += 1;
              if (result.memoryId) {
                let memoryIds = affectedMemoryIds.get(job.vault.id);
                if (!memoryIds) {
                  memoryIds = new Set<string>();
                  affectedMemoryIds.set(job.vault.id, memoryIds);
                }
                memoryIds.add(result.memoryId);
              }
            }

            extractionLagHistogram.record(Date.now() - new Date(job.createdAt).getTime(), {
              vault_id: job.vault.id,
              session_id: job.sessionId,
              dedup_action: result.action
            });
          }

          await completeExtractionJob(job);
          });
        } catch (error) {
          if (error instanceof AiBudgetDeferredError) {
            aiBudgetWaitHistogram.record(error.waitMs, { role: error.role, queue: 'extraction', vault_id: queuedJob.vault_id });
            aiBudgetThrottledJobsCounter.add(1, { role: error.role, queue: 'extraction', vault_id: queuedJob.vault_id });
            console.info(JSON.stringify({
              level: 30,
              msg: 'deferring extraction job for ai budget',
              queue_id: queuedJob.queue_id,
              role: error.role,
              available_at: error.availableAt.toISOString(),
              wait_ms: error.waitMs
            }));
            await deferQueuedJob(queuedJob.queue_id, error);
            return;
          }
          if (error instanceof CircuitBreakerOpenError) {
            console.warn(JSON.stringify({
              level: 40,
              msg: 'skipping extraction job while circuit breaker is open',
              queue_id: queuedJob.queue_id,
              retry_after_ms: error.retryAfterMs
            }));
            await releaseQueuedJob(queuedJob.queue_id, error.message);
            return;
          }
          const lastError = error instanceof Error ? error.message : 'Unknown extraction error';
          console.error(getSpanAttributes({ error, queueId: queuedJob.queue_id }), 'Extraction job failed');
          await failQueuedJob(queuedJob, lastError);
        }
      } catch (error) {
        const lastError = error instanceof Error ? error.message : 'Unknown extraction error';
        console.error(getSpanAttributes({ error, queueId: queuedJob.queue_id }), 'Extraction job failed');
        await failQueuedJob(queuedJob, lastError);
      }
    };

    try {
      const limit = pLimit(config.EXTRACTION_WORKER_CONCURRENCY);
      const results = await Promise.allSettled(
        claimedResult.rows.map(queuedJob =>
          limit(() => processOneJob(queuedJob, sessionContextCache))
        )
      );

      for (const result of results) {
        if (result.status === 'rejected') {
          console.error(JSON.stringify({
            level: 50,
            msg: 'unexpected batch job rejection',
            error: result.reason instanceof Error ? result.reason.message : String(result.reason)
          }));
        }
      }
    } finally {
      for (const [batchVaultId, memoryIds] of affectedMemoryIds.entries()) {
        try {
          await scanForContradictions(batchVaultId, Array.from(memoryIds), extractor);
        } catch (error) {
          console.error(getSpanAttributes({ error, vaultId: batchVaultId }), 'Contradiction scan failed');
        }
      }
      span.setAttribute('extraction.memories_created', memoriesCreated);
      await archiveStaleMemories();
    }

    return claimedResult.rows.length;
  });
}

async function completeExtractionJob(job: LoadedJob): Promise<void> {
  await withTransaction(async (client) => {
    await client.query(`DELETE FROM extraction_queue WHERE id = $1`, [job.queueId]);
    await client.query(
      `UPDATE raw_chunks
       SET processed = true
       WHERE id = ANY($1::uuid[])`,
      [job.chunkIds]
    );

    if (job.segmentId) {
      const enqueue = config.CURATOR_AUTO_RUN && await isCuratorEnabled(job.vault.id, client);
      await enqueueCurationIfSegmentReady(client, {
        vaultId: job.vault.id,
        segmentId: job.segmentId,
        enqueue
      });
    }

    await completePersistentJobIfReady(client, job.jobId);
  });
}

async function deadLetterQueuedJob(queuedJob: QueuedWorkRow, retryCount: number, lastError: string) {
  await withTransaction(async (client) => {
    await client.query(
      `INSERT INTO extraction_dead_letter (vault_id, chunk_id, segment_id, retry_count, last_error, job_id)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [queuedJob.vault_id, queuedJob.chunk_id, queuedJob.segment_id, retryCount, lastError, queuedJob.job_id]
    );
    await client.query(`DELETE FROM extraction_queue WHERE id = $1`, [queuedJob.queue_id]);
    if (queuedJob.segment_id) {
      const enqueue = config.CURATOR_AUTO_RUN && await isCuratorEnabled(queuedJob.vault_id, client);
      await enqueueCurationIfSegmentReady(client, {
        vaultId: queuedJob.vault_id,
        segmentId: queuedJob.segment_id,
        enqueue
      });
    }
    await failPersistentJob(client, queuedJob.job_id, lastError);
  });
}

async function failQueuedJob(queuedJob: QueuedWorkRow, lastError: string) {
  // This limit is based on the persisted queue retry_count, unlike rate-limit
  // retries which happen in memory during a single job attempt.
  const nextRetryCount = queuedJob.retry_count + 1;

  if (nextRetryCount >= config.MAX_EXTRACTION_RETRIES) {
    console.warn(JSON.stringify({
      level: 40,
      msg: 'dead-lettering extraction job after retry limit',
      queue_id: queuedJob.queue_id,
      retries: nextRetryCount,
      max_retries: config.MAX_EXTRACTION_RETRIES,
      error: lastError
    }));
    await deadLetterQueuedJob(queuedJob, nextRetryCount, lastError);
    return;
  }

  await query(
    `UPDATE extraction_queue
     SET retry_count = retry_count + 1,
         last_error = $2,
         claimed_at = NULL,
         claimed_by = NULL
     WHERE id = $1`,
    [queuedJob.queue_id, lastError]
  );
}

async function releaseQueuedJob(queueId: string, lastError: string) {
  await query(
    `UPDATE extraction_queue
     SET last_error = $2,
         claimed_at = NULL,
         claimed_by = NULL
     WHERE id = $1`,
    [queueId, lastError]
  );
}

async function deferQueuedJob(queueId: string, error: AiBudgetDeferredError) {
  await query(
    `UPDATE extraction_queue
     SET available_at = $2,
         last_error = $3,
         claimed_at = NULL,
         claimed_by = NULL
     WHERE id = $1`,
    [queueId, error.availableAt.toISOString(), error.message]
  );
}

async function loadQueuedJob(queuedJob: QueuedWorkRow): Promise<LoadedJob> {
  if (queuedJob.segment_id) {
    const segmentResult = await query<SegmentRow & VaultContextRow>(
      `SELECT s.id, s.vault_id, s.session_id, s.chunk_ids, s.created_at,
              v.encrypted_dek, v.vault_encryption_enabled, v.purpose, v.plan_id,
              v.type, v.custom_extraction_prompt, v.custom_curation_prompt
       FROM segments s
       JOIN vaults v ON v.id = s.vault_id
       WHERE s.id = $1
       LIMIT 1`,
      [queuedJob.segment_id]
    );

    if (!segmentResult.rowCount) {
      throw new Error(`Segment ${queuedJob.segment_id} not found`);
    }

    const segment = segmentResult.rows[0];
    const chunksResult = await query<RawChunkRow>(
      `SELECT id, vault_id, session_id, role, blob_store, blob_key, created_at
       FROM raw_chunks
       WHERE id = ANY($1::uuid[])
         AND blob_key IS NOT NULL`,
      [segment.chunk_ids]
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
      createdAt: segment.created_at
    };
  }

  if (!queuedJob.chunk_id) {
    throw new Error(`Queue row ${queuedJob.queue_id} has no chunk_id or segment_id`);
  }

  const chunkResult = await query<RawChunkRow & VaultContextRow>(
    `SELECT rc.id, rc.vault_id, rc.session_id, rc.role, rc.blob_store, rc.blob_key, rc.created_at,
            v.encrypted_dek, v.vault_encryption_enabled, v.purpose, v.plan_id,
            v.type, v.custom_extraction_prompt, v.custom_curation_prompt
     FROM raw_chunks rc
     JOIN vaults v ON v.id = rc.vault_id
     WHERE rc.id = $1
       AND rc.blob_key IS NOT NULL
     LIMIT 1`,
    [queuedJob.chunk_id]
  );

  if (!chunkResult.rowCount) {
    throw new Error(`Chunk ${queuedJob.chunk_id} not found or has no blob_key`);
  }

  const chunk = chunkResult.rows[0];
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
    createdAt: chunk.created_at
  };
}

async function decryptVaultPromptContext(vault: VaultContextRow): Promise<VaultPromptContext> {
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

async function readRawChunkContent(chunk: RawChunkRow): Promise<string> {
  if (!chunk.blob_key) {
    throw new Error(`Raw chunk ${chunk.id} has no blob_key`);
  }
  if (chunk.blob_store && chunk.blob_store !== rawChunkStorage.store) {
    throw new Error(`Raw chunk ${chunk.id} is stored in ${chunk.blob_store}, but configured storage is ${rawChunkStorage.store}`);
  }
  return rawChunkStorage.get(chunk.blob_key);
}

function getLatestChunkTimestamp(chunks: RawChunkRow[]): string | null {
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
  conversation: string
): Promise<{ context: string | null; isNew: boolean }> {
  const existing = await query<{ context: string }>(
    `SELECT context
     FROM session_contexts
     WHERE vault_id = $1 AND session_id = $2
     LIMIT 1`,
    [vault.id, sessionId]
  );

  if (existing.rowCount) {
    return { context: await decryptForVault(vault, existing.rows[0].context), isNew: false };
  }

  const summary = await extractor.extractSessionContext(conversation, buildPromptHeader(vault.purpose, null), vault.id);
  if (!summary) {
    return { context: null, isNew: false };
  }

  const storedContext = await encryptForVault(vault, summary);
  const inserted = await query<{ context: string }>(
    `INSERT INTO session_contexts (vault_id, session_id, context)
     VALUES ($1, $2, $3)
     ON CONFLICT (vault_id, session_id) DO NOTHING
     RETURNING context`,
    [vault.id, sessionId, storedContext]
  );

  if (inserted.rowCount) {
    return { context: summary, isNew: true };
  }

  const conflictRead = await query<{ context: string }>(
    `SELECT context
     FROM session_contexts
     WHERE vault_id = $1 AND session_id = $2
     LIMIT 1`,
    [vault.id, sessionId]
  );
  const conflictContext = conflictRead.rowCount ? await decryptForVault(vault, conflictRead.rows[0].context) : summary;
  return { context: conflictContext, isNew: false };
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
        console.warn(JSON.stringify({
          level: 40,
          msg: 'dead-lettering extraction job after rate limit retries',
          queue_id: queuedJob.queue_id,
          retries: attempt,
          error: lastError
        }));
        await deadLetterQueuedJob(queuedJob, attempt, lastError);
        return;
      }

      const delayMs = Math.min(
        EXTRACTION_RATE_LIMIT_MAX_DELAY_MS,
        EXTRACTION_RATE_LIMIT_BASE_DELAY_MS * (2 ** attempt)
      );
      attempt += 1;
      console.warn(JSON.stringify({
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
  vaultId: string,
  aliases: Array<{ alias: string; canonical: string }>
): Promise<void> {
  const normalisedAliases = aliases.map(({ alias, canonical }) => ({
    alias: normaliseSubject(alias),
    canonical: normaliseSubject(canonical)
  }));

  if (normalisedAliases.length === 0) {
    return;
  }

  await query(
    `INSERT INTO entity_aliases (vault_id, alias, canonical)
     SELECT $1, alias, canonical
     FROM UNNEST($2::text[], $3::text[]) AS t(alias, canonical)
     ON CONFLICT (vault_id, alias) DO NOTHING`,
    [
      vaultId,
      normalisedAliases.map(({ alias }) => alias),
      normalisedAliases.map(({ canonical }) => canonical)
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
      console.error('Extraction loop iteration failed', error);
    }

    await new Promise((resolve) => setTimeout(resolve, config.EXTRACTION_INTERVAL_MS));
  }
}

async function handleRunOnce(message: WorkerRequest) {
  if (!parentPort) {
    return;
  }

  if (message.jobId) {
    parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'running' });
  }

  try {
    await processBatch(message.vaultId);
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
      void handleRunOnce(message);
    }
  });
}

void runLoop().catch(async (error) => {
  console.error(getSpanAttributes({ error }), 'Extraction worker terminated');
  await closePool();
  process.exit(1);
});
