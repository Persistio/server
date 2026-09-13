import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('ingest');
import crypto from 'node:crypto';

import type { FastifyInstance } from 'fastify';
import type { PoolClient } from 'pg';
import { z } from 'zod';

import { getConfig } from '../config';
import { withTransaction } from '../db/client';
import { ingestChunksCounter } from '../metrics';
import { requireVaultWriteAuth } from '../middleware/auth';
import type { VaultContext } from '../middleware/auth';
import { prepareVaultCrypto } from '../services/crypto';
import { recordCustomerMetric } from '../services/customer-metrics';
import { OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT, estimateEmbeddingTokens, getEmbedder } from '../services/embedder';
import { createRawChunkBlobKey, getRawChunkStorage, type RawChunkReference, type RawChunkStorage } from '../services/raw-chunk-storage';
import { applyRateLimitHeaders, checkQuota, consumeNormalIngestRateLimit, isPremiumPlan, recordCommittedApiQuotaReservation, reserveApiQuotaInTransaction, type RateLimitSnapshot } from '../services/usage';
import { assertIngestReplayMatches, classifyIngestReplay, ingestPayloadHash, loadLockedIngestRows } from '../services/ingest-replay';
import { beginRawChunkUploads, cleanupRawChunkWrite, confirmRawChunkUpload, lockCompletedRawChunkWrites, registerRawChunkWrites } from '../services/raw-chunk-write-lifecycle';
import { rawChunkPreparationPool, rawChunkUploadPool } from '../services/raw-chunk-work-pool';
import { withSpan } from '../telemetry';
import { cosineSimilarity } from '../utils/math';
import { contextIdentitySchema, isFutureSourceTimestamp, recallContextSchema, type RecallContext } from '../services/memory-applicability';
import { sourceEventKey } from '../services/transport-provenance';
import { provenanceIdentitySchema } from '../services/provenance-identity';

import { captureProvenanceSchema, ingestTimestampSchema } from '../services/ingest-provenance-schema';

const sourceEventSchema = z.object({
  namespace: provenanceIdentitySchema(256),
  id: provenanceIdentitySchema(),
  message_id: provenanceIdentitySchema().optional(),
  ordinal: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER).default(0)
}).strict();

const chunkSchema = z.object({
  role: z.enum(['user', 'assistant', 'tool']),
  content: z.string().min(1),
  timestamp: ingestTimestampSchema,
  source_event: sourceEventSchema.optional(),
  provenance: captureProvenanceSchema.optional()
});

export const ingestSchema = z.object({
  session_id: contextIdentitySchema,
  context: recallContextSchema.omit({ session_id: true }).optional().default({}),
  chunks: z.array(chunkSchema).min(1)
}).superRefine((body, ctx) => {
  const identities = new Set<string>();
  body.chunks.forEach((chunk, index) => {
    if (!chunk.source_event) return;
    const identity = JSON.stringify([
      chunk.source_event.namespace,
      chunk.source_event.id,
      chunk.source_event.ordinal
    ]);
    if (identities.has(identity)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['chunks', index, 'source_event'],
        message: 'Source event namespace/id/ordinal must be unique within one ingest request'
      });
    }
    identities.add(identity);
  });
});

export const bulkIngestSchema = ingestSchema;

type IngestChunk = z.infer<typeof chunkSchema>;
type QueuePriority = 'normal' | 'bulk';
type IngestChunkOutcome = {
  id: string;
  created_at: string;
  outcome: 'inserted' | 'replayed';
};
type IngestResult = {
  accepted: number;
  inserted: number;
  replayed: number;
  chunks: IngestChunkOutcome[];
  segmentsQueued: number;
  jobId?: string;
  quotaSnapshot: RateLimitSnapshot;
};

export async function registerIngestRoutes(
  app: FastifyInstance,
  triggerExtraction?: (jobId: string, vaultId?: string) => void
) {
  app.post('/v1/ingest', { preHandler: requireVaultWriteAuth }, async (request, reply) => {
    const config = getConfig();
    if (exceedsChunkCount(request.body, config.MAX_INGEST_CHUNKS)) {
      return reply.code(413).send({ error: `Too many chunks: maximum is ${config.MAX_INGEST_CHUNKS}` });
    }
    const parsed = ingestSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: 'Invalid ingest payload' });
    const body = parsed.data;
    const futureTimestampError = getFutureTimestampError(body.chunks);
    if (futureTimestampError) {
      return reply.code(400).send({ error: futureTimestampError });
    }
    const contentLimitError = getChunkContentLimitError(body.chunks, config.INGEST_CHUNK_MAX_CHARS, config.EMBEDDER_PROVIDER);
    if (contentLimitError) {
      return reply.code(413).send({ error: contentLimitError });
    }

    consumeNormalIngestRateLimit(request.vault.id, request.vault.plan_id, config.INGEST_RATE_LIMIT_RPM);

    return withSpan('ingest.request', {
      'vault.id': request.vault.id,
      'ingest.chunks_count': body.chunks.length,
      'ingest.session_id': body.session_id
    }, async (span) => {
      const result = await ingestChunksForVault(
        request.vault,
        body.session_id,
        body.context,
        body.chunks,
        'normal'
      );
      bestEffortAcceptanceTelemetry(() => {
        applyRateLimitHeaders(reply, result.quotaSnapshot);
        ingestChunksCounter.add(result.inserted, {
          vault_id: request.vault.id,
          session_id: body.session_id
        });
        span.setAttribute('ingest.accepted', result.accepted);
        span.setAttribute('ingest.inserted', result.inserted);
        span.setAttribute('ingest.replayed', result.replayed);
      });

      return reply.code(202).send({
        accepted: result.accepted,
        inserted: result.inserted,
        replayed: result.replayed,
        chunks: result.chunks
      });
    });
  });

  app.post('/v1/ingest/bulk', {
    preHandler: requireVaultWriteAuth,
    bodyLimit: getConfig().BULK_INGEST_BODY_LIMIT_BYTES
  }, async (request, reply) => {
    const config = getConfig();
    if (exceedsChunkCount(request.body, config.BULK_INGEST_MAX_CHUNKS)) {
      return reply.code(413).send({ error: `Too many chunks: maximum is ${config.BULK_INGEST_MAX_CHUNKS}` });
    }
    const parsed = bulkIngestSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: 'Invalid ingest payload' });
    const body = parsed.data;
    const futureTimestampError = getFutureTimestampError(body.chunks);
    if (futureTimestampError) {
      return reply.code(400).send({ error: futureTimestampError });
    }
    if (!isPremiumPlan(request.vault.plan_id)) {
      return reply.code(403).send({ error: 'Bulk ingest requires a premium plan' });
    }

    const contentLimitError = getChunkContentLimitError(body.chunks, config.INGEST_CHUNK_MAX_CHARS, config.EMBEDDER_PROVIDER);
    if (contentLimitError) {
      return reply.code(413).send({ error: contentLimitError });
    }

    return withSpan('ingest.bulk_request', {
      'vault.id': request.vault.id,
      'ingest.chunks_count': body.chunks.length,
      'ingest.session_id': body.session_id
    }, async (span) => {
      const jobId = crypto.randomUUID();
      const result = await ingestChunksForVault(
        request.vault,
        body.session_id,
        { ...body.context, trigger_type: 'backfill' },
        body.chunks,
        'bulk',
        jobId
      );
      bestEffortAcceptanceTelemetry(() => applyRateLimitHeaders(reply, result.quotaSnapshot));
      try {
        if (result.segmentsQueued > 0) triggerExtraction?.(result.jobId ?? jobId, request.vault.id);
      } catch (error) {
        request.log.error({ err: error, jobId: result.jobId ?? jobId }, 'Failed to trigger extraction worker for bulk ingest job');
      }

      bestEffortAcceptanceTelemetry(() => {
        ingestChunksCounter.add(result.inserted, {
          vault_id: request.vault.id,
          session_id: body.session_id
        });
        span.setAttribute('ingest.accepted', result.accepted);
        span.setAttribute('ingest.inserted', result.inserted);
        span.setAttribute('ingest.replayed', result.replayed);
        span.setAttribute('ingest.segments_queued', result.segmentsQueued);
      });

      return reply.code(202).send({
        accepted: result.accepted,
        inserted: result.inserted,
        replayed: result.replayed,
        chunks: result.chunks,
        job_id: result.jobId ?? jobId
      });
    });
  });
}

async function ingestChunksForVault(
  vault: VaultContext,
  sessionId: string,
  context: Omit<RecallContext, 'session_id'>,
  chunks: IngestChunk[],
  priority: QueuePriority,
  jobId?: string
): Promise<IngestResult> {
  const replayContext:RecallContext={...context,session_id:sessionId};
  chunks = chunks.map((chunk, index) => chunk.source_event ? chunk : {
    ...chunk,
    source_event: {
      namespace: 'persistio-content-fallback-v1',
      id: crypto.createHash('sha256')
        .update(JSON.stringify([sessionId, chunk.role, chunk.timestamp, chunk.content]))
        .digest('hex'),
      ordinal: index
    }
  });
  const config = getConfig();
  const sourceEvents = chunks.map((chunk) => chunk.source_event!);
  const sourceEventKeys = sourceEvents.map((event) => sourceEventKey(vault.id, event));
  const chunkIds = sourceEventKeys.map((key) => uuidFromSha256(key));
  const stableJobKey = jobId
    ? crypto.createHash('sha256').update(JSON.stringify(['bulk_ingest', vault.id, sourceEventKeys])).digest('hex')
    : null;
  const effectiveJobId = jobId && stableJobKey ? uuidFromSha256(stableJobKey) : jobId;
  let replay = await classifyIngestReplay(vault, chunks, sourceEventKeys,replayContext);
  if (replay.size < chunks.length) {
    try {
      await checkQuota(vault.id, 'ingest_events');
    } catch (error) {
      // A concurrent request may have consumed the final quota unit by accepting
      // this very payload between preflight lookup and the quota snapshot.
      replay = await classifyIngestReplay(vault, chunks, sourceEventKeys,replayContext);
      if (replay.size < chunks.length) throw error;
    }
  }
  const preparedIndexes = chunks.flatMap((_chunk, index) => replay.has(sourceEventKeys[index]) ? [] : [index]);
  const newChunks = preparedIndexes.map(index => chunks[index]);
  // An exact metadata-bound replay does not construct or read object storage.
  const storage = newChunks.length ? getRawChunkStorage() : undefined;
  const blobInputs = preparedIndexes.map(() => ({
    key: createRawChunkBlobKey(vault.id, sessionId, crypto.randomUUID()),
    content: '',
    storageBytes: 0
  }));
  if (storage) await registerRawChunkWrites(vault.id, storage.store, blobInputs.map(blob => blob.key), preparedIndexes.map(index => sourceEventKeys[index]));

  try {
    // Resolve the exact accepted vault key once outside mutation locks. Exact
    // replays require neither a key-provider call nor any new encrypted data.
    const preparedCrypto = newChunks.length ? await prepareVaultCrypto(vault) : undefined;
    // Admission above precedes embedding/encryption, and preparation shares a
    // process-wide bound. A late preparation result cannot initiate an upload.
    const [preparedEmbeddings = []] = await rawChunkPreparationPool.map(newChunks.length ? [newChunks] : [], batch =>
      getEmbedder().embedBatch(batch.map(chunk => chunk.content),
        { vaultId: vault.id, modelRole: 'embedding', source: 'api', inputType: 'document' }));
    const embeddings = new Map(preparedIndexes.map((index, localIndex) => [index, preparedEmbeddings[localIndex]]));
    const storedContents = newChunks.map(chunk => preparedCrypto!.encrypt(vault, chunk.content));
    blobInputs.forEach((blob, index) => {
      blob.content = storedContents[index];
      blob.storageBytes = Buffer.byteLength(blob.content, 'utf8');
    });
    const blobRefs = storage ? await putRawChunkBlobs(storage, blobInputs) : [];
    const result = await withTransaction(async (client) => {
      // One consistent SQL lock order per vault. Provider calls happened before
      // this transaction; raw lineage and the actual charge commit together.
      const owner = await client.query('SELECT id FROM vaults WHERE id = $1 FOR UPDATE', [vault.id]);
      if (!owner.rowCount) throw new Error('Vault disappeared before ingest commit');
      await preparedCrypto?.assertCurrent(client);
      if (storage) await lockCompletedRawChunkWrites(client, storage.store, blobInputs.map((blob) => blob.key));
      const existing = await loadLockedIngestRows(client, vault.id, sourceEventKeys);
      chunks.forEach((chunk, index) => {
        const row = existing.get(sourceEventKeys[index]);
        if (row) assertIngestReplayMatches(row, chunk, index,replayContext);
        else if (replay.has(sourceEventKeys[index])) {
          throw Object.assign(new Error('Previously accepted source evidence is no longer available'), { statusCode: 503 });
        }
      });
      let jobInserted = false;
      if (effectiveJobId) {
        const jobResult = await client.query(
          `INSERT INTO jobs (id, vault_id, kind, status, idempotency_key)
           VALUES ($1, $2, 'bulk_ingest', 'queued', $3)
           ON CONFLICT DO NOTHING
           RETURNING id`,
          [effectiveJobId, vault.id, stableJobKey]
        );
        jobInserted = (jobResult.rowCount ?? 0) > 0;
      }

      const localInserted = newChunks.length ? await insertRawChunks(
        client,
        vault.id,
        sessionId,
        preparedIndexes.map(index => chunkIds[index]),
        newChunks,
        blobRefs,
        blobInputs.map((blob) => blob.storageBytes),
        preparedEmbeddings,
        effectiveJobId ?? null,replayContext
      ) : [];
      const inserted = localInserted.map(row => ({ ...row, input_index: preparedIndexes[row.input_index] }));
      const outcomes = await resolveStableIngestOutcomes(
        client,
        vault.id,
        chunks,
        sourceEventKeys,
        inserted,replayContext
      );
      const quotaReservation = inserted.length > 0
        ? await reserveApiQuotaInTransaction(client, vault.id, 'ingest_events', 'api')
        : undefined;
      const insertedIndexes = inserted.map((insertedChunk) => insertedChunk.input_index);
      const insertedWithEmbeddings = inserted.map((insertedChunk) => {
        const index = insertedChunk.input_index;
        return {
          id: insertedChunk.id,
          created_at: insertedChunk.created_at,
          role: chunks[index].role,
          content: chunks[index].content,
          embedding: embeddings.get(index)!
        };
      });
      const segments = buildSegments(
        insertedWithEmbeddings,
        config.SEGMENTATION_THRESHOLD
      );

      for (const segment of segments) {
        const segmentId = crypto.randomUUID();
        const storedContext = segment.context
          ? preparedCrypto!.encrypt(vault, segment.context)
          : null;
        await client.query(
          `INSERT INTO segments (
             id, vault_id, session_id, chunk_ids, context,
             project_id, task_id, agent_id, trigger_type
           )
           VALUES ($1, $2, $3, $4::uuid[], $5, $6, $7, $8, $9)`,
          [
            segmentId,
            vault.id,
            sessionId,
            segment.chunkIds,
            storedContext,
            context.project_id ?? null,
            context.task_id ?? null,
            context.agent_id ?? null,
            context.trigger_type ?? null
          ]
        );
        await client.query(
          `INSERT INTO extraction_queue (segment_id, vault_id, priority, job_id)
           VALUES ($1, $2, $3, $4)`,
          [segmentId, vault.id, priority, effectiveJobId ?? null]
        );
      }

      if (effectiveJobId && jobInserted && segments.length === 0) {
        await client.query(
          `UPDATE jobs SET status = 'completed', updated_at = now() WHERE id = $1`,
          [effectiveJobId]
        );
      }

      // Transfer ownership of confirmed blobs to their raw rows atomically.
      // A lost COMMIT acknowledgement leaves either both or neither durable;
      // successful large ingests need no per-blob post-commit transactions.
      const insertedIndexSet = new Set(insertedIndexes);
      const committedBlobKeys = blobInputs.filter((_blob, index) =>
        insertedIndexSet.has(preparedIndexes[index])
      ).map(blob => blob.key);
      if (storage && committedBlobKeys.length) {
        await client.query(
          'DELETE FROM raw_chunk_blob_write_intents WHERE blob_store = $1 AND blob_key = ANY($2::text[])',
          [storage.store, committedBlobKeys]
        );
      }

      return {
        inserted,
        outcomes,
        segmentsQueued: segments.length,
        insertedIndexes,
        quotaReservation
      };
    });
    if (result.quotaReservation) bestEffortAcceptanceTelemetry(() => recordCommittedApiQuotaReservation(result.quotaReservation!));
    const insertedIndexSet = new Set(result.insertedIndexes);
    const reconciliationErrors = storage ? await reconcileRawChunkBlobWrites(
      storage,
      blobInputs.filter((_blob, index) => !insertedIndexSet.has(preparedIndexes[index])).map(blob => blob.key)
    ) : [];
    if (reconciliationErrors.length > 0) {
      operationalLog.warn(JSON.stringify({
        level: 40,
        msg: 'raw chunk blob reconciliation deferred after accepted ingest',
        vault_id: vault.id,
        blobs: blobInputs.length,
        errors: reconciliationErrors.map(String)
      }));
    }
    bestEffortAcceptanceTelemetry(() => recordRawChunkStorageDelta(
      vault,
      blobInputs.filter((_, index) => insertedIndexSet.has(preparedIndexes[index]))
    ));
    return {
      accepted: result.outcomes.length,
      inserted: result.inserted.length,
      replayed: result.outcomes.length - result.inserted.length,
      chunks: result.outcomes,
      quotaSnapshot: result.quotaReservation?.snapshot ?? await checkQuota(vault.id, 'ingest_events', false).catch(() => ({
        limit: null, remaining: null, resetAtEpochSeconds: null, retryAfterSeconds: null
      })),
      segmentsQueued: result.segmentsQueued,
      ...(effectiveJobId ? { jobId: effectiveJobId } : {})
    };
  } catch (error) {
    const reconciliationErrors = storage ? await reconcileRawChunkBlobWrites(
      storage,
      blobInputs.map((blob) => blob.key)
    ) : [];
    if (reconciliationErrors.length > 0) {
      // Cleanup is durable and independent of the request outcome. Preserve the
      // primary error (including quota/collision status) instead of turning it
      // into an unrelated 500 whenever the provider is also unavailable.
      operationalLog.warn(JSON.stringify({ level: 40, msg: 'raw upload cleanup deferred after failed ingest',
        vault_id: vault.id, deferred: reconciliationErrors.length }));
    }
    throw error;
  }
}

function exceedsChunkCount(payload: unknown, limit: number): boolean {
  if (!payload || typeof payload !== 'object') return false;
  const chunks = (payload as { chunks?: unknown }).chunks;
  return Array.isArray(chunks) && chunks.length > limit;
}

function sourceEventPayloadHash(chunk: IngestChunk,context:RecallContext): string {
  return ingestPayloadHash(chunk,context);
}

function uuidFromSha256(value: string): string {
  const hex = crypto.createHash('sha256').update(value).digest('hex').slice(0, 32).split('');
  hex[12] = '5';
  hex[16] = ((Number.parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
  return `${hex.slice(0, 8).join('')}-${hex.slice(8, 12).join('')}-${hex.slice(12, 16).join('')}-${hex.slice(16, 20).join('')}-${hex.slice(20).join('')}`;
}

async function reconcileRawChunkBlobWrites(storage: RawChunkStorage, blobKeys: string[]): Promise<unknown[]> {
  // Only four inline attempts, concurrently, each with a one-second IO deadline.
  // Remaining intents are already durable and handled by the fair background
  // reconciler. SQL proofs are still awaited; timing out SQL is not cancellation.
  const inline = blobKeys.slice(0, 4);
  const errors: unknown[] = blobKeys.length > inline.length
    ? [new Error('Remaining raw upload cleanup deferred to the reconciler')] : [];
  await Promise.all(inline.map(async blobKey => {
    try {
      const result = await cleanupRawChunkWrite(storage, { blobKey }, withTransaction, 1000);
      if (result === 'failed' || result === 'quarantined') {
        errors.push(new Error('Raw upload cleanup remains durably pending'));
      }
    } catch (error) {
      // A failed proof/settlement leaves the durable intent for a later sweep.
      errors.push(error);
    }
  }));
  return errors;
}

async function resolveStableIngestOutcomes(
  client: PoolClient,
  vaultId: string,
  chunks: IngestChunk[],
  sourceEventKeys: string[],
  inserted: Array<{ id: string; created_at: string; input_index: number }>,
  replayContext:RecallContext
): Promise<IngestChunkOutcome[]> {
  const insertedByIndex = new Map(inserted.map(row => [row.input_index, row]));
  const rows = await loadLockedIngestRows(client, vaultId, sourceEventKeys);
  return chunks.map((chunk, index) => {
    const newlyInserted = insertedByIndex.get(index);
    if (newlyInserted) return { id: newlyInserted.id, created_at: newlyInserted.created_at, outcome: 'inserted' as const };
    const row = rows.get(sourceEventKeys[index]);
    if (!row) throw new Error(`Ingest did not durably account for input chunk ${index}`);
    assertIngestReplayMatches(row, chunk, index,replayContext);
    return { id: row.id, created_at: row.created_at, outcome: 'replayed' as const };
  });
}

function bestEffortAcceptanceTelemetry(work: () => void): void {
  try { work(); } catch { operationalLog.warn('Ingest accepted; nonessential acceptance telemetry unavailable'); }
}

function recordRawChunkStorageDelta(
  vault: VaultContext,
  blobs: Array<{ storageBytes: number }>
): void {
  if (!vault.account_id) return;
  const storageBytes = blobs.reduce((total, blob) => total + blob.storageBytes, 0);
  if (storageBytes === 0) return;

  recordCustomerMetric({
    event_type: 'storage_delta',
    operation: 'raw_chunk_blob_write',
    source: 'api',
    storage_bytes_delta: storageBytes,
    vault_id: vault.id,
    workspace_id: vault.account_id
  });
}

async function putRawChunkBlobs(
  storage: RawChunkStorage,
  blobs: Array<{ key: string; content: string }>
): Promise<RawChunkReference[]> {
  return rawChunkUploadPool.map(blobs, async (blob) => {
      // The process slot is held before the durable transition. Revoked queued
      // work cannot start a PUT after timeout/failure cleanup.
      await beginRawChunkUploads(storage.store, [blob.key]);
      const ref = await storage.put(blob.key, blob.content);
      if (ref.blobStore !== storage.store || ref.blobKey !== blob.key) {
        throw new Error('Storage returned a different raw upload identity');
      }
      await confirmRawChunkUpload(storage.store, blob.key);
      return ref;
  });
}

function getChunkContentLimitError(chunks: IngestChunk[], maxChars: number, embedderProvider: string): string | undefined {
  const oversizedIndex = chunks.findIndex((chunk) => chunk.content.length > maxChars);
  if (oversizedIndex !== -1) {
    return `Chunk ${oversizedIndex} content is too large: maximum is ${maxChars} characters`;
  }

  if (embedderProvider !== 'openai') {
    return undefined;
  }

  const providerLimitIndex = chunks.findIndex((chunk) =>
    estimateEmbeddingTokens(chunk.content) > OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT
  );
  if (providerLimitIndex !== -1) {
    return `Chunk ${providerLimitIndex} content is too large for embedding: estimated token maximum is ${OPENAI_EMBEDDING_MAX_TOKENS_PER_INPUT}`;
  }

  return undefined;
}

export function getFutureTimestampError(chunks: IngestChunk[], now = new Date()): string | undefined {
  const futureIndex = chunks.findIndex((chunk) => isFutureSourceTimestamp(chunk.timestamp, now));
  return futureIndex === -1
    ? undefined
    : `Chunk ${futureIndex} timestamp is more than 5 minutes in the future`;
}

async function insertRawChunks(
  client: PoolClient,
  vaultId: string,
  sessionId: string,
  chunkIds: string[],
  chunks: IngestChunk[],
  blobRefs: RawChunkReference[],
  storageBytes: number[],
  embeddings: number[][],
  jobId: string | null,
  replayContext:RecallContext
): Promise<Array<{ id: string; created_at: string; input_index: number }>> {
  const sourceEvents = chunks.map((chunk) => chunk.source_event ?? null);
  const sourceEventKeys = sourceEvents.map((event) => event ? sourceEventKey(vaultId, event) : null);
  const result = await client.query<{ id: string; created_at: string; input_index: number }>(
    `WITH input AS (
      SELECT *
      FROM UNNEST(
         $3::uuid[], $4::text[], $5::text[], $6::text[], $7::bigint[],
         $8::text[], $9::timestamptz[], $10::jsonb[], $11::text[], $12::text[], $13::text[], $14::text[],
         $15::bigint[], $16::text[]
       ) WITH ORDINALITY
         AS input(id, role, blob_store, blob_key, storage_bytes, embedding, created_at, provenance,
                  source_event_namespace, source_event_id, source_message_id, source_event_key,
                  source_event_ordinal, source_event_payload_sha256, input_ordinal)
     ),
     inserted AS (
       INSERT INTO raw_chunks (
         id, vault_id, session_id, role, blob_store, blob_key, storage_bytes, embedding,
         created_at, provenance, source_event_namespace, source_event_id, source_message_id,
         source_event_key, source_event_ordinal, source_event_payload_sha256, ingest_job_id,capture_context
       )
       SELECT input.id, $1, $2, input.role, input.blob_store, input.blob_key,
              input.storage_bytes, input.embedding::vector, input.created_at, input.provenance,
              input.source_event_namespace, input.source_event_id, input.source_message_id,
              input.source_event_key, input.source_event_ordinal, input.source_event_payload_sha256, $17::uuid,$18::jsonb
       FROM input
       ORDER BY input.input_ordinal
       ON CONFLICT (vault_id, source_event_key) WHERE source_event_key IS NOT NULL DO NOTHING
       RETURNING id, created_at
     )
     SELECT inserted.id, inserted.created_at, (input.input_ordinal - 1)::int AS input_index
     FROM input
     JOIN inserted
       ON inserted.id = input.id
     ORDER BY input.input_ordinal`,
    [
      vaultId,
      sessionId,
      chunkIds,
      chunks.map((chunk) => chunk.role),
      blobRefs.map((ref) => ref.blobStore),
      blobRefs.map((ref) => ref.blobKey),
      storageBytes,
      embeddings.map((embedding) => JSON.stringify(embedding)),
      chunks.map((chunk) => chunk.timestamp),
      chunks.map((chunk) => chunk.provenance ? JSON.stringify(chunk.provenance) : null),
      sourceEvents.map((event) => event?.namespace ?? null),
      sourceEvents.map((event) => event?.id ?? null),
      sourceEvents.map((event) => event?.message_id ?? null),
      sourceEventKeys,
      sourceEvents.map((event) => event?.ordinal ?? null),
      chunks.map((chunk) => chunk.source_event ? sourceEventPayloadHash(chunk,replayContext) : null),
      jobId,JSON.stringify(replayContext)
    ]
  );

  return result.rows;
}

interface InsertedChunk {
  id: string;
  role: string;
  content: string;
  embedding: number[];
}

interface SegmentDraft {
  chunkIds: string[];
  context: string | null;
}

function buildSegments(
  chunks: InsertedChunk[],
  threshold: number
): SegmentDraft[] {
  const minSize = 3;
  const maxSize = 40;
  if (!chunks.length) {
    return [];
  }

  const drafts: InsertedChunk[][] = [];
  let current: InsertedChunk[] = [chunks[0]];

  for (let index = 1; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    const similarity = cosineSimilarity(current[current.length - 1].embedding, chunk.embedding);
    const remaining = chunks.length - index;
    const shouldSplitForSimilarity = similarity < threshold && current.length >= minSize && remaining >= minSize;
    const shouldSplitForSize = current.length >= maxSize;

    if (shouldSplitForSimilarity || shouldSplitForSize) {
      drafts.push(current);
      current = [chunk];
      continue;
    }

    current.push(chunk);
  }

  if (current.length < minSize && drafts.length) {
    drafts[drafts.length - 1].push(...current);
  } else {
    drafts.push(current);
  }

  return drafts.map((draft) => ({
    chunkIds: draft.map((chunk) => chunk.id),
    context: buildSegmentContext(draft)
  }));
}

function buildSegmentContext(chunks: InsertedChunk[]): string | null {
  const preview = chunks
    .map((chunk) => chunk.content.trim())
    .find((content) => content.length > 0);

  return preview ? preview.slice(0, 280) : null;
}
