import crypto from 'node:crypto';
import { parentPort } from 'node:worker_threads';

import { getConfig } from '../config';
import { closePool, query, runMigrations } from '../db/client';
import { extractionJobsCounter, extractionLagHistogram } from '../metrics';
import { decryptForVault, initCryptoClient } from '../services/crypto';
import { deduplicateMemory } from '../services/dedup';
import { getEmbedder } from '../services/embedder';
import { ExtractorService } from '../services/extractor';
import { archiveStaleMemories } from '../services/staleness';
import { getSpanAttributes, withSpan } from '../telemetry';

interface RawChunkRow {
  id: string;
  queue_id: string;
  vault_id: string;
  encrypted_dek: string | null;
  session_id: string;
  role: string;
  content: string;
  created_at: string;
  vault_encryption_enabled: boolean;
}

interface WorkerRequest {
  type: 'run-once';
  jobId?: string;
  vaultId?: string;
}

const config = getConfig();
const embedder = getEmbedder();
const extractor = new ExtractorService();
const workerId = crypto.randomUUID();

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

    const claimedResult = await query<{ queue_id: string; chunk_id: string; vault_id: string }>(
      `WITH claimed AS (
         SELECT eq.id AS queue_id, eq.chunk_id, eq.vault_id
         FROM extraction_queue eq
         WHERE eq.claimed_at IS NULL
           ${vaultClause}
         ORDER BY eq.enqueued_at ASC
         LIMIT $${vaultId ? 2 : 1}
         FOR UPDATE SKIP LOCKED
       )
       UPDATE extraction_queue eq
       SET claimed_at = now(), claimed_by = $${vaultId ? 3 : 2}
       FROM claimed
       WHERE eq.id = claimed.queue_id
       RETURNING claimed.queue_id, claimed.chunk_id, claimed.vault_id`,
      values
    );

    span.setAttribute('extraction.batch_size', claimedResult.rows.length);
    if (!claimedResult.rowCount) {
      span.setAttribute('extraction.memories_created', 0);
      return 0;
    }

    const queueIdByChunkId = new Map(claimedResult.rows.map((row) => [row.chunk_id, row.queue_id]));
    const chunkIds = claimedResult.rows.map((row) => row.chunk_id);
    const chunksResult = await query<RawChunkRow>(
      `SELECT rc.id, $1::uuid AS queue_id, rc.vault_id, v.encrypted_dek, rc.session_id, rc.role, rc.content,
              rc.created_at, v.vault_encryption_enabled
       FROM raw_chunks rc
       JOIN vaults v ON v.id = rc.vault_id
       WHERE rc.id = ANY($2::uuid[])`,
      ['', chunkIds]
    );

    const chunks = chunksResult.rows.map((row) => ({
      ...row,
      queue_id: queueIdByChunkId.get(row.id) ?? ''
    }));

    span.setAttribute('extraction.batch_size', chunks.length);
    let memoriesCreated = 0;

    try {
      const grouped = new Map<string, RawChunkRow[]>();
      for (const chunk of chunks) {
        const key = `${chunk.vault_id}:${chunk.session_id}`;
        const existing = grouped.get(key) ?? [];
        existing.push(chunk);
        grouped.set(key, existing);
      }

      for (const sessionChunks of grouped.values()) {
        try {
          const decryptedChunks = await Promise.all(sessionChunks.map(async (chunk) => {
            const content = await decryptForVault(chunk, chunk.content);
            const embedding = await embedder.embed(content);
            await query(
              `UPDATE raw_chunks
               SET embedding = $2::vector
               WHERE id = $1`,
              [chunk.id, JSON.stringify(embedding)]
            );
            return {
              ...chunk,
              decryptedContent: content
            };
          }));

          const conversation = decryptedChunks
            .map((chunk) => `${chunk.role}: ${chunk.decryptedContent}`)
            .join('\n');
          const facts = await extractor.extractFacts(conversation);
          const threshold = getConfig().EXTRACTION_SCORE_THRESHOLD;
          const filteredFacts = facts.filter((fact) => fact.score >= threshold);
          if (filteredFacts.length < facts.length) {
            console.log(JSON.stringify({ level: 30, msg: 'score threshold filtered facts', dropped: facts.length - filteredFacts.length, total: facts.length, threshold }));
          }

          for (const fact of filteredFacts) {
            const embedding = await embedder.embed(fact.fact);
            const result = await deduplicateMemory({
              vaultId: sessionChunks[0].vault_id,
              fact: fact.fact,
              score: fact.score,
              subject: fact.subject,
              embedding,
              sourceChunks: sessionChunks.map((chunk) => chunk.id)
            }, undefined, extractor);

            if (result.action === 'inserted' || result.action === 'updated') {
              memoriesCreated += 1;
            }

            const oldestChunkAt = Math.min(...sessionChunks.map((chunk) => new Date(chunk.created_at).getTime()));
            extractionLagHistogram.record(Date.now() - oldestChunkAt, {
              vault_id: sessionChunks[0].vault_id,
              session_id: sessionChunks[0].session_id,
              dedup_action: result.action
            });
          }

          for (const chunk of sessionChunks) {
            await query(`DELETE FROM extraction_queue WHERE id = $1`, [chunk.queue_id]);
            await query(`UPDATE raw_chunks SET processed = true WHERE id = $1`, [chunk.id]);
          }
        } catch (error) {
          const lastError = error instanceof Error ? error.message : 'Unknown extraction error';
          console.error(getSpanAttributes({ error, sessionId: sessionChunks[0]?.session_id }), 'Extraction session failed');

          for (const chunk of sessionChunks) {
            await query(
              `UPDATE extraction_queue
               SET retry_count = retry_count + 1,
                   last_error = $2,
                   claimed_at = NULL
               WHERE id = $1`,
              [chunk.queue_id, lastError]
            );
          }
        }
      }
    } finally {
      span.setAttribute('extraction.memories_created', memoriesCreated);
      await archiveStaleMemories();
    }

    return claimedResult.rows.length;
  });
}

async function runLoop() {
  await runMigrations();
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
