import { parentPort } from 'node:worker_threads';

import { getConfig } from '../config';
import { closePool, query, runMigrations } from '../db/client';
import { extractionJobsCounter, extractionLagHistogram } from '../metrics';
import { deduplicateMemory } from '../services/dedup';
import { getEmbedder } from '../services/embedder';
import { ExtractorService } from '../services/extractor';
import { archiveStaleMemories } from '../services/staleness';
import { getSpanAttributes, withSpan } from '../telemetry';

interface RawChunkRow {
  id: string;
  tenant_id: string;
  session_id: string;
  role: string;
  content: string;
  created_at: string;
}

interface WorkerRequest {
  type: 'run-once';
  jobId?: string;
  tenantId?: string;
}

const config = getConfig();
const embedder = getEmbedder();
const extractor = new ExtractorService();

async function processBatch(tenantId?: string) {
  return withSpan('extraction.process_batch', {
    'tenant.id': tenantId,
    'extraction.batch_limit': config.EXTRACTION_BATCH_SIZE
  }, async (span) => {
    const values: unknown[] = [];
    const tenantClause = tenantId ? `AND tenant_id = $1` : '';

    if (tenantId) {
      values.push(tenantId);
    }

    values.push(config.EXTRACTION_BATCH_SIZE);

    const chunksResult = await query<RawChunkRow>(
      `WITH claimed AS (
         SELECT id
         FROM raw_chunks
         WHERE processed = false
           ${tenantClause}
         ORDER BY created_at ASC
         LIMIT $${values.length}
         FOR UPDATE SKIP LOCKED
       )
       UPDATE raw_chunks AS rc
       SET processed = true
       FROM claimed
       WHERE rc.id = claimed.id
       RETURNING rc.id, rc.tenant_id, rc.session_id, rc.role, rc.content, rc.created_at`,
      values
    );

    span.setAttribute('extraction.batch_size', chunksResult.rows.length);
    if (!chunksResult.rowCount) {
      span.setAttribute('extraction.memories_created', 0);
      return 0;
    }

    const processedChunkIds: string[] = [];
    let memoriesCreated = 0;

    try {
      for (const chunk of chunksResult.rows) {
        const embedding = await embedder.embed(chunk.content);
        await query(
          `UPDATE raw_chunks
           SET embedding = $2::vector
           WHERE id = $1`,
          [chunk.id, JSON.stringify(embedding)]
        );
        processedChunkIds.push(chunk.id);
      }

      const grouped = new Map<string, RawChunkRow[]>();
      for (const chunk of chunksResult.rows) {
        const key = `${chunk.tenant_id}:${chunk.session_id}`;
        const existing = grouped.get(key) ?? [];
        existing.push(chunk);
        grouped.set(key, existing);
      }

      for (const sessionChunks of grouped.values()) {
        const conversation = sessionChunks
          .map((chunk) => `${chunk.role}: ${chunk.content}`)
          .join('\n');
        const facts = await extractor.extractFacts(conversation);

        for (const fact of facts) {
          const embedding = await embedder.embed(fact.fact);
          const result = await deduplicateMemory({
            tenantId: sessionChunks[0].tenant_id,
            fact: fact.fact,
            subject: fact.subject,
            embedding,
            sourceChunks: sessionChunks.map((chunk) => chunk.id)
          }, undefined, extractor);

          if (result.action === 'inserted' || result.action === 'updated') {
            memoriesCreated += 1;
          }

          const oldestChunkAt = Math.min(...sessionChunks.map((chunk) => new Date(chunk.created_at).getTime()));
          extractionLagHistogram.record(Date.now() - oldestChunkAt, {
            tenant_id: sessionChunks[0].tenant_id,
            session_id: sessionChunks[0].session_id,
            dedup_action: result.action
          });
        }
      }
    } catch (error) {
      console.error(getSpanAttributes({ error }), 'Extraction batch failed');
      throw error;
    } finally {
      span.setAttribute('extraction.memories_created', memoriesCreated);
      await archiveStaleMemories();
    }

    return processedChunkIds.length;
  });
}

async function runLoop() {
  await runMigrations();

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
    await processBatch(message.tenantId);
    extractionJobsCounter.add(1, {
      status: 'success',
      tenant_id: message.tenantId ?? 'all'
    });
    if (message.jobId) {
      parentPort.postMessage({ type: 'job-status', jobId: message.jobId, status: 'completed' });
    }
  } catch (error) {
    extractionJobsCounter.add(1, {
      status: 'error',
      tenant_id: message.tenantId ?? 'all'
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
