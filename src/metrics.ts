import type { ObservableResultLike } from './telemetry';
import { meter } from './telemetry';
import { query } from './db/client';
import { EXTRACTION_QUEUE_READY_PREDICATE } from './services/extraction-queue-eligibility';

export const recallDurationHistogram = meter.createHistogram('persistio.recall.duration', {
  description: 'Recall latency',
  unit: 'ms'
});

export const ingestChunksCounter = meter.createCounter('persistio.ingest.chunks.total', {
  description: 'Chunks ingested'
});

export const extractionJobsCounter = meter.createCounter('persistio.extraction.jobs.total', {
  description: 'Extraction jobs by status'
});

export const extractionLagHistogram = meter.createHistogram('persistio.extraction.lag_ms', {
  description: 'Time from ingest to memory creation',
  unit: 'ms'
});

export const extractionCandidatesCounter = meter.createCounter('persistio.extraction.candidates.total', {
  description: 'Memory candidates accepted or dropped during extraction'
});

export const aiBudgetWaitHistogram = meter.createHistogram('persistio.ai_budget.wait_ms', {
  description: 'Time background AI work waits for vault-local AI budget',
  unit: 'ms'
});

export const aiBudgetThrottledJobsCounter = meter.createCounter('persistio.ai_budget.throttled_jobs.total', {
  description: 'Background jobs deferred because vault-local AI budget was unavailable'
});

export const embeddingDurationHistogram = meter.createHistogram('persistio.embedding.duration', {
  description: 'Embedding call latency',
  unit: 'ms'
});

export const eventOutboxDispatchCounter = meter.createCounter('persistio.event_outbox.dispatch.total', {
  description: 'Event outbox dispatch cycle and row outcomes'
});

export const eventOutboxPublishFailureCounter = meter.createCounter('persistio.event_outbox.publish_failures.total', {
  description: 'Event outbox publish failures by adapter and event type'
});

export const eventOutboxLagHistogram = meter.createHistogram('persistio.event_outbox.lag_ms', {
  description: 'Age of oldest pending platform event outbox row during dispatch',
  unit: 'ms'
});

export const memoriesTotalGauge = meter.createObservableGauge('persistio.memories.total', {
  description: 'Total memories per vault'
});

meter.createObservableGauge('persistio.extraction_queue_depth', {
  description: 'Number of claimable rows in extraction_queue'
}).addCallback(async (result: ObservableResultLike) => {
  const { rows } = await query<{ depth: number }>(
    `SELECT COUNT(*)::int AS depth
     FROM extraction_queue eq
     WHERE ${EXTRACTION_QUEUE_READY_PREDICATE}`
  );
  result.observe(rows[0]?.depth ?? 0);
});

meter.createObservableGauge('persistio.ai_budget.waiting_jobs', {
  description: 'Queued jobs deferred until local AI budget becomes available'
}).addCallback(async (result: ObservableResultLike) => {
  const { rows } = await query<{ queue: string; depth: number }>(
    `SELECT 'extraction' AS queue, COUNT(*)::int AS depth
     FROM extraction_queue
     WHERE claimed_at IS NULL AND available_at > now()
     UNION ALL
     SELECT 'curation' AS queue, COUNT(*)::int AS depth
     FROM curation_queue
     WHERE claimed_at IS NULL AND available_at > now()`
  );
  for (const row of rows) result.observe(row.depth, { queue: row.queue });
});

memoriesTotalGauge.addCallback(async (observableResult: ObservableResultLike) => {
  const result = await query<{ vault_id: string; total: string }>(
    `SELECT vault_id, COUNT(*)::text AS total
     FROM memories
     WHERE archived_at IS NULL
     GROUP BY vault_id`
  );

  for (const row of result.rows) {
    observableResult.observe(Number(row.total), { vault_id: row.vault_id });
  }
});
