import type { ObservableResultLike } from './telemetry';
import { meter } from './telemetry';
import { query } from './db/client';

export const httpRequestDurationHistogram = meter.createHistogram('persistio.http.request.duration', {
  description: 'Request latency by route and status',
  unit: 'ms'
});

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

export const embeddingDurationHistogram = meter.createHistogram('persistio.embedding.duration', {
  description: 'Embedding call latency',
  unit: 'ms'
});

export const memoriesTotalGauge = meter.createObservableGauge('persistio.memories.total', {
  description: 'Total memories per tenant'
});

memoriesTotalGauge.addCallback(async (observableResult: ObservableResultLike) => {
  const result = await query<{ tenant_id: string; total: string }>(
    `SELECT tenant_id, COUNT(*)::text AS total
     FROM memories
     WHERE archived_at IS NULL
     GROUP BY tenant_id`
  );

  for (const row of result.rows) {
    observableResult.observe(Number(row.total), { tenant_id: row.tenant_id });
  }
});
