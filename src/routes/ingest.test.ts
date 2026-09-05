import { describe, expect, it } from 'vitest';

import { bulkIngestSchema, ingestSchema } from './ingest';

describe('ingest schema', () => {
  it('accepts ISO 8601 source timestamps with UTC or explicit offsets', () => {
    for (const timestamp of ['2026-05-12T16:00:00.000Z', '2026-05-12T16:00:00+02:00']) {
      expect(ingestSchema.safeParse({
        session_id: 'session-1',
        chunks: [{ role: 'user', content: 'Hello', timestamp }]
      }).success).toBe(true);
    }
  });

  it('still rejects timestamps without timezone information', () => {
    expect(ingestSchema.safeParse({
      session_id: 'session-1',
      chunks: [{ role: 'user', content: 'Hello', timestamp: '2026-05-12T16:00:00' }]
    }).success).toBe(false);
  });

  it('uses the same timestamp contract for bulk ingest', () => {
    expect(bulkIngestSchema.safeParse({
      session_id: 'session-1',
      chunks: [{ role: 'assistant', content: 'Bulk hello', timestamp: '2026-05-12T16:00:00.000Z' }]
    }).success).toBe(true);
  });

  it('accepts explicit capture provenance on chunks', () => {
    expect(ingestSchema.safeParse({
      session_id: 'agent:main:cron:daily',
      chunks: [{
        role: 'assistant',
        content: 'Cron observation.',
        timestamp: '2026-05-12T16:00:00.000Z',
        provenance: {
          source_class: 'agent_cron',
          actor_type: 'agent',
          trigger_type: 'scheduled',
          artifact_type: 'observation',
          authorship: 'generated',
          cadence: 'recurring',
          provenance_confidence: 0.99,
          provenance_basis: ['plugin_capture']
        }
      }]
    }).success).toBe(true);
  });

  it('rejects arbitrary provenance basis text', () => {
    expect(ingestSchema.safeParse({
      session_id: 'agent:main:cron:daily',
      chunks: [{
        role: 'assistant',
        content: 'Cron observation.',
        timestamp: '2026-05-12T16:00:00.000Z',
        provenance: {
          source_class: 'agent_cron',
          actor_type: 'agent',
          trigger_type: 'scheduled',
          artifact_type: 'observation',
          authorship: 'generated',
          cadence: 'recurring',
          provenance_basis: ['user-id:12345']
        }
      }]
    }).success).toBe(false);
  });
});
