import { describe, expect, it } from 'vitest';

import { bulkIngestSchema, getFutureTimestampError, ingestSchema } from './ingest';

describe('ingest schema', () => {
  it('shares normalized and bounded session identities across normal and bulk ingest', () => {
    for (const schema of [ingestSchema, bulkIngestSchema]) {
      const payload = { session_id: ' session-1 ', chunks: [{ role: 'user', content: 'hello', timestamp: '2026-05-12T16:00:00Z' }] };
      expect(schema.parse(payload).session_id).toBe('session-1');
      expect(schema.safeParse({ ...payload, session_id: 's'.repeat(512) }).success).toBe(true);
      for (const session_id of ['', ' ', '\nsession', 'session\t', 'bad\u0000id', 'bad\u007fid', 's'.repeat(513)]) {
        expect(schema.safeParse({ ...payload, session_id }).success).toBe(false);
      }
    }
  });
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

  it('accepts bounded source identity and nested transport/import provenance', () => {
    const parsed = ingestSchema.safeParse({
      session_id: 'replay-session',
      context: { trigger_type: 'backfill' },
      chunks: [{
        role: 'user',
        content: 'Historical event',
        timestamp: '2026-05-12T16:00:00.000Z',
        source_event: {
          namespace: 'persistio-replay-v2',
          id: 'stable-source-event',
          message_id: 'source-message-123'
        },
        provenance: {
          source_class: 'agent_slack',
          actor_type: 'agent',
          trigger_type: 'backfill',
          artifact_type: 'message',
          authorship: 'generated',
          cadence: 'batch',
          provenance_basis: ['api_provenance', 'transport_envelope'],
          payload_author: { actor_type: 'agent', authorship: 'generated', is_user: false },
          transport: {
            initiator_actor_type: 'import',
            initiator_id: 'job-123',
            receiver_actor_type: 'agent',
            source_session_id: 'agent:main:slack:direct:user',
            source_channel: 'slack',
            source_tool: 'sessions_send'
          },
          import: {
            importer: 'persistio-v2-replay',
            importer_version: '2.0.0',
            dataset_sha256: 'a'.repeat(64),
            import_job_id: 'job-123',
            original_timestamp: '2026-05-12T16:00:00.000Z'
          }
        }
      }]
    });
    expect(parsed.success).toBe(true);
    if (parsed.success) expect(parsed.data.chunks[0].source_event?.ordinal).toBe(0);
  });

  it('rejects partial, unbounded, and control-bearing source identities', () => {
    const base = {
      session_id: 'replay-session',
      chunks: [{
        role: 'user',
        content: 'Historical event',
        timestamp: '2026-05-12T16:00:00.000Z'
      }]
    };
    expect(ingestSchema.safeParse({
      ...base,
      chunks: [{ ...base.chunks[0], source_event: { namespace: 'replay' } }]
    }).success).toBe(false);
    expect(ingestSchema.safeParse({
      ...base,
      chunks: [{ ...base.chunks[0], source_event: { namespace: 'replay\nspoof', id: 'event' } }]
    }).success).toBe(false);
    expect(ingestSchema.safeParse({
      ...base,
      chunks: [{ ...base.chunks[0], source_event: { namespace: 'x'.repeat(257), id: 'event' } }]
    }).success).toBe(false);
  });

  it('rejects duplicate source-event parts within one request', () => {
    const sourceEvent = { namespace: 'openclaw-capture', id: 'message-1', ordinal: 0 };
    expect(ingestSchema.safeParse({
      session_id: 'session-1',
      chunks: [
        { role: 'user', content: 'First', timestamp: '2026-05-12T16:00:00.000Z', source_event: sourceEvent },
        { role: 'user', content: 'Second', timestamp: '2026-05-12T16:00:01.000Z', source_event: sourceEvent }
      ]
    }).success).toBe(false);
  });

  it('accepts bounded structural applicability context and rejects control text', () => {
    expect(ingestSchema.safeParse({
      session_id: 'session-1',
      context: { project_id: 'persistio', task_id: 'issue-349', agent_id: 'main', trigger_type: 'direct' },
      chunks: [{ role: 'user', content: 'Hello', timestamp: '2026-05-12T16:00:00.000Z' }]
    }).success).toBe(true);
    expect(ingestSchema.safeParse({
      session_id: 'session-1',
      context: { project_id: 'persistio\noverride' },
      chunks: [{ role: 'user', content: 'Hello', timestamp: '2026-05-12T16:00:00.000Z' }]
    }).success).toBe(false);
  });

  it('rejects source timestamps beyond the bounded clock-skew allowance', () => {
    const chunks = ingestSchema.parse({
      session_id: 'session-1',
      chunks: [{ role: 'user', content: 'Hello', timestamp: '2026-05-12T16:06:00.000Z' }]
    }).chunks;
    expect(getFutureTimestampError(chunks, new Date('2026-05-12T16:00:00.000Z'))).toContain('more than 5 minutes');
    expect(getFutureTimestampError(chunks, new Date('2026-05-12T16:01:00.000Z'))).toBeUndefined();
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
