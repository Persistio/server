import { beforeAll, describe, expect, it } from 'vitest';
import { ingestSchema, bulkIngestSchema } from '../routes/ingest';
import { inferExtractionProvenance, getProvenancePreGate, requiresBehavioralReview } from './extraction-provenance';

let malformed: Array<{name: string; value: any}>;
let human: any;
let prepare: any;
beforeAll(async () => {
  const fixture = await import(new URL('../../../../scripts/lib/replay-contract-cases.mjs', import.meta.url).href);
  malformed = fixture.malformedProvenanceCases(); human = fixture.humanSource;
  ({ prepareReplayDataset: prepare } = await import(new URL('../../../../scripts/lib/replay-dataset.mjs', import.meta.url).href));
});
const humanHeader = '[Inter-session message] sourceSession=sender sourceChannel=internal sourceTool=sessions_send isUser=true\nHuman source';
const base = { role: 'user', content: 'source', timestamp: '2026-06-01T00:00:00Z' };
const profile = (chunks: any[], triggerType: 'backfill' | 'direct' = 'backfill') => inferExtractionProvenance({ sessionId: 'session', chunks, triggerType });

describe('whole provenance contract across API, replay and historical extraction', () => {
  it('rejects every malformed field at both HTTP schemas and blocks historical equivalents in any grouping', () => {
    for (const { name, value } of malformed) {
      for (const schema of [ingestSchema, bulkIngestSchema]) {
        expect(schema.safeParse({ session_id: 'session', chunks: [{ ...base, provenance: value }] }).success, name).toBe(false);
      }
      for (const content of ['source', humanHeader]) {
        const bad = { ...base, content, provenance: value };
        for (const chunks of [[bad], [bad, base], [base, bad]]) {
          for (const trigger of ['backfill', 'direct'] as const) {
            expect(getProvenancePreGate(profile(chunks, trigger))?.decision, name).toBe('noop');
          }
        }
      }
    }
  });

  it('keeps contradictory, generated, mixed and unknown valid source evidence restrictive through full conversion', () => {
    const variants = [
      { actor_type: 'agent', authorship: 'generated' },
      { actor_type: 'unknown', authorship: 'unknown' },
      { actor_type: 'human', authorship: 'mixed' },
      { payload_author: { actor_type: 'agent', authorship: 'original', is_user: true } },
      { payload_author: { actor_type: 'human', authorship: 'generated', is_user: true } },
      { payload_author: { actor_type: 'human', authorship: 'original', is_user: false } },
      { payload_author: { actor_type: 'human', authorship: 'original', is_user: null } }
    ];
    for (const variant of variants) for (const content of ['source', humanHeader]) {
      const historical = { ...base, content, provenance: { ...human, ...variant } };
      for (const chunks of [[historical], [historical, base], [base, historical]]) {
        expect(getProvenancePreGate(profile(chunks))?.decision, JSON.stringify(variant)).toBe('noop');
      }
      const rows = [
        { segment_id: 'header', session_id: 'session', created_at: base.timestamp,
          chunks: [{ id: 'header', ...base, content, event_id: 'event', provenance: { ...human, ...variant } }] },
        { segment_id: 'tail', session_id: 'session', created_at: base.timestamp,
          chunks: [{ id: 'tail', ...base, content: 'continuation', event_id: 'event' }] }
      ];
      for (const order of [rows, [...rows].reverse()]) {
        const prepared = prepare(order, { datasetSha256: 'a'.repeat(64), importJobId: 'job' });
        for (const segment of prepared) expect(getProvenancePreGate(profile(segment.chunks))?.decision, JSON.stringify(variant)).toBe('noop');
        expect(getProvenancePreGate(profile(prepared.flatMap((row: any) => row.chunks)))?.decision).toBe('noop');
      }
    }
  });

  it('retains legitimate ordinary imports, ordinary conversations and human transport with behavioural review', () => {
    const ordinary = prepare([{ segment_id: 'ordinary', session_id: 'session', created_at: base.timestamp,
      chunks: ['user', 'assistant', 'tool'].map((role, index) => ({ ...base, id: String(index), role })) }],
    { datasetSha256: 'a'.repeat(64), importJobId: 'job' });
    expect(getProvenancePreGate(profile(ordinary[0].chunks))).toBeNull();
    expect(requiresBehavioralReview(profile(ordinary[0].chunks), 'user_rule')).toBe(true);
    const carried = prepare([{ segment_id: 'human', session_id: 'session', created_at: base.timestamp,
      chunks: [{ ...base, content: humanHeader, provenance: human }] }], { datasetSha256: 'a'.repeat(64), importJobId: 'job' });
    expect(getProvenancePreGate(profile(carried[0].chunks))).toBeNull();
    expect(requiresBehavioralReview(profile(carried[0].chunks), 'user_preference')).toBe(true);
    expect(getProvenancePreGate(profile([{ ...base, role: 'user' }, { ...base, role: 'assistant' }], 'direct'))).toBeNull();
  });
});
