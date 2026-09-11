import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { captureProvenanceSchema } from './ingest-provenance-schema';
import { combineProvenanceEvidence, evaluateProvenanceEvidence, INITIAL_EVIDENCE } from './provenance-evidence';
import { getProvenancePreGate, inferExtractionProvenance, requiresBehavioralReview,
  type ProvenanceChunk, type ProvenanceTriggerType } from './extraction-provenance';

// Independent policy oracle: no importer/transport-bearing shared human fixture.
const human = { actor_type: 'human', authorship: 'original', trigger_type: 'direct',
  artifact_type: 'observation', cadence: 'one_off',
  payload_author: { actor_type: 'human', authorship: 'original', is_user: true } };
const importMetadata = { importer: 'persistio-v2-replay', importer_version: '2.3.0',
  dataset_sha256: 'a'.repeat(64), import_job_id: 'test', original_timestamp: '2026-06-01T00:00:00Z' };
const transport = { initiator_actor_type: 'agent', receiver_actor_type: 'agent' };
const actors = ['human', 'assistant', 'agent', 'tool', 'system', 'import', 'unknown'];
const authorships = ['original', 'generated', 'transcribed', 'imported', 'mixed', 'unknown'];
const humanIdentities = ['human/original', 'human/transcribed', 'human/imported'];
const generatedIdentities = ['assistant/generated', 'agent/generated', 'tool/generated', 'system/generated'];
const chunk = (provenance: unknown = human, content = 'A factual observation'): ProvenanceChunk => ({ role: 'user', content, provenance });
const profile = (chunks: ProvenanceChunk[], triggerType?: ProvenanceTriggerType, sessionId = 'direct-api-client') =>
  inferExtractionProvenance({ sessionId, chunks, triggerType });
const verdict = (chunks: ProvenanceChunk[], trigger?: ProvenanceTriggerType) => {
  const p = profile(chunks, trigger);
  return { blocked: getProvenancePreGate(p) !== null, review: requiresBehavioralReview(p, 'user_rule') };
};
const header = (user: string) => `[Inter-session message] sourceSession=sender isUser=${user}\nA factual observation`;

describe('complete source consistency without relying on transport presence', () => {
  it('combines restrictions associatively, commutatively and idempotently without losing unknown constituents', () => {
    const inputs = [null, human, { ...human, import: importMetadata },
      { ...human, payload_author: { actor_type: 'agent', authorship: 'original', is_user: true } }];
    const summaries = inputs.map(source => evaluateProvenanceEvidence(source === null ? null : captureProvenanceSchema.parse(source), false, null));
    summaries.push(evaluateProvenanceEvidence(null, true, null));
    const combine = combineProvenanceEvidence;
    for (const a of summaries) {
      expect(combine(a, a)).toEqual(a);
      expect(combine(a, INITIAL_EVIDENCE)).toEqual(a);
      for (const b of summaries) {
        expect(combine(a, b)).toEqual(combine(b, a));
        for (const c of summaries) expect(combine(combine(a, b), c)).toEqual(combine(a, combine(b, c)));
      }
    }
  });
  it('exhausts the no-wrapper primary/nested identity table, including absence and null', () => {
    for (const actor_type of actors) for (const authorship of authorships) {
      const primary = `${actor_type}/${authorship}`;
      const { payload_author: _unused, ...base } = human;
      const absent = { ...base, actor_type, authorship };
      expect(verdict([chunk(absent)]), `${primary}, nested absent`).toEqual({
        blocked: ![...humanIdentities, ...generatedIdentities].includes(primary), review: true
      });
      for (const nestedActor of actors) for (const nestedAuthorship of authorships) for (const is_user of [true, false, null]) {
        const nested = `${nestedActor}/${nestedAuthorship}`;
        const validHuman = humanIdentities.includes(primary) && humanIdentities.includes(nested) && is_user === true;
        const validGenerated = generatedIdentities.includes(primary) && generatedIdentities.includes(nested) && is_user === false;
        const value = { ...absent, payload_author: { actor_type: nestedActor, authorship: nestedAuthorship, is_user } };
        expect(captureProvenanceSchema.safeParse(value).success).toBe(true);
        expect(verdict([chunk(value)]), `${primary}, ${nested}, ${is_user}`).toEqual({
          blocked: !validHuman && !validGenerated,
          review: !(primary === 'human/original' && nested === 'human/original' && is_user === true)
        });
      }
    }
  });

  it('keeps every unsafe author class blocked across optional wrapper combinations, ordering and context', () => {
    const unsafe = [
      { ...human, payload_author: { actor_type: 'agent', authorship: 'original', is_user: true } },
      { ...human, payload_author: { actor_type: 'human', authorship: 'generated', is_user: true } },
      { ...human, payload_author: { actor_type: 'human', authorship: 'original', is_user: null } },
      { ...human, payload_author: { actor_type: 'human', authorship: 'original', is_user: false } },
      { ...human, payload_author: { actor_type: 'unknown', authorship: 'unknown', is_user: null } },
      { ...human, authorship: 'mixed' },
      { ...human, actor_type: 'unknown' },
      { ...human, actor_type: 'agent', authorship: 'generated' }
    ];
    for (const source of unsafe) for (const transported of [false, true]) for (const imported of [false, true]) {
      for (const content of ['source', header('true'), header('false'), header('unknown')]) {
        const bad = chunk({ ...source, ...(transported ? { transport } : {}), ...(imported ? { import: importMetadata } : {}) }, content);
        for (const chunks of [[bad], [chunk(), bad], [bad, chunk()], [chunk(), bad, chunk(), bad]]) {
          for (const trigger of [undefined, 'direct', 'api', 'backfill', 'scheduled', 'event', 'delegated', 'unknown'] as const) {
            expect(verdict(chunks, trigger), JSON.stringify({ source, transported, imported, content, trigger })).toEqual({ blocked: true, review: true });
          }
        }
      }
    }
  });

  it('retains no-wrapper contradictions through actual full-export conversion and checkpoint-excluded groups', async () => {
    const { prepareReplayDataset } = await import(new URL('../../../../scripts/lib/replay-dataset.mjs', import.meta.url).href);
    for (const payload_author of [
      { actor_type: 'agent', authorship: 'original', is_user: true },
      { actor_type: 'human', authorship: 'generated', is_user: true },
      { actor_type: 'human', authorship: 'original', is_user: null },
      { actor_type: 'unknown', authorship: 'mixed', is_user: null }
    ]) {
      const rows = [
        { segment_id: 'evidence', session_id: 'replay', created_at: '2026-06-01T00:00:00Z',
          chunks: [{ ...chunk({ ...human, payload_author }), id: 'one', event_id: 'same-event' }] },
        { segment_id: 'tail', session_id: 'replay', created_at: '2026-06-01T00:00:00Z',
          chunks: [{ role: 'user', content: 'Continuation', id: 'two', event_id: 'same-event' }] }
      ];
      for (const order of [rows, [...rows].reverse()]) {
        const prepared = prepareReplayDataset(order, { datasetSha256: 'a'.repeat(64), importJobId: 'test' });
        for (const row of prepared) expect(verdict(row.chunks, 'backfill')).toEqual({ blocked: true, review: true });
        // Selection/resume occurs only after whole-export classification.
        const tail = prepared.find((row: any) => row.segment_id === 'tail');
        expect(verdict(tail.chunks, 'backfill')).toEqual({ blocked: true, review: true });
      }
    }
  });

  it('accepts real distributed plugin capture shapes and preserves transport restrictions on every continuation', async () => {
    const scenarios = [
      { session: 'C123-topic-direct', messages: [{ role: 'user', content: 'Direct human statement.' }], blocked: false, review: false },
      { session: 'agent:main:cron:job', messages: [{ role: 'assistant', content: 'Scheduled factual observation.' }], blocked: false, review: true },
      { session: 'C123-topic-mixed', messages: [{ role: 'user', content: 'Human question.' }, { role: 'assistant', content: 'Generated answer.' }], blocked: false, review: true },
      ...['true', 'false', 'unknown'].map(user => ({ session: `C123-topic-${user}`,
        messages: [{ role: 'user', content: header(user) + ' Long factual continuation.'.repeat(40) }], blocked: user !== 'true', review: true }))
    ];
    const requests = JSON.parse(execFileSync(process.execPath, [
      fileURLToPath(new URL('../../../plugin/test/capture-provenance-fixture.mjs', import.meta.url)),
      JSON.stringify(scenarios)
    ], { encoding: 'utf8' }));
    expect(requests).toHaveLength(scenarios.length);
    scenarios.forEach((scenario, index) => {
      const request = requests[index];
      const p = profile(request.chunks, request.context.trigger_type, request.session_id);
      expect(getProvenancePreGate(p) !== null, scenario.session).toBe(scenario.blocked);
      expect(requiresBehavioralReview(p, 'user_rule'), scenario.session).toBe(scenario.review);
      if (scenario.messages[0].content.startsWith('[Inter-session')) {
        expect(request.chunks.length).toBeGreaterThan(1);
        for (const part of request.chunks) expect(verdict([part])).toEqual({ blocked: scenario.blocked, review: true });
      }
    });
  });

  it('retains positive factual controls across wrappers but never delivery-based behavioural exemptions', () => {
    for (const transported of [false, true]) for (const imported of [false, true]) {
      for (const content of ['source', header('true'), header('false'), header('unknown')]) {
        const value = { ...human, ...(transported ? { transport } : {}), ...(imported ? { import: importMetadata } : {}) };
        expect(verdict([chunk(value, content)])).toEqual({ blocked: content === header('false') || content === header('unknown'),
          review: transported || imported || content !== 'source' });
      }
    }
    for (const authorship of ['imported', 'transcribed']) {
      expect(verdict([chunk({ ...human, authorship })])).toEqual({ blocked: false, review: true });
    }
  });

  it('cannot ignore restrictive context, source class, session identity, cadence or constituent delivery', () => {
    for (const trigger of ['scheduled', 'delegated', 'event', 'backfill', 'unknown'] as const) {
      expect(verdict([chunk()], trigger)).toEqual({ blocked: false, review: true });
      expect(verdict([chunk({ ...human, trigger_type: trigger })], 'direct')).toEqual({ blocked: false, review: true });
    }
    for (const source_class of ['agent_cron', 'agent_hook', 'agent_subagent', 'agent_slack', 'agent_other']) {
      expect(verdict([chunk({ ...human, source_class })])).toEqual({ blocked: false, review: true });
    }
    for (const session of ['agent:main:cron:job', 'agent:main:hook:job', 'agent:main:subagent:job', 'agent:main:slack:job', 'agent:main:other:job']) {
      expect(requiresBehavioralReview(profile([chunk()], 'direct', session), 'user_rule')).toBe(true);
    }
    for (const cadence of ['recurring', 'batch', 'unknown']) {
      expect(verdict([chunk({ ...human, cadence })])).toEqual({ blocked: false, review: true });
    }
    const { payload_author: _unused, ...primaryOnly } = human;
    for (const other of [chunk(primaryOnly), { role: 'user' }, chunk({ ...human, import: importMetadata }),
      chunk({ ...human, transport }), chunk({ ...human, trigger_type: 'scheduled' })]) {
      for (const chunks of [[chunk(), other], [other, chunk()]]) {
        expect(verdict(chunks)).toEqual({ blocked: false, review: true });
      }
    }
    for (const trigger of [undefined, 'direct', 'api'] as const) {
      expect(verdict([chunk(), chunk()], trigger)).toEqual({ blocked: false, review: false });
    }
  });

  it('preserves ordinary conversation, cron alias and canonical import facts without broad exemptions', async () => {
    const generated = chunk({ ...human, actor_type: 'agent', authorship: 'generated', trigger_type: 'scheduled',
      payload_author: { actor_type: 'assistant', authorship: 'generated', is_user: false } });
    expect(verdict([generated])).toEqual({ blocked: false, review: true });
    const assistant = { ...generated, role: 'assistant', provenance: { ...generated.provenance as object, artifact_type: 'message' } };
    expect(verdict([assistant])).toEqual({ blocked: true, review: true });
    for (const chunks of [[chunk(), assistant], [assistant, chunk()], [{ role: 'user' }, { role: 'assistant' }, { role: 'tool' }]]) {
      expect(verdict(chunks)).toEqual({ blocked: false, review: true });
    }
    const { prepareReplayDataset } = await import(new URL('../../../../scripts/lib/replay-dataset.mjs', import.meta.url).href);
    const prepared = prepareReplayDataset([{ segment_id: 'ordinary', session_id: 'ordinary', created_at: '2026-06-01T00:00:00Z',
      chunks: [{ id: 'one', role: 'user', content: 'historical statement' }] }], { datasetSha256: 'a'.repeat(64), importJobId: 'test' });
    const imported = prepared[0].chunks[0];
    expect(verdict([imported], 'backfill')).toEqual({ blocked: false, review: true });
    for (const key of ['transport', 'import', 'payload_author']) {
      const changed = { ...imported.provenance }; delete changed[key];
      expect(verdict([chunk(changed)])).toEqual({ blocked: true, review: true });
    }
  });

  it('fails closed for empty, malformed and forged/unevaluated profiles', () => {
    expect(verdict([])).toEqual({ blocked: true, review: true });
    for (const value of [{ ...human, payload_author: null }, { ...human, payload_author: {} },
      { ...human, decision: { blockSemantic: false, requireBehavioralReview: false } }]) {
      expect(verdict([chunk(value)])).toEqual({ blocked: true, review: true });
    }
    const evaluated = profile([chunk()]);
    const copied = { ...evaluated };
    expect(getProvenancePreGate(copied)).not.toBeNull();
    expect(requiresBehavioralReview(copied, 'user_rule')).toBe(true);
    // A display/log field is not a reusable authorisation token.
    evaluated.actor_type = 'unknown';
    evaluated.semantic_block_reason = 'display changed';
    expect(getProvenancePreGate(evaluated)).toBeNull();
  });
});
