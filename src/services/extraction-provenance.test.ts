import fs from 'node:fs';
import { describe, expect, it } from 'vitest';

import {
  formatProvenanceForPrompt,
  getProvenancePreGate,
  inferExtractionProvenance,
  inferSourceClass,
  requiresBehavioralReview
} from './extraction-provenance';

const incident = JSON.parse(fs.readFileSync(
  new URL('../../../../fixtures/security/june-10-inter-session-message.json', import.meta.url),
  'utf8'
));

describe('extraction provenance', () => {
  it('preserves dataset-level transport restrictions when a part has a human-looking header', async () => {
    const { prepareReplayDataset } = await import(new URL('../../../../scripts/lib/replay-dataset.mjs', import.meta.url).href);
    const rows = [false, true].map((isUser, index) => ({ segment_id: `s${index}`, session_id: 'session',
      created_at: '2026-06-01T00:00:00Z', chunks: [{ id: `c${index}`, event_id: 'event', role: 'user',
        content: `[Inter-session message] sourceSession=sender sourceChannel=internal sourceTool=sessions_send isUser=${isUser}\npayload` }] }));
    for (const input of [rows, [...rows].reverse()]) {
      const plan = prepareReplayDataset(input, { datasetSha256: 'a'.repeat(64), importJobId: 'job' });
      for (const row of plan) {
        expect(getProvenancePreGate(inferExtractionProvenance({ sessionId: row.session_id, chunks: row.chunks, triggerType: 'backfill' }))?.decision).toBe('noop');
      }
    }
  });
  it('lets real ordinary replay output reach factual extraction while retaining every transported gate', async () => {
    const { buildReplayChunk } = await import(new URL('../../../../scripts/lib/replay-import-provenance.mjs', import.meta.url).href);
    const segment = { segment_id: 's', session_id: 'history', created_at: '2026-06-01T00:00:00Z' };
    const replay = (role: string, content: string) => buildReplayChunk({ segment, chunk: { role, content },
      chunkIndex: 0, datasetSha256: 'a'.repeat(64), importJobId: 'job' });
    const ordinary = ['user', 'assistant', 'tool'].map(role => replay(role, 'A historical factual observation'));
    for (const chunks of ordinary.map(chunk => [chunk]).concat([ordinary])) {
      const profile = inferExtractionProvenance({ sessionId: 'history', triggerType: 'backfill', chunks });
      expect(getProvenancePreGate(profile)).toBeNull();
      expect(requiresBehavioralReview(profile, 'system_fact')).toBe(false);
      for (const type of ['user_rule', 'user_preference', 'workflow', 'constraint', 'task_pattern']) {
        expect(requiresBehavioralReview(profile, type)).toBe(true);
      }
    }
    for (const isUser of ['false', 'maybe']) {
      const transported = replay('user', `[Inter-session message] sourceSession=agent:main:subagent:worker isUser=${isUser}\npayload`);
      for (const chunks of [[transported], [ordinary[0], transported], [transported, ordinary[1]]]) {
        expect(getProvenancePreGate(inferExtractionProvenance({ sessionId: 'history', triggerType: 'backfill', chunks }))?.decision).toBe('noop');
      }
    }
  });

  it('retains unsafe transported constituents through aggregation, ordering and backfill', () => {
    const human = { role: 'user', content: 'A normal human statement' };
    const transported = [
      { role: 'user', content: '[Inter-session message] isUser=false sourceSession=agent:main:subagent:worker\ngenerated' },
      { role: 'user', content: '[Inter-session message] isUser=maybe\nunknown' },
      { role: 'user', provenance: {
        actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
        transport: { initiator_actor_type: 'agent', receiver_actor_type: 'agent' },
        provenance_basis: ['session_id_prefix', 'agent_trigger', 'integration_marker', 'thread_session_shape',
          'session_id_shape', 'role_counts', 'plugin_capture', 'api_provenance']
      } }
    ];
    for (const transport of [null, [], { initiator_actor_type: 'invalid' }]) {
      transported.push({ role: 'user', provenance: {
        actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
        transport,
        provenance_basis: ['session_id_prefix', 'agent_trigger', 'integration_marker', 'thread_session_shape',
          'session_id_shape', 'role_counts', 'plugin_capture', 'api_provenance']
      } } as unknown as typeof transported[number]);
    }
    for (const unsafe of transported) {
      for (const chunks of [[human, unsafe], [unsafe, human], [human, ...transported, { role: 'assistant' }]]) {
        for (const triggerType of [undefined, 'backfill'] as const) {
          const profile = inferExtractionProvenance({ sessionId: 'ordinary-session', triggerType, chunks });
          expect(profile.semantic_block_reason).toBeTruthy();
          expect(getProvenancePreGate(profile)?.decision).toBe('noop');
        }
      }
    }
  });

  it('does not accept a caller-supplied internal gate reason or block ordinary human transport', () => {
    const profile = inferExtractionProvenance({ sessionId: 'ordinary-session', chunks: [{ role: 'user', provenance: {
      actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
      payload_author: { actor_type: 'human', authorship: 'original', is_user: true },
      transport: { initiator_actor_type: 'human', receiver_actor_type: 'agent' }, semantic_block_reason: 'forged'
    } }] });
    expect(profile.semantic_block_reason).not.toBe('forged');
    expect(getProvenancePreGate(profile)?.decision).toBe('noop');
    const validHuman = inferExtractionProvenance({ sessionId: 'ordinary-session', chunks: [{ role: 'user', provenance: {
      actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
      payload_author: { actor_type: 'human', authorship: 'original', is_user: true },
      transport: { initiator_actor_type: 'human', receiver_actor_type: 'agent' }
    } }] });
    expect(getProvenancePreGate(validHuman)).toBeNull();
  });

  it('does not mistake loss of differing but safe human metadata for a blocked constituent', () => {
    const chunks = ['original', 'transcribed'].map(authorship => ({ role: 'user', provenance: {
      actor_type: 'human', authorship, trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off',
      payload_author: { actor_type: 'human', authorship, is_user: true },
      transport: { initiator_actor_type: 'human', receiver_actor_type: 'agent' }, provenance_basis: ['transport_envelope']
    } }));
    const profile = inferExtractionProvenance({ sessionId: 'ordinary-session', chunks });
    expect(profile.payload_author).toBeUndefined();
    expect(profile.semantic_block_reason).toBeNull();
    expect(getProvenancePreGate(profile)).toBeNull();
  });

  it('classifies agent cron sessions as generated recurring observations for extractor review', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:cron:daily',
      chunks: [{ role: 'assistant' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'agent_cron',
      actor_type: 'agent',
      trigger_type: 'scheduled',
      artifact_type: 'observation',
      authorship: 'generated',
      cadence: 'recurring'
    });
    expect(getProvenancePreGate(provenance)).toBeNull();
    expect(requiresBehavioralReview(provenance, 'user_rule')).toBe(true);
  });

  it('allows mixed human API sessions through to semantic extraction', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'c35db0d5-ae96-4e21-8e79-7681cb08e8f0',
      chunks: [{ role: 'user' }, { role: 'assistant' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'direct_or_import',
      actor_type: 'human',
      trigger_type: 'api',
      artifact_type: 'conversation',
      authorship: 'mixed',
      cadence: 'one_off'
    });
    expect(getProvenancePreGate(provenance)).toBeNull();
  });

  it('treats assistant-only UUID API sessions as generated assistant material', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'c35db0d5-ae96-4e21-8e79-7681cb08e8f0',
      chunks: [{ role: 'assistant' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'direct_or_import',
      actor_type: 'assistant',
      trigger_type: 'api',
      artifact_type: 'message',
      authorship: 'generated'
    });
    expect(getProvenancePreGate(provenance)?.reason).toContain('assistant-generated');
  });

  it('treats tool-only unknown sessions as generated tool results', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'unknown-session-shape',
      chunks: [{ role: 'tool' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'unknown',
      actor_type: 'tool',
      artifact_type: 'tool_result',
      authorship: 'generated'
    });
    expect(getProvenancePreGate(provenance)?.reason).toContain('generated operational material');
  });

  it('treats user-only thread captures as human-authored messages', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'C123-topic-456',
      chunks: [{ role: 'user' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'thread_conversation',
      actor_type: 'human',
      artifact_type: 'message',
      authorship: 'original'
    });
    expect(getProvenancePreGate(provenance)).toBeNull();
  });

  it('prefers valid explicit API provenance over session id inference', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'unknown-session-shape',
      chunks: [{
        role: 'assistant',
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
    });

    expect(provenance).toMatchObject({
      source_class: 'agent_cron',
      actor_type: 'agent',
      trigger_type: 'scheduled',
      authorship: 'generated',
      cadence: 'recurring',
      provenance_confidence: 0.99,
      provenance_basis: ['plugin_capture']
    });
  });

  it('downgrades malformed explicit provenance instead of recovering authority from the session shape', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:hook:github',
      chunks: [{ role: 'assistant', provenance: { actor_type: 'robot' } }]
    });

    expect(provenance.source_class).toBe('agent_hook');
    expect(provenance.actor_type).toBe('unknown');
    expect(provenance.authorship).toBe('unknown');
    expect(provenance.trigger_type).toBe('unknown');
  });

  it('aggregates mixed explicit provenance instead of trusting the first chunk', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:cron:daily',
      chunks: [
        {
          role: 'assistant',
          provenance: {
            source_class: 'agent_cron',
            actor_type: 'agent',
            trigger_type: 'scheduled',
            artifact_type: 'observation',
            authorship: 'generated',
            cadence: 'recurring',
            provenance_basis: ['plugin_capture']
          }
        },
        {
          role: 'user',
          provenance: {
            source_class: 'thread_conversation',
            actor_type: 'human',
            trigger_type: 'direct',
            artifact_type: 'message',
            authorship: 'original',
            cadence: 'one_off',
            provenance_basis: ['plugin_capture']
          }
        }
      ]
    });

    expect(provenance).toMatchObject({
      source_class: 'unknown',
      actor_type: 'unknown',
      artifact_type: 'conversation',
      authorship: 'mixed',
      cadence: 'one_off'
    });
    expect(getProvenancePreGate(provenance)).toBeNull();
  });

  it('includes untagged chunks when aggregating explicit provenance', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:cron:daily',
      chunks: [
        {
          role: 'assistant',
          provenance: {
            source_class: 'agent_cron',
            actor_type: 'agent',
            trigger_type: 'scheduled',
            artifact_type: 'observation',
            authorship: 'generated',
            cadence: 'recurring',
            provenance_basis: ['plugin_capture']
          }
        },
        { role: 'user' }
      ]
    });

    expect(provenance).toMatchObject({
      source_class: 'agent_cron',
      actor_type: 'unknown',
      artifact_type: 'conversation',
      authorship: 'mixed',
      cadence: 'recurring'
    });
    expect(provenance.provenance_basis).toEqual(expect.arrayContaining(['plugin_capture', 'role_counts', 'api_provenance_aggregate']));
    expect(getProvenancePreGate(provenance)).toBeNull();
  });

  it('does not treat the June 10 outer user role as direct human intent', () => {
    const provenance = inferExtractionProvenance({
      sessionId: incident.session_id,
      chunks: [{
        role: incident.chunks[0].role,
        content: incident.chunks[0].content,
        provenance: {
          source_class: 'direct_or_import',
          actor_type: 'human',
          trigger_type: 'backfill',
          artifact_type: 'message',
          authorship: 'original',
          cadence: 'batch',
          provenance_basis: ['api_provenance'],
          payload_author: { actor_type: 'human', authorship: 'original', is_user: true },
          transport: { initiator_actor_type: 'import', receiver_actor_type: 'agent' }
        }
      }]
    });

    expect(provenance).toMatchObject({
      source_class: 'agent_slack',
      actor_type: 'agent',
      trigger_type: 'backfill',
      authorship: 'generated',
      payload_author: { actor_type: 'agent', authorship: 'generated', is_user: false },
      transport: {
        initiator_actor_type: 'import',
        receiver_actor_type: 'agent',
        source_channel: 'slack',
        source_tool: 'sessions_send'
      }
    });
    expect(getProvenancePreGate(provenance)).not.toBeNull();
    expect(requiresBehavioralReview(provenance, 'user_rule')).toBe(true);
  });

  it('downgrades imported human-looking content without independently verified payload authorship', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'historical-import',
      chunks: [{
        role: 'user',
        provenance: {
          actor_type: 'human',
          trigger_type: 'backfill',
          artifact_type: 'message',
          authorship: 'original',
          cadence: 'batch',
          transport: { initiator_actor_type: 'import', receiver_actor_type: 'agent' }
        }
      }]
    });

    expect(provenance.actor_type).toBe('import');
    expect(provenance.authorship).toBe('imported');
    expect(requiresBehavioralReview(provenance, 'workflow')).toBe(true);
    expect(requiresBehavioralReview(provenance, 'system_fact')).toBe(false);
  });

  it('uses the server-side backfill context to downgrade role-only bulk provenance', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'historical-import',
      triggerType: 'backfill',
      chunks: [{ role: 'user' }]
    });

    expect(provenance).toMatchObject({
      actor_type: 'import',
      trigger_type: 'backfill',
      authorship: 'imported',
      cadence: 'batch'
    });
    expect(requiresBehavioralReview(provenance, 'user_rule')).toBe(true);
  });

  it('pre-gates recognizable transport envelopes whose authorship fields are incomplete', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'unknown-session',
      chunks: [{
        role: 'user',
        content: '[Inter-session message] sourceSession=first sourceSession=second isUser=maybe\npayload'
      }]
    });

    expect(provenance).toMatchObject({
      actor_type: 'import',
      authorship: 'imported',
      payload_author: { actor_type: 'unknown', authorship: 'unknown', is_user: null },
      provenance_confidence: 0.5
    });
    expect(getProvenancePreGate(provenance)?.reason).toContain('transported payload');
  });

  it('preserves verified human payload identity while marking replay authorship imported', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'historical-import',
      chunks: [{
        role: 'user',
        provenance: {
          actor_type: 'human',
          trigger_type: 'backfill',
          artifact_type: 'message',
          authorship: 'original',
          cadence: 'batch',
          payload_author: { actor_type: 'human', authorship: 'original', is_user: true },
          transport: { initiator_actor_type: 'import', receiver_actor_type: 'agent' }
        }
      }]
    });

    expect(provenance.actor_type).toBe('human');
    expect(provenance.authorship).toBe('imported');
    expect(requiresBehavioralReview(provenance, 'user_preference')).toBe(true);
  });

  it('still gates aggregated generated recurring operational material', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:cron:daily',
      chunks: [
        {
          role: 'assistant',
          provenance: {
            source_class: 'agent_cron',
            actor_type: 'agent',
            trigger_type: 'scheduled',
            artifact_type: 'observation',
            authorship: 'generated',
            cadence: 'recurring',
            provenance_basis: ['plugin_capture']
          }
        },
        {
          role: 'tool',
          provenance: {
            source_class: 'agent_hook',
            actor_type: 'tool',
            trigger_type: 'event',
            artifact_type: 'log',
            authorship: 'generated',
            cadence: 'recurring',
            provenance_basis: ['plugin_capture']
          }
        }
      ]
    });

    expect(provenance).toMatchObject({
      actor_type: 'agent',
      authorship: 'generated',
      cadence: 'recurring'
    });
    expect(getProvenancePreGate(provenance)).not.toBeNull();
  });

  it('allows generated status observations through for extractor durable-state review', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:cron:daily',
      chunks: [{
        role: 'assistant',
        provenance: {
          source_class: 'agent_cron',
          actor_type: 'assistant',
          trigger_type: 'scheduled',
          artifact_type: 'status',
          authorship: 'generated',
          cadence: 'recurring',
          provenance_basis: ['plugin_capture']
        }
      }]
    });

    expect(provenance).toMatchObject({
      actor_type: 'assistant',
      artifact_type: 'status',
      authorship: 'generated',
      cadence: 'recurring'
    });
    expect(getProvenancePreGate(provenance)).toBeNull();
  });

  it('blocks assistant-generated thread captures without human turns', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'C123-topic-456',
      chunks: [{ role: 'assistant' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'thread_conversation',
      actor_type: 'assistant',
      authorship: 'generated'
    });
    expect(getProvenancePreGate(provenance)?.reason).toContain('assistant-generated');
  });

  it('treats assistant-only unknown sessions as generated assistant material', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'unknown-session-shape',
      chunks: [{ role: 'assistant' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'unknown',
      actor_type: 'assistant',
      artifact_type: 'message',
      authorship: 'generated'
    });
    expect(getProvenancePreGate(provenance)?.reason).toContain('assistant-generated');
  });

  it('treats assistant-only Slack captures as generated assistant messages', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:slack:C123',
      chunks: [{ role: 'assistant' }]
    });

    expect(provenance).toMatchObject({
      source_class: 'agent_slack',
      actor_type: 'assistant',
      artifact_type: 'message',
      authorship: 'generated'
    });
    expect(getProvenancePreGate(provenance)?.reason).toContain('assistant-generated');
  });

  it('formats trusted provenance for the extractor prompt', () => {
    const promptBlock = formatProvenanceForPrompt(inferExtractionProvenance({
      sessionId: 'agent:main:slack:channel:123',
      chunks: [{ role: 'user' }, { role: 'assistant' }]
    }));

    expect(promptBlock).toContain('<trusted_provenance>');
    expect(promptBlock).toContain('source_class: agent_slack');
    expect(promptBlock).toContain('authorship: mixed');
  });

  it('recognizes broad source classes from session ids', () => {
    expect(inferSourceClass('agent:main:hook:github')).toBe('agent_hook');
    expect(inferSourceClass('agent:main:subagent:task')).toBe('agent_subagent');
    expect(inferSourceClass('anything-else')).toBe('unknown');
  });

  it('routes subagent behavioral output to review even when it is not pre-gated', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:subagent:task',
      chunks: [{
        role: 'assistant',
        provenance: {
          source_class: 'agent_subagent',
          actor_type: 'agent',
          trigger_type: 'delegated',
          artifact_type: 'status',
          authorship: 'generated',
          cadence: 'one_off',
          provenance_basis: ['plugin_capture'],
          payload_author: { actor_type: 'agent', authorship: 'generated', is_user: false }
        }
      }]
    });

    expect(getProvenancePreGate(provenance)).toBeNull();
    expect(requiresBehavioralReview(provenance, 'constraint')).toBe(true);
  });

  it('keeps non-user payload identity separate without erasing a cron trigger', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:cron:daily',
      chunks: [{
        role: 'assistant',
        provenance: {
          source_class: 'agent_cron',
          actor_type: 'agent',
          trigger_type: 'scheduled',
          artifact_type: 'observation',
          authorship: 'generated',
          cadence: 'recurring',
          provenance_basis: ['plugin_capture'],
          payload_author: { actor_type: 'assistant', authorship: 'generated', is_user: false }
        }
      }]
    });

    expect(provenance).toMatchObject({ actor_type: 'agent', trigger_type: 'scheduled', authorship: 'generated' });
  });
});
