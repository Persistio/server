import { describe, expect, it } from 'vitest';

import {
  formatProvenanceForPrompt,
  getProvenancePreGate,
  inferExtractionProvenance,
  inferSourceClass
} from './extraction-provenance';

describe('extraction provenance', () => {
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

  it('falls back to session inference when explicit provenance is malformed', () => {
    const provenance = inferExtractionProvenance({
      sessionId: 'agent:main:hook:github',
      chunks: [{ role: 'assistant', provenance: { actor_type: 'robot' } }]
    });

    expect(provenance.source_class).toBe('agent_hook');
    expect(provenance.actor_type).toBe('agent');
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
      actor_type: 'human',
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
      actor_type: 'human',
      artifact_type: 'conversation',
      authorship: 'mixed',
      cadence: 'recurring'
    });
    expect(provenance.provenance_basis).toEqual(expect.arrayContaining(['plugin_capture', 'role_counts', 'api_provenance_aggregate']));
    expect(getProvenancePreGate(provenance)).toBeNull();
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
});
