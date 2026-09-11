import { describe, expect, it } from 'vitest';

import { parseInterSessionEnvelope, sourceEventKey } from './transport-provenance';

describe('transport provenance', () => {
  it('parses inter-session fields independently of their serialized order', () => {
    expect(parseInterSessionEnvelope(
      '[Inter-session message] isUser=false sourceTool=sessions_send sourceChannel=slack sourceSession=agent:main:slack:direct:user\npayload'
    )).toEqual({
      source_session_id: 'agent:main:slack:direct:user',
      source_channel: 'slack',
      source_tool: 'sessions_send',
      is_user: false
    });
  });

  it('recognizes incomplete and contradictory envelopes without asserting user identity', () => {
    expect(parseInterSessionEnvelope(
      '[Inter-session message] sourceSession=first sourceSession=second isUser=maybe\npayload'
    )).toEqual({
      source_session_id: 'unknown',
      source_channel: 'unknown',
      source_tool: 'unknown',
      is_user: null
    });
  });

  it('binds stable source identity to a vault without including message metadata', () => {
    const source = { namespace: 'openclaw', id: 'event-1', message_id: 'message-a', ordinal: 0 };
    expect(sourceEventKey('vault-a', source)).toBe(sourceEventKey('vault-a', { ...source, message_id: 'message-b' }));
    expect(sourceEventKey('vault-a', source)).not.toBe(sourceEventKey('vault-b', source));
    expect(sourceEventKey('vault-a', source)).not.toBe(sourceEventKey('vault-a', { ...source, ordinal: 1 }));
  });
});
