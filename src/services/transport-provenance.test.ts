import { describe, expect, it } from 'vitest';

import { sourceEventKey } from './transport-provenance';

describe('transport provenance', () => {
  it('binds stable source identity to a vault without including message metadata', () => {
    const source = { namespace: 'integration', id: 'event-1', message_id: 'message-a', ordinal: 0 };
    expect(sourceEventKey('vault-a', source)).toBe(sourceEventKey('vault-a', { ...source, message_id: 'message-b' }));
    expect(sourceEventKey('vault-a', source)).not.toBe(sourceEventKey('vault-b', source));
    expect(sourceEventKey('vault-a', source)).not.toBe(sourceEventKey('vault-a', { ...source, ordinal: 1 }));
  });
});
