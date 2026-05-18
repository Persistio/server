import { describe, expect, it } from 'vitest';

import { formatConversationForExtraction } from './extraction-formatting';

describe('formatConversationForExtraction', () => {
  it('includes ISO timestamps when formatting chunks for extraction', () => {
    expect(formatConversationForExtraction([
      {
        role: 'user',
        decryptedContent: 'I went yesterday.',
        created_at: '2026-05-16T12:34:56.000Z'
      },
      {
        role: 'assistant',
        decryptedContent: 'Thanks for sharing.',
        created_at: '2026-05-16T12:35:10.000Z'
      }
    ])).toBe([
      '[2026-05-16T12:34:56.000Z] user: I went yesterday.',
      '[2026-05-16T12:35:10.000Z] assistant: Thanks for sharing.'
    ].join('\n'));
  });

  it('falls back to the old role prefix when a chunk timestamp is invalid', () => {
    expect(formatConversationForExtraction([
      {
        role: 'user',
        decryptedContent: 'Hello.',
        created_at: 'not-a-date'
      }
    ])).toBe('user: Hello.');
  });
});
