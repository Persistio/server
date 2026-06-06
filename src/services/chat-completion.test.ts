import { describe, expect, it } from 'vitest';

import { withSystemPromptPrefix } from './chat-completion';

describe('chat completion helpers', () => {
  it('prefixes string system and first user prompts when configured', () => {
    const input = {
      model: 'test-model',
      messages: [
        { role: 'system' as const, content: 'Return JSON only.' },
        { role: 'user' as const, content: 'hello' }
      ]
    };

    const result = withSystemPromptPrefix(input, '/no_think');

    expect(result.messages[0]).toEqual({
      role: 'system',
      content: '/no_think\n\nReturn JSON only.'
    });
    expect(result.messages[1]).toEqual({
      role: 'user',
      content: '/no_think\n\nhello'
    });
  });

  it('does not duplicate an existing prefix', () => {
    const input = {
      model: 'test-model',
      messages: [
        { role: 'system' as const, content: '/no_think\n\nReturn JSON only.' }
      ]
    };

    const result = withSystemPromptPrefix(input, '/no_think');

    expect(result.messages[0]).toBe(input.messages[0]);
  });

  it('prefixes the first text part for multipart user content', () => {
    const input = {
      model: 'test-model',
      messages: [
        {
          role: 'user' as const,
          content: [
            { type: 'text' as const, text: 'Part one' },
            { type: 'text' as const, text: 'Part two' }
          ]
        }
      ]
    };

    const result = withSystemPromptPrefix(input, '/no_think');

    expect(result.messages[0].content).toEqual([
      { type: 'text', text: '/no_think\n\nPart one' },
      { type: 'text', text: 'Part two' }
    ]);
  });

  it('passes through provider reasoning effort when configured', () => {
    const input = {
      model: 'test-model',
      messages: [
        { role: 'user' as const, content: 'hello' }
      ]
    };

    const result = withSystemPromptPrefix(input, '', 'none');

    expect(result).toEqual({
      ...input,
      reasoning_effort: 'none'
    });
  });
});
