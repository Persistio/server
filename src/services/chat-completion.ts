import type OpenAI from 'openai';

import { getConfig } from '../config';

type ChatCompletionInput = OpenAI.Chat.Completions.ChatCompletionCreateParamsNonStreaming;
type ChatCompletionMessage = ChatCompletionInput['messages'][number];

export function withSystemPromptPrefix(
  input: ChatCompletionInput,
  prefix = getConfig().LLM_SYSTEM_PROMPT_PREFIX.trim(),
  reasoningEffort = getConfig().LLM_REASONING_EFFORT.trim()
): ChatCompletionInput {
  const inputWithReasoningEffort = reasoningEffort
    ? {
        ...input,
        reasoning_effort: reasoningEffort
      } as ChatCompletionInput
    : input;

  if (!prefix) {
    return inputWithReasoningEffort;
  }

  return {
    ...inputWithReasoningEffort,
    messages: inputWithReasoningEffort.messages.map((message, index) => {
      if (message.role === 'system' && typeof message.content === 'string') {
        return prefixStringContent(message as ChatCompletionMessage & { content: string }, prefix);
      }

      if (message.role === 'user' && index === firstUserMessageIndex(inputWithReasoningEffort.messages)) {
        return prefixUserContent(message, prefix);
      }

      return message;
    }) as ChatCompletionInput['messages']
  };
}

function firstUserMessageIndex(messages: ChatCompletionInput['messages']): number {
  return messages.findIndex((message) => message.role === 'user');
}

function prefixStringContent<T extends ChatCompletionMessage & { content: string }>(message: T, prefix: string): T {
  if (message.content.trimStart().startsWith(prefix)) {
    return message;
  }

  return {
    ...message,
    content: `${prefix}\n\n${message.content}`
  } as T;
}

function prefixUserContent(
  message: ChatCompletionMessage,
  prefix: string
): ChatCompletionMessage {
  if (typeof message.content === 'string') {
    return prefixStringContent(message as typeof message & { content: string }, prefix);
  }

  if (!Array.isArray(message.content)) {
    return message;
  }

  let prefixed = false;
  const content = message.content.map((part) => {
    if (prefixed || part.type !== 'text') {
      return part;
    }

    const text = part.text.trimStart().startsWith(prefix)
      ? part.text
      : `${prefix}\n\n${part.text}`;
    prefixed = true;
    return {
      ...part,
      text
    };
  });

  return prefixed
    ? {
        ...message,
        content: content as typeof message.content
      } as ChatCompletionMessage
    : message;
}
