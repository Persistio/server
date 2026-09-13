import type OpenAI from 'openai';

export { modelResponseFormat } from './model-output-schema';

type ChatInput = OpenAI.Chat.Completions.ChatCompletionCreateParamsNonStreaming;
type NativeText = { type: 'text'; text: string };
interface NativeRequest {
  model: string;
  max_tokens: number;
  temperature?: number;
  system?: string;
  messages: Array<{ role: 'user'; content: string | NativeText[] }>;
  output_config: { format: { type: 'json_schema'; schema: Record<string, unknown> } };
}

export interface ModelCompletion {
  choices: Array<{ finish_reason: string | null; message: { content: string | null; refusal?: string | null } }>;
  usage?: OpenAI.CompletionUsage;
  /** Provider evidence for existing private audit storage, never operational logs. */
  native_response?: unknown;
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function usesNativeMessages(input: ChatInput, baseURL: string): boolean {
  if (input.response_format?.type !== 'json_schema') return false;
  try {
    const url = new URL(baseURL);
    return url.protocol === 'https:' && url.hostname === 'api.anthropic.com' && !url.port
      && !url.username && !url.password && !url.search && !url.hash
      && (url.pathname === '/v1' || url.pathname === '/v1/');
  } catch {
    return false;
  }
}

function invalidRequest(): never {
  throw new Error('Unsupported native structured model request');
}

/** Deterministic wire body used by preparation, accounting, hashing and dispatch. */
export function modelRequestBody(input: ChatInput, baseURL: string): ChatInput | NativeRequest {
  if (!usesNativeMessages(input, baseURL)) return input;
  if (input.response_format?.type !== 'json_schema' || !object(input.response_format.json_schema.schema)) invalidRequest();
  const system: string[] = [];
  const messages: NativeRequest['messages'] = [];
  for (const message of input.messages) {
    if (message.role === 'system') {
      if (typeof message.content !== 'string') invalidRequest();
      system.push(message.content);
    } else if (message.role === 'user') {
      if (typeof message.content === 'string') {
        messages.push({ role: 'user', content: message.content });
      } else if (Array.isArray(message.content) && message.content.length > 0) {
        const content = message.content.map(part => {
          if (part.type !== 'text' || typeof part.text !== 'string') invalidRequest();
          return { type: 'text' as const, text: part.text };
        });
        messages.push({ role: 'user', content });
      } else invalidRequest();
    } else invalidRequest();
  }
  if (messages.length === 0) invalidRequest();
  const maxTokens = input.max_tokens ?? input.max_completion_tokens ?? 8192;
  if (!Number.isSafeInteger(maxTokens) || maxTokens <= 0) invalidRequest();
  return {
    model: input.model,
    max_tokens: maxTokens,
    ...(input.temperature === undefined || input.temperature === null ? {} : { temperature: input.temperature }),
    ...(system.length === 0 ? {} : { system: system.join('\n\n') }),
    messages,
    output_config: { format: { type: 'json_schema', schema: input.response_format.json_schema.schema } }
  };
}

export function serializeModelRequest(input: ChatInput, baseURL: string): string {
  // Keep identical to the installed SDK's JSON body encoding; transport tests
  // compare this with the actual fetch body so an SDK change cannot drift silently.
  return JSON.stringify(modelRequestBody(input, baseURL), null, 2);
}

function normalizedNativeResponse(raw: unknown): ModelCompletion {
  const body = object(raw) ? raw : {};
  const nativeUsage = object(body.usage) ? body.usage : {};
  const inputTokens = nativeUsage.input_tokens;
  const outputTokens = nativeUsage.output_tokens;
  const cacheCreationTokens = Object.hasOwn(nativeUsage, 'cache_creation_input_tokens') ? nativeUsage.cache_creation_input_tokens : 0;
  const cacheReadTokens = Object.hasOwn(nativeUsage, 'cache_read_input_tokens') ? nativeUsage.cache_read_input_tokens : 0;
  const validTokens = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
  let usage: OpenAI.CompletionUsage | undefined;
  if (validTokens(inputTokens) && validTokens(outputTokens) && validTokens(cacheCreationTokens) && validTokens(cacheReadTokens)) {
    // Claude's input_tokens excludes both cache counters. cache_creation is only
    // a breakdown of cache_creation_input_tokens, not additional consumption.
    const promptTokens = inputTokens + cacheCreationTokens + cacheReadTokens;
    const totalTokens = promptTokens + outputTokens;
    if (Number.isSafeInteger(promptTokens) && Number.isSafeInteger(totalTokens)) {
      usage = { prompt_tokens: promptTokens, completion_tokens: outputTokens, total_tokens: totalTokens };
    }
  }
  // Normalisation must never throw away paid usage by throwing a content/schema
  // error here. The service settles returned tokens before rejecting completion.
  const isMessage = body.type === 'message' && body.role === 'assistant';
  const textOnly = Array.isArray(body.content) && body.content.length > 0
    && body.content.every(block => object(block) && block.type === 'text' && typeof block.text === 'string');
  const content = textOnly ? (body.content as NativeText[]).map(block => block.text).join('') : null;
  const refused = body.stop_reason === 'refusal'
    || (object(body.stop_details) && body.stop_details.type === 'refusal');
  let finishReason: string | null = null;
  if (isMessage) {
    if (refused) finishReason = 'content_filter';
    else if (body.stop_reason === 'max_tokens') finishReason = 'length';
    else if (body.stop_reason === 'end_turn' && content?.trim()) finishReason = 'stop';
  }
  return { choices: [{ finish_reason: finishReason, message: { content,
    ...(isMessage && refused ? { refusal: 'Model refused structured output' } : {})
  } }], ...(usage ? { usage } : {}), native_response: raw };
}

export async function completeModelRequest(client: OpenAI, input: ChatInput, baseURL: string): Promise<ModelCompletion> {
  if (!usesNativeMessages(input, baseURL)) return client.chat.completions.create(input);
  const body = modelRequestBody(input, baseURL);
  const response = await client.post<ChatInput | NativeRequest, unknown>('/messages', {
    body,
    headers: { 'x-api-key': client.apiKey, 'anthropic-version': '2023-06-01', authorization: null }
  });
  return normalizedNativeResponse(response);
}
