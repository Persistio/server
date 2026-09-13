import OpenAI from 'openai';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { extractedFactSchema } from '../extraction-contract';
import { curatorResultSchema } from '../curator-contract';
import { completeModelRequest, modelRequestBody, modelResponseFormat, serializeModelRequest } from '../model-completion';

type Input = OpenAI.Chat.Completions.ChatCompletionCreateParamsNonStreaming;
const nativeURL = 'https://api.anthropic.com/v1/';
const sampleSchema = z.object({ facts: z.array(z.object({ fact: z.string(), day: z.string().nullable() }).strict()) }).strict();
const request = (overrides: Partial<Input> = {}): Input => ({
  model: 'claude-sonnet-4-6', temperature: 0,
  messages: [{ role: 'system', content: 'Instruction "with\\escaping"' },
    { role: 'user', content: [{ type: 'text', text: 'First\nblock' }, { type: 'text', text: 'Second é block' }] }],
  response_format: modelResponseFormat(sampleSchema, 'memories'), ...overrides
});
const nativeResponse = (overrides: Record<string, unknown> = {}) => ({
  id: 'msg_test', type: 'message', role: 'assistant', model: 'claude-sonnet-4-6', stop_reason: 'end_turn',
  content: [{ type: 'text', text: '{"facts":[]}' }], usage: { input_tokens: 17, output_tokens: 11 }, ...overrides
});
function fixture(response: unknown, baseURL = nativeURL, status = 200) {
  const fetch = vi.fn(async (_input: unknown, _init?: RequestInit) => new Response(JSON.stringify(response), {
    status, headers: { 'content-type': 'application/json' }
  }));
  const client = new OpenAI({ apiKey: 'test-private-provider-key', baseURL, maxRetries: 0, fetch: fetch as typeof globalThis.fetch });
  return { client, fetch };
}

describe('generation-only schema projection', () => {
  it.each([
    ['extraction', z.object({ facts: z.array(extractedFactSchema).max(100) }).strict()],
    ['curation', curatorResultSchema]
  ] as const)('projects the complete %s contract without provider-unsupported bounds or refs', (name, validator) => {
    const format = modelResponseFormat(validator, name);
    expect(format.type).toBe('json_schema');
    expect(format.json_schema.strict).toBe(true);
    const schema = format.json_schema.schema!;
    const walk = (value: any): void => {
      expect(value).not.toHaveProperty('$ref');
      expect(value).not.toHaveProperty('definitions');
      expect(value).not.toHaveProperty('$defs');
      for (const constraint of ['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'minLength', 'maxLength', 'minItems', 'maxItems']) {
        expect(value).not.toHaveProperty(constraint);
      }
      if (value.type === 'object') {
        expect(value.additionalProperties).toBe(false);
        expect([...value.required].sort()).toEqual(Object.keys(value.properties).sort());
        Object.values(value.properties).forEach(walk);
      }
      if (value.items) walk(value.items);
      if (value.anyOf) value.anyOf.forEach(walk);
    };
    walk(schema);
    expect(schema.type).toBe('object');
    const text = JSON.stringify(schema);
    expect(text).toContain('"null"');
    expect(text).toContain('user_preference');
    expect(text).toContain('"pattern"');
    if (name === 'extraction') {
      expect((schema.properties as any).facts.items.properties.type.enum).toHaveLength(9);
      expect((schema.properties as any).facts.items.properties.source_refs.items.pattern).toBe('^S[1-9][0-9]*$');
    } else {
      expect((schema.properties as any).schema_version.enum).toEqual(['curation-plan.v2']);
      expect((schema.properties as any).update.items.properties.memory).toEqual((schema.properties as any).consolidate.items.properties.memory);
    }
  });

  it('does not remove local numeric, length, cardinality or refinement validation', () => {
    const validator = z.object({
      value: z.number().gt(0).max(1), text: z.string().min(1).max(3),
      ids: z.array(z.string()).min(2).max(3), day: z.string().refine(value => value === 'accepted')
    }).strict();
    modelResponseFormat(validator, 'local_constraints');
    expect(validator.safeParse({ value: 0.5, text: 'abc', ids: ['a', 'b'], day: 'accepted' }).success).toBe(true);
    for (const invalid of [{ value: 0 }, { text: '' }, { ids: ['a'] }, { day: 'not accepted' }]) {
      expect(validator.safeParse({ value: 0.5, text: 'abc', ids: ['a', 'b'], day: 'accepted', ...invalid }).success).toBe(false);
    }
  });

  it('inlines reused local refs', () => {
    const shared = z.object({ label: z.string().nullable() }).strict();
    const format = modelResponseFormat(z.object({ first: shared, other: shared }).strict(), 'references');
    const properties = format.json_schema.schema!.properties as any;
    expect(properties.other).toEqual(properties.first);
    expect(JSON.stringify(format)).not.toContain('$ref');
  });

  it('rejects a malformed local pointer instead of guessing the referenced field', () => {
    // This SDK version emits an unescaped slash in the reference it generates
    // for this artificial key. Our application contracts use simple static keys.
    const shared = z.object({ label: z.string() }).strict();
    expect(() => modelResponseFormat(z.object({ 'a/b': shared, other: shared }).strict(), 'bad_reference'))
      .toThrow('Unsupported model generation schema');
  });

  it.each([
    z.array(z.string()),
    z.object({ value: z.union([z.string(), z.number()]) }).strict(),
    z.object({ value: z.record(z.string()) }).strict(),
    z.object({ value: z.tuple([z.string(), z.number()]) }).strict(),
    z.object({ value: z.any() }).strict()
  ])('rejects unsupported structure instead of deleting a structural rule', schema => {
    expect(() => modelResponseFormat(schema, 'unsupported')).toThrow('Unsupported model generation schema');
  });

  it('rejects recursive schemas rather than looping or silently dropping recursion', () => {
    const recursive: z.ZodTypeAny = z.lazy(() => z.object({ children: z.array(recursive) }).strict());
    expect(() => modelResponseFormat(recursive, 'recursive')).toThrow('Unsupported model generation schema');
  });
});

describe('structured model transport boundary', () => {
  it.each(['https://api.anthropic.com/v1', nativeURL])('uses native wire body, headers and exact serialization for %s', async baseURL => {
    const { client, fetch } = fixture(nativeResponse(), baseURL);
    const input = request({ max_tokens: 2048, reasoning_effort: 'high', seed: 23 });
    const response = await completeModelRequest(client, input, baseURL);
    expect(fetch).toHaveBeenCalledTimes(1);
    const [url, init] = fetch.mock.calls[0];
    expect(String(url)).toBe('https://api.anthropic.com/v1/messages');
    expect(init!.body).toBe(serializeModelRequest(input, baseURL));
    const headers = new Headers(init!.headers);
    expect(headers.get('x-api-key')).toBe('test-private-provider-key');
    expect(headers.get('anthropic-version')).toBe('2023-06-01');
    expect(headers.get('authorization')).toBeNull();
    const body = JSON.parse(String(init!.body));
    expect(body).toEqual({ model: input.model, temperature: 0, max_tokens: 2048,
      system: 'Instruction "with\\escaping"', messages: [input.messages[1]],
      output_config: { format: { type: 'json_schema', schema: input.response_format!.type === 'json_schema' ? input.response_format!.json_schema.schema : null } }
    });
    expect(body).not.toHaveProperty('reasoning_effort');
    expect(body).not.toHaveProperty('thinking');
    expect(body).not.toHaveProperty('response_format');
    expect(response.choices[0]).toEqual({ finish_reason: 'stop', message: { content: '{"facts":[]}' } });
    expect(response.usage).toEqual({ prompt_tokens: 17, completion_tokens: 11, total_tokens: 28 });
  });

  it('preserves text order and explicit output caps without mutating the input', () => {
    const input = request({ messages: [
      { role: 'system', content: 'First system' }, { role: 'user', content: 'First user' },
      { role: 'system', content: 'Second system' }, { role: 'user', content: [{ type: 'text', text: 'Second user' }] }
    ] });
    const before = JSON.stringify(input);
    expect(modelRequestBody(input, nativeURL)).toMatchObject({ max_tokens: 8192, system: 'First system\n\nSecond system',
      messages: [{ role: 'user', content: 'First user' }, { role: 'user', content: [{ type: 'text', text: 'Second user' }] }] });
    expect(modelRequestBody({ ...input, max_tokens: 17 }, nativeURL)).toHaveProperty('max_tokens', 17);
    expect(modelRequestBody({ ...input, max_completion_tokens: 31 }, nativeURL)).toHaveProperty('max_tokens', 31);
    expect(JSON.stringify(input)).toBe(before);
  });

  it.each([
    'https://generativelanguage.googleapis.com/v1beta/openai/',
    'https://api.openai.com/v1/', 'https://api.anthropic.com.example/v1/',
    'https://proxy.example/v1/', 'http://api.anthropic.com/v1/',
    'https://api.anthropic.com:8443/v1/', 'https://api.anthropic.com/custom/',
    'https://api.anthropic.com/v1/?other=1', 'https://user:pass@api.anthropic.com/v1/'
  ])('leaves non-exact endpoints untouched: %s', baseURL => {
    const input = request();
    expect(modelRequestBody(input, baseURL)).toBe(input);
    expect(serializeModelRequest(input, baseURL)).toBe(JSON.stringify(input, null, 2));
  });

  it.each([undefined, { type: 'json_object' as const }, { type: 'text' as const }])('leaves non-schema Anthropic requests on compatibility API (%j)', async responseFormat => {
    const reply = { choices: [{ finish_reason: 'stop', message: { content: 'ordinary reply' } }] };
    const { client, fetch } = fixture(reply);
    const input = request({ response_format: responseFormat });
    expect(modelRequestBody(input, nativeURL)).toBe(input);
    expect(await completeModelRequest(client, input, nativeURL)).toEqual(reply);
    expect(String(fetch.mock.calls[0][0])).toBe('https://api.anthropic.com/v1/chat/completions');
    expect(fetch.mock.calls[0][1]!.body).toBe(serializeModelRequest(input, nativeURL));
    expect(new Headers(fetch.mock.calls[0][1]!.headers).get('authorization')).toBe('Bearer test-private-provider-key');
  });

  it('sends schema-constrained Google requests unchanged through compatibility API', async () => {
    const baseURL = 'https://generativelanguage.googleapis.com/v1beta/openai/';
    const reply = { choices: [{ finish_reason: 'stop', message: { content: '{"facts":[]}', refusal: null } }] };
    const { client, fetch } = fixture(reply, baseURL);
    const input = request({ model: 'gemini-2.5-flash' });
    expect(await completeModelRequest(client, input, baseURL)).toEqual(reply);
    expect(String(fetch.mock.calls[0][0])).toBe(`${baseURL}chat/completions`);
    expect(fetch.mock.calls[0][1]!.body).toBe(serializeModelRequest(input, baseURL));
    expect(JSON.parse(String(fetch.mock.calls[0][1]!.body))).toHaveProperty('response_format.json_schema.strict', true);
  });

  it.each([
    { messages: [{ role: 'assistant', content: 'not an allowed request' }] },
    { messages: [{ role: 'user', content: [{ type: 'image_url', image_url: { url: 'https://example.com/image' } }] }] },
    { messages: [] }, { max_tokens: 0 }, { max_tokens: -1 }, { max_tokens: 1.5 }
  ])('rejects unsupported native request shapes before HTTP (%j)', async overrides => {
    const { client, fetch } = fixture(nativeResponse());
    await expect(completeModelRequest(client, request(overrides as Partial<Input>), nativeURL)).rejects.toThrow('Unsupported native structured model request');
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    [{ stop_reason: 'max_tokens' }, 'length'],
    [{ stop_reason: 'refusal' }, 'content_filter'],
    [{ stop_details: { type: 'refusal', explanation: 'private reason' } }, 'content_filter'],
    [{ stop_reason: 'tool_use' }, null],
    [{ stop_reason: 'unexpected' }, null],
    [{ stop_reason: null }, null],
    [{ content: [{ type: 'thinking', thinking: 'not response JSON' }, { type: 'text', text: '{"facts":[]}' }] }, null],
    [{ content: [] }, null], [{ content: [{ type: 'text', text: ' ' }] }, null],
    [{ content: null }, null], [{ role: 'user' }, null], [{ type: 'unexpected' }, null]
  ] as const)('preserves paid usage for incomplete/non-text responses (%j)', async (overrides, finishReason) => {
    const raw = nativeResponse(overrides);
    const { client } = fixture(raw);
    const result = await completeModelRequest(client, request(), nativeURL);
    expect(result.choices[0].finish_reason).toBe(finishReason);
    expect(result.usage).toEqual({ prompt_tokens: 17, completion_tokens: 11, total_tokens: 28 });
    expect(result.native_response).toEqual(raw);
    if (finishReason === 'content_filter') expect(result.choices[0].message.refusal).toBe('Model refused structured output');
  });

  it('joins ordered native text blocks, without extracting JSON from unsupported blocks', async () => {
    const { client } = fixture(nativeResponse({ content: [{ type: 'text', text: '{"facts":' }, { type: 'text', text: '[]}' }] }));
    const result = await completeModelRequest(client, request(), nativeURL);
    expect(result.choices[0]).toEqual({ finish_reason: 'stop', message: { content: '{"facts":[]}' } });
  });

  it.each([undefined, null, {}, { input_tokens: 5 }, { input_tokens: -1, output_tokens: 2 },
    { input_tokens: '17', output_tokens: 11 }, { input_tokens: Number.MAX_SAFE_INTEGER, output_tokens: 1 },
    { input_tokens: 17, output_tokens: 11, cache_read_input_tokens: null },
    { input_tokens: 17, output_tokens: 11, cache_creation_input_tokens: '3' },
    { input_tokens: 17, output_tokens: 11, cache_creation_input_tokens: -1 },
    { input_tokens: 17, output_tokens: 11, cache_read_input_tokens: Number.MAX_SAFE_INTEGER }])('does not fabricate missing/invalid usage (%j)', async usage => {
    const { client } = fixture(nativeResponse({ usage }));
    expect((await completeModelRequest(client, request(), nativeURL)).usage).toBeUndefined();
  });

  it('counts all native input usage once, including cache counters but not their breakdown', async () => {
    const { client } = fixture(nativeResponse({ usage: { input_tokens: 17, output_tokens: 11,
      cache_creation_input_tokens: 8, cache_read_input_tokens: 4,
      cache_creation: { ephemeral_5m_input_tokens: 3, ephemeral_1h_input_tokens: 5 } } }));
    expect((await completeModelRequest(client, request(), nativeURL)).usage)
      .toEqual({ prompt_tokens: 29, completion_tokens: 11, total_tokens: 40 });
  });

  it('preserves explicit zero usage', async () => {
    const { client } = fixture(nativeResponse({ usage: { input_tokens: 0, output_tokens: 0 } }));
    expect((await completeModelRequest(client, request(), nativeURL)).usage)
      .toEqual({ prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 });
  });

  it.each([null, [], 'not a message', 17])('returns incomplete for malformed native bodies (%j)', async raw => {
    const { client } = fixture(raw);
    const result = await completeModelRequest(client, request(), nativeURL);
    expect(result.choices[0]).toEqual({ finish_reason: null, message: { content: null } });
    expect(result.usage).toBeUndefined();
  });

  it.each([401, 429, 500])('preserves SDK HTTP status %i without a schema-free retry', async status => {
    const { client, fetch } = fixture({ type: 'error', error: { type: 'test_error', message: 'synthetic failure' } }, nativeURL, status);
    await expect(completeModelRequest(client, request(), nativeURL)).rejects.toMatchObject({ status });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(JSON.parse(String(fetch.mock.calls[0][1]!.body))).toHaveProperty('output_config.format.type', 'json_schema');
  });
});
