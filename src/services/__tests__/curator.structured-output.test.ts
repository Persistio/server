import crypto from 'node:crypto';
import OpenAI from 'openai';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { getConfig } from '../../config';
import { CuratorPreparationDeferredError, CuratorService, type CuratorMemory } from '../curator';
import { CURATOR_CONTRACT, type CuratorResult, type CuratorProposedMemory, type CuratorRawSource } from '../curator-contract';
import { memoryTypeSchema, nonHumanMemoryTypeSchema } from '../extraction-contract';
import { serializeModelRequest } from '../model-completion';

const usage = vi.hoisted(() => ({ acquire: vi.fn(), settle: vi.fn(), record: vi.fn() }));
vi.mock('../usage', () => ({ acquireAiBudget: usage.acquire, settleAiUsage: usage.settle, recordModelUsage: usage.record }));

const providers = [
  { name: 'Gemini compatible', baseURL: 'https://generativelanguage.googleapis.com/v1beta/openai/', model: 'gemini-2.5-flash', native: false },
  { name: 'Anthropic native', baseURL: 'https://api.anthropic.com/v1/', model: 'claude-sonnet-4-6', native: true }
] as const;
const vaultId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const prefix = 'STRUCTURED_TEST_PREFIX "with\\escaping"';
const privateText = 'PRIVATE_MODEL_OUTPUT_SENTINEL';
const memory = (id = 'target', data = 'The service uses PostgreSQL.'): CuratorMemory => ({
  id, subject: 'Service database', data, type: 'system_fact', scope: 'project', scope_key: 'project-a',
  salience: 0.8, confidence: 0.9, sensitivity: 'low', polarity: 'neutral', volatility: 'low', parent_id: null
});
const plan = (count = 1): CuratorResult => ({
  schema_version: 'curation-plan.v2', keep: Array.from({ length: count }, (_, index) => ({ id: `M${index + 1}`, reason: 'Already useful' })),
  update: [], consolidate: [], archive: [], edges: [], scope_changes: []
});
const preference: CuratorProposedMemory = {
  statement: 'The user prefers UK English.', subject: 'Preferred English', type: 'user_preference',
  confidence: 0.9, salience: 0.8, sensitivity: 'low', polarity: 'positive', volatility: 'low',
  valid_from: null, valid_until: null, evidence: 'Supported by the existing preference memory.'
};

describe.each(providers)('Curator structured output through real SDK: $name', provider => {
  const config = getConfig();
  let originals: Pick<typeof config, 'CURATOR_BASE_URL' | 'CURATOR_API_KEY' | 'CURATOR_MODEL' | 'LLM_SYSTEM_PROMPT_PREFIX' | 'LLM_REASONING_EFFORT'>;
  beforeEach(() => {
    originals = { CURATOR_BASE_URL: config.CURATOR_BASE_URL, CURATOR_API_KEY: config.CURATOR_API_KEY,
      CURATOR_MODEL: config.CURATOR_MODEL, LLM_SYSTEM_PROMPT_PREFIX: config.LLM_SYSTEM_PROMPT_PREFIX,
      LLM_REASONING_EFFORT: config.LLM_REASONING_EFFORT };
    Object.assign(config, { CURATOR_BASE_URL: provider.baseURL, CURATOR_API_KEY: 'test-structured-curator-key',
      CURATOR_MODEL: provider.model, LLM_SYSTEM_PROMPT_PREFIX: prefix, LLM_REASONING_EFFORT: '' });
    Object.values(usage).forEach(mock => mock.mockReset());
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });
  afterEach(() => { Object.assign(config, originals); vi.restoreAllMocks(); });

  function harness() {
    const order: string[] = [];
    const state = { text: JSON.stringify(plan()), stop: provider.native ? 'end_turn' : 'stop',
      includeUsage: true, refusal: false, malformedNativeContent: false };
    const fetchMock = vi.fn(async (_url: unknown, _options: unknown) => {
      order.push('provider');
      const midpoint = Math.floor(state.text.length / 2);
      const body = provider.native ? {
        id: 'msg-test', type: 'message', role: 'assistant', model: provider.model,
        content: state.malformedNativeContent ? [{ type: 'tool_use', input: privateText }] : [
          { type: 'text', text: state.text.slice(0, midpoint) }, { type: 'text', text: state.text.slice(midpoint) }
        ],
        stop_reason: state.stop,
        ...(state.includeUsage ? { usage: { input_tokens: 120, output_tokens: 30 } } : {})
      } : {
        id: 'chatcmpl-test', object: 'chat.completion', created: 1, model: provider.model,
        choices: [{ index: 0, finish_reason: state.stop, message: { role: 'assistant', content: state.text,
          ...(state.refusal ? { refusal: privateText } : {}) } }],
        ...(state.includeUsage ? { usage: { prompt_tokens: 120, completion_tokens: 30, total_tokens: 150 } } : {})
      };
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });
    });
    const service = new CuratorService();
    // Only replace the transport dependency; service preparation, dispatch,
    // SDK serialization/normalization and whole-plan validation remain real.
    (service as unknown as { client: OpenAI }).client = new OpenAI({
      apiKey: config.CURATOR_API_KEY, baseURL: provider.baseURL, maxRetries: 0, fetch: fetchMock as typeof fetch
    });
    usage.acquire.mockImplementation(async () => { order.push('reserve'); });
    usage.settle.mockImplementation(async () => { order.push('settle'); });
    usage.record.mockImplementation(async () => { order.push('record'); });
    const accounting = {
      beforeRequest: vi.fn(async () => { order.push('request'); }),
      returnedUsage: vi.fn(async () => { order.push('usage'); })
    };
    const wire = () => {
      expect(fetchMock).toHaveBeenCalledOnce();
      const [url, options] = fetchMock.mock.calls[0] as [string, RequestInit];
      expect(typeof options.body).toBe('string');
      return { url, text: options.body as string, body: JSON.parse(options.body as string), headers: new Headers(options.headers) };
    };
    return { service, state, fetchMock, accounting, order, wire };
  }

  function expectPaidAttempt(h: ReturnType<typeof harness>) {
    const wire = h.wire();
    const expectedEstimate = Math.max(256, Math.ceil(wire.text.length / 4));
    expect(h.order).toEqual(['reserve', 'request', 'provider', 'usage', 'settle', 'record']);
    expect(h.accounting.beforeRequest).toHaveBeenCalledOnce();
    expect(h.accounting.returnedUsage).toHaveBeenCalledExactlyOnceWith({ promptTokens: 120, completionTokens: 30, totalTokens: 150 });
    expect(usage.acquire).toHaveBeenCalledExactlyOnceWith(vaultId, 'curation', expectedEstimate);
    expect(usage.settle).toHaveBeenCalledExactlyOnceWith(vaultId, 'curation', expectedEstimate, 150);
    expect(usage.record).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ vaultId, model: provider.model,
      modelRole: 'curation', requestCount: 1, promptTokens: 120, completionTokens: 30, totalTokens: 150 }));
  }

  function expectWireSchema(h: ReturnType<typeof harness>, batch: ReturnType<CuratorService['prepare']>, behavioral: boolean) {
    const wire = h.wire();
    expect(wire.text).toBe(serializeModelRequest(batch.request, provider.baseURL));
    expect(batch.requestHash).toBe(crypto.createHash('sha256').update(wire.text).digest('hex'));
    expect(wire.text.length).toBeLessThanOrEqual(47000);
    const schema = provider.native ? wire.body.output_config.format.schema : wire.body.response_format.json_schema.schema;
    const expectedTypes = behavioral ? memoryTypeSchema.options : nonHumanMemoryTypeSchema.options;
    for (const disposition of ['update', 'consolidate']) {
      expect(schema.properties[disposition].items.properties.memory.properties.type.enum).toEqual(expectedTypes);
    }
    return wire;
  }

  it.each([false, true])('sends exactly the prepared, hashed and budgeted wire body with behavioral schema=%s and original output cap', async behavioral => {
    const h = harness();
    const candidate = behavioral ? { ...memory(), type: 'user_preference' as const, data: preference.statement } : memory();
    const batch = h.service.prepare([candidate], [], null, { maxInputTokens: 12000, maxOutputTokens: 2000 });
    const preparedWire = serializeModelRequest(batch.request, provider.baseURL);
    const result = await h.service.curatePrepared(batch, vaultId, h.accounting);
    const wire = h.wire();
    expect(Buffer.from(wire.text)).toEqual(Buffer.from(preparedWire));
    expect(batch.requestHash).toBe(crypto.createHash('sha256').update(wire.text).digest('hex'));
    expect(wire.text.length).toBeLessThanOrEqual(12000 * 4 - 1000);
    expect(wire.body.max_tokens).toBe(2000);
    expect(wire.body.model).toBe(provider.model);
    const schema = provider.native ? wire.body.output_config.format.schema : wire.body.response_format.json_schema.schema;
    expect(schema).toMatchObject({ type: 'object', additionalProperties: false,
      required: ['schema_version', 'keep', 'update', 'consolidate', 'archive', 'edges', 'scope_changes'] });
    expect(wire.text.length).toBeGreaterThan(JSON.stringify(batch.request.messages, null, 2).length);
    const system = provider.native ? wire.body.system : wire.body.messages[0].content;
    const user = provider.native ? wire.body.messages[0].content : wire.body.messages[1].content;
    expect(system).toContain(CURATOR_CONTRACT);
    for (const text of [system, user[0].text]) {
      expect(text.startsWith(prefix)).toBe(true);
      expect(text.split(prefix)).toHaveLength(2);
    }
    expect(user.slice(1).every((part: { text: string }) => !part.text.includes(prefix))).toBe(true);
    if (provider.native) {
      expect(wire.url).toBe('https://api.anthropic.com/v1/messages');
      expect(wire.headers.get('x-api-key')).toBe('test-structured-curator-key');
      expect(wire.headers.get('anthropic-version')).toBe('2023-06-01');
      expect(wire.headers.has('authorization')).toBe(false);
      expect(wire.body.response_format).toBeUndefined();
    } else {
      expect(wire.url).toBe('https://generativelanguage.googleapis.com/v1beta/openai/chat/completions');
      expect(wire.body.response_format).toMatchObject({ type: 'json_schema', json_schema: { strict: true } });
    }
    expect(result.result).toEqual(plan());
    expect(result.audit.promptHash).toBe(batch.requestHash);
    expectWireSchema(h, batch, behavioral);
    expectPaidAttempt(h);
  });

  it.each(['target', 'context'] as const)('improves an existing behavioral %s without any optional raw sources', async location => {
    const h = harness();
    const behavioral = { ...memory('preference', preference.statement), type: 'user_preference' as const };
    const batch = h.service.prepare(location === 'target' ? [behavioral] : [memory()], location === 'context' ? [behavioral] : [], null,
      { maxInputTokens: 12000, maxOutputTokens: 2000 });
    const updatedAlias = location === 'target' ? 'M1' : 'M2';
    const response = { ...plan(location === 'target' ? 0 : 1),
      update: [{ id: updatedAlias, memory: preference, source_refs: [updatedAlias], reason: 'Clarify supported preference' }] };
    h.state.text = JSON.stringify(response);
    const result = await h.service.curatePrepared(batch, vaultId, h.accounting);
    expect(batch.rawSources).toEqual([]);
    expect(result.result).toEqual(response);
    expectWireSchema(h, batch, true);
    expectPaidAttempt(h);
  });

  it.each([false, true])('rejects unsupported behavioral replacement even with behavioral reviewed context=%s', async hasBehavioralContext => {
    const h = harness();
    const context = hasBehavioralContext ? [{ ...memory('other', preference.statement), type: 'user_preference' as const }] : [];
    const batch = h.service.prepare([memory()], context, null, { maxInputTokens: 12000, maxOutputTokens: 2000 });
    h.state.text = JSON.stringify({ ...plan(0), update: [
      { id: 'M1', memory: preference, source_refs: ['M1'], reason: 'Unsupported behavioral reclassification' }
    ] });
    // An unrelated behavioral context memory makes the full wire enum possible,
    // but does not authorize an action citing only its original factual target.
    await expect(h.service.curatePrepared(batch, vaultId, h.accounting)).rejects.toMatchObject({
      name: 'CuratorPlanValidationError', audit: { validationErrors: ['Unsupported behavioural reclassification'] }
    });
    expectWireSchema(h, batch, hasBehavioralContext);
    expectPaidAttempt(h);
  });

  it('does not enable behavioral types from excluded context or a deferred behavioral candidate', async () => {
    const h = harness();
    const large = { ...memory('large', preference.statement), type: 'user_preference' as const };
    const context = [{ ...memory('large-context', 'x'.repeat(50000)), relevant_candidate_ids: ['large'] },
      { ...memory('unrelated', preference.statement), type: 'user_rule' as const, relevant_candidate_ids: ['not-selected'] }];
    const batch = h.service.prepare([large, memory('small')], context, null, { maxInputTokens: 12000, maxOutputTokens: 2000 });
    expect(batch.candidates.map(candidate => candidate.id)).toEqual(['small']);
    expect(batch.deferredCandidateIds).toEqual(['large']);
    expect(batch.activeMemories).toEqual([]);
    await h.service.curatePrepared(batch, vaultId, h.accounting);
    expectWireSchema(h, batch, false);
  });

  it.each([88, 112])('recomputes eligibility for the selected set at an output cap of %i', async maxOutputTokens => {
    const h = harness();
    const batch = h.service.prepare([memory('first'), memory('second')], [
      { ...memory('associated-preference', preference.statement), type: 'user_preference' as const, relevant_candidate_ids: ['second'] }
    ], null, { maxInputTokens: 12000, maxOutputTokens });
    const bothFit = maxOutputTokens === 112;
    expect(batch.candidates).toHaveLength(bothFit ? 2 : 1);
    expect(batch.activeMemories).toHaveLength(bothFit ? 1 : 0);
    h.state.text = JSON.stringify(plan(batch.candidates.length));
    await h.service.curatePrepared(batch, vaultId, h.accounting);
    expectWireSchema(h, batch, bothFit);
    expect(h.wire().body.max_tokens).toBe(maxOutputTokens);
  });

  it('renders delivery separately from human eligibility without enabling behavioral replacements from raw evidence alone', async () => {
    const h = harness();
    const base = { role: 'user', created_at: '2026-09-12T12:00:00Z', current: true,
      content: 'The service uses PostgreSQL.', context: { project_id: 'project-a' } };
    const provenance = { actor_type: 'agent', authorship: 'generated', trigger_type: 'delegated', artifact_type: 'message', cadence: 'one_off' };
    const rawSources: CuratorRawSource[] = [
      { ...base, id: 'direct-human', provenance: null },
      { ...base, id: 'agent-in-user', provenance },
      { ...base, id: 'routed-human', role: 'assistant', provenance: { ...provenance, authorship: 'transcribed',
        payload_author: { actor_type: 'human', authorship: 'original', is_user: true } } }
    ];
    const original = structuredClone(rawSources);
    const batch = h.service.prepare([{ ...memory(), source_chunks: rawSources.map(source => source.id) }], [], null,
      { maxInputTokens: 12000, maxOutputTokens: 2000, rawSources });
    await h.service.curatePrepared(batch, vaultId, h.accounting);
    const wire = expectWireSchema(h, batch, false);
    const parts = provider.native ? wire.body.messages[0].content : wire.body.messages[1].content;
    const sources = JSON.parse(parts[2].text).sources;
    expect(sources.map((source: { transport_role: string; human_intent_source: boolean }) => [source.transport_role, source.human_intent_source]))
      .toEqual([['user', true], ['user', false], ['assistant', true]]);
    expect(sources.every((source: object) => !Object.hasOwn(source, 'role'))).toBe(true);
    expect(rawSources).toEqual(original);
    expect(batch.rawSources).toEqual(original);
  });

  it.each(['json', 'schema', 'refusal', 'truncated', 'completion'] as const)(
    'retains the exact paid attempt before rejecting %s output', async stage => {
      const h = harness();
      if (stage === 'json') h.state.text = `{"${privateText}":`;
      if (stage === 'schema') h.state.text = JSON.stringify({ [privateText]: privateText });
      if (stage === 'refusal') {
        h.state.refusal = true;
        h.state.stop = provider.native ? 'refusal' : 'stop';
      }
      if (stage === 'truncated') h.state.stop = provider.native ? 'max_tokens' : 'length';
      if (stage === 'completion') h.state.stop = privateText;
      const batch = h.service.prepare([memory()], [], null, { maxInputTokens: 12000, maxOutputTokens: 2000 });
      const error = await h.service.curatePrepared(batch, vaultId, h.accounting).catch(error => error);
      expect(error).toMatchObject({ name: 'CuratorPlanValidationError', outputFailure: { operation: 'curation', stage } });
      expect(error.message).not.toContain(privateText);
      expect(JSON.stringify(error.outputFailure)).not.toContain(privateText);
      expect(h.wire().text).toBe(serializeModelRequest(batch.request, provider.baseURL));
      expectPaidAttempt(h);
    }
  );

  it('does not invent usage when an otherwise completed response omits it', async () => {
    const h = harness(); h.state.includeUsage = false;
    const result = await h.service.curatePrepared(h.service.prepare([memory()], [], null), vaultId, h.accounting);
    expect(result.result).toEqual(plan());
    expect(result.usage).toBeNull();
    expect(h.order).toEqual(['reserve', 'request', 'provider']);
    expect(h.accounting.beforeRequest).toHaveBeenCalledOnce();
    expect(h.accounting.returnedUsage).not.toHaveBeenCalled();
    expect(usage.settle).not.toHaveBeenCalled();
    expect(usage.record).not.toHaveBeenCalled();
    expect(h.wire().body.max_tokens).toBe(provider.native ? 8192 : undefined);
  });

  it.each([false, true])('fits complete records and custom instructions around behavioral schema=%s at the enforced minimum input budget', async behavioral => {
    const h = harness();
    const candidates = Array.from({ length: 30 }, (_, index) => ({
      ...memory(`target-${index}`, `Record ${index}: ` + 'durable fact '.repeat(150)),
      ...(behavioral ? { type: 'user_preference' as const } : {})
    }));
    const batch = h.service.prepare(candidates, [], null, { maxInputTokens: 12000, maxOutputTokens: 2000,
      vaultPromptContext: { type: 'custom', custom_curation_prompt: 'Preserve useful evidence. '.repeat(900) } });
    expect(batch.candidates.length).toBeGreaterThan(0);
    expect(batch.deferredCandidateIds.length).toBeGreaterThan(0);
    h.state.text = JSON.stringify(plan(batch.candidates.length));
    await h.service.curatePrepared(batch, vaultId, h.accounting);
    const wire = h.wire();
    expect(wire.text).toBe(serializeModelRequest(batch.request, provider.baseURL));
    expect(wire.text.length).toBeLessThanOrEqual(47000);
    expect(batch.requestHash).toBe(crypto.createHash('sha256').update(wire.text).digest('hex'));
    const system = provider.native ? wire.body.system : wire.body.messages[0].content;
    const user = provider.native ? wire.body.messages[0].content : wire.body.messages[1].content;
    expect(system).toContain('Preserve useful evidence.');
    expect(system).toContain(CURATOR_CONTRACT);
    const represented = JSON.parse(user[0].text.slice(prefix.length).trim()).memories;
    expect(represented.map((item: { statement: string }) => item.statement)).toEqual(batch.candidates.map(item => item.data));
    expectWireSchema(h, batch, behavioral);
    expectPaidAttempt(h);
  });

  it('defers an indivisible target/context set without dispatching or charging when it cannot fit', async () => {
    const h = harness();
    const candidate = memory('target', 'x'.repeat(10000));
    const active = Array.from({ length: 5 }, (_, index) => ({ ...memory(`context-${index}`, 'y'.repeat(10000)), relevant_candidate_ids: ['target'] }));
    await expect(h.service.curate([candidate], active, null, vaultId, { maxInputTokens: 12000, maxOutputTokens: 2000 }))
      .rejects.toBeInstanceOf(CuratorPreparationDeferredError);
    expect(h.fetchMock).not.toHaveBeenCalled();
    expect(usage.acquire).not.toHaveBeenCalled();
    expect(usage.settle).not.toHaveBeenCalled();
    expect(usage.record).not.toHaveBeenCalled();
  });

  it('never dispatches when the pre-request receipt rejects the live claim', async () => {
    const h = harness();
    h.accounting.beforeRequest.mockRejectedValue(new Error('Live claim lost'));
    await expect(h.service.curatePrepared(h.service.prepare([memory()], [], null), vaultId, h.accounting))
      .rejects.toThrow('Live claim lost');
    expect(h.fetchMock).not.toHaveBeenCalled();
    expect(h.accounting.returnedUsage).not.toHaveBeenCalled();
    expect(usage.settle).not.toHaveBeenCalled();
    expect(usage.record).not.toHaveBeenCalled();
  });

  if (provider.native) it('retains valid usage when native content contains an unexpected non-text block', async () => {
    const h = harness(); h.state.malformedNativeContent = true;
    const batch = h.service.prepare([memory()], [], null);
    await expect(h.service.curatePrepared(batch, vaultId, h.accounting)).rejects.toMatchObject({
      name: 'CuratorPlanValidationError', outputFailure: { operation: 'curation', stage: 'completion' }
    });
    expectPaidAttempt(h);
  });
});
