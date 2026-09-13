import { parseModelJson } from './model-json';
import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('curator');
import crypto from 'node:crypto';
import OpenAI from 'openai';
import { z } from 'zod';

import { getConfig } from '../config';
import { CircuitBreakerOpenError, ServiceCircuitBreaker, isAuthFailureError } from './ai-resilience';
import { PromptLoader } from './prompt-loader';
import { acquireAiBudget, recordModelUsage, settleAiUsage } from './usage';
import { CURATOR_CONTRACT, CURATOR_SCHEMA_VERSION, CURATOR_PROMPT_VERSION, curatorResultSchema, nonBehavioralCuratorResultSchema, validateCuratorContract, type CuratorResult, type CuratorRawSource } from './curator-contract';
import { sourceHasHumanIntent } from './extraction-contract';
import { completeModelRequest, modelResponseFormat, serializeModelRequest } from './model-completion';
import { ModelOutputContractError } from './model-output-error';
import { MIN_CURATOR_INPUT_TOKENS } from './curator-limits';
export { CURATOR_SCHEMA_VERSION, CURATOR_PROMPT_VERSION, type CuratorResult } from './curator-contract';
import { withSystemPromptPrefix } from './chat-completion';
import {
  MAX_CUSTOM_CURATION_PROMPT_BYTES,
  resolveVaultPrompt,
  type VaultPromptContext
} from './vault-prompts';
import type { MemoryScope } from './memory-scope';
import { isSecretLikeMemoryContent } from './deterministic-filter';
import { compileCuratorGraph, type CompiledCuratorGraph } from './curator-graph';

export type MemoryType = 'user_preference' | 'user_rule' | 'task_pattern' | 'workflow' | 'project' | 'constraint' | 'decision' | 'system_fact' | 'domain_knowledge';
export type EdgeType = 'applies_to' | 'part_of' | 'depends_on' | 'supports' | 'contradicts' | 'supersedes' | 'refines' | 'relevant_when';

export interface CuratorMemory {
  id: string;
  subject: string;
  data: string;
  type: MemoryType | null;
  scope: MemoryScope;
  scope_key: string | null;
  salience: number;
  confidence?: number;
  sensitivity: 'low' | 'medium' | 'high' | 'restricted';
  polarity: 'positive' | 'negative' | 'neutral';
  volatility: 'very_low' | 'low' | 'medium' | 'high';
  evidence?: string | null;
  evidence_record?: unknown;
  source_chunks?: string[];
  source_timestamp?: string | null;
  /** Substantive memory revision captured when the curator context was loaded. */
  row_version?: string;
  parent_id: string | null;
  valid_from?: string | null;
  valid_until?: string | null;
  /** Retrieval provenance. Missing means all supplied candidates, never no context. */
  relevant_candidate_ids?: string[];
}

export interface CuratorAliasMaps {
  aliasToId: Map<string, string>;
  idToAlias: Map<string, string>;
}

export interface CuratorUsage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
}

export interface CuratorAttemptAccounting {
  beforeRequest(): Promise<void>;
  returnedUsage(usage: CuratorUsage): Promise<void>;
}

const GRAPH_CONTRACT = 'Graph endpoints use final M/N aliases, not subjects. Preserve evidence, current bindings and distinct temporal states. No standalone new memories.';
const curatorResponseFormat = modelResponseFormat(curatorResultSchema, 'curation_plan');
const nonBehavioralCuratorResponseFormat = modelResponseFormat(nonBehavioralCuratorResultSchema, 'curation_plan');
const HARDCODED_PROMPT = 'Improve the organisation and usefulness of already extracted durable memories. Do not gate their availability.';
const CURATOR_SYSTEM_OVERHEAD_CHARS = 1000;
const MIN_CURATOR_SYSTEM_PROMPT_CHARS = 2000;
const MIN_CURATOR_USER_CONTENT_CHARS = 4000;
const TARGET_CURATOR_USER_CONTENT_CHARS = 12000;

export interface CuratorValidationAudit {
  model: string;
  schemaVersion: typeof CURATOR_SCHEMA_VERSION;
  promptVersion: typeof CURATOR_PROMPT_VERSION;
  promptHash: string;
  validationErrors: string[];
  rawResponse: unknown;
}

export class CuratorPlanValidationError extends Error {
  constructor(message: string, readonly audit: CuratorValidationAudit, readonly outputFailure?: ModelOutputContractError) {
    // Queue/dead-letter records persist this message, whereas the private review
    // audit stores validationErrors. Retain the same bounded structural detail in
    // both paths; never interpolate the provider response or a raw Zod error.
    super(outputFailure && message !== outputFailure.message ? `${message}: ${outputFailure.message}` : message);
    this.name = 'CuratorPlanValidationError';
  }
}

export class CuratorPreparationDeferredError extends Error {
  constructor(message = 'Curator candidate and its retrieved context exceed the configured request/output capacity') {
    super(message);
    this.name = 'CuratorPreparationDeferredError';
  }
}

export interface PreparedCuratorBatch {
  candidates: CuratorMemory[];
  activeMemories: CuratorMemory[];
  deferredCandidateIds: string[];
  aliasMaps: CuratorAliasMaps;
  request: OpenAI.Chat.Completions.ChatCompletionCreateParamsNonStreaming;
  requestHash: string;
  rawSources: CuratorRawSource[];
}

type CuratorOptions = { rawSources?: CuratorRawSource[]; maxInputTokens?: number; maxOutputTokens?: number; vaultPromptContext?: VaultPromptContext | null };

export function buildAliasMaps(targets: CuratorMemory[], context: CuratorMemory[]): CuratorAliasMaps {
  const aliasToId = new Map<string,string>();
  const idToAlias = new Map<string,string>();
  for (const [index,memory] of [...targets,...context].entries()) {
    if (idToAlias.has(memory.id)) throw new Error('Duplicate curator input');
    const alias = `M${index+1}`;
    aliasToId.set(alias,memory.id); idToAlias.set(memory.id,alias);
  }
  return {aliasToId,idToAlias};
}

function formatMemories(title: string, memories: CuratorMemory[], aliases: CuratorAliasMaps): string {
  // Full records only: a model must not replace a complete memory from a clipped
  // excerpt. prepare() defers records that cannot fit the configured capacity.
  return JSON.stringify({section:title,memories:memories.map(memory => ({
    id:aliases.idToAlias.get(memory.id),subject:memory.subject,statement:memory.data,
    type:memory.type,scope:memory.scope,scope_key:memory.scope_key,
    salience:memory.salience,confidence:memory.confidence,sensitivity:memory.sensitivity,
    polarity:memory.polarity,volatility:memory.volatility,evidence:memory.evidence,
    source_timestamp:memory.source_timestamp ?? null,
    valid_from:memory.valid_from ?? null,valid_until:memory.valid_until ?? null,
    parent:memory.parent_id ? aliases.idToAlias.get(memory.parent_id) ?? '(outside reviewed context)' : null
  }))});
}

function truncateText(value: string, maxChars: number): string {
  if (value.length <= maxChars) return value;
  if (maxChars <= 0) return '';
  const marker = '\n...[truncated]';
  if (maxChars <= marker.length) return value.slice(0, maxChars);
  return `${value.slice(0, maxChars - marker.length)}${marker}`;
}

function buildBoundedSystemPrompt(basePrompt: string, maxChars: number): string {
  const separator = '\n\n';
  const contract = `${CURATOR_CONTRACT}\n${GRAPH_CONTRACT}`;
  const requiredChars = separator.length + contract.length;
  if (maxChars < requiredChars) {
    throw new CuratorPreparationDeferredError('Curator system-prompt budget is too small for the mandatory contract');
  }
  return `${truncateText(basePrompt, maxChars - requiredChars)}${separator}${contract}`;
}

function allocateCuratorPromptBudget(systemPromptLength: number, maxPromptChars: number): { system: number; user: number } {
  const available = Math.max(0, maxPromptChars - CURATOR_SYSTEM_OVERHEAD_CHARS);
  if (available <= 0) {
    return { system: 0, user: 0 };
  }

  const desiredSystem = Math.min(systemPromptLength, MAX_CUSTOM_CURATION_PROMPT_BYTES);
  if (desiredSystem <= 0) {
    return { system: 0, user: available };
  }

  if (available <= MIN_CURATOR_SYSTEM_PROMPT_CHARS + MIN_CURATOR_USER_CONTENT_CHARS) {
    const system = Math.min(desiredSystem, Math.max(1, Math.floor(available * 0.4)));
    return { system, user: Math.max(0, available - system) };
  }

  const userReserve = Math.min(
    TARGET_CURATOR_USER_CONTENT_CHARS,
    Math.max(MIN_CURATOR_USER_CONTENT_CHARS, Math.floor(available * 0.35))
  );
  const system = Math.min(desiredSystem, Math.max(1, available - userReserve));
  return { system, user: Math.max(0, available - system) };
}

export class CuratorService {
  // This breaker is keyed to the shared curator API key, not per-vault state. A bad key
  // affects every vault using this service, so opening the breaker process-wide is correct.
  private static readonly circuitBreaker = new ServiceCircuitBreaker('curator');
  private readonly client: OpenAI;
  private readonly model: string;
  private readonly provider: string;
  private readonly baseURL: string;
  private readonly promptLoader: PromptLoader;

  constructor() {
    const config = getConfig();
    this.client = new OpenAI({
      apiKey: config.CURATOR_API_KEY,
      baseURL: config.CURATOR_BASE_URL
    });
    this.model = config.CURATOR_MODEL;
    this.baseURL = config.CURATOR_BASE_URL;
    this.provider = getProviderLabel(config.CURATOR_BASE_URL);
    this.promptLoader = new PromptLoader({
      promptFile: config.CURATOR_PROMPT_FILE,
      promptsDir: config.PROMPTS_DIR,
      fallback: HARDCODED_PROMPT,
      label: 'curator'
    });
  }

  prepare(
    candidates: CuratorMemory[],
    activeMemories: CuratorMemory[],
    rawConversation: string | null,
    options: CuratorOptions = {}
  ): PreparedCuratorBatch {
    const resolvedPrompt = resolveVaultPrompt({
      role: 'curation',
      defaultPrompt: this.promptLoader.getPrompt(),
      vault: options.vaultPromptContext
    });
    // Vault-specific prompts may add policy, but can never replace or suppress
    // the server-owned output contract.
    const resolvedSystemPrompt = `${resolvedPrompt}\n\n${CURATOR_CONTRACT}\n${GRAPH_CONTRACT}`;
    const inputTokens = options.maxInputTokens ?? MIN_CURATOR_INPUT_TOKENS;
    if (!Number.isSafeInteger(inputTokens) || inputTokens < MIN_CURATOR_INPUT_TOKENS) {
      throw new CuratorPreparationDeferredError(`Curator input budget must be at least ${MIN_CURATOR_INPUT_TOKENS} whole tokens`);
    }
    const maxPromptChars = inputTokens * 4;
    if (!Number.isFinite(maxPromptChars) || maxPromptChars <= 0
      || (options.maxOutputTokens !== undefined && (!Number.isFinite(options.maxOutputTokens) || options.maxOutputTokens <= 0))) {
      throw new CuratorPreparationDeferredError('Invalid curator request capacity');
    }
    // Capture prefixes once. Measure the SDK's actual wire serialization,
    // including the static schema and native provider request envelope.
    const config = getConfig();
    const prefix = config.LLM_SYSTEM_PROMPT_PREFIX.trim();
    const reasoningEffort = config.LLM_REASONING_EFFORT.trim();
    const emptyRequest: OpenAI.Chat.Completions.ChatCompletionCreateParamsNonStreaming = {
      model: this.model, temperature: 0, response_format: curatorResponseFormat, max_tokens: options.maxOutputTokens,
      messages: [{ role: 'system', content: '' }, { role: 'user', content: [{ type: 'text', text: '' }] }]
    };
    const fixedRequestChars = serializeModelRequest(withSystemPromptPrefix(emptyRequest, prefix, reasoningEffort), this.baseURL).length;
    const requiredSystemChars = CURATOR_CONTRACT.length + GRAPH_CONTRACT.length + 3;
    if (maxPromptChars - fixedRequestChars - CURATOR_SYSTEM_OVERHEAD_CHARS < requiredSystemChars) {
      throw new CuratorPreparationDeferredError('Curator system-prompt budget is too small for the mandatory contract');
    }
    const promptBudget = allocateCuratorPromptBudget(resolvedSystemPrompt.length, maxPromptChars - fixedRequestChars);
    const systemPrompt = buildBoundedSystemPrompt(resolvedPrompt, Math.max(requiredSystemChars, promptBudget.system));
    const build = (selected: CuratorMemory[], sourceCount: number) => {
      const selectedIds = new Set(selected.map(memory => memory.id));
      const active = activeMemories.filter(memory => !selectedIds.has(memory.id) && (!memory.relevant_candidate_ids
        || memory.relevant_candidate_ids.some(id => selectedIds.has(id))));
      const aliasMaps = buildAliasMaps(selected, active);
      // Match the existing per-action validator, using precisely this trial's
      // reviewed memories. Optional raw evidence is not a prerequisite for
      // improving an already extracted preference or rule.
      const responseFormat = [...selected, ...active].some(memory => memory.type === 'user_rule' || memory.type === 'user_preference')
        ? curatorResponseFormat : nonBehavioralCuratorResponseFormat;
      const sourceIds = new Set([...selected,...active].flatMap(m => m.source_chunks ?? []));
      const rawSources = (options.rawSources ?? []).filter(s => sourceIds.has(s.id)).slice(0,sourceCount);
      const request = withSystemPromptPrefix({ model: this.model, temperature: 0, messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: [
          { type: 'text', text: formatMemories('Selected active improvement targets', selected, aliasMaps) },
          { type: 'text', text: formatMemories('Reviewed active context', active, aliasMaps) },
          { type: 'text', text: JSON.stringify({sources:rawSources.map((s,i) => ({ref:`S${i+1}`,transport_role:s.role,human_intent_source:sourceHasHumanIntent(s),content:s.content,provenance:s.provenance,observed_at:s.created_at,available_scope_bindings:s.context}))}) }
        ] }
      ], max_tokens: options.maxOutputTokens, response_format: responseFormat }, prefix, reasoningEffort);
      return { candidates: selected, activeMemories: active, aliasMaps, request, rawSources };
    };
    const fits = (batch: ReturnType<typeof build>) => serializeModelRequest(batch.request, this.baseURL).length
      <= maxPromptChars - CURATOR_SYSTEM_OVERHEAD_CHARS;
    const outputFits = (count: number) => options.maxOutputTokens === undefined
      || options.maxOutputTokens >= 64 + count * 24; // Minimum disposition envelope, not an output guarantee.
    let selected: CuratorMemory[] = [];
    for (const candidate of candidates) {
      const trial = [...selected, candidate];
      if (outputFits(trial.length) && fits(build(trial, 0))) selected = trial;
    }
    if ((candidates.length > 0 && selected.length === 0) || !outputFits(0) || !fits(build(selected, 0))) {
      throw new CuratorPreparationDeferredError();
    }
    // Raw evidence supports optional scope changes. Never block ordinary
    // improvement merely because its raw supporting object cannot fit as well.
    let sourceCount = Math.min(options.rawSources?.length ?? 0,8);
    while (sourceCount>0 && !fits(build(selected,sourceCount))) sourceCount--;
    const batch = build(selected, sourceCount);
    const selectedIds = new Set(selected.map(memory => memory.id));
    return { ...batch, deferredCandidateIds: candidates.filter(memory => !selectedIds.has(memory.id)).map(memory => memory.id),
      requestHash: crypto.createHash('sha256').update(serializeModelRequest(batch.request, this.baseURL)).digest('hex') };
  }

  async curate(candidates: CuratorMemory[], activeMemories: CuratorMemory[], rawConversation: string | null,
    vaultId?: string, options: CuratorOptions = {}) {
    return this.curatePrepared(this.prepare(candidates, activeMemories, rawConversation, options), vaultId);
  }

  async curatePrepared(batch: PreparedCuratorBatch, vaultId?: string, accounting?: CuratorAttemptAccounting): Promise<{
    result: CuratorResult; graph: CompiledCuratorGraph; aliasMaps: CuratorAliasMaps;
    rawResponse: unknown; usage: CuratorUsage | null;
    audit: Omit<CuratorValidationAudit, 'validationErrors' | 'rawResponse'>;
  }> {
    const { candidates, activeMemories, aliasMaps } = batch;
    const response = await this.createChatCompletion(batch.request, vaultId, accounting);

    const usage = response.usage;
    if (usage) {
      operationalLog.log(JSON.stringify({
        level: 30,
        msg: 'curator token usage',
        model: this.model,
        model_role: 'curation',
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens,
        candidates_count: candidates.length,
        active_memories_count: activeMemories.length
      }));
    }

    const promptHash = batch.requestHash;
    const rawResponse = {
      request: {
        model: this.model,
        schema_version: CURATOR_SCHEMA_VERSION,
        prompt_version: CURATOR_PROMPT_VERSION,
        prompt_hash: promptHash,
        candidate_ids: candidates.map(memory => memory.id),
        active_ids: activeMemories.map(memory => memory.id),
        deferred_candidate_ids: batch.deferredCandidateIds
      },
      response
    };
    const finishReason = response.choices[0]?.finish_reason;
    if (finishReason !== 'stop' || response.choices[0]?.message?.refusal) {
      const outputFailure = new ModelOutputContractError('curation', response.choices[0]?.message?.refusal
        ? 'refusal' : finishReason === 'length' ? 'truncated' : 'completion');
      const reason = outputFailure.message;
      throw new CuratorPlanValidationError(reason, {
        model: this.model,
        schemaVersion: CURATOR_SCHEMA_VERSION,
        promptVersion: CURATOR_PROMPT_VERSION,
        promptHash,
        validationErrors: [reason],
        rawResponse
      }, outputFailure);
    }

    const rawText = response.choices[0]?.message?.content?.trim();
    if (!rawText) {
      throw new CuratorPlanValidationError('Empty response from curator model', {
        model: this.model,
        schemaVersion: CURATOR_SCHEMA_VERSION,
        promptVersion: CURATOR_PROMPT_VERSION,
        promptHash,
        validationErrors: ['Response content is empty'],
        rawResponse
      }, new ModelOutputContractError('curation', 'json'));
    }

    let parsed: unknown;
    try {
      parsed = parseModelJson(rawText);
    } catch (error) {
      const reason = 'Invalid curator response JSON';
      throw new CuratorPlanValidationError(reason, {
        model: this.model,
        schemaVersion: CURATOR_SCHEMA_VERSION,
        promptVersion: CURATOR_PROMPT_VERSION,
        promptHash,
        validationErrors: [reason],
        rawResponse
      }, new ModelOutputContractError('curation', 'json'));
    }

    let result: CuratorResult;
    let graph: CompiledCuratorGraph;
    try {
      ({ result, graph } = validateAndCompileCuratorResult(parsed, candidates, activeMemories, aliasMaps, batch.rawSources));
    } catch (error) {
      const validationErrors = error instanceof CuratorPlanValidationError
        ? error.audit.validationErrors
        : [error instanceof Error ? error.message : String(error)];
      throw new CuratorPlanValidationError('Curator response failed closed validation', {
        model: this.model,
        schemaVersion: CURATOR_SCHEMA_VERSION,
        promptVersion: CURATOR_PROMPT_VERSION,
        promptHash,
        validationErrors,
        rawResponse
      }, error instanceof ModelOutputContractError ? error : undefined);
    }

    return {
      result,
      graph,
      aliasMaps,
      usage: usage
        ? {
          promptTokens: usage.prompt_tokens,
          completionTokens: usage.completion_tokens,
          totalTokens: usage.total_tokens
        }
        : null,
      rawResponse,
      audit: {
        model: this.model,
        schemaVersion: CURATOR_SCHEMA_VERSION,
        promptVersion: CURATOR_PROMPT_VERSION,
        promptHash
      }
    };
  }

  private async createChatCompletion(
    input: OpenAI.Chat.Completions.ChatCompletionCreateParamsNonStreaming,
    vaultId?: string,
    accounting?: CuratorAttemptAccounting
  ) {
    CuratorService.circuitBreaker.beforeRequest();
    const requestInput = input;
    const estimatedTokens = Math.max(256, Math.ceil(serializeModelRequest(requestInput, this.baseURL).length / 4));

    try {
      if (vaultId) {
        // TODO: This reserves request/token quota before the API call. If the call later fails
        // with a retriable non-auth, non-rate-limit error, there is no refund path yet. Fixing
        // that would require tracking and reconciling pre-call quota reservations.
        await acquireAiBudget(vaultId, 'curation', estimatedTokens);
      }

      await accounting?.beforeRequest();
      const response = await completeModelRequest(this.client, requestInput, this.baseURL);
      if (response.usage) await accounting?.returnedUsage({promptTokens:response.usage.prompt_tokens,
        completionTokens:response.usage.completion_tokens,totalTokens:response.usage.total_tokens});
      if (vaultId && response.usage?.total_tokens) {
        try {
          await settleAiUsage(vaultId, 'curation', estimatedTokens, response.usage.total_tokens);
        } catch (error) {
          operationalLog.warn(JSON.stringify({
            level: 40,
            msg: 'settle_ai_usage_overage',
            service: 'curator',
            vaultId,
            error: error instanceof Error ? error.message : String(error)
          }));
        }
        try {
          await recordModelUsage({
            vaultId,
            provider: this.provider,
            model: this.model,
            modelRole: 'curation',
            source: 'curation_worker',
            requestCount: 1,
            promptTokens: response.usage.prompt_tokens,
            completionTokens: response.usage.completion_tokens,
            totalTokens: response.usage.total_tokens
          });
        } catch (error) {
          operationalLog.warn(JSON.stringify({
            level: 40,
            msg: 'failed to record model usage',
            service: 'curator',
            vaultId,
            model: this.model,
            provider: this.provider,
            error: error instanceof Error ? error.message : String(error)
          }));
        }
      }
      CuratorService.circuitBreaker.onSuccess();
      return response;
    } catch (error) {
      const breakerResult = CuratorService.circuitBreaker.onFailure(error);
      if (breakerResult.opened && isAuthFailureError(error)) {
        operationalLog.warn(JSON.stringify({
          level: 40,
          msg: 'circuit_breaker_open',
          service: 'curator',
          next_probe_at: breakerResult.nextProbeAt ? new Date(breakerResult.nextProbeAt).toISOString() : null
        }));
      }

      if (error instanceof CircuitBreakerOpenError) {
        operationalLog.warn(JSON.stringify({
          level: 40,
          msg: 'circuit_breaker_open',
          service: 'curator',
          retry_after_ms: error.retryAfterMs
        }));
      }
      throw error;
    }
  }
}

function getProviderLabel(baseURL: string): string {
  try {
    const host = new URL(baseURL).hostname.toLowerCase();
    if (host.includes('anthropic.com')) return 'anthropic';
    if (host.includes('generativelanguage.googleapis.com')) return 'google';
    if (host.includes('openai.com')) return 'openai';
    if (host.includes('ollama')) return 'ollama';
    return host;
  } catch {
    return 'unknown';
  }
}

export function validateCuratorResult(
  value: unknown,
  candidates: CuratorMemory[],
  activeMemories: CuratorMemory[],
  aliasMaps: CuratorAliasMaps = buildAliasMaps(candidates, activeMemories)
): CuratorResult {
  return validateAndCompileCuratorResult(value, candidates, activeMemories, aliasMaps).result;
}

function validateAndCompileCuratorResult(
  value: unknown, candidates: CuratorMemory[], activeMemories: CuratorMemory[], aliasMaps: CuratorAliasMaps, rawSources: CuratorRawSource[] = []
): { result: CuratorResult; graph: CompiledCuratorGraph } {
  const result = validateCuratorContract(value,candidates,activeMemories,aliasMaps,rawSources);
  return {result,graph:compileCuratorGraph(result,candidates,activeMemories,aliasMaps)};
}
