import crypto from 'node:crypto';
import { z } from 'zod';

import { withTransaction } from '../db/client';

export interface RecallDeliveryItem {
  memoryId: string;
  authorityVersion: number;
  section: string;
  memoryType: string | null;
  retrievalReason: string;
  scope: string;
  scopeBinding: string | null;
  authorityState: string;
  authorityRequired: boolean;
  authorityApprovalValid: boolean;
  similarity: number | null;
}

export interface RecallDeliveryInput {
  vaultId: string;
  query: string;
  responseFormat: 'json' | 'bundle' | 'bundle_v2';
  mode: 'agent' | 'factual';
  clientName: string | null;
  clientVersion: string | null;
  context: {
    session_id?: string;
    project_id?: string;
    task_id?: string;
    agent_id?: string;
    trigger_type?: string;
  };
  topK: number;
  minSimilarity: number;
  globalRulePolicy: string;
  includeGlobalRulesRequested: boolean;
  includeGlobalRulesEffective: boolean;
  items: RecallDeliveryItem[];
}

export interface RenderedDeliveryInput {
  vaultId: string;
  deliveryId: string;
  renderedIds: string[];
  dropped: Array<{ id: string; reason: string }>;
  tokenBudget: number;
  renderedTokens: number;
  truncated: boolean;
  renderTarget: 'prompt_context' | 'tool_response';
}

interface StoredDeliveryItem {
  memory_id: string;
  authorityVersion: number;
  section: string;
  memoryType: string | null;
  retrievalReason: string;
  scope: string;
  scopeBinding: string | null;
  authorityState: string;
  authorityRequired: boolean;
  authorityApprovalValid: boolean;
  similarity: number | null;
}

interface StoredTerminalOutcome {
  memory_id: string;
  stage: 'rendered' | 'dropped';
  drop_reason: string | null;
  token_budget: number;
  rendered_tokens: number;
  truncated: boolean;
  render_target: 'prompt_context' | 'tool_response';
}

const directiveMemoryTypes = new Set([
  'user_rule', 'user_preference', 'task_pattern', 'workflow', 'constraint'
]);

export function isGlobalRuleDelivery(item: RecallDeliveryItem | StoredDeliveryItem): boolean {
  return item.memoryType === 'user_rule' && item.scope === 'global';
}

export function isUnapprovedDirectiveDelivery(item: RecallDeliveryItem | StoredDeliveryItem): boolean {
  return directiveMemoryTypes.has(item.memoryType ?? '') && !item.authorityApprovalValid;
}

export class DeliveryNotFoundError extends Error {}
export class InvalidDeliveryOutcomeError extends Error {}

export const renderedDeliverySchema = z.object({
  rendered_ids: z.array(z.string().uuid().transform(id => id.toLowerCase())).max(100),
  dropped: z.array(z.object({
    id: z.string().uuid().transform(id => id.toLowerCase()),
    reason: z.enum(['token_budget', 'duplicate', 'invalid', 'client_policy', 'empty_block'])
  }).strict()).max(100),
  token_budget: z.number().int().min(0).max(2147483647),
  rendered_tokens: z.number().int().min(0).max(2147483647),
  truncated: z.boolean(),
  render_target: z.enum(['prompt_context', 'tool_response'])
}).strict().superRefine((body, ctx) => {
  const ids = [...body.rendered_ids, ...body.dropped.map(item => item.id)];
  if (ids.length > 100 || new Set(ids).size !== ids.length || body.rendered_tokens > body.token_budget) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'Invalid delivery partition or accounting' });
  }
});

export function canonicalDeliveryOutcome(input: RenderedDeliveryInput) {
  const parsed = renderedDeliverySchema.safeParse({
    rendered_ids: input.renderedIds, dropped: input.dropped, token_budget: input.tokenBudget,
    rendered_tokens: input.renderedTokens, truncated: input.truncated, render_target: input.renderTarget
  });
  if (!parsed.success || !z.string().uuid().safeParse(input.vaultId).success
    || !z.string().uuid().safeParse(input.deliveryId).success) {
    throw new InvalidDeliveryOutcomeError('Invalid delivery outcome');
  }
  return { ...parsed.data, rendered_ids: parsed.data.rendered_ids.sort(),
    dropped: parsed.data.dropped.sort((a, b) => a.id.localeCompare(b.id)) };
}

export function hashRecallQuery(queryText: string): string {
  return crypto.createHash('sha256').update(queryText).digest('hex');
}

export function validateRenderedDeliveryPartition(
  selectedIds: ReadonlySet<string>,
  renderedIdsInput: string[],
  droppedIdsInput: string[]
): void {
  const renderedIds = new Set(renderedIdsInput);
  const droppedIds = new Set(droppedIdsInput);
  const outcomeIds = new Set([...renderedIds, ...droppedIds]);
  if (renderedIds.size !== renderedIdsInput.length
    || droppedIds.size !== droppedIdsInput.length
    || [...renderedIds].some((id) => droppedIds.has(id))
    || outcomeIds.size !== selectedIds.size
    || [...outcomeIds].some((id) => !selectedIds.has(id))) {
    throw new InvalidDeliveryOutcomeError('Rendered and dropped IDs must partition the selected delivery exactly');
  }
}

export function isExactTerminalDeliveryRetry(
  existing: StoredTerminalOutcome[],
  selectedIds: ReadonlySet<string>,
  input: RenderedDeliveryInput
): boolean {
  if (existing.length !== selectedIds.size) return false;
  const renderedIds = new Set(input.renderedIds);
  const dropReasons = new Map(input.dropped.map((item) => [item.id, item.reason]));
  return existing.every((item) => selectedIds.has(item.memory_id)
    && item.stage === (renderedIds.has(item.memory_id) ? 'rendered' : 'dropped')
    && item.drop_reason === (renderedIds.has(item.memory_id) ? null : dropReasons.get(item.memory_id))
    && item.token_budget === input.tokenBudget
    && item.rendered_tokens === input.renderedTokens
    && item.truncated === input.truncated
    && item.render_target === input.renderTarget);
}

export async function recordRecallDelivery(input: RecallDeliveryInput): Promise<string> {
  input = { ...input, context: { ...input.context }, items: input.items.map(item => ({ ...item, memoryId: item.memoryId.toLowerCase() })) };
  if (input.items.length > 100 || new Set(input.items.map(item => item.memoryId)).size !== input.items.length) {
    throw new InvalidDeliveryOutcomeError('Invalid delivery selection');
  }
  return withTransaction(async (client) => {
    const run = await client.query<{ id: string }>(
      `INSERT INTO memory_delivery_runs (
         vault_id, query_hash, response_format, mode, client_name, client_version,
         session_id, project_id, task_id, agent_id, trigger_type, top_k,
         min_similarity, global_rule_policy, include_global_rules_requested,
         include_global_rules_effective, selected_count, global_selected_count
       ) VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
       ) RETURNING id`,
      [
        input.vaultId,
        hashRecallQuery(input.query),
        input.responseFormat,
        input.mode,
        input.clientName,
        input.clientVersion,
        input.context.session_id ?? null,
        input.context.project_id ?? null,
        input.context.task_id ?? null,
        input.context.agent_id ?? null,
        input.context.trigger_type ?? null,
        input.topK,
        input.minSimilarity,
        input.globalRulePolicy,
        input.includeGlobalRulesRequested,
        input.includeGlobalRulesEffective,
        input.items.length,
        input.items.filter(isGlobalRuleDelivery).length
      ]
    );
    const deliveryId = run.rows[0]?.id;
    if (!deliveryId) throw new Error('Failed to create recall delivery evidence');

    if (input.items.length > 0) {
      const serialized = JSON.stringify(input.items.map((item) => ({
        memory_id: item.memoryId,
        authority_version: item.authorityVersion,
        section: item.section,
        memory_type: item.memoryType,
        retrieval_reason: item.retrievalReason,
        scope: item.scope,
        scope_binding: item.scopeBinding,
        authority_state: item.authorityState,
        authority_required: item.authorityRequired,
        authority_approval_valid: item.authorityApprovalValid,
        similarity: item.similarity
      })));
      // Insert selection before returned events: the DB verifies each snapshot.
      for (const stage of ['selected', 'returned']) await client.query(
        `WITH selected AS (
           SELECT * FROM jsonb_to_recordset($3::jsonb) AS item(
             memory_id UUID, authority_version BIGINT, section TEXT, memory_type TEXT,
             retrieval_reason TEXT, scope TEXT, scope_binding TEXT, authority_state TEXT,
             authority_required BOOLEAN, authority_approval_valid BOOLEAN, similarity DOUBLE PRECISION
           )
         )
         INSERT INTO memory_delivery_events (
           delivery_id, vault_id, memory_id, authority_version, stage, section,
           memory_type, retrieval_reason, scope, scope_binding, authority_state,
           authority_required, authority_approval_valid, similarity
         )
         SELECT $1, $2, memory_id, authority_version, $4, section, memory_type,
                retrieval_reason, scope, scope_binding, authority_state,
                authority_required, authority_approval_valid, similarity
         FROM selected`,
        [deliveryId, input.vaultId, serialized, stage]
      );
    }
    return deliveryId;
  });
}

export async function recordRenderedDelivery(input: RenderedDeliveryInput): Promise<{
  inserted: number;
  rendered: number;
  dropped: number;
  globalRendered: number;
  unapprovedDirectiveRendered: number;
  responseFormat: 'json' | 'bundle' | 'bundle_v2';
  globalRulePolicy: string;
}> {
  const canonical = canonicalDeliveryOutcome(input);
  // Own immutable input before the first await; callers cannot change a retry.
  input = { ...input, vaultId: input.vaultId.toLowerCase(), deliveryId: input.deliveryId.toLowerCase(),
    renderedIds: canonical.rendered_ids, dropped: canonical.dropped,
    tokenBudget: canonical.token_budget, renderedTokens: canonical.rendered_tokens,
    truncated: canonical.truncated, renderTarget: canonical.render_target };
  return withTransaction(async (client) => {
    // Lock the delivery before reading terminal events. Concurrent acknowledgements
    // then serialize, so only an exact retry can follow the first committed outcome.
    const run = await client.query<{
      selected_count: number;
      response_format: 'json' | 'bundle' | 'bundle_v2';
      global_rule_policy: string;
      completion_protocol: number;
    }>(
      `SELECT selected_count, response_format, global_rule_policy, completion_protocol
       FROM memory_delivery_runs WHERE id = $1 AND vault_id = $2 FOR UPDATE`,
      [input.deliveryId, input.vaultId]
    );
    if (!run.rowCount) throw new DeliveryNotFoundError('Recall delivery was not found for this vault');

    const evidence = await client.query<{ selection_valid: boolean; terminal_valid: boolean; terminal_count: number }>(
      'SELECT * FROM persistio_delivery_integrity($1,$2)', [input.deliveryId,input.vaultId]);
    if (!evidence.rows[0]?.selection_valid || (evidence.rows[0].terminal_count > 0 && !evidence.rows[0].terminal_valid)) {
      throw new InvalidDeliveryOutcomeError('Recall delivery has inconsistent retained evidence');
    }
    const duplicate = await client.query<{ exact: boolean }>(
      `SELECT outcome=$3::jsonb AS exact FROM memory_delivery_acknowledgements WHERE delivery_id=$1 AND vault_id=$2`,
      [input.deliveryId,input.vaultId,JSON.stringify(canonical)]);
    const replayResult = () => ({ inserted: 0, rendered: 0, dropped: 0, globalRendered: 0,
      unapprovedDirectiveRendered: 0, responseFormat: run.rows[0]!.response_format,
      globalRulePolicy: run.rows[0]!.global_rule_policy });
    if (duplicate.rowCount) {
      if (!duplicate.rows[0].exact) throw new InvalidDeliveryOutcomeError('Recall delivery already has a different immutable terminal outcome');
      return replayResult();
    }

    const selected = await client.query<StoredDeliveryItem>(
      `SELECT memory_id, authority_version AS "authorityVersion", section,
              memory_type AS "memoryType", retrieval_reason AS "retrievalReason",
              scope, scope_binding AS "scopeBinding", authority_state AS "authorityState",
              authority_required AS "authorityRequired",
              authority_approval_valid AS "authorityApprovalValid", similarity
       FROM memory_delivery_events
       WHERE delivery_id = $1 AND vault_id = $2 AND stage = 'selected'
       ORDER BY memory_id`,
      [input.deliveryId, input.vaultId]
    );
    const selectedIds = new Set(selected.rows.map((row) => row.memory_id));
    const renderedIds = new Set(input.renderedIds);
    const droppedIds = new Set(input.dropped.map((item) => item.id));
    if (Number(run.rows[0]?.selected_count) !== selectedIds.size
      || input.renderedTokens > input.tokenBudget) {
      throw new InvalidDeliveryOutcomeError('Delivery counts or token accounting do not match the selected delivery');
    }
    validateRenderedDeliveryPartition(selectedIds, input.renderedIds, input.dropped.map((item) => item.id));

    const existing = await client.query<StoredTerminalOutcome>(
      `SELECT memory_id, stage, drop_reason, token_budget, rendered_tokens, truncated, render_target
       FROM memory_delivery_events
       WHERE delivery_id = $1 AND vault_id = $2 AND stage IN ('rendered', 'dropped')
       ORDER BY memory_id`,
      [input.deliveryId, input.vaultId]
    );
    if (existing.rowCount) {
      if (run.rows[0].completion_protocol !== 0 || !isExactTerminalDeliveryRetry(existing.rows, selectedIds, input)) {
        throw new InvalidDeliveryOutcomeError('Recall delivery already has a different immutable terminal outcome');
      }
      await client.query(`INSERT INTO memory_delivery_acknowledgements(delivery_id,vault_id,outcome,origin,original_terminal_at)
        SELECT $1,$2,$3::jsonb,'legacy_terminal_events',last_terminal_at FROM persistio_delivery_integrity($1,$2)`,
      [input.deliveryId,input.vaultId,JSON.stringify(canonical)]);
      return replayResult();
    }

    const dropReasons = new Map(input.dropped.map((item) => [item.id, item.reason]));
    let inserted = 0;
    for (const item of selected.rows) {
      const isRendered = renderedIds.has(item.memory_id);
      const result = await client.query(
        `INSERT INTO memory_delivery_events (
           delivery_id, vault_id, memory_id, authority_version, stage, section,
           memory_type, retrieval_reason, scope, scope_binding, authority_state,
           authority_required, authority_approval_valid, similarity, drop_reason,
           token_budget, rendered_tokens, truncated, render_target
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
         )`,
        [
          input.deliveryId, input.vaultId, item.memory_id, item.authorityVersion,
          isRendered ? 'rendered' : 'dropped', item.section, item.memoryType,
          item.retrievalReason, item.scope, item.scopeBinding, item.authorityState,
          item.authorityRequired, item.authorityApprovalValid, item.similarity,
          isRendered ? null : dropReasons.get(item.memory_id), input.tokenBudget,
          input.renderedTokens, input.truncated, input.renderTarget
        ]
      );
      inserted += result.rowCount ?? 0;
    }
    // Even an empty selection has one immutable run-level outcome.
    await client.query(`INSERT INTO memory_delivery_acknowledgements(delivery_id,vault_id,outcome,origin)
      VALUES($1,$2,$3::jsonb,'client_ack')`, [input.deliveryId,input.vaultId,JSON.stringify(canonical)]);
    return {
      inserted,
      rendered: renderedIds.size,
      dropped: droppedIds.size,
      globalRendered: selected.rows.filter((item) => renderedIds.has(item.memory_id)
        && isGlobalRuleDelivery(item)).length,
      unapprovedDirectiveRendered: selected.rows.filter((item) => renderedIds.has(item.memory_id)
        && isUnapprovedDirectiveDelivery(item)).length,
      responseFormat: run.rows[0]!.response_format,
      globalRulePolicy: run.rows[0]!.global_rule_policy
    };
  });
}
