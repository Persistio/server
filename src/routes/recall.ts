import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig } from '../config';
import { query } from '../db/client';
import {
  globalRuleDeliveryCounter,
  memoryPolicyEventCounter,
  recallDeliveryCounter,
  recallDeliveryMissingAckCounter,
  recallDurationHistogram
} from '../services/observability-effects';
import { requireVaultReadAuth } from '../middleware/auth';
import { decryptForVault } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { PENDING_RECALL_WINDOW_MS, pendingRecallCutoff } from '../services/pending-memory';
import { getRawChunkStorage } from '../services/raw-chunk-storage';
import { applyRateLimitHeaders, consumeApiQuota } from '../services/usage';
import {
  DeliveryNotFoundError,
  InvalidDeliveryOutcomeError,
  isGlobalRuleDelivery,
  isUnapprovedDirectiveDelivery,
  recordRecallDelivery,
  recordRenderedDelivery,
  renderedDeliverySchema,
  type RecallDeliveryItem
} from '../services/memory-observability';
import { withSpan } from '../telemetry';
import {
  isGlobalRuleRecallable,
  activeRevocationExistsSql,
  approvalEventExistsSql,
  legacyMigrationEventExistsSql,
  memoryAuthorityPredicateSql,
  isMemoryRecallable,
  type GlobalRulePolicy,
  type MemoryAuthorityState
} from '../services/memory-authority';
import { isMemoryValidAt, memoryValidityPredicateSql, toDateOnly } from '../services/memory-validity';
import type { MemoryScope } from '../services/memory-scope';
import { provenanceIdentitySchema } from '../services/provenance-identity';
import {
  canIncludeGlobalRules,
  isMemoryApplicable,
  memoryApplicabilityPredicateSql,
  memoryEligibilityPredicateSql,
  recallContextSchema,
  type RecallContext
} from '../services/memory-applicability';

export const recallSchema = z.object({
  query: z.string().min(1),
  top_k: z.number().int().positive().max(100).optional(),
  min_similarity: z.number().min(0).max(1).optional(),
  include_raw: z.boolean().optional().default(false),
  include_evidence: z.boolean().optional().default(false),
  include_pending: z.boolean().optional().default(false),
  include_related: z.boolean().optional().default(true),
  include_global_rules: z.boolean().optional().default(false),
  client: z.object({
    name: provenanceIdentitySchema(100),
    version: provenanceIdentitySchema(100)
  }).strict().optional(),
  context: recallContextSchema.optional().default({}),
  mode: z.enum(['agent', 'factual']).optional().default('agent')
});

const recallQuerySchema = z.object({
  format: z.enum(['bundle', 'bundle_v2']).optional()
});

const renderedDeliveryParamsSchema = z.object({ deliveryId: z.string().uuid() });

interface RecallMemoryRow {
  id: string;
  data: string;
  subject: string;
  categories: string[];
  confidence: number;
  score: number;
  salience: string;
  sensitivity: string;
  type: string | null;
  scope: MemoryScope;
  scope_key: string | null;
  polarity: string;
  status: string;
  authority_state: MemoryAuthorityState;
  authority_version: string | number;
  approved_by: string | null;
  approved_at: string | null;
  approval_source: string | null;
  revoked_by: string | null;
  revoked_at: string | null;
  authority_required: boolean;
  authority_approval_valid: boolean;
  authority_revocation_active: boolean;
  authority_legacy_valid: boolean;
  valid_from: string | null;
  valid_until: string | null;
  source_timestamp: string | null;
  source_segment_id: string | null;
  source_chunks: string[] | null;
  provenance_source_classes?: string[];
  provenance_authorships?: string[];
  similarity: number | null;
  source?: 'global_behavioral' | 'semantic' | 'graph';
  edge_type?: string | null;
  created_at: string;
  updated_at: string;
  recall_count: number;
  last_recalled: string | null;
}

type RecallMemory = RecallMemoryRow;
type RecallMode = 'agent' | 'factual';
const MAX_EVIDENCE_CHUNKS = 200;
const GRAPH_RECALL_LIMIT = 20;
const RECALL_OVERFETCH_MULTIPLIER = 4;
const MIN_RECALL_CANDIDATE_LIMIT = 25;
const RECENCY_BOOST_MAX = 0.04;
const RECENCY_BOOST_WINDOW_MS = 14 * 24 * 60 * 60 * 1000;
const rawChunkStorage = getRawChunkStorage();

export { memoryAuthorityPredicateSql } from '../services/memory-authority';

export function evidenceAuthorityRecheckSql(
  memoryAlias: string,
  selectedAlias: string,
  includePendingParameter: string,
  pendingCutoffParameter: string,
  globalPolicyParameter: string,
  referenceDateParameter: string,
  sessionParameter: string,
  projectParameter: string,
  taskParameter: string,
  includeGlobalRulesParameter: string,
  recallTimeParameter: string
): string {
  return `${memoryAlias}.authority_version = ${selectedAlias}.authority_version
    AND ${memoryAlias}.archived_at IS NULL
    AND (
      ${memoryAlias}.status = 'active'
      OR (
        ${includePendingParameter}::boolean
        AND ${memoryAlias}.status = 'candidate'
        AND COALESCE(${memoryAlias}.source_timestamp, ${memoryAlias}.created_at) >= ${pendingCutoffParameter}::timestamptz
      )
    )
    AND ${memoryAuthorityPredicateSql(memoryAlias, globalPolicyParameter)}
    AND ${memoryValidityPredicateSql(memoryAlias, referenceDateParameter)}
    AND ${memoryApplicabilityPredicateSql(memoryAlias, sessionParameter, projectParameter, taskParameter, includeGlobalRulesParameter)}
    AND ${memoryEligibilityPredicateSql(memoryAlias, recallTimeParameter)}`;
}

export function evidenceRecallSql(): string {
  return `WITH selected_memories AS (
           SELECT selected.memory_id, selected.authority_version
           FROM jsonb_to_recordset($1::jsonb) AS selected(memory_id uuid, authority_version integer)
         ), evidence_memories AS (
           SELECT m.id, m.source_chunks, m.source_segment_id, m.scope, m.scope_key
           FROM selected_memories selected
           JOIN memories m ON m.id = selected.memory_id
           WHERE m.vault_id = $2
             AND ${evidenceAuthorityRecheckSql('m', 'selected', '$4', '$5', '$6', '$7', '$8', '$9', '$10', '$11', '$12')}
         ), evidence_sources AS (
           SELECT m.id AS memory_id, unnest(m.source_chunks) AS chunk_id, m.source_segment_id, m.scope, m.scope_key
           FROM evidence_memories m
           UNION
           SELECT m.id AS memory_id, unnest(s.chunk_ids) AS chunk_id, m.source_segment_id, m.scope, m.scope_key
           FROM evidence_memories m
           JOIN segments s ON s.id = m.source_segment_id
         ), ranked_chunks AS (
           SELECT es.memory_id, rc.id, rc.session_id, rc.role, rc.blob_store, rc.blob_key, rc.created_at,
                  ROW_NUMBER() OVER (PARTITION BY es.memory_id ORDER BY rc.created_at, rc.id) AS rank
           FROM evidence_sources es
           JOIN raw_chunks rc ON rc.id = es.chunk_id
           LEFT JOIN segments source_segment
             ON source_segment.id = es.source_segment_id
            AND source_segment.vault_id = $2
           WHERE rc.vault_id = $2
             AND rc.blob_key IS NOT NULL
             AND (
               es.scope = 'global'
               OR (es.scope = 'session' AND es.scope_key IS NOT NULL AND rc.session_id = es.scope_key)
               OR (es.scope = 'project' AND es.scope_key IS NOT NULL AND source_segment.project_id = es.scope_key)
               OR (es.scope = 'task' AND es.scope_key IS NOT NULL AND source_segment.task_id = es.scope_key)
             )
         )
         SELECT memory_id, id, session_id, role, blob_store, blob_key, created_at
         FROM ranked_chunks
         WHERE rank <= 6
         ORDER BY memory_id, created_at, id
         LIMIT $3`;
}

interface RecallRawChunk {
  id: string;
  session_id: string;
  role: string;
  blob_store: string | null;
  blob_key: string | null;
  content?: string;
  similarity: number;
  created_at: string;
}

interface RecallEvidenceChunk {
  memory_id: string;
  id: string;
  session_id: string;
  role: string;
  blob_store: string | null;
  blob_key: string | null;
  content?: string;
  created_at: string;
}

interface RecallSourceProvenanceRow {
  memory_id: string;
  source_classes: string[];
  authorships: string[];
}

export function recallSourceProvenanceSql(): string {
  return `WITH selected AS (
           SELECT input.memory_id, input.source_chunk_ids
           FROM jsonb_to_recordset($1::jsonb)
             AS input(memory_id uuid, source_chunk_ids uuid[])
         )
         SELECT selected.memory_id,
                COALESCE(
                  array_agg(DISTINCT rc.provenance->>'source_class' ORDER BY rc.provenance->>'source_class')
                    FILTER (WHERE rc.provenance->>'source_class' IS NOT NULL AND rc.provenance->>'source_class' <> ''),
                  '{}'::text[]
                ) AS source_classes,
                COALESCE(
                  array_agg(DISTINCT rc.provenance->>'authorship' ORDER BY rc.provenance->>'authorship')
                    FILTER (WHERE rc.provenance->>'authorship' IS NOT NULL AND rc.provenance->>'authorship' <> ''),
                  '{}'::text[]
                ) AS authorships
         FROM selected
         LEFT JOIN raw_chunks rc
           ON rc.vault_id = $2
          AND rc.id = ANY(COALESCE(selected.source_chunk_ids, '{}'::uuid[]))
         GROUP BY selected.memory_id
         ORDER BY selected.memory_id`;
}

async function attachRecallSourceProvenance(
  rows: RecallMemoryRow[],
  vaultId: string
): Promise<RecallMemoryRow[]> {
  if (rows.length === 0) return rows;
  const uniqueRows = Array.from(new Map(rows.map((row) => [row.id, row])).values());
  const result = await query<RecallSourceProvenanceRow>(recallSourceProvenanceSql(), [
    JSON.stringify(uniqueRows.map((row) => ({ memory_id: row.id, source_chunk_ids: row.source_chunks ?? [] }))),
    vaultId
  ]);
  const provenanceById = new Map(result.rows.map((row) => [row.memory_id, row]));
  return rows.map((row) => ({
    ...row,
    provenance_source_classes: provenanceById.get(row.id)?.source_classes ?? [],
    provenance_authorships: provenanceById.get(row.id)?.authorships ?? []
  }));
}

interface RecallDeliveryReceipt {
  id: string;
  selected_count: number;
  global_selected_count: number;
  selected_ids: string[];
}

interface RecallResponse {
  delivery: RecallDeliveryReceipt;
  memories: Array<Omit<RecallMemory, 'authority_approval_valid' | 'authority_revocation_active' | 'authority_legacy_valid'>>;
  related_memories: Array<Omit<RecallMemory, 'authority_approval_valid' | 'authority_revocation_active' | 'authority_legacy_valid'>>;
  evidence_chunks: RecallEvidenceChunk[];
  raw_chunks: RecallRawChunk[];
}

interface RecallBundle {
  global_user_rules: string[];
  user_rules: string[];
  user_preferences: string[];
  task_patterns: string[];
  workflows: string[];
  project: string[];
  constraints: string[];
  decisions: string[];
  system_facts: string[];
  domain_knowledge: string[];
}

interface RecallBundleResponse {
  delivery?: RecallDeliveryReceipt;
  bundle: RecallBundle;
  bundle_ids: RecallBundle;
  related_bundle?: RecallBundle;
  related_bundle_ids?: RecallBundle;
}

type StructuredRecallSection =
  | 'approved_preferences_and_rules'
  | 'historical_facts'
  | 'candidates'
  | 'graph_context';

interface StructuredRecallMemory {
  id: string;
  data: string;
  subject: string;
  type: string | null;
  status: string;
  confidence: number;
  sensitivity: string;
  scope: {
    kind: MemoryScope;
    binding: string | null;
  };
  authority: {
    state: MemoryAuthorityState;
    required: boolean;
    version: number;
    approved_by: string | null;
    approved_at: string | null;
    approval_source: string | null;
  };
  provenance: {
    source_timestamp: string | null;
    source_segment_id: string | null;
    source_chunk_ids: string[];
    source_classes: string[];
    authorships: string[];
  };
  validity: {
    valid_from: string | null;
    valid_until: string | null;
  };
  retrieval: {
    reason: 'global_behavioral' | 'semantic' | 'graph';
    similarity: number | null;
    edge_type: string | null;
  };
}

export interface StructuredRecallBundleResponse {
  schema_version: 'persistio.recall_bundle.v2';
  generated_at: string;
  authority_boundary: {
    classification: 'lower_authority_historical_data';
    may_override_current_instructions: false;
    commands_authorized: false;
  };
  delivery?: RecallDeliveryReceipt;
  sections: Record<StructuredRecallSection, StructuredRecallMemory[]>;
}

function deliveryItemsForRows(rows: RecallMemory[]): RecallDeliveryItem[] {
  const seen = new Set<string>();
  return rows.flatMap((memory) => {
    if (seen.has(memory.id)) return [];
    seen.add(memory.id);
    return [{
      memoryId: memory.id,
      authorityVersion: Number(memory.authority_version),
      section: getStructuredSection(memory),
      memoryType: memory.type,
      retrievalReason: memory.source ?? 'semantic',
      scope: memory.scope,
      scopeBinding: memory.scope_key,
      authorityState: memory.authority_state,
      authorityRequired: memory.authority_required,
      authorityApprovalValid: memory.authority_approval_valid,
      similarity: memory.similarity
    }];
  });
}

const bundleKeys = [
  'global_user_rules',
  'user_rules',
  'user_preferences',
  'task_patterns',
  'workflows',
  'project',
  'constraints',
  'decisions',
  'system_facts',
  'domain_knowledge'
] as const;

type RecallBundleKey = typeof bundleKeys[number];

const typeToBundleKey: Record<string, RecallBundleKey> = {
  user_rule: 'user_rules',
  user_preference: 'user_preferences',
  task_pattern: 'task_patterns',
  workflow: 'workflows',
  project: 'project',
  constraint: 'constraints',
  decision: 'decisions',
  system_fact: 'system_facts',
  domain_knowledge: 'domain_knowledge'
};

const agentTypeBoosts: Record<string, number> = {
  user_rule: 0.08,
  user_preference: 0.07,
  task_pattern: 0.06,
  workflow: 0.04,
  constraint: 0.03,
  decision: 0.02,
  project: 0.01,
  system_fact: 0,
  domain_knowledge: 0
};

const factualTypeBoosts: Record<string, number> = {
  system_fact: 0.08,
  domain_knowledge: 0.08,
  project: 0.06,
  decision: 0.06,
  constraint: 0.05,
  workflow: 0.02,
  user_preference: 0.01,
  task_pattern: 0.01,
  user_rule: 0
};

function createEmptyBundle(): RecallBundle {
  return {
    global_user_rules: [],
    user_rules: [],
    user_preferences: [],
    task_patterns: [],
    workflows: [],
    project: [],
    constraints: [],
    decisions: [],
    system_facts: [],
    domain_knowledge: []
  };
}

function getBundleKey(type: string | null): RecallBundleKey {
  if (!type) {
    return 'system_facts';
  }

  return typeToBundleKey[type] ?? 'system_facts';
}

const preferenceAndRuleTypes = new Set(['user_rule', 'user_preference']);

function createEmptyStructuredSections(): Record<StructuredRecallSection, StructuredRecallMemory[]> {
  return {
    approved_preferences_and_rules: [],
    historical_facts: [],
    candidates: [],
    graph_context: []
  };
}

function toStructuredRecallMemory(memory: RecallMemory): StructuredRecallMemory {
  return {
    id: memory.id,
    data: memory.data,
    subject: memory.subject,
    type: memory.type,
    status: memory.status,
    confidence: Number(memory.confidence),
    sensitivity: memory.sensitivity,
    scope: {
      kind: memory.scope,
      binding: memory.scope_key
    },
    authority: {
      state: memory.authority_state,
      required: memory.authority_required,
      version: Number(memory.authority_version),
      approved_by: memory.approved_by,
      approved_at: memory.approved_at,
      approval_source: memory.approval_source
    },
    provenance: {
      source_timestamp: memory.source_timestamp,
      source_segment_id: memory.source_segment_id,
      source_chunk_ids: memory.source_chunks === null ? [] : memory.source_chunks,
      source_classes: memory.provenance_source_classes ?? [],
      authorships: memory.provenance_authorships ?? []
    },
    validity: {
      valid_from: memory.valid_from,
      valid_until: memory.valid_until
    },
    retrieval: {
      reason: memory.source ?? 'semantic',
      similarity: memory.similarity,
      edge_type: memory.edge_type ?? null
    }
  };
}

function getStructuredSection(memory: RecallMemory): StructuredRecallSection {
  if (memory.source === 'graph') return 'graph_context';
  if (memory.status === 'candidate') return 'candidates';
  if (memory.authority_state === 'approved' && preferenceAndRuleTypes.has(memory.type ?? '')) {
    return 'approved_preferences_and_rules';
  }
  return 'historical_facts';
}

function getSimilarity(row: RecallMemoryRow): number {
  return row.similarity ?? 0;
}

export function toPublicRecallMemory(
  row: RecallMemory
): Omit<RecallMemory, 'authority_approval_valid' | 'authority_revocation_active' | 'authority_legacy_valid'> {
  const {
    authority_approval_valid: _internalApprovalCheck,
    authority_revocation_active: _internalRevocationCheck,
    authority_legacy_valid: _internalLegacyCheck,
    ...memory
  } = row;
  return memory;
}

function compareGlobalRows(left: RecallMemoryRow, right: RecallMemoryRow): number {
  return Number(right.salience) - Number(left.salience)
    || new Date(right.created_at).getTime() - new Date(left.created_at).getTime()
    || left.id.localeCompare(right.id);
}

function getModeTypeBoost(type: string | null, mode: RecallMode): number {
  if (!type) {
    return 0;
  }

  const boosts = mode === 'agent' ? agentTypeBoosts : factualTypeBoosts;
  return boosts[type] ?? 0;
}

function getMemoryTimestampMs(row: RecallMemoryRow): number {
  const timestamp = row.source_timestamp ?? row.updated_at ?? row.created_at;
  const parsed = new Date(timestamp).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function getPendingTimestampMs(row: RecallMemoryRow): number {
  const timestamp = row.source_timestamp ?? row.created_at;
  const parsed = new Date(timestamp).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function isFreshPendingMemory(row: RecallMemoryRow, now: Date): boolean {
  if (row.status !== 'candidate') {
    return false;
  }

  const timestampMs = getPendingTimestampMs(row);
  const nowMs = now.getTime();
  if (!timestampMs || !Number.isFinite(nowMs)) {
    return false;
  }

  return Math.max(0, nowMs - timestampMs) <= PENDING_RECALL_WINDOW_MS;
}

function isRecallableRow(
  row: RecallMemoryRow,
  includePending: boolean,
  now: Date,
  globalRulePolicy: GlobalRulePolicy,
  context: RecallContext,
  includeGlobalRules: boolean
): boolean {
  return isMemoryRecallable(
    row.type,
    row.scope,
    row.authority_state,
    row.authority_approval_valid,
    globalRulePolicy,
    row.authority_required,
    row.authority_revocation_active,
    row.authority_legacy_valid
  )
    && isMemoryValidAt(row, now)
    && isMemoryApplicable(row, context, includeGlobalRules, now)
    && (row.status === 'active' || (includePending && isFreshPendingMemory(row, now)));
}

function getRecencyBoost(row: RecallMemoryRow, now: Date): number {
  const timestampMs = getMemoryTimestampMs(row);
  const nowMs = now.getTime();
  if (!timestampMs || !Number.isFinite(nowMs)) {
    return 0;
  }

  const ageMs = Math.max(0, nowMs - timestampMs);
  if (ageMs >= RECENCY_BOOST_WINDOW_MS) {
    return 0;
  }

  return RECENCY_BOOST_MAX * (1 - (ageMs / RECENCY_BOOST_WINDOW_MS));
}

function getModeRankScore(row: RecallMemoryRow, mode: RecallMode, now: Date): number {
  return getSimilarity(row) + getModeTypeBoost(row.type, mode) + getRecencyBoost(row, now);
}

function compareModeRankedRows(mode: RecallMode, now: Date) {
  return (left: RecallMemoryRow, right: RecallMemoryRow): number => (
    getModeRankScore(right, mode, now) - getModeRankScore(left, mode, now)
      || getSimilarity(right) - getSimilarity(left)
      || Number(right.salience) - Number(left.salience)
      || getMemoryTimestampMs(right) - getMemoryTimestampMs(left)
      || left.id.localeCompare(right.id)
  );
}

export function recallCandidateLimit(topK: number): number {
  return Math.max(topK, MIN_RECALL_CANDIDATE_LIMIT, topK * RECALL_OVERFETCH_MULTIPLIER);
}

export function remainingRecallBudget(topK: number, ...selectedLanes: RecallMemoryRow[][]): number {
  const selectedIds = new Set(selectedLanes.flatMap((lane) => lane.map((row) => row.id)));
  return Math.max(0, topK - selectedIds.size);
}

export function composeRecallRows(
  rows: RecallMemoryRow[],
  topK: number,
  mode: RecallMode,
  minSimilarity = 0,
  now = new Date(),
  includePending = false,
  globalRulePolicy: GlobalRulePolicy = 'approved_only',
  context: RecallContext = {},
  includeGlobalRules = false
): RecallMemoryRow[] {
  const queryRelevantRows = rows.filter((row) => (
    row.source !== 'global_behavioral'
      && row.source !== 'graph'
      && isRecallableRow(row, includePending, now, globalRulePolicy, context, includeGlobalRules)
      && (row.source !== 'semantic' || getSimilarity(row) >= minSimilarity)
  ));

  return queryRelevantRows
    .sort(compareModeRankedRows(mode, now))
    .slice(0, topK);
}

export function composeRelatedRecallRows(
  rows: RecallMemoryRow[],
  directRows: RecallMemoryRow[],
  limit = GRAPH_RECALL_LIMIT,
  globalRulePolicy: GlobalRulePolicy = 'approved_only',
  now = new Date(),
  context: RecallContext = {},
  includeGlobalRules = false
): RecallMemoryRow[] {
  if (limit <= 0) return [];
  const seenIds = new Set(directRows.map((row) => row.id));
  const relatedRows: RecallMemoryRow[] = [];

  for (const row of rows) {
    if (
      row.source !== 'graph'
      || row.status !== 'active'
      || !isMemoryValidAt(row, now)
      || !isMemoryApplicable(row, context, includeGlobalRules, now)
      || !isMemoryRecallable(
        row.type,
        row.scope,
        row.authority_state,
        row.authority_approval_valid,
        globalRulePolicy,
        row.authority_required,
        row.authority_revocation_active,
        row.authority_legacy_valid
      )
      || seenIds.has(row.id)
    ) {
      continue;
    }

    seenIds.add(row.id);
    relatedRows.push(row);
    if (relatedRows.length >= limit) {
      break;
    }
  }

  return relatedRows;
}

export function combineSemanticCandidateRows(activeRows: RecallMemoryRow[], pendingRows: RecallMemoryRow[]): RecallMemoryRow[] {
  const seenIds = new Set<string>();
  const combinedRows: RecallMemoryRow[] = [];

  for (const row of [...activeRows, ...pendingRows]) {
    if (seenIds.has(row.id)) {
      continue;
    }

    seenIds.add(row.id);
    combinedRows.push(row);
  }

  return combinedRows;
}

export function buildRecallBundle(
  memories: RecallMemory[],
  globalUserRules: RecallMemory[] = [],
  globalRulePolicy: GlobalRulePolicy = 'approved_only',
  now = new Date(),
  context: RecallContext = {},
  includeGlobalRules = false
): RecallBundleResponse {
  const grouped = memories.filter((memory) => !(memory.type === 'user_rule' && memory.scope === 'global')
    && isMemoryValidAt(memory, now)
    && isMemoryApplicable(memory, context, includeGlobalRules, now)
    && isMemoryRecallable(
    memory.type,
    memory.scope,
    memory.authority_state,
    memory.authority_approval_valid,
    globalRulePolicy,
    memory.authority_required,
    memory.authority_revocation_active,
    memory.authority_legacy_valid
  ))
    .reduce<Record<RecallBundleKey, RecallMemory[]>>((bundle, memory) => {
    const key = getBundleKey(memory.type);
    bundle[key].push(memory);
    return bundle;
  }, {
    global_user_rules: [],
    user_rules: [],
    user_preferences: [],
    task_patterns: [],
    workflows: [],
    project: [],
    constraints: [],
    decisions: [],
    system_facts: [],
    domain_knowledge: []
  });

  const bundle = createEmptyBundle();
  const bundleIds = createEmptyBundle();
  const globals = globalUserRules
    .filter((memory) => includeGlobalRules
      && isMemoryApplicable(memory, context, includeGlobalRules, now)
      && isMemoryValidAt(memory, now) && isGlobalRuleRecallable(
      globalRulePolicy,
      memory.authority_state,
      memory.authority_approval_valid,
      memory.authority_revocation_active,
      memory.authority_legacy_valid
    ))
    .sort(compareGlobalRows);
  for (const memory of globals) {
    bundle.global_user_rules.push(memory.data);
    bundleIds.global_user_rules.push(memory.id);
  }

  for (const key of bundleKeys.filter((key) => key !== 'global_user_rules')) {
    for (const memory of grouped[key]) {
      bundle[key].push(memory.data);
      bundleIds[key].push(memory.id);
    }
  }

  return { bundle, bundle_ids: bundleIds };
}

export function buildStructuredRecallBundle(
  memories: RecallMemory[],
  relatedMemories: RecallMemory[] = [],
  globalUserRules: RecallMemory[] = [],
  globalRulePolicy: GlobalRulePolicy = 'approved_only',
  now = new Date(),
  context: RecallContext = {},
  includeGlobalRules = false
): StructuredRecallBundleResponse {
  const direct = memories.filter((memory) => !(memory.type === 'user_rule' && memory.scope === 'global')
    && (memory.status === 'active' || memory.status === 'candidate')
    && isMemoryValidAt(memory, now)
    && isMemoryApplicable(memory, context, includeGlobalRules, now)
    && isMemoryRecallable(
      memory.type,
      memory.scope,
      memory.authority_state,
      memory.authority_approval_valid,
      globalRulePolicy,
      memory.authority_required,
      memory.authority_revocation_active,
      memory.authority_legacy_valid
    ));
  const globals = globalUserRules
    .filter((memory) => includeGlobalRules
      && memory.type === 'user_rule'
      && memory.scope === 'global'
      && memory.scope_key === null
      && memory.status === 'active'
      && memory.source === 'global_behavioral'
      && isMemoryApplicable(memory, context, includeGlobalRules, now)
      && isMemoryValidAt(memory, now)
      && isGlobalRuleRecallable(
        globalRulePolicy,
        memory.authority_state,
        memory.authority_approval_valid,
        memory.authority_revocation_active,
        memory.authority_legacy_valid
      ))
    .sort(compareGlobalRows);
  const related = relatedMemories.filter((memory) => memory.source === 'graph'
    && memory.status === 'active'
    && isMemoryValidAt(memory, now)
    && isMemoryApplicable(memory, context, includeGlobalRules, now)
    && isMemoryRecallable(
      memory.type,
      memory.scope,
      memory.authority_state,
      memory.authority_approval_valid,
      globalRulePolicy,
      memory.authority_required,
      memory.authority_revocation_active,
      memory.authority_legacy_valid
    ));

  const sections = createEmptyStructuredSections();
  const seenIds = new Set<string>();
  for (const memory of [...globals, ...direct, ...related]) {
    if (seenIds.has(memory.id)) continue;
    seenIds.add(memory.id);
    sections[getStructuredSection(memory)].push(toStructuredRecallMemory(memory));
  }

  return {
    schema_version: 'persistio.recall_bundle.v2',
    generated_at: now.toISOString(),
    authority_boundary: {
      classification: 'lower_authority_historical_data',
      may_override_current_instructions: false,
      commands_authorized: false
    },
    sections
  };
}

/** Project once; ledger identity is extracted from the actual response, not earlier candidates. */
export function projectRecallDelivery(input: {
  format: 'json' | 'bundle' | 'bundle_v2'; direct: RecallMemory[]; related: RecallMemory[];
  globals: RecallMemory[]; policy: GlobalRulePolicy; now: Date; context: RecallContext;
  includeGlobalRules: boolean; includeRelated: boolean;
  evidenceChunks: RecallEvidenceChunk[]; rawChunks: RecallRawChunk[];
}): { response: Omit<RecallResponse, 'delivery'> | RecallBundleResponse | StructuredRecallBundleResponse; items: RecallDeliveryItem[] } {
  const structured = buildStructuredRecallBundle(input.direct, input.includeRelated ? input.related : [], input.globals,
    input.policy, input.now, input.context, input.includeGlobalRules);
  const rows = new Map([...input.globals, ...input.direct, ...input.related].map(row => [row.id, row]));
  const eligibleIds = new Set(Object.values(structured.sections).flat().map(memory => memory.id));
  const projected = [...input.globals, ...input.direct, ...input.related].filter(row => eligibleIds.has(row.id));
  let response: Omit<RecallResponse, 'delivery'> | RecallBundleResponse | StructuredRecallBundleResponse;
  let ids: string[];
  if (input.format === 'bundle_v2') {
    response = structured;
    ids = Object.values(structured.sections).flat().map(memory => memory.id);
  } else if (input.format === 'bundle') {
    response = buildRecallBundle(projected.filter(row => row.source !== 'graph' && row.source !== 'global_behavioral'),
      projected.filter(row => row.source === 'global_behavioral'), input.policy, input.now, input.context, input.includeGlobalRules);
    if (input.includeRelated) {
      const related = buildRecallBundle(projected.filter(row => row.source === 'graph'), [], input.policy,
        input.now, input.context, input.includeGlobalRules);
      response.related_bundle = related.bundle;
      response.related_bundle_ids = related.bundle_ids;
    }
    ids = [...Object.values(response.bundle_ids).flat(), ...Object.values(response.related_bundle_ids ?? {}).flat()];
  } else {
    // Preserve retrieval rank (the section grouping above is not a rank ordering).
    const allowed = new Set(projected.map(row => row.id));
    const direct = [...input.globals, ...input.direct].filter(row => allowed.has(row.id));
    const related = input.includeRelated ? input.related.filter(row => allowed.has(row.id)) : [];
    response = { memories: direct.map(toPublicRecallMemory), related_memories: related.map(toPublicRecallMemory),
      evidence_chunks: input.evidenceChunks, raw_chunks: input.rawChunks };
    ids = [...response.memories, ...response.related_memories].map(memory => memory.id);
  }
  if (ids.length > 100 || new Set(ids).size !== ids.length || ids.some(id => !rows.has(id))) {
    throw new InvalidDeliveryOutcomeError('Recall projection has inconsistent identity');
  }
  return { response, items: deliveryItemsForRows(ids.map(id => rows.get(id)!)) };
}

export async function registerRecallRoutes(app: FastifyInstance) {
  app.post('/v1/recall', { preHandler: requireVaultReadAuth }, async (request, reply) => {
    const parsedBody = recallSchema.safeParse(request.body);
    const parsedQuery = recallQuerySchema.safeParse(request.query);
    if (!parsedBody.success || !parsedQuery.success) {
      return reply.code(400).send({ error: 'Invalid recall request' });
    }
    const body = parsedBody.data;
    const qs = parsedQuery.data;
    const isBundleFormat = qs.format === 'bundle' || qs.format === 'bundle_v2';
    if (body.include_raw && (!body.context.session_id || isBundleFormat)) {
      return reply.code(400).send({ error: 'Raw recall requires context.session_id and the default JSON format' });
    }
    const config = getConfig();
    const topK = body.top_k ?? config.DEFAULT_RECALL_TOP_K;
    const includeGlobalRules = canIncludeGlobalRules(body.include_global_rules, body.mode, body.context);
    // All formats use the same bounded global lane; semantic/graph selection
    // must not independently reintroduce global directives into the partition.
    const includeGlobalRulesInContext = false;
    const rateLimit = await consumeApiQuota(request.vault.id, 'searches', 'api');
    applyRateLimitHeaders(reply, rateLimit);

    return withSpan('recall.request', {
      'vault.id': request.vault.id,
      'recall.include_raw': body.include_raw,
      'recall.include_evidence': body.include_evidence,
      'recall.include_pending': body.include_pending,
      'recall.include_related': body.include_related,
      'recall.include_global_rules_requested': body.include_global_rules,
      'recall.include_global_rules_effective': includeGlobalRules,
      'recall.top_k': topK,
      'recall.mode': body.mode,
      'recall.min_similarity': body.min_similarity ?? config.MIN_RECALL_SIMILARITY
    }, async (span) => {
      const start = performance.now();
      const recallTime = new Date();
      const embedder = getEmbedder();
      const embedding = await embedder.embed(body.query, { vaultId: request.vault.id, modelRole: 'embedding', source: 'api', inputType: 'query' });
      const minSimilarity = body.min_similarity ?? config.MIN_RECALL_SIMILARITY;
      const candidateLimit = recallCandidateLimit(topK);
      const pendingCutoff = pendingRecallCutoff(recallTime);
      const recallDate = toDateOnly(recallTime);
      if (!recallDate) {
        throw new Error('Unable to derive a valid recall date');
      }

      const globalRuleLimit = Math.min(5, topK);
      const globalRuleResult = includeGlobalRules && config.GLOBAL_RULE_POLICY !== 'off'
        ? await query<RecallMemoryRow>(
        `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.scope_key, m.polarity,
                m.status, m.authority_state, m.authority_version, m.approved_by, m.approved_at, m.approval_source, m.revoked_by, m.revoked_at, m.authority_required,
                ${approvalEventExistsSql('m')} AS authority_approval_valid,
                ${activeRevocationExistsSql('m')} AS authority_revocation_active,
                ${legacyMigrationEventExistsSql('m')} AS authority_legacy_valid,
                m.valid_from, m.valid_until, m.source_timestamp, m.source_segment_id, m.source_chunks,
                m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                0.0::double precision AS similarity,
                'global_behavioral' AS source
         FROM memories m
         WHERE m.vault_id = $1
           AND m.type = 'user_rule'
           AND m.scope = 'global'
           AND m.scope_key IS NULL
           AND m.status = 'active'
           AND ${memoryAuthorityPredicateSql('m', '$2')}
           AND ${memoryValidityPredicateSql('m', '$3')}
           AND ${memoryEligibilityPredicateSql('m', '$4')}
           AND m.archived_at IS NULL
         ORDER BY m.salience DESC, m.created_at DESC, m.id
         LIMIT $5`,
        [request.vault.id, config.GLOBAL_RULE_POLICY, recallDate, recallTime.toISOString(), globalRuleLimit]
      )
        : { rows: [] as RecallMemoryRow[] };

      const semanticResult = await query<RecallMemoryRow>(
        `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.scope_key, m.polarity,
                m.status, m.authority_state, m.authority_version, m.approved_by, m.approved_at, m.approval_source, m.revoked_by, m.revoked_at, m.authority_required,
                ${approvalEventExistsSql('m')} AS authority_approval_valid,
                ${activeRevocationExistsSql('m')} AS authority_revocation_active,
                ${legacyMigrationEventExistsSql('m')} AS authority_legacy_valid,
                m.valid_from, m.valid_until, m.source_timestamp, m.source_segment_id, m.source_chunks,
                m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                1 - (me.embedding <=> $2::vector) AS similarity,
                'semantic' AS source
         FROM memories m
         JOIN memory_embeddings me ON me.memory_id = m.id
         WHERE m.vault_id = $1
           AND m.archived_at IS NULL
           AND m.status = 'active'
           AND ${memoryAuthorityPredicateSql('m', '$4')}
           AND ${memoryValidityPredicateSql('m', '$5')}
           AND ${memoryApplicabilityPredicateSql('m', '$6', '$7', '$8', '$9')}
           AND ${memoryEligibilityPredicateSql('m', '$10')}
         ORDER BY me.embedding <=> $2::vector
         LIMIT $3`,
        [
          request.vault.id, JSON.stringify(embedding), candidateLimit, config.GLOBAL_RULE_POLICY, recallDate,
          body.context.session_id ?? null, body.context.project_id ?? null, body.context.task_id ?? null,
          includeGlobalRulesInContext, recallTime.toISOString()
        ]
      );
      const pendingResult = body.include_pending
        ? await query<RecallMemoryRow>(
          `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.scope_key, m.polarity,
                  m.status, m.authority_state, m.authority_version, m.approved_by, m.approved_at, m.approval_source, m.revoked_by, m.revoked_at, m.authority_required,
                  ${approvalEventExistsSql('m')} AS authority_approval_valid,
                  ${activeRevocationExistsSql('m')} AS authority_revocation_active,
                  ${legacyMigrationEventExistsSql('m')} AS authority_legacy_valid,
                  m.valid_from, m.valid_until, m.source_timestamp, m.source_segment_id, m.source_chunks,
                  m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                  1 - (me.embedding <=> $2::vector) AS similarity,
                  'semantic' AS source
           FROM memories m
           JOIN memory_embeddings me ON me.memory_id = m.id
           WHERE m.vault_id = $1
             AND m.archived_at IS NULL
             AND m.status = 'candidate'
             AND ${memoryAuthorityPredicateSql('m', '$5')}
             AND ${memoryValidityPredicateSql('m', '$6')}
             AND ${memoryApplicabilityPredicateSql('m', '$7', '$8', '$9', '$10')}
             AND ${memoryEligibilityPredicateSql('m', '$11')}
             AND COALESCE(m.source_timestamp, m.created_at) >= $4::timestamptz
           ORDER BY me.embedding <=> $2::vector
           LIMIT $3`,
          [
            request.vault.id, JSON.stringify(embedding), candidateLimit, pendingCutoff.toISOString(), config.GLOBAL_RULE_POLICY, recallDate,
            body.context.session_id ?? null, body.context.project_id ?? null, body.context.task_id ?? null,
            includeGlobalRulesInContext, recallTime.toISOString()
          ]
        )
        : { rows: [] as RecallMemoryRow[] };

      const globalIds = new Set(globalRuleResult.rows.map((row) => row.id));
      const directBudget = remainingRecallBudget(topK, globalRuleResult.rows);
      const semanticRows = combineSemanticCandidateRows(semanticResult.rows, pendingResult.rows)
        .filter((row) => !globalIds.has(row.id) && getSimilarity(row) >= minSimilarity);
      const directRows = composeRecallRows(
        semanticRows,
        directBudget,
        body.mode,
        minSimilarity,
        recallTime,
        body.include_pending,
        config.GLOBAL_RULE_POLICY,
        body.context,
        includeGlobalRulesInContext
      );
      const semanticIds = directRows.map((row) => row.id);
      const neighborResult = body.include_related && semanticIds.length
        ? await query<RecallMemoryRow>(
          // Directed traversal is intentional here: edges are stored as A -> B, so querying A finds B
          // neighbors, but querying B does not walk back to A in the current retrieval model.
          `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.scope_key, m.polarity,
                  m.status, m.authority_state, m.authority_version, m.approved_by, m.approved_at, m.approval_source, m.revoked_by, m.revoked_at, m.authority_required,
                  ${approvalEventExistsSql('m')} AS authority_approval_valid,
                  ${activeRevocationExistsSql('m')} AS authority_revocation_active,
                  ${legacyMigrationEventExistsSql('m')} AS authority_legacy_valid,
                  m.valid_from, m.valid_until, m.source_timestamp, m.source_segment_id, m.source_chunks,
                  m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                  NULL::double precision AS similarity, 'graph' AS source, e.type AS edge_type
           FROM memory_edges e
           JOIN memories m ON m.id = e.to_memory_id
           WHERE e.from_memory_id = ANY($1::uuid[])
             AND m.vault_id = $2
             AND e.vault_id = $2
             AND m.status = 'active'
             AND ${memoryAuthorityPredicateSql('m', '$4')}
             AND ${memoryValidityPredicateSql('m', '$5')}
             AND ${memoryApplicabilityPredicateSql('m', '$6', '$7', '$8', '$9')}
             AND ${memoryEligibilityPredicateSql('m', '$10')}
             AND m.archived_at IS NULL
           ORDER BY m.salience DESC, e.confidence DESC, m.updated_at DESC, m.id
           LIMIT $3`,
          [
            semanticIds, request.vault.id, GRAPH_RECALL_LIMIT, config.GLOBAL_RULE_POLICY, recallDate,
            body.context.session_id ?? null, body.context.project_id ?? null, body.context.task_id ?? null,
            includeGlobalRulesInContext, recallTime.toISOString()
          ]
        )
        : { rows: [] as RecallMemoryRow[] };

      const relatedRows = composeRelatedRecallRows(
        neighborResult.rows,
        [...globalRuleResult.rows, ...directRows],
        Math.min(GRAPH_RECALL_LIMIT, remainingRecallBudget(topK, globalRuleResult.rows, directRows)),
        config.GLOBAL_RULE_POLICY,
        recallTime,
        body.context,
        includeGlobalRulesInContext
      );
      const recalledRows = [...globalRuleResult.rows, ...directRows, ...relatedRows];

      if (recalledRows.length) {
        void query(
          `UPDATE memories
           SET last_recalled = now(),
               recall_count = recall_count + 1
           WHERE id = ANY($1::uuid[])
             AND vault_id = $2`,
          [recalledRows.map((row) => row.id), request.vault.id]
        ).catch((error: unknown) => {
          request.log.warn({ err: error, vault_id: request.vault.id }, 'Failed to record recall metadata');
        });
      }

      let evidenceChunks: RecallEvidenceChunk[] = [];
      if (body.include_evidence && !isBundleFormat && recalledRows.length) {
        const selectedAuthorityVersions = recalledRows.map((row) => ({
          memory_id: row.id,
          authority_version: Number(row.authority_version)
        }));
        const evidenceResult = await query<RecallEvidenceChunk>(
          evidenceRecallSql(),
          [
            JSON.stringify(selectedAuthorityVersions),
            request.vault.id,
            MAX_EVIDENCE_CHUNKS,
            body.include_pending,
            pendingCutoff.toISOString(),
            config.GLOBAL_RULE_POLICY,
            recallDate,
            body.context.session_id ?? null,
            body.context.project_id ?? null,
            body.context.task_id ?? null,
            includeGlobalRules,
            recallTime.toISOString()
          ]
        );
        evidenceChunks = evidenceResult.rows;
      }

      let rawChunks: RecallRawChunk[] = [];

      if (body.include_raw && !isBundleFormat && body.context.session_id) {
        const rawResult = await query<RecallRawChunk>(
          `SELECT id, session_id, role, blob_store, blob_key, created_at,
                  1 - (embedding <=> $2::vector) AS similarity
           FROM raw_chunks
           WHERE vault_id = $1
             AND session_id = $4
             AND embedding IS NOT NULL
             AND blob_key IS NOT NULL
           ORDER BY embedding <=> $2::vector
           LIMIT $3`,
          [request.vault.id, JSON.stringify(embedding), candidateLimit, body.context.session_id]
        );
        rawChunks = rawResult.rows
          .filter((row) => row.similarity >= minSimilarity)
          .slice(0, topK);
      }

      const responseRows = qs.format === 'bundle_v2'
        ? await attachRecallSourceProvenance(recalledRows, request.vault.id)
        : recalledRows;
      const responseRowsById = new Map(responseRows.map((row) => [row.id, row]));
      const responseDirectRows = directRows.map((row) => responseRowsById.get(row.id) ?? row);
      const responseRelatedRows = relatedRows.map((row) => responseRowsById.get(row.id) ?? row);
      const responseGlobalRows = globalRuleResult.rows.map((row) => responseRowsById.get(row.id) ?? row);

      const decryptedMemories = await Promise.all(responseDirectRows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedRelatedMemories = await Promise.all(responseRelatedRows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedGlobalUserRules = await Promise.all(responseGlobalRows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedEvidenceChunks = await Promise.all(evidenceChunks.map(async (row) => ({
        ...row,
        content: await decryptForVault(request.vault, await readRawChunkContent(row))
      })));
      const decryptedRawChunks = await Promise.all(rawChunks.map(async (row) => ({
        ...row,
        content: await decryptForVault(request.vault, await readRawChunkContent(row))
      })));

      const responseFormat = qs.format ?? 'json';
      const projection = projectRecallDelivery({ format: responseFormat,
        direct: decryptedMemories, related: decryptedRelatedMemories, globals: decryptedGlobalUserRules,
        policy: config.GLOBAL_RULE_POLICY, now: recallTime, context: body.context,
        includeGlobalRules, includeRelated: body.include_related,
        evidenceChunks: decryptedEvidenceChunks, rawChunks: decryptedRawChunks });
      const deliveryItems = projection.items;
      const deliveryId = await recordRecallDelivery({
        vaultId: request.vault.id,
        query: body.query,
        responseFormat,
        mode: body.mode,
        clientName: body.client?.name ?? null,
        clientVersion: body.client?.version ?? null,
        context: body.context,
        topK,
        minSimilarity,
        globalRulePolicy: config.GLOBAL_RULE_POLICY,
        includeGlobalRulesRequested: body.include_global_rules,
        includeGlobalRulesEffective: includeGlobalRules,
        items: deliveryItems
      });
      recallDeliveryCounter.add(deliveryItems.length, { stage: 'selected', response_format: responseFormat });
      recallDeliveryCounter.add(deliveryItems.length, { stage: 'returned', response_format: responseFormat });
      const globalSelectedCount = deliveryItems.filter(isGlobalRuleDelivery).length;
      const delivery: RecallDeliveryReceipt = {
        id: deliveryId,
        selected_count: deliveryItems.length,
        global_selected_count: globalSelectedCount,
        selected_ids: deliveryItems.map((item) => item.memoryId)
      };
      if (globalSelectedCount > 0) {
        globalRuleDeliveryCounter.add(globalSelectedCount, { stage: 'selected', policy: config.GLOBAL_RULE_POLICY });
      }
      const unapprovedDirectiveCount = deliveryItems.filter(isUnapprovedDirectiveDelivery).length;
      if (unapprovedDirectiveCount > 0) {
        memoryPolicyEventCounter.add(unapprovedDirectiveCount, {
          event: 'unapproved_directive_recall',
          source: 'recall_delivery',
          outcome: 'selected'
        });
      }

      const durationMs = performance.now() - start;
      recallDurationHistogram.record(durationMs, {
        vault_id: request.vault.id,
        include_raw: String(body.include_raw)
      });
      span.setAttribute('recall.results_returned', directRows.length);
      span.setAttribute('recall.semantic_candidates_returned', semanticResult.rows.length);
      span.setAttribute('recall.pending_candidates_returned', pendingResult.rows.length);
      span.setAttribute('recall.semantic_candidates_accepted', semanticRows.length);
      span.setAttribute('recall.pending_candidates_accepted', directRows.filter((row) => row.status === 'candidate').length);
      span.setAttribute('recall.related_results_returned', relatedRows.length);
      span.setAttribute('recall.evidence_results_returned', evidenceChunks.length);
      span.setAttribute('recall.raw_results_returned', rawChunks.length);
      span.setAttribute('recall.duration_ms', durationMs);

      return { ...projection.response, delivery };
    });
  });

  app.post('/v1/recall/:deliveryId/rendered', { preHandler: requireVaultReadAuth }, async (request, reply) => {
    const parsedParams = renderedDeliveryParamsSchema.safeParse(request.params);
    const parsedBody = renderedDeliverySchema.safeParse(request.body);
    if (!parsedParams.success || !parsedBody.success) {
      recallDeliveryMissingAckCounter.add(1, { reason: 'invalid_request' });
      return reply.code(400).send({ error: 'Invalid delivery acknowledgement' });
    }
    const params = parsedParams.data;
    const body = parsedBody.data;
    try {
      const outcome = await recordRenderedDelivery({
        vaultId: request.vault.id,
        deliveryId: params.deliveryId,
        renderedIds: body.rendered_ids,
        dropped: body.dropped,
        tokenBudget: body.token_budget,
        renderedTokens: body.rendered_tokens,
        truncated: body.truncated,
        renderTarget: body.render_target
      });
      recallDeliveryCounter.add(outcome.rendered, { stage: 'rendered', response_format: outcome.responseFormat });
      recallDeliveryCounter.add(outcome.dropped, { stage: 'dropped', response_format: outcome.responseFormat });
      if (outcome.globalRendered > 0) {
        globalRuleDeliveryCounter.add(outcome.globalRendered, { stage: 'rendered', policy: outcome.globalRulePolicy });
      }
      if (outcome.unapprovedDirectiveRendered > 0) {
        memoryPolicyEventCounter.add(outcome.unapprovedDirectiveRendered, {
          event: 'unapproved_directive_recall',
          source: 'recall_delivery',
          outcome: 'rendered'
        });
      }
      return { accepted: true, recorded: outcome.inserted };
    } catch (error) {
      recallDeliveryMissingAckCounter.add(1, {
        reason: error instanceof DeliveryNotFoundError ? 'not_found'
          : error instanceof InvalidDeliveryOutcomeError ? 'invalid_outcome' : 'persistence'
      });
      if (error instanceof DeliveryNotFoundError) {
        return reply.code(404).send({ error: error.message });
      }
      if (error instanceof InvalidDeliveryOutcomeError) {
        return reply.code(400).send({ error: error.message });
      }
      throw error;
    }
  });
}

async function readRawChunkContent(row: Pick<RecallRawChunk, 'id' | 'blob_store' | 'blob_key'>): Promise<string> {
  if (!row.blob_key) {
    throw new Error(`Raw chunk ${row.id} has no blob_key`);
  }
  if (row.blob_store && row.blob_store !== rawChunkStorage.store) {
    throw new Error(`Raw chunk ${row.id} is stored in ${row.blob_store}, but configured storage is ${rawChunkStorage.store}`);
  }
  return rawChunkStorage.get(row.blob_key);
}
