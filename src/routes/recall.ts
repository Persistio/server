import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig } from '../config';
import { query } from '../db/client';
import { recallDurationHistogram } from '../metrics';
import { requireVaultReadAuth } from '../middleware/auth';
import { decryptForVault } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { PENDING_RECALL_WINDOW_MS, pendingRecallCutoff } from '../services/pending-memory';
import { getRawChunkStorage } from '../services/raw-chunk-storage';
import { applyRateLimitHeaders, consumeApiQuota } from '../services/usage';
import { withSpan } from '../telemetry';

const recallSchema = z.object({
  query: z.string().min(1),
  top_k: z.number().int().positive().max(100).optional(),
  min_similarity: z.number().min(0).max(1).optional(),
  include_raw: z.boolean().optional().default(false),
  include_evidence: z.boolean().optional().default(false),
  include_pending: z.boolean().optional().default(false),
  include_related: z.boolean().optional().default(true),
  mode: z.enum(['agent', 'factual']).optional().default('agent')
});

const recallQuerySchema = z.object({
  format: z.enum(['bundle']).optional()
});

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
  scope: string;
  polarity: string;
  status: string;
  valid_from: string | null;
  valid_until: string | null;
  source_timestamp: string | null;
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

interface RecallResponse {
  memories: RecallMemory[];
  related_memories: RecallMemory[];
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
  bundle: RecallBundle;
  related_bundle?: RecallBundle;
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

function getSimilarity(row: RecallMemoryRow): number {
  return row.similarity ?? 0;
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

function isRecallableRow(row: RecallMemoryRow, includePending: boolean, now: Date): boolean {
  return row.status === 'active' || (includePending && isFreshPendingMemory(row, now));
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

export function composeRecallRows(
  rows: RecallMemoryRow[],
  topK: number,
  mode: RecallMode,
  minSimilarity = 0,
  now = new Date(),
  includePending = false
): RecallMemoryRow[] {
  const queryRelevantRows = rows.filter((row) => (
    row.source !== 'global_behavioral'
      && row.source !== 'graph'
      && isRecallableRow(row, includePending, now)
      && (row.source !== 'semantic' || getSimilarity(row) >= minSimilarity)
  ));

  return queryRelevantRows
    .sort(compareModeRankedRows(mode, now))
    .slice(0, topK);
}

export function composeRelatedRecallRows(
  rows: RecallMemoryRow[],
  directRows: RecallMemoryRow[],
  limit = GRAPH_RECALL_LIMIT
): RecallMemoryRow[] {
  const seenIds = new Set(directRows.map((row) => row.id));
  const relatedRows: RecallMemoryRow[] = [];

  for (const row of rows) {
    if (row.source !== 'graph' || seenIds.has(row.id)) {
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

export function buildRecallBundle(memories: RecallMemory[], globalUserRules: RecallMemory[] = []): RecallBundleResponse {
  const grouped = memories.reduce<Record<RecallBundleKey, RecallMemory[]>>((bundle, memory) => {
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
  bundle.global_user_rules = [...globalUserRules]
    .sort(compareGlobalRows)
    .map((memory) => memory.data);

  for (const key of bundleKeys.filter((key) => key !== 'global_user_rules')) {
    bundle[key] = grouped[key].map((memory) => memory.data);
  }

  return { bundle };
}

export async function registerRecallRoutes(app: FastifyInstance) {
  app.post('/v1/recall', { preHandler: requireVaultReadAuth }, async (request, reply) => {
    const body = recallSchema.parse(request.body);
    const qs = recallQuerySchema.parse(request.query);
    const config = getConfig();
    const topK = body.top_k ?? config.DEFAULT_RECALL_TOP_K;
    const rateLimit = await consumeApiQuota(request.vault.id, 'searches', 'api');
    applyRateLimitHeaders(reply, rateLimit);

    return withSpan('recall.request', {
      'vault.id': request.vault.id,
      'recall.include_raw': body.include_raw,
      'recall.include_evidence': body.include_evidence,
      'recall.include_pending': body.include_pending,
      'recall.include_related': body.include_related,
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

      // Always-on global rules are kept separate from subject-oriented recall so they
      // remain available to agents without consuming the query top_k budget.
      const globalRuleResult = body.mode === 'agent' && qs.format === 'bundle'
        ? await query<RecallMemoryRow>(
        `SELECT id, data, subject, categories, confidence, score, salience, sensitivity, type, scope, polarity,
                status, valid_from, valid_until, source_timestamp, created_at, updated_at, recall_count, last_recalled,
                0.0 AS similarity,
                'global_behavioral' AS source
         FROM memories
         WHERE vault_id = $1
           AND type = 'user_rule'
           AND scope = 'global'
           AND status = 'active'
           AND archived_at IS NULL
         ORDER BY salience DESC, created_at DESC, id
         LIMIT 5`,
        [request.vault.id]
      )
        : { rows: [] as RecallMemoryRow[] };

      const semanticResult = await query<RecallMemoryRow>(
        `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.polarity,
                m.status, m.valid_from, m.valid_until, m.source_timestamp, m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                1 - (me.embedding <=> $2::vector) AS similarity,
                'semantic' AS source
         FROM memories m
         JOIN memory_embeddings me ON me.memory_id = m.id
         WHERE m.vault_id = $1
           AND m.archived_at IS NULL
           AND m.status = 'active'
         ORDER BY me.embedding <=> $2::vector
         LIMIT $3`,
        [request.vault.id, JSON.stringify(embedding), candidateLimit]
      );
      const pendingResult = body.include_pending
        ? await query<RecallMemoryRow>(
          `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.polarity,
                  m.status, m.valid_from, m.valid_until, m.source_timestamp, m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                  1 - (me.embedding <=> $2::vector) AS similarity,
                  'semantic' AS source
           FROM memories m
           JOIN memory_embeddings me ON me.memory_id = m.id
           WHERE m.vault_id = $1
             AND m.archived_at IS NULL
             AND m.status = 'candidate'
             AND COALESCE(m.source_timestamp, m.created_at) >= $4::timestamptz
           ORDER BY me.embedding <=> $2::vector
           LIMIT $3`,
          [request.vault.id, JSON.stringify(embedding), candidateLimit, pendingCutoff.toISOString()]
        )
        : { rows: [] as RecallMemoryRow[] };

      const semanticRows = combineSemanticCandidateRows(semanticResult.rows, pendingResult.rows)
        .filter((row) => getSimilarity(row) >= minSimilarity);
      const directRows = composeRecallRows(semanticRows, topK, body.mode, minSimilarity, recallTime, body.include_pending);
      const semanticIds = directRows.map((row) => row.id);
      const neighborResult = body.include_related && semanticIds.length
        ? await query<RecallMemoryRow>(
          // Directed traversal is intentional here: edges are stored as A -> B, so querying A finds B
          // neighbors, but querying B does not walk back to A in the current retrieval model.
          `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.polarity,
                  m.status, m.valid_from, m.valid_until, m.source_timestamp, m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                  NULL::double precision AS similarity, 'graph' AS source, e.type AS edge_type
           FROM memory_edges e
           JOIN memories m ON m.id = e.to_memory_id
           WHERE e.from_memory_id = ANY($1::uuid[])
             AND m.vault_id = $2
             AND e.vault_id = $2
             AND m.status = 'active'
             AND m.archived_at IS NULL
           ORDER BY m.salience DESC, e.confidence DESC, m.updated_at DESC, m.id
           LIMIT $3`,
          [semanticIds, request.vault.id, GRAPH_RECALL_LIMIT]
        )
        : { rows: [] as RecallMemoryRow[] };

      const relatedRows = composeRelatedRecallRows(neighborResult.rows, directRows);
      const recalledRows = [...directRows, ...relatedRows];

      if (recalledRows.length) {
        void query(
          `UPDATE memories
           SET last_recalled = now(),
               recall_count = recall_count + 1
           WHERE id = ANY($1::uuid[])`,
          [recalledRows.map((row) => row.id)]
        ).catch((error: unknown) => {
          request.log.warn({ err: error, vault_id: request.vault.id }, 'Failed to record recall metadata');
        });
      }

      let evidenceChunks: RecallEvidenceChunk[] = [];
      if (body.include_evidence && qs.format !== 'bundle' && recalledRows.length) {
        const evidenceResult = await query<RecallEvidenceChunk>(
          `WITH evidence_sources AS (
             SELECT m.id AS memory_id, unnest(m.source_chunks) AS chunk_id
             FROM memories m
             WHERE m.id = ANY($1::uuid[])
             UNION
             SELECT m.id AS memory_id, unnest(s.chunk_ids) AS chunk_id
             FROM memories m
             JOIN segments s ON s.id = m.source_segment_id
             WHERE m.id = ANY($1::uuid[])
           ), ranked_chunks AS (
             SELECT es.memory_id, rc.id, rc.session_id, rc.role, rc.blob_store, rc.blob_key, rc.created_at,
                    ROW_NUMBER() OVER (PARTITION BY es.memory_id ORDER BY rc.created_at, rc.id) AS rank
             FROM evidence_sources es
             JOIN raw_chunks rc ON rc.id = es.chunk_id
             WHERE rc.vault_id = $2
               AND rc.blob_key IS NOT NULL
           )
           SELECT memory_id, id, session_id, role, blob_store, blob_key, created_at
           FROM ranked_chunks
           WHERE rank <= 6
           ORDER BY memory_id, created_at, id
           LIMIT $3`,
          [recalledRows.map((row) => row.id), request.vault.id, MAX_EVIDENCE_CHUNKS]
        );
        evidenceChunks = evidenceResult.rows;
      }

      let rawChunks: RecallRawChunk[] = [];

      if (body.include_raw && qs.format !== 'bundle') {
        const rawResult = await query<RecallRawChunk>(
          `SELECT id, session_id, role, blob_store, blob_key, created_at,
                  1 - (embedding <=> $2::vector) AS similarity
           FROM raw_chunks
           WHERE vault_id = $1
             AND embedding IS NOT NULL
             AND blob_key IS NOT NULL
           ORDER BY embedding <=> $2::vector
           LIMIT $3`,
          [request.vault.id, JSON.stringify(embedding), candidateLimit]
        );
        rawChunks = rawResult.rows
          .filter((row) => row.similarity >= minSimilarity)
          .slice(0, topK);
      }

      const decryptedMemories = await Promise.all(directRows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedRelatedMemories = await Promise.all(relatedRows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedGlobalUserRules = await Promise.all(globalRuleResult.rows.map(async (row) => ({
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

      if (qs.format === 'bundle') {
        const response: RecallBundleResponse = buildRecallBundle(decryptedMemories, decryptedGlobalUserRules);
        if (body.include_related) {
          response.related_bundle = buildRecallBundle(decryptedRelatedMemories).bundle;
        }
        return response;
      }

      const response: RecallResponse = {
        memories: decryptedMemories,
        related_memories: decryptedRelatedMemories,
        evidence_chunks: decryptedEvidenceChunks,
        raw_chunks: decryptedRawChunks
      };

      return response;
    });
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
