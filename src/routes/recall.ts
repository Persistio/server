import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig } from '../config';
import { query } from '../db/client';
import { recallDurationHistogram } from '../metrics';
import { requireVaultAuth } from '../middleware/auth';
import { decryptForVault } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { applyRateLimitHeaders, consumeApiQuota } from '../services/usage';
import { withSpan } from '../telemetry';

const recallSchema = z.object({
  query: z.string().min(1),
  top_k: z.number().int().positive().max(100).optional(),
  include_raw: z.boolean().optional().default(false),
  include_evidence: z.boolean().optional().default(false),
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
  similarity: number;
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

interface RecallRawChunk {
  id: string;
  session_id: string;
  role: string;
  content: string;
  similarity: number;
  created_at: string;
}

interface RecallEvidenceChunk {
  memory_id: string;
  id: string;
  session_id: string;
  role: string;
  content: string;
  created_at: string;
}

interface RecallResponse {
  memories: RecallMemory[];
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

function compareQueryRelevantRows(left: RecallMemoryRow, right: RecallMemoryRow): number {
  return right.similarity - left.similarity || Number(right.salience) - Number(left.salience);
}

function compareGlobalRows(left: RecallMemoryRow, right: RecallMemoryRow): number {
  return Number(right.salience) - Number(left.salience)
    || new Date(right.created_at).getTime() - new Date(left.created_at).getTime()
    || left.id.localeCompare(right.id);
}

export function composeRecallRows(
  rows: RecallMemoryRow[],
  topK: number,
  mode: RecallMode
): RecallMemoryRow[] {
  const queryRelevantRows = rows.filter((row) => row.source !== 'global_behavioral');
  return queryRelevantRows.slice(0, topK);
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
    bundle[key] = grouped[key]
      .sort(compareQueryRelevantRows)
      .map((memory) => memory.data);
  }

  return { bundle };
}

export async function registerRecallRoutes(app: FastifyInstance) {
  app.post('/v1/recall', { preHandler: requireVaultAuth }, async (request, reply) => {
    const body = recallSchema.parse(request.body);
    const qs = recallQuerySchema.parse(request.query);
    const config = getConfig();
    const topK = body.top_k ?? config.DEFAULT_RECALL_TOP_K;
    const rateLimit = await consumeApiQuota(request.vault.id, 'searches');
    applyRateLimitHeaders(reply, rateLimit);

    return withSpan('recall.request', {
      'vault.id': request.vault.id,
      'recall.include_raw': body.include_raw,
      'recall.include_evidence': body.include_evidence,
      'recall.top_k': topK,
      'recall.mode': body.mode
    }, async (span) => {
      const start = performance.now();
      const embedder = getEmbedder();
      const embedding = await embedder.embed(body.query);

      // Always-on global rules are kept separate from subject-oriented recall so they
      // remain available to agents without consuming the query top_k budget.
      const globalRuleResult = body.mode === 'agent' && qs.format === 'bundle'
        ? await query<RecallMemoryRow>(
        `SELECT id, data, subject, categories, confidence, score, salience, sensitivity, type, scope, polarity,
                status, valid_from, valid_until, created_at, updated_at, recall_count, last_recalled,
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
                m.status, m.valid_from, m.valid_until, m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                1 - (me.embedding <=> $2::vector) AS similarity,
                'semantic' AS source
         FROM memories m
         JOIN memory_embeddings me ON me.memory_id = m.id
         WHERE m.vault_id = $1
           AND m.archived_at IS NULL
           AND m.status = 'active'
         ORDER BY me.embedding <=> $2::vector
         LIMIT $3`,
        [request.vault.id, JSON.stringify(embedding), topK]
      );

      const semanticIds = semanticResult.rows.map((row) => row.id);
      const neighborResult = semanticIds.length
        ? await query<RecallMemoryRow>(
          // Directed traversal is intentional here: edges are stored as A -> B, so querying A finds B
          // neighbors, but querying B does not walk back to A in the current retrieval model.
          `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.polarity,
                  m.status, m.valid_from, m.valid_until, m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                  0.5 AS similarity, 'graph' AS source, e.type AS edge_type
           FROM memory_edges e
           JOIN memories m ON m.id = e.to_memory_id
           WHERE e.from_memory_id = ANY($1::uuid[])
             AND m.vault_id = $2
             AND e.vault_id = $2
             AND m.status = 'active'
             AND m.archived_at IS NULL
           ORDER BY m.salience DESC, e.confidence DESC, m.updated_at DESC, m.id
           LIMIT 20`,
          [semanticIds, request.vault.id]
        )
        : { rows: [] as RecallMemoryRow[] };

      const uniqueRows = [...semanticResult.rows, ...neighborResult.rows]
        .filter((row, index, rows) => rows.findIndex((candidate) => candidate.id === row.id) === index);
      const combinedRows = composeRecallRows(uniqueRows, topK, body.mode);

      if (combinedRows.length) {
        await query(
          `UPDATE memories
           SET last_recalled = now(),
               recall_count = recall_count + 1
           WHERE id = ANY($1::uuid[])`,
          [combinedRows.map((row) => row.id)]
        );
      }

      let evidenceChunks: RecallEvidenceChunk[] = [];
      if (body.include_evidence && qs.format !== 'bundle' && combinedRows.length) {
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
             SELECT es.memory_id, rc.id, rc.session_id, rc.role, rc.content, rc.created_at,
                    ROW_NUMBER() OVER (PARTITION BY es.memory_id ORDER BY rc.created_at, rc.id) AS rank
             FROM evidence_sources es
             JOIN raw_chunks rc ON rc.id = es.chunk_id
             WHERE rc.vault_id = $2
           )
           SELECT memory_id, id, session_id, role, content, created_at
           FROM ranked_chunks
           WHERE rank <= 6
           ORDER BY memory_id, created_at, id
           LIMIT $3`,
          [combinedRows.map((row) => row.id), request.vault.id, MAX_EVIDENCE_CHUNKS]
        );
        evidenceChunks = evidenceResult.rows;
      }

      let rawChunks: RecallRawChunk[] = [];

      if (body.include_raw && qs.format !== 'bundle') {
        const rawResult = await query<RecallRawChunk>(
          `SELECT id, session_id, role, content, created_at,
                  1 - (embedding <=> $2::vector) AS similarity
           FROM raw_chunks
           WHERE vault_id = $1
             AND embedding IS NOT NULL
           ORDER BY embedding <=> $2::vector
           LIMIT $3`,
          [request.vault.id, JSON.stringify(embedding), topK]
        );
        rawChunks = rawResult.rows;
      }

      const decryptedMemories = await Promise.all(combinedRows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedGlobalUserRules = await Promise.all(globalRuleResult.rows.map(async (row) => ({
        ...row,
        data: await decryptForVault(request.vault, row.data)
      })));
      const decryptedEvidenceChunks = await Promise.all(evidenceChunks.map(async (row) => ({
        ...row,
        content: typeof row.content === 'string' ? await decryptForVault(request.vault, row.content) : row.content
      })));
      const decryptedRawChunks = await Promise.all(rawChunks.map(async (row) => ({
        ...row,
        content: typeof row.content === 'string' ? await decryptForVault(request.vault, row.content) : row.content
      })));

      const durationMs = performance.now() - start;
      recallDurationHistogram.record(durationMs, {
        vault_id: request.vault.id,
        include_raw: String(body.include_raw)
      });
      span.setAttribute('recall.results_returned', combinedRows.length);
      span.setAttribute('recall.evidence_results_returned', evidenceChunks.length);
      span.setAttribute('recall.raw_results_returned', rawChunks.length);
      span.setAttribute('recall.duration_ms', durationMs);

      if (qs.format === 'bundle') {
        return buildRecallBundle(decryptedMemories, decryptedGlobalUserRules);
      }

      const response: RecallResponse = {
        memories: decryptedMemories,
        evidence_chunks: decryptedEvidenceChunks,
        raw_chunks: decryptedRawChunks
      };

      return response;
    });
  });
}
