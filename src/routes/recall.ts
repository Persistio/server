import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig } from '../config';
import { query } from '../db/client';
import { recallDurationHistogram } from '../metrics';
import { requireVaultAuth } from '../middleware/auth';
import { decryptForVault } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { checkQuota, incrementUsage } from '../services/usage';
import { withSpan } from '../telemetry';

const recallSchema = z.object({
  query: z.string().min(1),
  top_k: z.number().int().positive().max(100).optional(),
  include_raw: z.boolean().optional().default(false)
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
  source?: 'behavioral' | 'semantic' | 'graph';
  edge_type?: string | null;
  created_at: string;
  updated_at: string;
  recall_count: number;
  last_recalled: string | null;
}

type RecallMemory = RecallMemoryRow;

interface RecallRawChunk {
  id: string;
  session_id: string;
  role: string;
  content: string;
  similarity: number;
  created_at: string;
}

interface RecallResponse {
  memories: RecallMemory[];
  raw_chunks: RecallRawChunk[];
}

interface RecallBundle {
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

function compareRecallRows(left: RecallMemoryRow, right: RecallMemoryRow): number {
  return Number(right.salience) - Number(left.salience) || right.similarity - left.similarity;
}

function buildRecallBundle(memories: RecallMemory[]): RecallBundleResponse {
  const grouped = memories.reduce<Record<RecallBundleKey, RecallMemory[]>>((bundle, memory) => {
    const key = getBundleKey(memory.type);
    bundle[key].push(memory);
    return bundle;
  }, {
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

  for (const key of bundleKeys) {
    bundle[key] = grouped[key]
      .sort(compareRecallRows)
      .map((memory) => memory.data);
  }

  return { bundle };
}

export async function registerRecallRoutes(app: FastifyInstance) {
  app.post('/v1/recall', { preHandler: requireVaultAuth }, async (request) => {
    const body = recallSchema.parse(request.body);
    const qs = recallQuerySchema.parse(request.query);
    const config = getConfig();
    const topK = body.top_k ?? config.DEFAULT_RECALL_TOP_K;
    await checkQuota(request.vault.id, 'searches');

    return withSpan('recall.request', {
      'vault.id': request.vault.id,
      'recall.include_raw': body.include_raw,
      'recall.top_k': topK
    }, async (span) => {
      const start = performance.now();
      const embedder = getEmbedder();
      const embedding = await embedder.embed(body.query);

      // Behavioral memories are always injected as top-5 context regardless of query so agent behavior
      // rules and preferences are never missed by semantic matching alone.
      const behavioralResult = await query<RecallMemoryRow>(
        `SELECT id, data, subject, categories, confidence, score, salience, sensitivity, type, scope, polarity,
                status, valid_from, valid_until, created_at, updated_at, recall_count, last_recalled,
                1.0 AS similarity,
                'behavioral' AS source
         FROM memories
         WHERE vault_id = $1
           AND type IN ('user_rule', 'user_preference', 'task_pattern')
           AND status = 'active'
           AND archived_at IS NULL
         ORDER BY salience DESC
         LIMIT 5`,
        [request.vault.id]
      );

      const semanticResult = await query<RecallMemoryRow>(
        `SELECT m.id, m.data, m.subject, m.categories, m.confidence, m.score, m.salience, m.sensitivity, m.type, m.scope, m.polarity,
                m.status, m.valid_from, m.valid_until, m.created_at, m.updated_at, m.recall_count, m.last_recalled,
                1 - (me.embedding <=> $2::vector) AS similarity,
                'semantic' AS source
         FROM memories m
         JOIN memory_embeddings me ON me.memory_id = m.id
         WHERE m.vault_id = $1
           AND m.archived_at IS NULL
           AND m.status <> 'candidate'
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
           LIMIT 20`,
          [semanticIds, request.vault.id]
        )
        : { rows: [] as RecallMemoryRow[] };

      const uniqueRows = [...behavioralResult.rows, ...semanticResult.rows, ...neighborResult.rows]
        .filter((row, index, rows) => rows.findIndex((candidate) => candidate.id === row.id) === index);
      const behavioralRows = uniqueRows.filter((row) => row.source === 'behavioral');
      const nonBehavioralRows = uniqueRows
        .filter((row) => row.source !== 'behavioral')
        .slice(0, Math.max(topK - behavioralRows.length, 0));
      const combinedRows = [...behavioralRows, ...nonBehavioralRows];

      if (combinedRows.length) {
        await query(
          `UPDATE memories
           SET last_recalled = now(),
               recall_count = recall_count + 1
           WHERE id = ANY($1::uuid[])`,
          [combinedRows.map((row) => row.id)]
        );
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
      const decryptedRawChunks = await Promise.all(rawChunks.map(async (row) => ({
        ...row,
        content: typeof row.content === 'string' ? await decryptForVault(request.vault, row.content) : row.content
      })));

      await incrementUsage(request.vault.id, 'searches');

      const durationMs = performance.now() - start;
      recallDurationHistogram.record(durationMs, {
        vault_id: request.vault.id,
        include_raw: String(body.include_raw)
      });
      span.setAttribute('recall.results_returned', combinedRows.length);
      span.setAttribute('recall.raw_results_returned', rawChunks.length);
      span.setAttribute('recall.duration_ms', durationMs);

      if (qs.format === 'bundle') {
        return buildRecallBundle(decryptedMemories);
      }

      const response: RecallResponse = {
        memories: decryptedMemories,
        raw_chunks: decryptedRawChunks
      };

      return response;
    });
  });
}
