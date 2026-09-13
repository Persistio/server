import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { getConfig } from '../config';
import { query } from '../db/client';
import { requireVaultReadAuth } from '../middleware/auth';
import { prepareVaultCrypto } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { getRawChunkStorage } from '../services/raw-chunk-storage';
import { applyRateLimitHeaders, consumeApiQuota } from '../services/usage';
import { getCuratorLimits } from '../services/curation-capacity';
import { prepareRecallBundle, recallBundleBudgetSchema, type BundleMemory } from '../services/recall-bundle';
import { isMemoryApplicable, memoryApplicabilityPredicateSql,
  memoryEligibilityPredicateSql, recallContextSchema, type RecallContext } from '../services/memory-applicability';
import { provenanceIdentitySchema } from '../services/provenance-identity';
import { recallDurationHistogram } from '../metrics';

export const recallSchema = z.object({
  query: z.string().trim().min(1).max(32768),
  top_k: z.number().int().positive().max(100).optional(),
  min_similarity: z.number().min(0).max(1).optional(),
  include_raw: z.boolean().default(false),
  include_evidence: z.boolean().default(false),
  include_related: z.boolean().default(true),
  max_bundle_bytes: recallBundleBudgetSchema,
  client: z.object({ name: provenanceIdentitySchema(100),version: provenanceIdentitySchema(100) }).strict().optional(),
  context: recallContextSchema.default({}),
  mode: z.enum(['agent','factual']).default('agent')
}).strict();
const recallQuerySchema = z.object({ format: z.literal('bundle_v3').optional() }).strict();

interface RecallMemory extends BundleMemory {
  id: string; revision: string; status: string; archived_at: string | null;
  scope: 'global' | 'project' | 'task' | 'session'; scope_key: string | null;
  sensitivity: string; confidence: number; salience: number; similarity: number | null;
  source_chunks: string[]; subject_encrypted: string | null; source_segment_id: string | null;
  categories: string[]; polarity: string; score: number; created_at: string; updated_at: string;
}
const columns = `m.id,m.data,m.subject,m.subject_encrypted,m.categories,m.type,m.scope,m.scope_key,
  m.status,m.archived_at,m.confidence,m.salience,m.sensitivity,m.polarity,m.score,
  m.valid_from::text,m.valid_until::text,m.source_timestamp::text,m.source_segment_id,m.source_chunks,
  m.revision::text,m.created_at::text,m.updated_at::text`;
const eligible = (alias: string) => `${alias}.status='active' AND ${alias}.archived_at IS NULL
  AND ${memoryEligibilityPredicateSql(alias,'$5')}`;
const applicable = (alias: string) => memoryApplicabilityPredicateSql(alias,'$2','$3','$4');
const parameters = (vaultId: string,context: RecallContext,now: Date) =>
  [vaultId,context.session_id ?? null,context.project_id ?? null,context.task_id ?? null,now.toISOString()];
export const recallCandidateLimit = (topK: number) => Math.min(400,Math.max(25,topK*4));

// These counters are best-effort metadata. Never wait for a substantive memory
// writer or a cascading delete while holding another returned memory's lock.
export const RECALL_COUNTER_UPDATE_SQL = `WITH targets AS MATERIALIZED (
  SELECT id FROM memories
  WHERE vault_id=$1 AND id=ANY($2::uuid[])
  ORDER BY id FOR NO KEY UPDATE SKIP LOCKED
)
UPDATE memories m SET last_recalled=now(),recall_count=m.recall_count+1
FROM targets t WHERE m.vault_id=$1 AND m.id=t.id`;

function currentEligible(row: RecallMemory,context: RecallContext,now: Date): boolean {
  return row.status === 'active' && row.archived_at === null && isMemoryApplicable(row,context,now);
}
function publicMemory(row: RecallMemory) {
  const { subject_encrypted: _encrypted,archived_at: _archived,...memory } = row;
  return memory;
}

export async function registerRecallRoutes(app: FastifyInstance) {
  app.post('/v1/recall',{ preHandler: requireVaultReadAuth },async (request,reply) => {
    const parsed = recallSchema.safeParse(request.body);
    const format = recallQuerySchema.safeParse(request.query);
    if (!parsed.success || !format.success) return reply.code(400).send({ error: 'Invalid recall request' });
    const body = parsed.data;
    const bundle = format.data.format === 'bundle_v3';
    if ((body.include_raw || body.include_evidence) && (!body.context.session_id || bundle)) {
      return reply.code(400).send({ error: 'Source retrieval requires session context and JSON format' });
    }
    const started = performance.now();
    const quota = await consumeApiQuota(request.vault.id,'searches','api');
    applyRateLimitHeaders(reply,quota);
    const config = getConfig();
    const topK = Math.min(100,body.top_k ?? config.DEFAULT_RECALL_TOP_K);
    const minSimilarity = body.min_similarity ?? config.MIN_RECALL_SIMILARITY;
    const now = new Date();
    const args = parameters(request.vault.id,body.context,now);
    const preparedCrypto = await prepareVaultCrypto(request.vault);
    const embedding = await getEmbedder().embed(body.query,{
      vaultId: request.vault.id,modelRole: 'embedding',source: 'api',inputType: 'query'
    });
    const semantic = (await query<RecallMemory>(
      `SELECT ${columns},1-(me.embedding <=> $6::vector) AS similarity,'semantic' AS source
       FROM memories m JOIN memory_embeddings me ON me.memory_id=m.id
       WHERE m.vault_id=$1 AND ${eligible('m')} AND ${applicable('m')}
       AND 1-(me.embedding <=> $6::vector)>=$7
       ORDER BY me.embedding <=> $6::vector,m.id LIMIT $8`,
      [...args,JSON.stringify(embedding),minSimilarity,recallCandidateLimit(topK)])).rows;
    semantic.sort(compareSemantic(body.mode,now));
    const direct = semantic.slice(0,topK);
    const remaining = topK-direct.length;
    const graphEnabled = body.include_related && remaining>0 && direct.length>0
      && (await getCuratorLimits(request.vault.id)).curator_enabled;
    const graph = graphEnabled ? (await query<RecallMemory>(
      `SELECT DISTINCT ${columns},NULL::float AS similarity,'graph' AS source
       FROM memory_edges e JOIN memories m ON m.id=e.to_memory_id AND m.vault_id=e.vault_id
       JOIN memories origin ON origin.id=e.from_memory_id AND origin.vault_id=e.vault_id
       WHERE e.vault_id=$1 AND ${eligible('m')} AND ${applicable('m')}
         AND ${eligible('origin')} AND ${applicable('origin')}
         AND origin.scope=m.scope AND origin.scope_key IS NOT DISTINCT FROM m.scope_key
         AND e.from_memory_id=ANY($6::uuid[]) AND NOT(m.id=ANY($6::uuid[]))
       ORDER BY m.salience DESC,m.id LIMIT $7`,[...args,direct.map(row => row.id),Math.min(20,remaining)])).rows : [];
    const selected = [...direct,...graph];

    // Final row-version/binding/state check, after retrieval. This is the response
    // snapshot boundary, not a promise about what a model later does with context.
    const fresh = selected.length ? (await query<RecallMemory>(
      `WITH selected AS (SELECT * FROM jsonb_to_recordset($6::jsonb) AS x(id uuid,revision bigint,source text,similarity float))
       SELECT ${columns},selected.source,selected.similarity,
         EXISTS(SELECT 1 FROM contradiction_scan_log l JOIN memories other ON other.vault_id=l.vault_id AND other.id<>m.id
           AND ((l.memory_id_a=other.id AND l.revision_a=other.revision) OR (l.memory_id_b=other.id AND l.revision_b=other.revision))
           WHERE l.vault_id=m.vault_id AND l.decision='keep_both'
             AND ((l.memory_id_a=m.id AND l.revision_a=m.revision) OR (l.memory_id_b=m.id AND l.revision_b=m.revision))
             AND other.status='active' AND other.archived_at IS NULL) AS unresolved_conflict
       FROM selected
       JOIN memories m ON m.id=selected.id AND m.revision=selected.revision
       JOIN vaults v ON v.id=m.vault_id JOIN plans p ON p.id=v.plan_id
       WHERE m.vault_id=$1 AND ${eligible('m')} AND ${applicable('m')}
         AND v.encrypted_dek IS NOT DISTINCT FROM $7::text
         AND v.vault_encryption_enabled=$8
         AND (selected.source<>'graph' OR (
           COALESCE((v.rate_limit_override->>'curator_enabled')::boolean,(p.limits->>'curator_enabled')::boolean,false)
           AND EXISTS(SELECT 1 FROM memory_edges e JOIN selected seed ON seed.id=e.from_memory_id AND seed.source<>'graph'
             JOIN memories origin ON origin.id=seed.id AND origin.vault_id=m.vault_id AND origin.revision=seed.revision
             WHERE e.vault_id=m.vault_id AND e.to_memory_id=m.id AND ${eligible('origin')} AND ${applicable('origin')}
               AND origin.scope=m.scope AND origin.scope_key IS NOT DISTINCT FROM m.scope_key)
         ))`,
      [...args,JSON.stringify(selected.map(row => ({ id: row.id,revision: row.revision,source: row.source,similarity: row.similarity }))),
        request.vault.encrypted_dek,request.vault.vault_encryption_enabled])).rows : [];
    const byId = new Map(fresh.map(row => [row.id,row]));
    const ordered = selected.flatMap(row => {
      const current = byId.get(row.id);
      return current && currentEligible(current,body.context,new Date()) ? [current] : [];
    });
    const decoded = ordered.map(row => ({
      ...row,data: preparedCrypto.decrypt(request.vault,row.data),
      subject: row.subject_encrypted ? preparedCrypto.decrypt(request.vault,row.subject_encrypted) : row.subject,
      unresolved_conflict: row.unresolved_conflict===true
    }));
    let evidence: unknown[] = [];
    let raw: unknown[] = [];
    if (body.include_evidence || body.include_raw) {
      // One finite source-byte budget for the entire explicit source response.
      const storage = getRawChunkStorage();
      let remainingBytes = 65536;
      const sourceRows = await query<{ id: string; role: string; blob_store: string; blob_key: string; storage_bytes: string; created_at: string }>(
        `SELECT id,role,blob_store,blob_key,storage_bytes::text,created_at::text FROM raw_chunks
         WHERE vault_id=$1 AND session_id=$2 AND blob_key IS NOT NULL AND blob_store=$10 AND storage_bytes BETWEEN 0 AND 65536
           AND capture_context->>'project_id' IS NOT DISTINCT FROM $8::text
           AND capture_context->>'task_id' IS NOT DISTINCT FROM $9::text
           AND (($3::boolean AND embedding IS NOT NULL AND 1-(embedding <=> $4::vector)>=$5)
             OR id=ANY($6::uuid[]))
         ORDER BY (id=ANY($6::uuid[])) DESC,
           CASE WHEN id=ANY($6::uuid[]) THEN NULL ELSE embedding <=> $4::vector END ASC NULLS LAST,id LIMIT $7`,
        [request.vault.id,body.context.session_id,body.include_raw,JSON.stringify(embedding),minSimilarity,
          body.include_evidence ? [...new Set(decoded.flatMap(row => row.source_chunks))] : [],topK,body.context.project_id ?? null,body.context.task_id ?? null,storage.store]);
      const supportingIds = new Set(decoded.flatMap(row => row.source_chunks));
      for (const row of sourceRows.rows) {
        if (row.blob_store!==storage.store || Number(row.storage_bytes)>remainingBytes) continue;
        const content = preparedCrypto.decrypt(request.vault,await storage.get(row.blob_key));
        const bytes = Buffer.byteLength(content);
        if (bytes>remainingBytes) continue;
        remainingBytes-=bytes;
        const item = { id: row.id,role: row.role,created_at: row.created_at,content };
        if (body.include_evidence && supportingIds.has(row.id)) evidence.push(item);
        else if (body.include_raw) raw.push(item);
      }
    }
    const assembled=bundle ? prepareRecallBundle(decoded,body.max_bundle_bytes,new Date()):null;
    const response = assembled ? assembled.response : {
      memories: decoded.filter(row => row.source!=='graph').map(publicMemory),
      related_memories: decoded.filter(row => row.source==='graph').map(publicMemory),
      evidence_chunks: evidence,raw_chunks: raw
    };
    // Internal memory usage counters are neither delivery events nor model-use
    // claims. A counter failure cannot suppress the response or retry the request.
    const returned=assembled?.included ?? decoded;
    recallDurationHistogram.record(performance.now()-started,{mode:body.mode});
    if (returned.length) void query(RECALL_COUNTER_UPDATE_SQL,
      [request.vault.id,returned.map(row => row.id)]).catch(() => {});
    try { request.log.info({ vault_id: request.vault.id,route: '/v1/recall',status: 200,
      duration_ms: performance.now()-started,result_count: returned.length,
      response_bytes: Buffer.byteLength(JSON.stringify(response)) },'Recall served'); } catch { /* A logger failure cannot suppress a valid recall. */ }
    return response;
  });
}

const agentTypeBoosts:Record<string,number>={user_rule:.08,user_preference:.07,task_pattern:.06,workflow:.04,constraint:.03,decision:.02,project:.01,system_fact:0,domain_knowledge:0};
const factualTypeBoosts:Record<string,number>={system_fact:.08,domain_knowledge:.08,project:.06,decision:.06,constraint:.05,workflow:.02,user_preference:.01,task_pattern:.01,user_rule:0};
function compareSemantic(mode:'agent'|'factual',now:Date){
  const timestamp=(m:RecallMemory)=>{const time=Date.parse(m.source_timestamp ?? m.updated_at ?? m.created_at);return Number.isFinite(time)?time:0;};
  const score=(m:RecallMemory)=>(m.similarity ?? 0)+((mode==='agent'?agentTypeBoosts:factualTypeBoosts)[m.type ?? ''] ?? 0)
    +(timestamp(m)? .04*Math.max(0,1-Math.max(0,now.getTime()-timestamp(m))/(14*24*60*60*1000)):0);
  return(a:RecallMemory,b:RecallMemory)=>score(b)-score(a) || (b.similarity ?? 0)-(a.similarity ?? 0)
    || Number(b.salience)-Number(a.salience) || timestamp(b)-timestamp(a) || a.id.localeCompare(b.id);
}
