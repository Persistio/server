import crypto from 'node:crypto';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { z } from 'zod';
import type { QueryResultRow } from 'pg';

import { query,withTransaction } from '../db/client';
import {
  memoryArchivedEventType,
  memoryCreatedEventType,
  type MemoryArchivedPayload,
  type MemoryCreatedPayload,
  type PlatformActor
} from '../events/platform-event';
import { getAuthAccountId, requireAdminScope, requireVaultReadAuth, requireVaultWriteAuth, type PlatformAuthContext, type VaultContext } from '../middleware/auth';
import { setCustomerMetricVaultId } from '../services/customer-api-request-metrics';
import { computeSubjectHmac, decryptForVault, encryptForVault, encryptSubjectForVault, isVaultEncryptionActive, unwrapDek } from '../services/crypto';
import { getEmbedder } from '../services/embedder';
import { prepareVaultCrypto } from '../services/crypto';
import { lockMemoryWriteVault } from '../services/dedup';
import { enqueueCurationWork } from '../services/curation-work';
import { isSecretLikeMemoryContent } from '../services/deterministic-filter';
import { isValidDateOnly } from '../services/memory-validity';
import { publishCommittedWorkerEffects,type WorkerEffect } from '../services/worker-effects';
import { checkMemoryCreationCapacity, reserveMemoryCreationInTransaction, recordMemoryCountDelta } from '../services/usage';
import { isScopeWidening, parseMemoryScope, resolveMemoryScopeChange, type MemoryScope } from '../services/memory-scope';
import { contextIdentitySchema } from '../services/memory-applicability';

const booleanQueryParam = z.preprocess((value) => {
  if (value === undefined) return false;
  if (Array.isArray(value)) return value[0];
  if (value === 'true' || value === true) return true;
  if (value === 'false' || value === false) return false;
  return value;
}, z.boolean());

export function platformActorForAudit(auth: PlatformAuthContext): PlatformActor {
  if (auth.method === 'oauth' && auth.actor) {
    return auth.actor;
  }
  return {
    id: auth.client_id ?? auth.subject ?? null,
    type: auth.method === 'api_key' ? 'api_key' : 'service'
  };
}

const listQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  category: z.string().optional(),
  filter: z.enum(['all', 'high-confidence', 'low-confidence', 'positive', 'negative']).optional().default('all'),
  include_children: booleanQueryParam,
  limit: z.coerce.number().int().min(1).max(200).default(50),
  offset: z.coerce.number().int().min(0).default(0),
  q: z.string().trim().min(1).max(200).optional(),
  sort: z.enum(['recent', 'oldest', 'confidence', 'salience']).optional().default('recent'),
  subject: z.string().trim().min(1).max(500).optional()
}).strict();
const adminListQuerySchema = listQuerySchema.strict();

const subjectListQuerySchema = z.object({
  archived: z.enum(['true', 'false']).optional().default('false'),
  limit: z.coerce.number().int().min(1).max(500).default(200),
  offset: z.coerce.number().int().min(0).default(0),
  q: z.string().trim().min(1).max(200).optional(),
  sort: z.enum(['count', 'recent', 'name']).optional().default('count')
});

const readMemoryQuerySchema = z.object({}).strict();

export const createMemoryShape = {
  data: z.string().trim().min(1).max(10000),
  subject: z.string().trim().min(1).max(500),
  categories: z.array(z.string().min(1)).optional().default([]),
  parent_id: z.string().uuid().nullable().optional(),
  type: z.enum(['user_preference', 'user_rule', 'task_pattern', 'workflow', 'project', 'constraint', 'decision', 'system_fact', 'domain_knowledge']).optional().default('system_fact'),
  scope: z.enum(['global', 'project', 'task', 'session']),
  scope_key: contextIdentitySchema.nullable().optional(),
  evidence: z.string().optional(),
  volatility: z.enum(['very_low', 'low', 'medium', 'high']).optional().default('low')
};
const createMemorySchema = z.object(createMemoryShape).strict().superRefine((body, context) => {
  if (body.scope === 'global' && body.scope_key != null) {
    context.addIssue({ code: z.ZodIssueCode.custom, path: ['scope_key'], message: 'Global memories must not have a scope_key' });
  } else if (body.scope !== 'global' && !body.scope_key) {
    context.addIssue({ code: z.ZodIssueCode.custom, path: ['scope_key'], message: 'Non-global memories require a scope_key' });
  }
});

export const updateMemoryShape = {
  data: z.string().trim().min(1).max(10000).optional(),
  subject: z.string().trim().min(1).max(500).optional(),
  categories: z.array(z.string().min(1)).optional(),
  confidence: z.number().positive().max(1).optional(),
  type: z.enum(['user_preference', 'user_rule', 'task_pattern', 'workflow', 'project', 'constraint', 'decision', 'system_fact', 'domain_knowledge']).optional(),
  scope: z.enum(['global', 'project', 'task', 'session']).optional(),
  scope_key: contextIdentitySchema.nullable().optional(),
  scope_change_reason: z.string().trim().min(1).max(500).optional(),
  evidence: z.string().nullable().optional(),
  archived: z.boolean().optional()
};
const updateMemorySchema = z.object(updateMemoryShape).strict().refine((body) => (
  Object.keys(body).some((field) => field !== 'scope_change_reason')
), {
  message: 'At least one memory field is required'
}).refine((body) => body.scope !== 'global' || body.scope_key == null, {
  path: ['scope_key'], message: 'Global memories must not have a scope_key'
});

async function assertCompatibleScopeRelationships(
  execute: (sql: string, values: unknown[]) => Promise<{rowCount: number | null}>,
  vaultId: string, id: string, parentId: string | null, scope: MemoryScope, scopeKey: string | null
): Promise<void> {
  // Preflight avoids known-invalid paid work; the locked call is authoritative.
  const incompatible=await execute(`SELECT 1 FROM memories m WHERE m.vault_id=$1 AND (m.id=$3 OR m.parent_id=$2)
    AND (m.scope<>$4 OR m.scope_key IS DISTINCT FROM $5::text)
    UNION ALL SELECT 1 FROM memory_edges e JOIN memories m ON m.id=CASE WHEN e.from_memory_id=$2 THEN e.to_memory_id ELSE e.from_memory_id END
    WHERE e.vault_id=$1 AND (e.from_memory_id=$2 OR e.to_memory_id=$2)
    AND (m.scope<>$4 OR m.scope_key IS DISTINCT FROM $5::text) LIMIT 1`,[vaultId,id,parentId,scope,scopeKey]);
  if(incompatible.rowCount)throw Object.assign(new Error('Scope change conflicts with linked memory bindings'),{statusCode:409});
}

const graphEdgeTypes = [
  'applies_to',
  'part_of',
  'depends_on',
  'supports',
  'contradicts',
  'supersedes',
  'refines',
  'relevant_when'
] as const;

const graphQuerySchema = z.object({
  seed_memory_id: z.string().uuid().optional(),
  depth: z.coerce.number().int().min(0).max(4).optional().default(1),
  limit: z.coerce.number().int().min(1).max(100).optional().default(50),
  direction: z.enum(['out', 'in', 'both']).optional().default('both'),
  edge_types: z.preprocess((value) => {
    if (value === undefined) return undefined;
    const values = Array.isArray(value) ? value : [value];
    return values
      .flatMap((item) => String(item).split(','))
      .map((item) => item.trim())
      .filter(Boolean);
  }, z.array(z.enum(graphEdgeTypes)).min(1).max(graphEdgeTypes.length).optional())
});

function memoryResponseSelect(source: string, edgeSource = source): string {
  return `${source}.id, ${source}.vault_id, ${source}.data, ${source}.subject, ${source}.subject_encrypted, ${source}.hash, ${source}.source_chunks,
       ${source}.categories, ${source}.confidence, ${source}.score, ${source}.salience, ${source}.sensitivity, ${source}.type, ${source}.scope, ${source}.scope_key, ${source}.evidence, ${source}.polarity, ${source}.status,
       ${source}.valid_from, ${source}.valid_until, ${source}.source_timestamp, ${source}.archived_at, ${source}.created_at, ${source}.updated_at, ${source}.parent_id, ${source}.volatility,
       COALESCE((SELECT COUNT(*)::int FROM memory_edges edge_counts WHERE edge_counts.from_memory_id = ${edgeSource}.id OR edge_counts.to_memory_id = ${edgeSource}.id), 0) AS edge_count`;
}

type AdminVaultContext = {
  id: string;
  name: string;
  purpose: string | null;
  settings: Record<string, unknown>;
  plan_id: string;
  status: string;
  account_id: string | null;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
};

export async function registerMemoryRoutes(app: FastifyInstance) {
  const vaultReadAuth = requireAdminScope('platform:vaults:read');
  const vaultWriteAuth = requireAdminScope('platform:vaults:update');

  app.get('/admin/vaults/:id/memories', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const qs = adminListQuerySchema.parse(request.query);
    const vault = await getAdminVaultContext(request, id);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });

    setCustomerMetricVaultId(request, vault.id);
    const result = await listMemories(vault, qs);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.get('/admin/vaults/:id/memories/subjects', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const qs = subjectListQuerySchema.parse(request.query);
    const vault = await getAdminVaultContext(request, id);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });

    setCustomerMetricVaultId(request, vault.id);
    const result = await listMemorySubjects(vault, qs);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.get('/admin/vaults/:id/memories/graph', { preHandler: vaultReadAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const vault = await getAdminVaultContext(request, id);
    if (!vault) return reply.code(404).send({ error: 'Vault not found' });

    setCustomerMetricVaultId(request, vault.id);
    if (!(await vaultCanViewMemoryGraph(vault))) {
      return reply.code(403).send({ error: 'Memory graph requires a graph-capable plan' });
    }

    const parsedQuery = graphQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) {
      return reply.code(400).send({ error: 'Invalid graph query' });
    }

    return memoryGraphResponse(vault, parsedQuery.data, reply);
  });

  app.get('/v1/memories', { preHandler: requireVaultReadAuth }, async (request) => {
    const qs = listQuerySchema.parse(request.query);
    const result = await listMemories(request.vault, qs);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.get('/v1/memories/subjects', { preHandler: requireVaultReadAuth }, async (request) => {
    const qs = subjectListQuerySchema.parse(request.query);
    const result = await listMemorySubjects(request.vault, qs);
    return {
      items: result.items,
      limit: qs.limit,
      offset: qs.offset,
      total: result.total
    };
  });

  app.post('/v1/memories',{preHandler:requireVaultWriteAuth},async(request,reply)=>{
    const parsed=createMemorySchema.safeParse(request.body);
    if(!parsed.success)return reply.code(400).send({error:'Invalid memory payload'});
    const body=parsed.data;
    if(isSecretLikeMemoryContent(body.subject+'\n'+body.data))return reply.code(400).send({error:'Memory contains unsupported sensitive content'});
    if(body.parent_id){
      const parent=await query(`SELECT id FROM memories WHERE id=$1 AND vault_id=$2
        AND status='active' AND archived_at IS NULL AND scope=$3 AND scope_key IS NOT DISTINCT FROM $4::text`,
        [body.parent_id,request.vault.id,body.scope,body.scope_key ?? null]);
      if(parent.rowCount!==1)return reply.code(400).send({error:'Parent must be an active same-binding memory'});
    }
    await checkMemoryCreationCapacity(request.vault.id);
    const prepared=await prepareVaultCrypto(request.vault);
    const embedding=await getEmbedder().embed(body.data,{vaultId:request.vault.id,modelRole:'embedding',source:'api',inputType:'document'});
    const subject=prepared.subject(request.vault,body.subject);
    const effects:WorkerEffect[]=[];
    const row=await withTransaction(async client=>{
      await lockMemoryWriteVault(client,request.vault.id);await prepared.assertCurrent(client);
      if(body.parent_id){
        const parent=await client.query(`SELECT id FROM memories WHERE id=$1 AND vault_id=$2
          AND status='active' AND archived_at IS NULL AND scope=$3 AND scope_key IS NOT DISTINCT FROM $4::text FOR KEY SHARE`,
          [body.parent_id,request.vault.id,body.scope,body.scope_key ?? null]);
        if(parent.rowCount!==1)throw Object.assign(new Error('Parent must be an active same-binding memory'),{statusCode:400});
      }
      const reservation=await reserveMemoryCreationInTransaction(client,request.vault.id,'api');
      const actor=platformActorForAudit(request.auth);
      const inserted=await client.query<MemoryResponseRow>(`INSERT INTO memories
        (vault_id,data,subject,subject_encrypted,subject_hmac,hash,embedding,categories,parent_id,type,scope,scope_key,evidence,volatility,status,source_timestamp)
        VALUES($1,$2,$3,$4,$5,$6,$7::vector,$8::text[],$9,$10,$11,$12,$13::jsonb,$14::memory_volatility,'active',now())
        RETURNING ${memoryResponseSelect('memories')}`,
        [request.vault.id,prepared.encrypt(request.vault,body.data),isVaultEncryptionActive(request.vault)?'':body.subject,
          subject?.encrypted ?? null,subject?.hmac ?? null,crypto.createHash('sha256').update(body.data).digest('hex'),JSON.stringify(embedding),
          body.categories,body.parent_id ?? null,body.type,body.scope,body.scope_key ?? null,
          JSON.stringify({summary:body.evidence ?? null,authored_via:'memory_api',actor}),body.volatility]);
      const row=inserted.rows[0];
      await client.query('INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)',[row.id,JSON.stringify(embedding)]);
      await enqueueCurationWork(client,{vaultId:request.vault.id,workKey:'manual:'+crypto.randomUUID(),memoryIds:[String(row.id)]});
      effects.push({kind:'quota',reservation},{kind:'memory-count',vaultId:request.vault.id,accountId:request.vault.account_id,delta:1,source:'api'});
      return row;
    });
    publishCommittedWorkerEffects(effects);
    await recordMemoryCreatedActivity(request,String(row.id));
    return reply.code(201).send(await decryptMemoryRow(request.vault,row));
  });

  app.get('/v1/memories/graph', { preHandler: requireVaultReadAuth }, async (request, reply) => {
    if (!(await vaultCanViewMemoryGraph(request.vault))) {
      return reply.code(403).send({ error: 'Memory graph requires a graph-capable plan' });
    }

    const parsedQuery = graphQuerySchema.safeParse(request.query);
    if (!parsedQuery.success) {
      return reply.code(400).send({ error: 'Invalid graph query' });
    }

    return memoryGraphResponse(request.vault, parsedQuery.data, reply);
  });

  app.get('/v1/memories/:id',{preHandler:requireVaultReadAuth},async(request,reply)=>{
    const {id}=z.object({id:z.string().uuid()}).parse(request.params);
    readMemoryQuerySchema.parse(request.query);
    const result=await query<MemoryResponseRow>(`SELECT ${memoryResponseSelect('memories')} FROM memories WHERE vault_id=$1 AND id=$2`,[request.vault.id,id]);
    if(!result.rowCount)return reply.code(404).send({error:'Memory not found'});
    return decryptMemoryRow(request.vault,result.rows[0]);
  });

  app.delete('/v1/memories/:id',{preHandler:requireVaultWriteAuth},async(request,reply)=>{
    const {id}=z.object({id:z.string().uuid()}).parse(request.params);
    const result=await withTransaction(async client=>{
      await lockMemoryWriteVault(client,request.vault.id);
      const previous=(await client.query<{archived_at:string|null}>('SELECT archived_at FROM memories WHERE vault_id=$1 AND id=$2 FOR UPDATE',[request.vault.id,id])).rows[0];
      if(!previous)return null;
      const updated=await client.query<{id:string;archived_at:string}>(`UPDATE memories SET archived_at=COALESCE(archived_at,now()),updated_at=now()
        WHERE vault_id=$1 AND id=$2 RETURNING id,archived_at`,[request.vault.id,id]);
      return{row:updated.rows[0],changed:previous.archived_at===null};
    });
    if(!result)return reply.code(404).send({error:'Memory not found'});
    if(result.changed){publishCommittedWorkerEffects([{kind:'memory-count',vaultId:request.vault.id,accountId:request.vault.account_id,delta:-1,source:'api'}]);
      await recordMemoryArchivedActivity(request,id);}
    return result.row;
  });

  app.patch('/v1/memories/:id',{preHandler:requireVaultWriteAuth},async(request,reply)=>{
    const {id}=z.object({id:z.string().uuid()}).parse(request.params);
    const parsed=updateMemorySchema.safeParse(request.body);
    if(!parsed.success)return reply.code(400).send({error:'Invalid memory payload'});
    const body=parsed.data;
    const snapshot=(await query<any>('SELECT * FROM memories WHERE vault_id=$1 AND id=$2',[request.vault.id,id])).rows[0];
    if(!snapshot)return reply.code(404).send({error:'Memory not found'});
    const proposedBinding=resolveMemoryScopeChange(snapshot,body);
    if(proposedBinding.changedScope)await assertCompatibleScopeRelationships(query,request.vault.id,id,snapshot.parent_id,
      proposedBinding.scope,proposedBinding.scopeKey);
    if(body.archived===false && snapshot.archived_at!==null)await checkMemoryCreationCapacity(request.vault.id);
    const prepared=await prepareVaultCrypto(request.vault);
    // Validate the effective content both before paid work and against locked
    // current state. Concurrent edits must not authorize a stale merged payload.
    const resolveContent=(current:{data:string;subject:string;subject_encrypted:string|null})=>{
      const fact=body.data ?? prepared.decrypt(request.vault,current.data);
      const subjectText=body.subject ?? (current.subject_encrypted ? prepared.decrypt(request.vault,current.subject_encrypted):current.subject);
      if(isSecretLikeMemoryContent(subjectText+'\n'+fact))throw Object.assign(new Error('Memory contains unsupported sensitive content'),{statusCode:400});
      return{fact,subjectText};
    };
    resolveContent(snapshot);
    const embedding=body.data!==undefined ? await getEmbedder().embed(body.data,{vaultId:request.vault.id,modelRole:'embedding',source:'api',inputType:'document'}):null;
    const effects:WorkerEffect[]=[];
    const result=await withTransaction(async client=>{
      await lockMemoryWriteVault(client,request.vault.id);await prepared.assertCurrent(client);
      const current=(await client.query<any>('SELECT * FROM memories WHERE vault_id=$1 AND id=$2 FOR UPDATE',[request.vault.id,id])).rows[0];
      if(!current)return null;
      const {fact,subjectText}=resolveContent(current);
      const {scope,scopeKey,changedScope}=resolveMemoryScopeChange(current,body);
      if(changedScope){
        // Do not quietly disconnect a memory from its graph to change its scope.
        await assertCompatibleScopeRelationships(client.query.bind(client),request.vault.id,id,current.parent_id,scope,scopeKey);
      }
      const restoring=body.archived===false && current.archived_at!==null;
      if(restoring){
        const reservation=await reserveMemoryCreationInTransaction(client,request.vault.id,'api');
        effects.push({kind:'quota',reservation});
      }
      const encryptedSubject=body.subject!==undefined ? prepared.subject(request.vault,subjectText):null;
      const fields:string[]=[],values:unknown[]=[request.vault.id,id];
      const set=(field:string,value:unknown,cast='')=>{values.push(value);fields.push(`${field}=$${values.length}${cast}`);};
      if(body.data!==undefined){set('data',prepared.encrypt(request.vault,fact));set('hash',crypto.createHash('sha256').update(fact).digest('hex'));set('embedding',JSON.stringify(embedding),'::vector');set('source_timestamp',new Date().toISOString(),'::timestamptz');}
      if(body.subject!==undefined){set('subject',isVaultEncryptionActive(request.vault)?'':subjectText);set('subject_encrypted',encryptedSubject?.encrypted ?? null);set('subject_hmac',encryptedSubject?.hmac ?? null);}
      if(body.type!==undefined)set('type',body.type);
      if(body.categories!==undefined)set('categories',body.categories,'::text[]');
      if(body.confidence!==undefined)set('confidence',body.confidence);
      if(changedScope){set('scope',scope);set('scope_key',scopeKey);}
      if(body.evidence!==undefined || body.data!==undefined || body.subject!==undefined){
        const previous=current.evidence && typeof current.evidence==='object' ? current.evidence:{};
        set('evidence',JSON.stringify({...previous,summary:body.evidence===undefined ? previous.summary ?? null:body.evidence,
          authored_via:'memory_api',actor:platformActorForAudit(request.auth)}),'::jsonb');
      }
      if(body.archived!==undefined){set('archived_at',body.archived ? current.archived_at ?? new Date().toISOString():null,'::timestamptz');if(restoring)set('status','active');}
      if(!fields.length)return{row:(await client.query<MemoryResponseRow>(
        `SELECT ${memoryResponseSelect('memories')} FROM memories WHERE vault_id=$1 AND id=$2`,
        [request.vault.id,id])).rows[0],delta:0};
      const updated=(await client.query<MemoryResponseRow>(`UPDATE memories SET ${fields.join(',')},updated_at=now()
        WHERE vault_id=$1 AND id=$2 RETURNING ${memoryResponseSelect('memories')}`,values)).rows[0];
      if(embedding)await client.query(`INSERT INTO memory_embeddings(memory_id,embedding) VALUES($1,$2::vector)
        ON CONFLICT(memory_id) DO UPDATE SET embedding=EXCLUDED.embedding,embedded_at=now()`,[id,JSON.stringify(embedding)]);
      if(changedScope){
        const actor=platformActorForAudit(request.auth);
        await client.query(`INSERT INTO memory_scope_change_log
          (vault_id,memory_id,old_scope,new_scope,old_scope_key,new_scope_key,actor_type,actor_id,source,reason)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,'api',$9)`,[request.vault.id,id,current.scope,scope,current.scope_key,scopeKey,actor.type,actor.id,body.scope_change_reason]);
      }
      // The database decides whether this was a substantive change.
      const revision=(await client.query('SELECT revision::text FROM memories WHERE id=$1 AND vault_id=$2',[id,request.vault.id])).rows[0].revision;
      if(revision!==String(current.revision))await enqueueCurationWork(client,{vaultId:request.vault.id,workKey:'manual:'+crypto.randomUUID(),memoryIds:[id]});
      const delta=current.archived_at===null && updated.archived_at!==null ? -1:restoring ? 1:0;
      effects.push({kind:'memory-count',vaultId:request.vault.id,accountId:request.vault.account_id,delta,source:'api'});
      return{row:updated,delta};
    });
    if(!result)return reply.code(404).send({error:'Memory not found'});
    publishCommittedWorkerEffects(effects);
    if(result.delta<0)await recordMemoryArchivedActivity(request,id);
    return decryptMemoryRow(request.vault,result.row);
  });
}

async function recordMemoryCreatedActivity(
  request: FastifyRequest,
  memoryId: string
): Promise<void> {
  const workspaceId = request.vault.account_id;
  if (!workspaceId) return;
  const actor = platformActorForAudit(request.auth);
  const payload: MemoryCreatedPayload = {
    actor,
    counts: { memories_added: 1 },
    memory_id: memoryId,
    platform_vault_id: request.vault.id,
    sensitivity: 'metadata_only',
    source: 'api',
    summary: 'New memory added',
    vault_id: request.vault.id,
    workspace_id: workspaceId
  };
  await writeActivityOutboxEvent(request, {
    eventType: memoryCreatedEventType,
    payload,
    subject: `vault:${request.vault.id}/memory:${memoryId}`
  });
}

async function recordMemoryArchivedActivity(
  request: FastifyRequest,
  memoryId: string
): Promise<void> {
  const workspaceId = request.vault.account_id;
  if (!workspaceId) return;
  const actor = platformActorForAudit(request.auth);
  const payload: MemoryArchivedPayload = {
    actor,
    counts: { memories_archived: 1 },
    memory_id: memoryId,
    platform_vault_id: request.vault.id,
    sensitivity: 'metadata_only',
    source: 'api',
    summary: 'Memory archived',
    vault_id: request.vault.id,
    workspace_id: workspaceId
  };
  await writeActivityOutboxEvent(request, {
    eventType: memoryArchivedEventType,
    payload,
    subject: `vault:${request.vault.id}/memory:${memoryId}`
  });
}

async function writeActivityOutboxEvent(
  request: FastifyRequest,
  input: {
    eventType: string;
    payload: MemoryArchivedPayload | MemoryCreatedPayload;
    subject: string;
  }
): Promise<void> {
  try {
    await query(
      `INSERT INTO platform_event_outbox (
         event_id, event_type, schema_version, occurred_at, subject, payload
       )
       VALUES (gen_random_uuid(), $1, 1, now(), $2, $3::jsonb)`,
      [input.eventType, input.subject, JSON.stringify(input.payload)]
    );
  } catch (error) {
    request.log.warn({
      err: error,
      event_type: input.eventType,
      vault_id: request.vault.id
    }, 'failed to write activity event outbox row');
  }
}

type GraphDirection = 'out' | 'in' | 'both';
type GraphEdgeType = typeof graphEdgeTypes[number];

interface GraphNodeRow extends MemoryResponseRow {
  id: string;
  depth: number;
}

interface GraphEdgeRow extends QueryResultRow {
  id: string;
  from_memory_id: string;
  to_memory_id: string;
  type: GraphEdgeType;
  confidence: number;
  reason: string | null;
  created_at: string;
  updated_at: string;
}

async function fetchSeededGraphNodes(
  vaultId: string,
  seedMemoryId: string,
  depth: number,
  limit: number,
  direction: GraphDirection,
  edgeTypes: GraphEdgeType[] | null
): Promise<GraphNodeRow[]> {
  const seed = await fetchVisibleGraphSeed(vaultId, seedMemoryId);
  if (!seed) return [];

  const nodes: GraphNodeRow[] = [seed];
  const seen = new Set<string>([seed.id]);
  let frontier: GraphNodeRow[] = [seed];

  for (let nextDepth = 1; nextDepth <= depth && nodes.length < limit && frontier.length > 0; nextDepth += 1) {
    const remaining = limit - nodes.length;
    const neighbors = await fetchGraphNeighborNodes(
      vaultId,
      frontier.map((node) => node.id),
      [...seen],
      nextDepth,
      remaining,
      direction,
      edgeTypes
    );
    const uniqueNeighbors = neighbors.filter((node) => {
      if (seen.has(node.id)) return false;
      seen.add(node.id);
      return true;
    });

    nodes.push(...uniqueNeighbors);
    frontier = uniqueNeighbors;
  }

  return nodes;
}

async function fetchVisibleGraphSeed(vaultId: string, seedMemoryId: string): Promise<GraphNodeRow | null> {
  const result = await query<GraphNodeRow>(
    `SELECT ${memoryResponseSelect('memories')}, 0 AS depth
     FROM memories
     WHERE vault_id = $1
       AND id = $2
       AND archived_at IS NULL
       AND status='active'
     LIMIT 1`,
    [vaultId, seedMemoryId]
  );

  return result.rows[0] ?? null;
}

function getNeighborEdgeSelect(direction: GraphDirection): string {
  const outbound = `SELECT e.to_memory_id AS neighbor_id, e.confidence, e.updated_at AS edge_updated_at
                   FROM memory_edges e
                   WHERE e.vault_id = $1
                     AND e.from_memory_id = frontier.id
                     AND NOT (e.to_memory_id = ANY($3::uuid[]))
                     AND ($4::text[] IS NULL OR e.type = ANY($4::text[]))`;
  const inbound = `SELECT e.from_memory_id AS neighbor_id, e.confidence, e.updated_at AS edge_updated_at
                  FROM memory_edges e
                  WHERE e.vault_id = $1
                    AND e.to_memory_id = frontier.id
                    AND NOT (e.from_memory_id = ANY($3::uuid[]))
                    AND ($4::text[] IS NULL OR e.type = ANY($4::text[]))`;

  if (direction === 'out') return outbound;
  if (direction === 'in') return inbound;
  return `${outbound} UNION ALL ${inbound}`;
}

async function fetchGraphNeighborNodes(
  vaultId: string,
  frontierIds: string[],
  excludedIds: string[],
  nextDepth: number,
  limit: number,
  direction: GraphDirection,
  edgeTypes: GraphEdgeType[] | null
): Promise<GraphNodeRow[]> {
  const perFrontierNodeLimit = Math.min(Math.max(limit * 2, 10), 50);
  const neighborEdgeSelect = getNeighborEdgeSelect(direction);
  const result = await query<GraphNodeRow>(
    `WITH frontier AS (
       SELECT unnest($2::uuid[]) AS id
     ), candidate_edges AS (
       SELECT candidate.neighbor_id, candidate.confidence, candidate.edge_updated_at
       FROM frontier
       CROSS JOIN LATERAL (
         ${neighborEdgeSelect}
         ORDER BY confidence DESC, edge_updated_at DESC, neighbor_id
         LIMIT $7
       ) candidate
     ), ranked_edges AS (
       SELECT neighbor_id,
              MAX(confidence) AS confidence,
              MAX(edge_updated_at) AS edge_updated_at
       FROM candidate_edges
       GROUP BY neighbor_id
     )
     SELECT ${memoryResponseSelect('memories')}, $5::int AS depth
     FROM ranked_edges
     JOIN memories ON memories.id = ranked_edges.neighbor_id
     WHERE memories.vault_id = $1
       AND memories.archived_at IS NULL
       AND memories.status='active'
     ORDER BY ranked_edges.confidence DESC, memories.salience DESC, ranked_edges.edge_updated_at DESC, memories.updated_at DESC, memories.id
     LIMIT $6`,
    [vaultId, frontierIds, excludedIds, edgeTypes, nextDepth, limit, perFrontierNodeLimit]
  );

  return result.rows;
}

async function fetchGraphOverviewNodes(vaultId: string, limit: number): Promise<GraphNodeRow[]> {
  const result = await query<GraphNodeRow>(
    `SELECT ${memoryResponseSelect('memories')}, 0 AS depth
     FROM memories
     WHERE vault_id = $1
       AND archived_at IS NULL
       AND status='active'
     ORDER BY salience DESC, updated_at DESC, created_at DESC, id
     LIMIT $2`,
    [vaultId, limit]
  );

  return result.rows;
}

async function fetchGraphEdges(
  vaultId: string,
  nodeIds: string[],
  edgeTypes: GraphEdgeType[] | null
): Promise<GraphEdgeRow[]> {
  const result = await query<GraphEdgeRow>(
    `SELECT id, from_memory_id, to_memory_id, type, confidence, reason, created_at, updated_at
     FROM memory_edges
     WHERE vault_id = $1
       AND from_memory_id = ANY($2::uuid[])
       AND to_memory_id = ANY($2::uuid[])
       AND ($3::text[] IS NULL OR type = ANY($3::text[]))
     ORDER BY type ASC, confidence DESC, updated_at DESC, id
     LIMIT 500`,
    [vaultId, nodeIds, edgeTypes]
  );

  return result.rows;
}

interface MemoryResponseRow extends QueryResultRow {
  categories?: unknown;
  confidence?: number | null;
  created_at?: unknown;
  data?: unknown;
  polarity?: unknown;
  salience?: unknown;
  scope?: unknown;
  source_timestamp?: unknown;
  subject?: unknown;
  subject_encrypted?: unknown;
  type?: unknown;
  updated_at?: unknown;
}

interface MemorySubjectRow extends QueryResultRow {
  subject: unknown;
  subject_encrypted?: unknown;
  subject_hmac?: unknown;
  count: unknown;
  latest_at: unknown;
}

interface MemorySubjectSummary {
  count: number;
  latest_at: string | null;
  subject: string;
}

async function getAdminVaultContext(request: FastifyRequest, id: string): Promise<AdminVaultContext | null> {
  const accountId = getAuthAccountId(request);
  const accountFilter = accountId ? 'AND account_id = $2::uuid' : '';
  const vaultParams: unknown[] = accountId ? [id, accountId] : [id];
  const vaultResult = await query<AdminVaultContext>(
    `SELECT id, name, purpose, settings, plan_id, status, account_id, encrypted_dek, vault_encryption_enabled
     FROM vaults
     WHERE id = $1
       ${accountFilter}
     LIMIT 1`,
    vaultParams
  );

  return vaultResult.rows[0] ?? null;
}

async function vaultCanViewMemoryGraph(vault: Pick<VaultContext, 'plan_id'>): Promise<boolean> {
  const planResult = await query<{ limits: Record<string, unknown> | null }>(
    `SELECT limits
     FROM plans
     WHERE id = $1
     LIMIT 1`,
    [vault.plan_id]
  );
  const limits = planResult.rows[0]?.limits ?? {};
  return readBoolean(limits.graphEnabled ?? limits.graph_enabled) === true;
}

async function memoryGraphResponse(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof graphQuerySchema>,
  reply: { code: (statusCode: number) => { send: (payload: unknown) => unknown } }
) {
  const edgeTypes = qs.edge_types ?? null;
  const nodes = qs.seed_memory_id
    ? await fetchSeededGraphNodes(vault.id, qs.seed_memory_id, qs.depth, qs.limit, qs.direction, edgeTypes)
    : await fetchGraphOverviewNodes(vault.id, qs.limit);

  if (qs.seed_memory_id && nodes.length === 0) {
    return reply.code(404).send({ error: 'Seed memory not found' });
  }

  const nodeIds = nodes.map((node) => String(node.id));
  const edges = nodeIds.length
    ? await fetchGraphEdges(vault.id, nodeIds, edgeTypes)
    : [];
  const decryptedNodes = await Promise.all(nodes.map((row) => decryptMemoryRow(vault, row)));

  return {
    seed_memory_id: qs.seed_memory_id ?? null,
    depth: qs.depth,
    limit: qs.limit,
    direction: qs.direction,
    edge_types: qs.edge_types ?? null,
    nodes: decryptedNodes,
    edges
  };
}

function readBoolean(value: unknown): boolean | null {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') return true;
    if (normalized === 'false') return false;
  }
  return null;
}

async function listMemories(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof listQuerySchema>
): Promise<{ items: Awaited<ReturnType<typeof decryptMemoryRow>>[]; total: number }> {
  if (isVaultEncryptionActive(vault) && qs.q) {
    return listEncryptedSearchMemories(vault, qs);
  }

  const rows = await listMemoryRows(vault, qs);
  const items = await Promise.all(rows.items.map((row) => decryptMemoryRow(vault, row)));
  return { items, total: rows.total };
}

async function listMemoryRows(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof listQuerySchema>
): Promise<{ items: MemoryResponseRow[]; total: number }> {
  const values: unknown[] = [vault.id];
  const conditions = [`vault_id = $1`];

  if (qs.archived === 'false') {
    conditions.push('archived_at IS NULL');
  } else {
    conditions.push('archived_at IS NOT NULL');
  }

  if (qs.category) {
    values.push(qs.category);
    conditions.push(`$${values.length} = ANY(categories)`);
  }

  if (qs.filter === 'high-confidence') {
    conditions.push('confidence >= 0.8');
  } else if (qs.filter === 'low-confidence') {
    conditions.push('(confidence IS NULL OR confidence < 0.5)');
  } else if (qs.filter === 'positive') {
    conditions.push(`polarity = 'positive'`);
  } else if (qs.filter === 'negative') {
    conditions.push(`polarity = 'negative'`);
  }

  if (qs.subject) {
    if (isVaultEncryptionActive(vault)) {
      values.push(await subjectHmacForVault(vault, qs.subject));
      conditions.push(`subject_hmac = $${values.length}`);
    } else {
      values.push(qs.subject);
      conditions.push(`subject = $${values.length}`);
    }
  }

  if (qs.q && !isVaultEncryptionActive(vault)) {
    values.push(`%${escapeLikePattern(qs.q)}%`);
    const index = values.length;
    conditions.push(`(subject ILIKE $${index} ESCAPE '\\' OR data ILIKE $${index} ESCAPE '\\' OR type ILIKE $${index} ESCAPE '\\' OR scope ILIKE $${index} ESCAPE '\\' OR EXISTS (SELECT 1 FROM unnest(categories) category WHERE category ILIKE $${index} ESCAPE '\\'))`);
  }

  let sql: string;
  let totalSql: string;
  if (qs.include_children) {
    const treeSql = `WITH RECURSIVE tree AS (
             SELECT *, 0 AS depth FROM memories WHERE ${conditions.join(' AND ')}
             UNION ALL
             SELECT m.*, t.depth + 1 FROM memories m
             JOIN tree t ON m.parent_id = t.id
             WHERE m.vault_id = $1
               AND ${qs.archived === 'false'
                 ? `m.archived_at IS NULL`
                 : `m.archived_at IS NOT NULL`}
               AND t.depth < 10
           )`;
    const finalArchivedClause = qs.archived === 'false'
      ? `archived_at IS NULL`
      : `archived_at IS NOT NULL`;
    sql = `${treeSql}
           SELECT ${memoryResponseSelect('tree')}
           FROM tree
           WHERE ${finalArchivedClause}
           ORDER BY created_at DESC
           LIMIT 1000`;
    totalSql = `${treeSql}
           SELECT COUNT(*)::int AS total
           FROM tree
           WHERE ${finalArchivedClause}`;
  } else {
    values.push(qs.limit, qs.offset);
    totalSql = `SELECT COUNT(*)::int AS total FROM memories WHERE ${conditions.join(' AND ')}`;
    sql = `SELECT ${memoryResponseSelect('memories')}
           FROM memories
           WHERE ${conditions.join(' AND ')}
           ORDER BY ${memoryOrderBy(qs.sort)}
           LIMIT $${values.length - 1}
           OFFSET $${values.length}`;
  }

  const countValues = qs.include_children ? values : values.slice(0, -2);
  const [result, countResult] = await Promise.all([
    query<MemoryResponseRow>(sql, values),
    query<{ total: number }>(totalSql, countValues)
  ]);
  const offset = parseInt(String(qs.offset)) || 0;
  const limit = parseInt(String(qs.limit)) || 50;
  const items = qs.include_children
    ? result.rows.slice(offset, offset + limit)
    : result.rows;
  return { items, total: Number(countResult.rows[0]?.total ?? items.length) };
}

async function listEncryptedSearchMemories(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof listQuerySchema>
): Promise<{ items: Awaited<ReturnType<typeof decryptMemoryRow>>[]; total: number }> {
  const pageSize = 200;
  const rows: MemoryResponseRow[] = [];
  let offset = 0;
  let total = 0;

  do {
    const page = await listMemoryRows(vault, { ...qs, limit: pageSize, offset, q: undefined });
    rows.push(...page.items);
    total = page.total;
    if (page.items.length === 0) break;
    offset += page.items.length;
  } while (offset < total && rows.length < total);

  const decrypted = await Promise.all(rows.map((row) => decryptMemoryRow(vault, row)));
  const normalized = normalizeSearch(qs.q ?? '');
  const filtered = decrypted
    .filter((memory) => memoryMatchesQuery(memory, normalized))
    .sort((a, b) => compareMemories(a, b, qs.sort));

  return {
    items: filtered.slice(qs.offset, qs.offset + qs.limit),
    total: filtered.length
  };
}

async function listMemorySubjects(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  qs: z.infer<typeof subjectListQuerySchema>
): Promise<{ items: MemorySubjectSummary[]; total: number }> {
  const values: unknown[] = [vault.id];
  const conditions = [`vault_id = $1`];
  if (qs.archived === 'false') {
    conditions.push('archived_at IS NULL');
  } else {
    conditions.push('archived_at IS NOT NULL');
  }

  const result = await query<MemorySubjectRow>(
    `SELECT
       COALESCE(subject_hmac, subject, 'Unknown subject') AS subject_hmac,
       MIN(subject) AS subject,
       MIN(subject_encrypted) AS subject_encrypted,
       COUNT(*)::int AS count,
       MAX(COALESCE(source_timestamp, updated_at, created_at)) AS latest_at
     FROM memories
     WHERE ${conditions.join(' AND ')}
     GROUP BY COALESCE(subject_hmac, subject, 'Unknown subject')`,
    values
  );

  const normalized = normalizeSearch(qs.q ?? '');
  const subjects = (await Promise.all(result.rows.map((row) => deserializeSubjectSummary(vault, row))))
    .filter((subject) => !normalized || normalizeSearch(subject.subject).includes(normalized))
    .sort((a, b) => compareSubjects(a, b, qs.sort));

  return {
    items: subjects.slice(qs.offset, qs.offset + qs.limit),
    total: subjects.length
  };
}

async function deserializeSubjectSummary(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  row: MemorySubjectRow
): Promise<MemorySubjectSummary> {
  const encryptedSubject = typeof row.subject_encrypted === 'string' ? row.subject_encrypted : null;
  const subject = encryptedSubject && isVaultEncryptionActive(vault)
    ? await decryptForVault(vault, encryptedSubject)
    : typeof row.subject === 'string' && row.subject.trim() ? row.subject : 'Unknown subject';

  return {
    count: Number(row.count ?? 0),
    latest_at: typeof row.latest_at === 'string' ? row.latest_at : row.latest_at instanceof Date ? row.latest_at.toISOString() : null,
    subject
  };
}

function memoryOrderBy(sort: z.infer<typeof listQuerySchema>['sort']): string {
  if (sort === 'oldest') return 'created_at ASC, updated_at ASC';
  if (sort === 'confidence') return 'confidence DESC NULLS LAST, updated_at DESC, created_at DESC';
  if (sort === 'salience') return 'salience DESC NULLS LAST, updated_at DESC, created_at DESC';
  return 'updated_at DESC, created_at DESC';
}

function compareSubjects(a: MemorySubjectSummary, b: MemorySubjectSummary, sort: z.infer<typeof subjectListQuerySchema>['sort']): number {
  if (sort === 'name') return a.subject.localeCompare(b.subject);
  if (sort === 'recent') return Date.parse(b.latest_at ?? '') - Date.parse(a.latest_at ?? '') || b.count - a.count || a.subject.localeCompare(b.subject);
  return b.count - a.count || Date.parse(b.latest_at ?? '') - Date.parse(a.latest_at ?? '') || a.subject.localeCompare(b.subject);
}

function compareMemories(
  a: Awaited<ReturnType<typeof decryptMemoryRow>>,
  b: Awaited<ReturnType<typeof decryptMemoryRow>>,
  sort: z.infer<typeof listQuerySchema>['sort']
): number {
  if (sort === 'oldest') return memoryTimestampMs(a) - memoryTimestampMs(b);
  if (sort === 'confidence') return (b.confidence ?? 0) - (a.confidence ?? 0) || memoryTimestampMs(b) - memoryTimestampMs(a);
  if (sort === 'salience') return Number(b.salience ?? 0) - Number(a.salience ?? 0) || memoryTimestampMs(b) - memoryTimestampMs(a);
  return memoryTimestampMs(b) - memoryTimestampMs(a);
}

function memoryTimestampMs(memory: { source_timestamp?: unknown; updated_at?: unknown; created_at?: unknown }): number {
  const value = memory.source_timestamp ?? memory.updated_at ?? memory.created_at;
  const parsed = typeof value === 'string' ? Date.parse(value) : 0;
  return Number.isFinite(parsed) ? parsed : 0;
}

function memoryMatchesQuery(memory: Awaited<ReturnType<typeof decryptMemoryRow>>, normalizedQuery: string): boolean {
  if (!normalizedQuery) return true;
  return [
    memory.subject,
    memory.data,
    memory.type,
    memory.scope,
    memory.polarity,
    ...(Array.isArray(memory.categories) ? memory.categories : [])
  ].some((value) => normalizeSearch(String(value ?? '')).includes(normalizedQuery));
}

function normalizeSearch(value: string): string {
  return value.trim().toLowerCase();
}

function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (match) => `\\${match}`);
}

async function subjectHmacForVault(
  vault: { encrypted_dek: string | null; vault_encryption_enabled: boolean },
  subject: string
): Promise<string> {
  if (!vault.encrypted_dek) throw new Error('Encrypted vault is missing encrypted_dek');
  return computeSubjectHmac(subject, await unwrapDek(vault.encrypted_dek));
}

async function decryptMemoryRow(
  vault: { id: string; encrypted_dek: string | null; vault_encryption_enabled: boolean },
  row: MemoryResponseRow
) {
  const decryptedSubject = typeof row.subject_encrypted === 'string' && isVaultEncryptionActive(vault)
    ? await decryptForVault(vault, row.subject_encrypted)
    : row.subject;

  const { subject_encrypted, ...safeRow } = row as typeof row & { subject_encrypted?: unknown };

  return {
    ...safeRow,
    data: typeof row.data === 'string' ? await decryptForVault(vault, row.data) : row.data,
    subject: decryptedSubject
  };
}
