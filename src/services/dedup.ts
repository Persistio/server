import { prepareConflictEvidence, conflictStillCurrent, sameSourceManifest, CONFLICT_BOUNDS, type ConflictSide } from './conflict-evidence';
import { BEHAVIORAL_MEMORY_TYPES, isMemoryTemporallyRecallable, memoryTemporalRecallPredicateSql, memoryTemporalState } from './memory-temporal-recall';
import { contextIdentitySchema } from './memory-applicability';
import crypto from 'node:crypto';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';
import { query, withTransaction } from '../db/client';
import { prepareVaultCrypto, type PreparedVaultCrypto, isVaultEncryptionActive, type VaultEncryptionContext } from './crypto';
import { normaliseSubject, resolveCanonical } from './entity-resolver';
import { ExtractorService, type ConflictResolution, type ConflictArbitrationContext, type ConflictAssessment, validatedConflictAssessment } from './extractor';
import { reserveMemoryCreationInTransaction, type ApiQuotaReservation } from './usage';
import { memoryCountEffect, captureShadowWorkerEffects, publishCommittedWorkerEffects, type WorkerEffect } from './worker-effects';
import { enqueueCurationWork } from './curation-work';
import type { MemoryScope } from './memory-scope';
import { mergeMemoryEvidence } from './memory-evidence';
import { isSecretLikeMemoryContent } from './deterministic-filter';
import { isValidDateOnly, validityWindowsOverlapPredicateSql } from './memory-validity';

export interface DedupInput {
  vaultId: string;
  fact: string;
  score: number;
  subject: string;
  embedding: number[];
  sourceChunks: string[];
  salience: number;
  sensitivity: 'low' | 'medium' | 'high';
  type: 'user_preference' | 'user_rule' | 'task_pattern' | 'workflow' | 'project' | 'constraint' | 'decision' | 'system_fact' | 'domain_knowledge' | null;
  scope: MemoryScope;
  scopeKey: string | null;
  polarity: 'positive' | 'negative' | 'neutral';
  status: 'active';
  volatility: 'very_low' | 'low' | 'medium' | 'high';
  evidence?: string | null;
  validFrom: string | null;
  validUntil: string | null;
  sourceSegmentId?: string | null;
  sourceTimestamp?: string | null;
  policyRejections?: Array<{
    code: string;
    field: string;
    reason: string;
  }>;
}

interface MemoryRow {
  account_id: string | null;
  id: string;
  data: string;
  confidence: number;
  score: number;
  salience: number;
  sensitivity: DedupInput['sensitivity'];
  type: DedupInput['type'];
  scope: MemoryScope;
  scope_key: string | null;
  polarity: DedupInput['polarity'];
  status: DedupInput['status'];
  volatility: DedupInput['volatility'];
  evidence: unknown;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
  row_version: string;
}

export type DedupResult =
  | { action: 'skipped'; memoryId?: string }
  | { action: 'updated'; memoryId: string }
  | { action: 'inserted'; memoryId: string }
  | { action: 'conflict'; memoryId: string };

interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<T>>;
}

export interface DedupOptions {
  /** Retain no-decision future intent across provider/transaction delays; never authorizes a decision. */
  preflight?: { inputFingerprint: string; referenceTime: string };
  precomputedConflictDecision?: ConflictResolution | ConflictAssessment;
  precomputedConflictMemoryId?: string;
  precomputedConflictMemoryRevision?: string;
  /** Internal one-shot retry after a scope binding changes between match and lock. */
  staleRetryAttempted?: boolean;
  /** Binds a prepared decision to the complete candidate, not just its target. */
  precomputedConflictInput?: string;
}

export interface DedupEscalationRequest {
  id: string;
  inputFingerprint: string;
  existingFact: string;
  newFact: string;
  existingMemoryId: string;
  existingMemoryRevision: string;
  reasons: string[];
  context: ConflictArbitrationContext;
}


interface DedupVaultContext extends VaultEncryptionContext { account_id: string | null; }
interface MatchedMemory extends MemoryRow {
  source_chunks: string[];
  valid_from: string | null;
  valid_until: string | null;
  source_timestamp: string | null;
  similarity: number;
}
interface Match {
  vault: DedupVaultContext;
  hash: string;
  canonicalSubject: string;
  exact?: MatchedMemory;
  related?: MatchedMemory;
  /** Separate model-eligible selection; never changes deterministic storage matching. */
  arbitrationRelated?: MatchedMemory;
}

function validateInput(input: DedupInput): void {
  if (input.status !== 'active' || input.policyRejections?.length) throw new Error('Invalid baseline memory state');
  if (!input.fact.trim() || !input.subject.trim() || input.fact.length > 10000 || input.subject.length > 500
    || isSecretLikeMemoryContent(input.subject + '\n' + input.fact)) throw new Error('Invalid memory content');
  if (!['global','project','task','session'].includes(input.scope)
    || (input.scope === 'global' ? input.scopeKey !== null
      : !input.scopeKey || input.scopeKey.trim() !== input.scopeKey || !contextIdentitySchema.safeParse(input.scopeKey).success)) {
    throw new Error('Invalid memory binding');
  }
  if (![input.validFrom,input.validUntil].every(value => value === null || isValidDateOnly(value))
    || (input.validFrom !== null && input.validUntil !== null && input.validFrom > input.validUntil)) throw new Error('Invalid memory dates');
  if (!Number.isFinite(input.salience) || input.salience < 0 || input.salience > 1
    || !Number.isInteger(input.score) || input.score < 1 || input.score > 10) throw new Error('Invalid memory quality');
}

export function fingerprintDedupInput(input: DedupInput): string {
  return crypto.createHash('sha256').update(JSON.stringify(input)).digest('hex');
}

export function dedupConflictSide(input:DedupInput):ConflictSide {
  return {binding:fingerprintDedupInput(input),text:input.fact,subject:input.subject,scope:input.scope,scopeKey:input.scopeKey,type:input.type,
    polarity:input.polarity,validFrom:input.validFrom,validUntil:input.validUntil,
    sourceTimestamp:input.sourceTimestamp??null,sourceChunks:[...new Set(input.sourceChunks)].sort()};
}

export async function deduplicateMemory(input: DedupInput, extractor?: ExtractorService, options: DedupOptions = {}): Promise<DedupResult> {
  validateInput(input);
  const completed = await committedDedupOutcome(input, { query });
  if (completed) return { action: 'skipped', memoryId: completed };
  const prepared = await prepareDedupCrypto(input.vaultId, { query });
  let decisionOptions = options;
  if (extractor && !options.precomputedConflictDecision) {
    const referenceTime = new Date();
    decisionOptions = { ...options, preflight: {
      inputFingerprint: fingerprintDedupInput(input), referenceTime: referenceTime.toISOString()
    } };
    const request = await getDedupEscalationRequest(input, 'preflight', { query }, prepared, referenceTime);
    if (request) decisionOptions = {
      ...decisionOptions,
      precomputedConflictDecision: await extractor.arbitrateConflict(request.existingFact, request.newFact, input.vaultId,request.context),
      precomputedConflictMemoryId: request.existingMemoryId,
      precomputedConflictMemoryRevision: request.existingMemoryRevision,
      precomputedConflictInput: request.inputFingerprint
    };
  }
  const effects: WorkerEffect[] = [];
  const result = await withTransaction(async client => {
    const result = await deduplicateMemoryInTransaction(input, client, prepared, effects, decisionOptions);
    if (result.action !== 'skipped' && result.memoryId) await enqueueCurationWork(client, {
      vaultId: input.vaultId, workKey: 'dedup:' + crypto.randomUUID(), memoryIds: [result.memoryId], segmentId: input.sourceSegmentId
    });
    await captureShadowWorkerEffects(client, effects);
    return result;
  });
  publishCommittedWorkerEffects(effects);
  return result;
}

/** Serialise short mutation/capacity checks per vault, never provider IO. */
export async function lockMemoryWriteVault(client: PoolClient, vaultId: string): Promise<void> {
  const result = await client.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE', [vaultId]);
  if (!result.rowCount) throw new Error('Memory vault does not exist');
}

export async function deduplicateMemoryInTransaction(
  input: DedupInput, db: PoolClient, preparedCrypto: PreparedVaultCrypto,
  effects: WorkerEffect[], options: DedupOptions = {}
): Promise<DedupResult> {
  validateInput(input);
  await lockMemoryWriteVault(db, input.vaultId);
  const completed = await committedDedupOutcome(input, db);
  if (completed) return { action: 'skipped', memoryId: completed };
  await preparedCrypto.assertCurrent(db);
  // Source arrays have no SQL element FK. Validate ownership before persistence.
  if (input.sourceChunks.length) {
    const sources = await db.query('SELECT id FROM raw_chunks WHERE vault_id=$1 AND id=ANY($2::uuid[]) FOR KEY SHARE',
      [input.vaultId,[...new Set(input.sourceChunks)]]);
    if (sources.rowCount !== new Set(input.sourceChunks).size) throw new Error('Memory source does not belong to vault');
  }
  // Re-read the decision's actual target, not the first storage-related row (or
  // a newly higher-ranked candidate). Exact storage matches still take priority.
  const now = new Date();
  const arbitration = options.precomputedConflictDecision && options.precomputedConflictMemoryId
    && options.precomputedConflictMemoryRevision && options.precomputedConflictInput === fingerprintDedupInput(input)
    && isMemoryTemporallyRecallable({type:input.type,valid_from:input.validFrom,valid_until:input.validUntil},now)
    ? { now, targetId: options.precomputedConflictMemoryId } : undefined;
  const match = await findMatch(input, db, preparedCrypto, arbitration);
  const related = match.arbitrationRelated ?? match.related;
  const previous = match.exact ?? related;
  if (previous) {
    const locked = await db.query<{ row_version: string }>(
      'SELECT revision::text AS row_version FROM memories WHERE id=$1 AND vault_id=$2 FOR UPDATE', [previous.id,input.vaultId]);
    if (locked.rows[0]?.row_version !== previous.row_version) throw new Error('Memory changed during deduplication');
  }
  if (match.exact) {
    const previous = match.exact;
    const sources = [...new Set([...previous.source_chunks,...input.sourceChunks])].sort();
    // An exact source retry is no new evidence, quota, revision or Curator work.
    const sensitivityRank={low:0,medium:1,high:2};
    if (sources.length === new Set(previous.source_chunks).size
      && sensitivityRank[input.sensitivity]<=sensitivityRank[previous.sensitivity]) return { action: 'skipped', memoryId: previous.id };
    await db.query(`UPDATE memories SET source_chunks=$3::uuid[],
      sensitivity=CASE WHEN sensitivity='high' OR $4='high' THEN 'high'
        WHEN sensitivity='medium' OR $4='medium' THEN 'medium' ELSE 'low' END,
      source_timestamp=GREATEST(source_timestamp,$5::timestamptz),updated_at=now() WHERE id=$1 AND vault_id=$2`,
      [previous.id,input.vaultId,sources,input.sensitivity,input.sourceTimestamp ?? null]);
    return { action: 'updated', memoryId: previous.id };
  }

  const sameMetadata = related && related.valid_from === input.validFrom && related.valid_until === input.validUntil
    && related.type === input.type && related.polarity === input.polarity;
  const receipt=validatedConflictAssessment(options.precomputedConflictDecision);
  const usableDecision = receipt && match.arbitrationRelated && related && isMemoryTemporallyRecallable(related,now)
    && sameMetadata && options.precomputedConflictInput === fingerprintDedupInput(input)
    && options.precomputedConflictMemoryId === related.id
    && options.precomputedConflictMemoryRevision === related.row_version
    && await conflictStillCurrent(receipt.evidence,db,input.vaultId,related.id,dedupConflictSide(input));
  const decision = usableDecision ? receipt!.decision : 'incomplete';
  // Similarity alone never proves equivalence or chooses which historical fact wins.
  if (related && (decision === 'merge' || decision === 'discard_new')) {
    const sources = [...new Set([...related.source_chunks,...input.sourceChunks])].sort();
    const changed = await db.query(`UPDATE memories SET source_chunks=$3::uuid[],
      sensitivity=CASE WHEN sensitivity='high' OR $4='high' THEN 'high'
        WHEN sensitivity='medium' OR $4='medium' THEN 'medium' ELSE 'low' END,
      source_timestamp=GREATEST(source_timestamp,$5::timestamptz),updated_at=now() WHERE id=$1 AND vault_id=$2
      AND (NOT (source_chunks @> $3::uuid[] AND source_chunks <@ $3::uuid[])
        OR (CASE sensitivity WHEN 'high' THEN 2 WHEN 'medium' THEN 1 ELSE 0 END)
          < (CASE $4 WHEN 'high' THEN 2 WHEN 'medium' THEN 1 ELSE 0 END)
        OR source_timestamp IS DISTINCT FROM GREATEST(source_timestamp,$5::timestamptz))`,
      [related.id,input.vaultId,sources,input.sensitivity,input.sourceTimestamp ?? null]);
    // No synthetic B memory is needed for an absorbed input. The committed
    // outcome identifies that exact input, not a recoverable model capability.
    await db.query(`INSERT INTO contradiction_scan_log
      (vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,dedup_input_key)
      SELECT $1,id,NULL,$3,$4,revision,$5 FROM memories WHERE vault_id=$1 AND id=$2`,
      [input.vaultId,related.id,decision,related.similarity,fingerprintDedupInput(input)]);
    return { action: changed.rowCount ? 'updated' : 'skipped', memoryId: related.id };
  }
  if (related && decision === 'supersede_old') {
    await db.query("UPDATE memories SET status='superseded',archived_at=now(),updated_at=now() WHERE id=$1 AND vault_id=$2", [related.id,input.vaultId]);
  }
  if (related && decision === 'supersede_old') effects.push(memoryCountEffect(input.vaultId,match.vault.account_id,-1,'extraction_worker'));
  const reservation = await reserveMemoryCreationInTransaction(db,input.vaultId);
  const inserted = await insertMemory(db,match.vault,input,match.hash,match.canonicalSubject,preparedCrypto);
  const id = inserted.rows[0].id;
  await syncEmbeddingRecord(db,id,input.embedding);
  recordSuccessfulInsert(effects,reservation,input.vaultId,match.vault.account_id);
  // A provisional keep_both is storage acceptance, not completed arbitration.
  // Certifying these unchanged revisions would hide the pair when valid_from
  // arrives. Preserve deterministic incompatible-metadata and historical logs;
  // exact retries have already returned above without reaching this branch.
  // A future preflight returns no request. Preserve that intent even when a
  // provider/lock wait crosses midnight; apply-time eligibility above still
  // independently fences every prepared mutation against the current clock.
  const preflightTime = options.preflight?.inputFingerprint === fingerprintDedupInput(input)
    ? new Date(options.preflight.referenceTime) : null;
  const window = {valid_from:input.validFrom,valid_until:input.validUntil};
  const deferFutureArbitration = !usableDecision && sameMetadata
    && (BEHAVIORAL_MEMORY_TYPES as readonly unknown[]).includes(input.type)
    && (memoryTemporalState(window,now) === 'future'
      || (preflightTime !== null && Number.isFinite(preflightTime.getTime())
        && memoryTemporalState(window,preflightTime) === 'future'));
  if (related && !deferFutureArbitration) {
    await db.query(`INSERT INTO contradiction_scan_log
      (vault_id,memory_id_a,memory_id_b,decision,similarity,revision_a,revision_b,dedup_input_key)
      SELECT $1,a.id,b.id,$4,$5,a.revision,b.revision,$6 FROM memories a,memories b
      WHERE a.id=$2 AND b.id=$3 AND a.vault_id=$1 AND b.vault_id=$1`,
      [input.vaultId,related.id,id,decision === 'supersede_old' ? 'supersede_old' : decision === 'keep_both' ? 'keep_both' : 'incomplete',related.similarity,usableDecision ? fingerprintDedupInput(input) : null]);
  }
  return { action: 'inserted', memoryId: id };
}

export async function getDedupEscalationRequest(
  input: DedupInput, id: string, db: Queryable = { query }, preparedCrypto?: PreparedVaultCrypto, now: Date = new Date()
): Promise<DedupEscalationRequest | null> {
  validateInput(input);
  if(input.sourceChunks.length>CONFLICT_BOUNDS.manifest)return null;
  // Model eligibility is separate from storage matching: retain historical/future
  // inputs and their exact dedup semantics without sending them to arbitration.
  if (!isMemoryTemporallyRecallable({type:input.type,valid_from:input.validFrom,valid_until:input.validUntil},now)) return null;
  if(await committedDedupOutcome(input,db))return null;
  const prepared = preparedCrypto ?? await prepareDedupCrypto(input.vaultId,db);
  const match = await findMatch(input,db,prepared,{now});
  const related = match.arbitrationRelated;
  if (match.exact || !related || !isMemoryTemporallyRecallable(related,now)) return null;
  const evidence=await prepareConflictEvidence(input.vaultId,related.id,dedupConflictSide(input),db);
  if(!evidence)return null;
  return { id, inputFingerprint: fingerprintDedupInput(input),
    existingFact: prepared.decrypt(match.vault,related.data), newFact: input.fact,
    existingMemoryId: related.id, existingMemoryRevision: related.row_version,
    context:{evidence,existing:{sourceTimestamp:related.source_timestamp,validFrom:related.valid_from,validUntil:related.valid_until,createdAt:null},
      incoming:{sourceTimestamp:input.sourceTimestamp ?? null,validFrom:input.validFrom,validUntil:input.validUntil,createdAt:null}},
    reasons: ['possible_conflict'] };
}

/** A committed outcome consumes this exact incoming input even if its survivor
 * is later edited/retired/deleted. This is replay suppression, never authority to
 * perform a new mutation, and contains no provider response to recover. */
async function committedDedupOutcome(input: DedupInput, db: Queryable): Promise<string | undefined> {
  return (await db.query<{ memory_id: string }>(`SELECT coalesce(memory_id_b,memory_id_a) AS memory_id
    FROM contradiction_scan_log WHERE vault_id=$1 AND dedup_input_key=$2 AND send_attempt IS NULL
      AND decision IN ('merge','discard_new','supersede_old','keep_both')`,
    [input.vaultId,fingerprintDedupInput(input)])).rows[0]?.memory_id;
}

async function findMatch(
  input: DedupInput, db: Queryable, prepared: PreparedVaultCrypto,
  arbitration?: { now: Date; targetId?: string }
): Promise<Match> {
  const vault = (await db.query<DedupVaultContext>(
    'SELECT id,account_id::text,encrypted_dek,vault_encryption_enabled FROM vaults WHERE id=$1', [input.vaultId])).rows[0];
  if (!vault) throw new Error('Memory vault does not exist');
  const canonicalSubject = await resolveCanonical(input.vaultId,normaliseSubject(input.subject),input.scope,input.scopeKey,db)
    ?? normaliseSubject(input.subject);
  const hash = crypto.createHash('sha256').update(input.fact).digest('hex');
  const selection = `SELECT m.id,m.data,m.confidence,m.score,m.salience,m.sensitivity,m.type,m.scope,m.scope_key,
       m.polarity,m.status,m.volatility,m.source_timestamp,
       CASE WHEN cardinality(m.source_chunks)<=16 THEN m.source_chunks END AS source_chunks,
       m.valid_from::text,m.valid_until::text,m.revision::text AS row_version,1-(m.embedding <=> $7::vector) AS similarity
     FROM memories m WHERE m.vault_id=$1 AND m.scope=$2 AND m.scope_key IS NOT DISTINCT FROM $3::text
       AND m.status='active' AND m.archived_at IS NULL AND m.sensitivity<>'restricted'
       AND octet_length(m.data)<=65536
       AND m.confidence>0 AND m.confidence<=1
       AND ${isVaultEncryptionActive(vault) ? 'm.subject_hmac' : 'm.subject'}=$5
       AND ${validityWindowsOverlapPredicateSql('m','$8','$9')}`;
  const values = [input.vaultId,input.scope,input.scopeKey,hash,prepared.subjectMatch(vault,canonicalSubject),0.8,
    JSON.stringify(input.embedding),input.validFrom,input.validUntil];
  const order = 'ORDER BY (m.hash=$4) DESC,similarity DESC NULLS LAST,m.id';
  // Preserve the original bounded hash/plaintext + metadata exact search and
  // storage fallback, including historical rows and hash-mismatched plaintext.
  const rows = await db.query<MatchedMemory>(`${selection}
       AND (m.hash=$4 OR 1-(m.embedding <=> $7::vector)>=$6)
     ${order} LIMIT 10`, values);
  const exact = rows.rows.find(row => row.polarity === input.polarity && row.type === input.type
    && row.valid_from === input.validFrom && row.valid_until === input.validUntil
    && prepared.decrypt(vault,row.data) === input.fact
    && sameSourceManifest(row.source_chunks,input.sourceChunks));
  let arbitrationRelated: MatchedMemory | undefined;
  if (!exact && arbitration) {
    // Filter in SQL BEFORE the candidate budget: arbitrarily many non-current
    // behavioral rows must not hide an eligible conflict below the storage cap.
    // Apply uses the prepared ID with these same binding/eligibility predicates;
    // it must never redirect a decision to another row based on similarity.
    const eligible = await db.query<MatchedMemory>(`${selection}
       AND 1-(m.embedding <=> $7::vector)>=$6
       AND ${memoryTemporalRecallPredicateSql('m','$10')}
       ${arbitration.targetId ? 'AND m.id=$11::uuid' : ''}
     ${order} LIMIT 1`, [...values,arbitration.now.toISOString(),...(arbitration.targetId ? [arbitration.targetId] : [])]);
    arbitrationRelated = eligible.rows[0];
  }
  return { vault,hash,canonicalSubject,exact,related: exact ? undefined : rows.rows.find(row => row.similarity >= 0.8),arbitrationRelated };
}

async function prepareDedupCrypto(vaultId: string, db: Queryable): Promise<PreparedVaultCrypto> {
  const result = await db.query<DedupVaultContext>(
    'SELECT id, encrypted_dek, vault_encryption_enabled FROM vaults WHERE id=$1', [vaultId]
  );
  if (!result.rows[0]) throw new Error('Dedup vault not found');
  return prepareVaultCrypto(result.rows[0]);
}

async function insertMemory(
  db: Queryable,
  vault: DedupVaultContext,
  input: DedupInput,
  hash: string,
  canonicalSubject: string,
  preparedCrypto: PreparedVaultCrypto
) {
  const storedFact = preparedCrypto.encrypt(vault, input.fact);
  const encryptedSubject = preparedCrypto.subject(vault, canonicalSubject);
  const result = await db.query<{ id: string }>(
     `INSERT INTO memories (
       vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding,
       source_chunks, score, salience, sensitivity, type, scope, scope_key, polarity, status, volatility, evidence, valid_from, valid_until, source_segment_id, source_timestamp
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::uuid[], $9, $10, $11, $12, $13, $14, $15, $16, $17::memory_volatility, $18::jsonb, $19::date, $20::date, $21, $22::timestamptz)
     RETURNING id`,
    [
      input.vaultId,
      storedFact,
      isVaultEncryptionActive(vault) ? '' : canonicalSubject,
      encryptedSubject?.encrypted ?? null,
      encryptedSubject?.hmac ?? null,
      hash,
      JSON.stringify(input.embedding),
      input.sourceChunks,
      input.score,
      input.salience,
      input.sensitivity,
      input.type,
      input.scope,
      input.scopeKey,
      input.polarity,
      input.status,
      input.volatility,
      serializeEvidence(input),
      input.validFrom,
      input.validUntil,
      input.sourceSegmentId ?? null,
      input.sourceTimestamp ?? null
    ]
  );
  return result;
}

function recordSuccessfulInsert(
  effects: WorkerEffect[],
  reservation: ApiQuotaReservation,
  vaultId: string,
  accountId: string | null
): void {
  effects.push(
    { kind: 'quota', reservation },
    memoryCountEffect(vaultId,accountId,1,'extraction_worker')
  );
}

function serializeEvidence(
  input: Pick<DedupInput, 'evidence' | 'policyRejections'>,
  existingEvidence?: unknown
): string | null {
  if (!input.evidence && (input.policyRejections?.length ?? 0) === 0) {
    return null;
  }
  let supplied: unknown;
  try { supplied = input.evidence ? JSON.parse(input.evidence) : null; } catch { supplied=null; }
  if (supplied && typeof supplied==='object' && !Array.isArray(supplied)) {
    return mergeMemoryEvidence({...((existingEvidence && typeof existingEvidence==='object') ? existingEvidence : {}),...supplied},undefined,input.policyRejections);
  }
  return mergeMemoryEvidence(existingEvidence,input.evidence ?? undefined,input.policyRejections);
}

async function syncEmbeddingRecord(db: Queryable, memoryId: string, embedding: number[]) {
  await db.query(
    `INSERT INTO memory_embeddings (memory_id, embedding, embedded_at)
     VALUES ($1, $2::vector, now())
     ON CONFLICT (memory_id)
     DO UPDATE SET embedding = EXCLUDED.embedding, embedded_at = now()`,
    [memoryId, JSON.stringify(embedding)]
  );
}
