import crypto from 'node:crypto';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';
import { query, withTransaction } from '../db/client';
import { prepareVaultCrypto, isVaultEncryptionActive, type PreparedVaultCrypto, type VaultEncryptionContext } from './crypto';
import { getRawChunkStorage } from './raw-chunk-storage';

// Reuse the extraction context's 32 KiB evidence ceiling, but reject overflow
// rather than selecting a subset. An encrypted object needs base64/GCM headroom.
export const CONFLICT_BOUNDS = Object.freeze({manifest:16, objectBytes:65536, decodedBytes:32768,
  requestBytes:65536, responseBytes:8192, wireResponseBytes:16384, pairs:16, completionTokens:1024});
export interface ConflictSide {
  id?:string; binding?:string; text:string; subject:string; scope:string; scopeKey:string|null;
  type:string|null; polarity:string; validFrom:string|null; validUntil:string|null;
  sourceTimestamp:string|null; sourceChunks:string[];
}
interface DB {query<T extends QueryResultRow=QueryResultRow>(sql:string,values?:unknown[]):Promise<QueryResult<T>>}
interface Source extends QueryResultRow {id:string;blob_key:string|null;blob_store:string|null;storage_bytes:string;metadata:string}
interface StoredSide {side:ConflictSide; revision:string; encryptedSubject?:boolean}
interface Snapshot {
  vaultId:string; crypt:PreparedVaultCrypto; a:StoredSide; b:StoredSide; sources:Source[];
  data:{A:ConflictSide & {sources:Array<{id:string;metadata:unknown;text:string}>};B:ConflictSide & {sources:Array<{id:string;metadata:unknown;text:string}>}};
  pairId:string; jobKey:string; objects:Array<{key:string;digest:string}>;store:string;
}
export interface PreparedConflict {readonly pairId:string}
const prepared=new WeakMap<object,Snapshot>();
export const conflictDigest=(value:unknown)=>crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const manifest=(ids:string[])=>[...new Set(ids)].sort();
export function sameSourceManifest(a:string[],b:string[]):boolean {
  return Array.isArray(a) && Array.isArray(b) && a.length>0 && b.length>0 && a.length<=CONFLICT_BOUNDS.manifest && b.length<=CONFLICT_BOUNDS.manifest
    && JSON.stringify(manifest(a))===JSON.stringify(manifest(b));
}
async function readSide(vaultId:string,id:string,db:DB,lock=false):Promise<StoredSide|null>{
  // Do not transfer an unbounded source array or data field to the process.
  const row=(await db.query(`SELECT id,revision::text,scope,scope_key,type,polarity,
    valid_from::text,valid_until::text,source_timestamp::text,
    CASE WHEN octet_length(data)<=65536 THEN data END AS data,
    CASE WHEN octet_length(subject)<=4096 THEN subject END AS subject,
    CASE WHEN octet_length(subject_encrypted)<=4096 THEN subject_encrypted END AS subject_encrypted,cardinality(source_chunks) AS source_count,
    CASE WHEN cardinality(source_chunks)<=$3 THEN source_chunks END AS source_chunks
    FROM memories WHERE vault_id=$1 AND id=$2 AND (scope_key IS NULL OR octet_length(scope_key)<=2048)
    AND (subject_encrypted IS NULL OR octet_length(subject_encrypted)<=4096) AND status='active' AND archived_at IS NULL
    ${lock?'FOR UPDATE':''}`,[vaultId,id,CONFLICT_BOUNDS.manifest])).rows[0];
  if(!row || !row.data || row.subject===null || !row.source_chunks || row.source_count>CONFLICT_BOUNDS.manifest)return null;
  return {revision:row.revision,encryptedSubject:row.subject_encrypted!==null,side:{id:row.id,text:row.data,subject:row.subject_encrypted??row.subject,scope:row.scope,scopeKey:row.scope_key,
    type:row.type,polarity:row.polarity,validFrom:row.valid_from,validUntil:row.valid_until,sourceTimestamp:row.source_timestamp,
    sourceChunks:manifest(row.source_chunks)}};
}
const sourceSql=`SELECT id,CASE WHEN octet_length(blob_key)<=8192 THEN blob_key END AS blob_key,blob_store,storage_bytes::text,
  CASE WHEN octet_length(session_id)<=2048 AND octet_length(role)<=64
    AND octet_length(coalesce(provenance::text,'null'))+octet_length(capture_context::text)<=32768 THEN
    jsonb_build_object('role',role,'session_id',session_id,'created_at',to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'provenance',provenance,
      'capture_context',capture_context,'source_event_payload_sha256',source_event_payload_sha256)::text END AS metadata
  FROM raw_chunks WHERE vault_id=$1 AND id=ANY($2::uuid[]) ORDER BY id`;

/** No role filtering, LIMIT, prefix, or convenient subset can certify completeness. */
export async function prepareConflictEvidence(vaultId:string,aId:string,b:string|ConflictSide,db:DB={query}):Promise<PreparedConflict|null>{
  try {
    if(typeof b!=='string' && (!Array.isArray(b.sourceChunks) || b.sourceChunks.length>CONFLICT_BOUNDS.manifest))return null;
    const a=await readSide(vaultId,aId,db);
    const incoming:StoredSide|null=typeof b==='string'?await readSide(vaultId,b,db):{side:structuredClone(b),revision:''};
    if(!a || !incoming)return null;
    const B=incoming.side;
    if(a.side.scope!==B.scope || a.side.scopeKey!==B.scopeKey)return null;
    for(const side of [a.side,B]){
      if(!Array.isArray(side.sourceChunks) || !side.sourceChunks.length || side.sourceChunks.length>CONFLICT_BOUNDS.manifest)return null;
      side.sourceChunks=manifest(side.sourceChunks);
      if(Buffer.byteLength(side.text)>65536 || Buffer.byteLength(side.subject)>4096)return null;
    }
    const ids=manifest([...a.side.sourceChunks,...B.sourceChunks]);
    const sources=(await db.query<Source>(sourceSql,[vaultId,ids])).rows;
    if(sources.length!==ids.length || sources.some(s=>!s.metadata || !s.blob_key || !Number.isSafeInteger(Number(s.storage_bytes))
      || Number(s.storage_bytes)<1 || Number(s.storage_bytes)>CONFLICT_BOUNDS.objectBytes))return null;
    const vault=(await db.query<VaultEncryptionContext>('SELECT id,encrypted_dek,vault_encryption_enabled FROM vaults WHERE id=$1',[vaultId])).rows[0];
    if(!vault)return null;
    // With inactive crypto, any retained key/flag/encrypted-column marker makes
    // the representation ambiguous, including legacy plaintext-subject rows
    // whose facts and blobs are encrypted. Never pass those bytes as plaintext.
    if(!isVaultEncryptionActive(vault)
      && (vault.encrypted_dek!==null || vault.vault_encryption_enabled || a.encryptedSubject || incoming.encryptedSubject))return null;
    const crypt=await prepareVaultCrypto(vault),storage=getRawChunkStorage();
    if(!storage.getBounded || sources.some(s=>s.blob_store!==storage.store))return null;
    // Accepted keys are immutable and vault-prefixed. Foreign references never read a blob.
    if(sources.some(s=>!s.blob_key!.startsWith(`vaults/${encodeURIComponent(vaultId)}/sessions/`)
      || s.blob_key!.split('/').some(part=>part==='.' || part==='..')
      || s.blob_key!.split('/').length!==6))return null;
    const texts=new Map<string,string>(),objects:Array<{key:string;digest:string}>=[];let bytes=0;
    for(const source of sources){
      const raw=await storage.getBounded(source.blob_key!,CONFLICT_BOUNDS.objectBytes);
      if(Buffer.byteLength(raw)!==Number(source.storage_bytes))return null;
      const text=crypt.decrypt(vault,raw);bytes+=Buffer.byteLength(text);
      if(!text.trim() || bytes>CONFLICT_BOUNDS.decodedBytes)return null;
      texts.set(source.id,text);objects.push({key:source.blob_key!,digest:crypto.createHash('sha256').update(raw).digest('hex')});
    }
    const decode=(side:ConflictSide,stored:boolean,encryptedSubject=false)=>({...side,
      text:stored?crypt.decrypt(vault,side.text):side.text,
      // Plain subjects are plaintext even in legacy encrypted vaults.
      subject:encryptedSubject?crypt.decrypt(vault,side.subject):side.subject,
      sources:side.sourceChunks.map(id=>({id,metadata:JSON.parse(sources.find(s=>s.id===id)!.metadata),text:texts.get(id)!}))});
    const data={A:decode(a.side,true,a.encryptedSubject),B:decode(B,typeof b==='string',incoming.encryptedSubject)};
    // The immutable evidence/content identity ignores aliases, embeddings,
    // counters, revision numbers and transport metadata. Stored/incoming and
    // reversed pairs share one allowance; exact IDs/revisions still bind apply.
    const semantic=(side:typeof data.A)=>conflictDigest({text:side.text,scope:side.scope,scopeKey:side.scopeKey,type:side.type,
      polarity:side.polarity,validFrom:side.validFrom,validUntil:side.validUntil,sources:side.sources});
    const jobKey=conflictDigest([vaultId,...[semantic(data.A),semantic(data.B)].sort()]);
    const pairId=conflictDigest({vaultId,a,b:incoming,data});
    const result=Object.freeze({pairId});prepared.set(result,{vaultId,crypt,a,b:incoming,sources,data,pairId,jobKey,objects,store:storage.store});return result;
  } catch {return null;}
}
export function conflictRequestData(value:PreparedConflict|undefined,vaultId:string|undefined,a:string,b:string){
  const s=value && prepared.get(value);
  if(!s || s.vaultId!==vaultId || s.data.A.text!==a || s.data.B.text!==b)return null;
  return {pairId:s.pairId,A:structuredClone(s.data.A),B:structuredClone(s.data.B)};
}
/** Recheck every bound object after provider work, before issuing authority.
 * Production PUTs are create-only; accepted-row deletion is separately fenced
 * by existing ownership/deletion protocols and apply's source-row locks. */
export async function verifyConflictObjects(value:PreparedConflict):Promise<boolean>{
  const s=prepared.get(value);if(!s)return false;
  try{
    const storage=getRawChunkStorage();if(storage.store!==s.store || !storage.getBounded)return false;
    for(const object of s.objects){
      const raw=await storage.getBounded(object.key,CONFLICT_BOUNDS.objectBytes);
      if(crypto.createHash('sha256').update(raw).digest('hex')!==object.digest)return false;
    }
    return true;
  }catch{return false;}
}
/** Called after local circuit, size, and budget admission, immediately before send. */
export async function reserveConflictSend(value:PreparedConflict,admit?:()=>()=>void):Promise<boolean>{
  const s=prepared.get(value);if(!s)return false;
  let refund:(()=>void)|undefined;
  try{return await withTransaction(async db=>{
    await db.query('SELECT id FROM vaults WHERE id=$1 FOR NO KEY UPDATE',[s.vaultId]);
    const n=Number((await db.query(`SELECT coalesce(max(send_attempt),0) AS n FROM contradiction_scan_log
      WHERE vault_id=$1 AND arbitration_job_key=$2`,[s.vaultId,s.jobKey])).rows[0].n);
    if(n>=3)return false;
    // The prepared admission is synchronous local bucket work, never a second
    // pool query or provider call while this short write lock is held.
    refund=admit?.();
    // An attempt is incomplete from reservation onward. A crash, timeout, or
    // ambiguous response never certifies keep_both and never refunds this slot.
    await db.query(`INSERT INTO contradiction_scan_log(vault_id,memory_id_a,memory_id_b,decision,similarity,
      revision_a,revision_b,arbitration_job_key,send_attempt)
      VALUES($1,$2,$3,'incomplete',0,$4,$5,$6,$7)`,[s.vaultId,s.a.side.id,s.b.side.id??null,s.a.revision,s.b.revision||null,s.jobKey,n+1]);
    return true;
  });}catch(error){refund?.();throw error;}
}
/** The caller already owns the established vault mutation lock. No blob/provider IO. */
export async function conflictStillCurrent(value:PreparedConflict,db:PoolClient,vaultId:string,aId:string,b:string|ConflictSide):Promise<boolean>{
  const s=prepared.get(value);if(!s || s.vaultId!==vaultId || s.a.side.id!==aId)return false;
  // Bind to the crypto identity that decoded this evidence, not a fresh context
  // supplied by an apply caller after a key/flag transition. This is SQL only.
  try{await s.crypt.assertCurrent(db);}catch{return false;}
  const a=await readSide(vaultId,aId,db,true);
  const incoming=typeof b==='string'?await readSide(vaultId,b,db,true):{side:{...b,sourceChunks:manifest(b.sourceChunks)},revision:''};
  if(JSON.stringify(a)!==JSON.stringify(s.a) || JSON.stringify(incoming)!==JSON.stringify(s.b))return false;
  const ids=s.sources.map(x=>x.id);
  const sources=(await db.query<Source>(sourceSql+' FOR KEY SHARE',[vaultId,ids])).rows;
  return JSON.stringify(sources)===JSON.stringify(s.sources);
}
