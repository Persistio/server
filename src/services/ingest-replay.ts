import crypto from 'node:crypto';
import type { PoolClient } from 'pg';
import { query } from '../db/client';
import type { VaultContext } from '../middleware/auth';
import type { RecallContext } from './memory-applicability';

export type ReplayPayload={role:string;content:string;timestamp:string;provenance?:unknown};
export type StoredIngestRow={id:string;created_at:string;source_event_key:string;source_event_payload_sha256:string;
  role:string;blob_store:string|null;blob_key:string|null};
const REPLAY_COLUMNS='id,created_at,source_event_key,source_event_payload_sha256,role,blob_store,blob_key';
function canonical(value:unknown):unknown{
  if(Array.isArray(value))return value.map(canonical);
  if(value && typeof value==='object')return Object.fromEntries(Object.entries(value).filter(([,v])=>v!==undefined).sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>[k,canonical(v)]));
  return value;
}
export function ingestPayloadHash(chunk:ReplayPayload,context:RecallContext):string{
  if(!context.session_id || !chunk.timestamp)throw new Error('Replay fingerprint lacks source metadata');
  return crypto.createHash('sha256').update(JSON.stringify(canonical({
    version:2,role:chunk.role,content:chunk.content,timestamp:chunk.timestamp,
    context,provenance:chunk.provenance ?? null
  }))).digest('hex');
}
export async function classifyIngestReplay(vault:VaultContext,chunks:ReplayPayload[],keys:string[],context:RecallContext):Promise<Map<string,StoredIngestRow>>{
  const rows=await query<StoredIngestRow>(`SELECT ${REPLAY_COLUMNS} FROM raw_chunks WHERE vault_id=$1 AND source_event_key=ANY($2::text[])`,[vault.id,keys]);
  const byKey=new Map(rows.rows.map(r=>[r.source_event_key,r]));
  chunks.forEach((chunk,index)=>{const r=byKey.get(keys[index]);if(r)assertIngestReplayMatches(r,chunk,index,context);});
  return byKey;
}
export async function loadLockedIngestRows(client:PoolClient,vaultId:string,keys:string[]):Promise<Map<string,StoredIngestRow>>{
  const rows=await client.query<StoredIngestRow>(`SELECT ${REPLAY_COLUMNS} FROM raw_chunks WHERE vault_id=$1 AND source_event_key=ANY($2::text[]) ORDER BY source_event_key FOR UPDATE`,[vaultId,keys]);
  return new Map(rows.rows.map(r=>[r.source_event_key,r]));
}
export function assertIngestReplayMatches(row:StoredIngestRow,chunk:ReplayPayload,index:number,context:RecallContext):void{
  if(row.source_event_payload_sha256!==ingestPayloadHash(chunk,context)){
    throw Object.assign(new Error(`Source event identity collision for input chunk ${index}`),{statusCode:409});
  }
}
