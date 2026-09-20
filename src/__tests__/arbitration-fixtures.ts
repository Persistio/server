// Shared fixtures for existing temporal/scheduling SQL tests. They exercise real
// source loading and the closed parser/receipt registry; only provider transport
// is deterministic. Actual SDK, reservation and restart tests live in
// arbitration-evidence.integration.test.ts.
import crypto from 'node:crypto';
import {vi} from 'vitest';
import type {Pool} from 'pg';
import type {ConflictArbitrationContext,ConflictResolution} from '../services/extractor';

import type {VaultEncryptionContext} from '../services/crypto';
const keys:string[]=[];
export async function fixtureSource(pool:Pool,vaultId:string,text='Existing observation. Explicit correction: incoming observation.',id=crypto.randomUUID()){
  const {prepareVaultCrypto}=await import('../services/crypto');
  const {getRawChunkStorage}=await import('../services/raw-chunk-storage');
  const vault=(await pool.query<VaultEncryptionContext>('SELECT id,encrypted_dek,vault_encryption_enabled FROM vaults WHERE id=$1',[vaultId])).rows[0];
  const prepared=await prepareVaultCrypto(vault),storage=getRawChunkStorage();
  const raw=prepared.encrypt(vault,text),key=`vaults/${vaultId}/sessions/s1/chunks/${id}.txt`;
  await storage.put(key,raw);keys.push(key);
  await pool.query(`INSERT INTO raw_chunks(id,vault_id,session_id,role,blob_store,blob_key,storage_bytes,provenance)
    VALUES($1,$2,'s1','user',$3,$4,$5,'{}')`,[id,vaultId,storage.store,key,Buffer.byteLength(raw)]);
  return id;
}
export async function cleanFixtureSources(){const {getRawChunkStorage}=await import('../services/raw-chunk-storage');const storage=getRawChunkStorage();for(const key of keys)await storage.delete(key);keys.length=0;}
export function fixtureExtractor(decide:(...args:any[])=>Promise<ConflictResolution>|ConflictResolution=()=> 'keep_both',method?:import('../services/extractor').ExtractorService['arbitrateConflict']){
  return {arbitrateConflict:vi.fn(async(a:string,b:string,vaultId?:string,context?:ConflictArbitrationContext)=>{
    const {ExtractorService}=await import('../services/extractor');
    const service=new ExtractorService();
    vi.spyOn(service as any,'createChatCompletion').mockImplementation(async(...parameters:unknown[])=>{
      const decision=await decide(a,b,vaultId,context);
      const data=JSON.parse((parameters[0] as any).messages[1].content);
      return{choices:[{finish_reason:'stop',message:{content:JSON.stringify({pairId:data.pairId,decision,
        citationsA:[{sourceId:data.A.sources[0].id,quote:data.A.sources[0].text.slice(0,64).trim()}],
        citationsB:[{sourceId:data.B.sources[0].id,quote:data.B.sources[0].text.slice(0,64).trim()}]})}}]};
    });
    return (method??service.arbitrateConflict).call(service,a,b,vaultId,context);
  })};
}
export async function fixtureAssessment(request:{existingFact:string;newFact:string;context:ConflictArbitrationContext},vaultId:string,decision:ConflictResolution,method?:import('../services/extractor').ExtractorService['arbitrateConflict']){
  return fixtureExtractor(()=>decision,method).arbitrateConflict(request.existingFact,request.newFact,vaultId,request.context);
}
