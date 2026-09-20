import { CONFLICT_BOUNDS } from './conflict-evidence';

/** Bound the response before the SDK JSON parser can allocate from it. */
export const boundedConflictFetch:typeof fetch=async(input,init)=>{
  const response=await fetch(input,{...init,redirect:'manual'});
  if(response.status>=300 && response.status<400){
    await response.body?.cancel().catch(()=>{});
    throw new Error('Arbitration redirect rejected');
  }
  const reader=response.body?.getReader();
  if(!reader)throw new Error('Missing arbitration response body');
  const chunks:Uint8Array[]=[];let size=0;
  try{
    for(;;){
      const {done,value}=await reader.read();if(done)break;
      size+=value.byteLength;
      if(size>CONFLICT_BOUNDS.wireResponseBytes)throw new Error('Arbitration response exceeds bound');
      chunks.push(value);
    }
  }catch(error){await reader.cancel().catch(()=>{});throw error;}
  finally{reader.releaseLock();}
  const bytes=new Uint8Array(size);let offset=0;
  for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
  // Reject corrupt encoding, rather than replacing bytes with invented text.
  new TextDecoder('utf-8',{fatal:true}).decode(bytes);
  return new Response(bytes,{status:response.status,statusText:response.statusText,headers:response.headers});
};
