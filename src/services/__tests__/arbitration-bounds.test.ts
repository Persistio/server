import http from 'node:http';
import {Readable} from 'node:stream';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {readBoundedRawStream} from '../raw-chunk-storage';
import {boundedConflictFetch} from '../conflict-transport';
import {CONFLICT_BOUNDS as B} from '../conflict-evidence';
afterEach(()=>vi.unstubAllGlobals());
describe('arbitration byte admission before unbounded parsing',()=>{
  it.each([B.objectBytes,B.objectBytes+1])('reads a whole object or rejects, bytes=%i',async size=>{
    const stream=Readable.from([Buffer.alloc(size,97)]);
    if(size>B.objectBytes)await expect(readBoundedRawStream(stream,B.objectBytes)).rejects.toThrow('bound');
    else expect(Buffer.byteLength(await readBoundedRawStream(stream,B.objectBytes))).toBe(size);
  });
  it('rejects corrupt UTF-8 rather than silently replacing bytes',async()=>{
    await expect(readBoundedRawStream(Readable.from([Buffer.from([0xff])]),B.objectBytes)).rejects.toThrow();
  });
  it('preserves a retained BOM instead of silently changing its byte snapshot',async()=>{
    const raw=Buffer.from('\uFEFFsource');expect(Buffer.byteLength(await readBoundedRawStream(Readable.from([raw]),raw.length))).toBe(raw.length);
  });
  it('handles a multi-byte codepoint split across stream chunks',async()=>{
    const raw=Buffer.from('😀');
    expect(await readBoundedRawStream(Readable.from([raw.subarray(0,2),raw.subarray(2)]),raw.length)).toBe('😀');
    await expect(readBoundedRawStream(Readable.from([raw]),raw.length-1)).rejects.toThrow('bound');
  });
  it.each([B.wireResponseBytes,B.wireResponseBytes+1])('bounds actual HTTP response before SDK JSON allocation: %i',async size=>{
    vi.stubGlobal('fetch',vi.fn(async()=>new Response('x'.repeat(size),{status:200})));
    if(size>B.wireResponseBytes)await expect(boundedConflictFetch('http://synthetic.invalid')).rejects.toThrow('bound');
    else expect((await (await boundedConflictFetch('http://synthetic.invalid')).text()).length).toBe(size);
  });
  it('rejects corrupt response UTF-8',async()=>{
    vi.stubGlobal('fetch',vi.fn(async()=>new Response(new Uint8Array([0xff]))));
    await expect(boundedConflictFetch('http://synthetic.invalid')).rejects.toThrow();
  });
  it('does not accumulate beyond the first overflowing chunk',async()=>{
    let reads=0,cancelled=false;
    const stream=new ReadableStream({pull(controller){reads++;controller.enqueue(new Uint8Array(B.wireResponseBytes+1));},cancel(){cancelled=true;}},{highWaterMark:0});
    vi.stubGlobal('fetch',vi.fn(async()=>new Response(stream)));
    await expect(boundedConflictFetch('http://synthetic.invalid')).rejects.toThrow('bound');
    expect(reads).toBe(1);expect(cancelled).toBe(true);
  });
  it.each([307,308])('does not follow %i redirects or forward evidence/custom credentials to another origin',async status=>{
    let received=0;const target=http.createServer((req,res)=>{received++;res.end('{}');});
    await new Promise<void>(resolve=>target.listen(0,'127.0.0.1',resolve));
    const origin=http.createServer((req,res)=>{req.resume();res.writeHead(status,{location:`http://127.0.0.1:${(target.address() as any).port}/collect`});res.end();});
    await new Promise<void>(resolve=>origin.listen(0,'127.0.0.1',resolve));
    try{
      await expect(boundedConflictFetch(`http://127.0.0.1:${(origin.address() as any).port}/messages`,{
        method:'POST',headers:{'x-api-key':'synthetic-redirect-sentinel'},body:'retained-evidence-sentinel'
      })).rejects.toThrow('redirect');
      expect(received).toBe(0);
    }finally{await new Promise<void>(resolve=>origin.close(()=>resolve()));await new Promise<void>(resolve=>target.close(()=>resolve()));}
  });

});
