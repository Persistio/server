import {describe,expect,it,vi} from 'vitest';
const mock=vi.hoisted(()=>({upload:vi.fn(),create:vi.fn(async()=>{}),stored:null as string|null}));
vi.mock('../config',()=>({getConfig:()=>({RAW_CHUNK_STORAGE_PROVIDER:'azure_blob',AZURE_STORAGE_CONNECTION_STRING:'synthetic-only',RAW_CHUNK_BLOB_CONTAINER:'test'})}));
vi.mock('@azure/storage-blob',()=>({BlobServiceClient:class{
  static fromConnectionString(){return new this();}
  getContainerClient(){return{createIfNotExists:mock.create,getBlockBlobClient:()=>({upload:mock.upload})};}
}}));
import {getRawChunkStorage} from './raw-chunk-storage';
describe('Azure retained-object write-once boundary',()=>{
  it('passes an immutable-create precondition to the actual SDK upload shape',async()=>{
    mock.upload.mockImplementation(async(content:string,_bytes:number,options:any)=>{
      expect(options.conditions).toEqual({ifNoneMatch:'*'});
      if(mock.stored!==null)throw new Error('Precondition failed');mock.stored=content;
    });
    const storage=getRawChunkStorage();await storage.put('vaults/test/sessions/s/chunks/c.txt','original');
    await expect(storage.put('vaults/test/sessions/s/chunks/c.txt','replacement')).rejects.toThrow('Precondition');
    expect(mock.stored).toBe('original');expect(mock.upload).toHaveBeenCalledTimes(2);
  });
});
