import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {parse} from 'yaml';
import {it,expect} from 'vitest';
import {recallSchema} from './routes/recall';
import {formatRecallBundle} from './services/recall-bundle';
const require=createRequire(import.meta.url);
const ajv=new (require('ajv/dist/2020'))({strict:true});require('ajv-formats')(ajv);
const doc=parse(readFileSync(new URL('../../../openapi.yaml',import.meta.url),'utf8'));
for(const [name,schema] of Object.entries(doc.components.schemas))ajv.addSchema(schema,'#/components/schemas/'+name);
const recall=ajv.compile(doc.paths['/v1/recall'].post.requestBody.content['application/json'].schema);
it('matches bounded recall request syntax and removes client-delivery handshake',()=>{
  const cases:unknown[]=[{query:'q'},{},{query:''},{query:'   '},{query:'x'.repeat(32769)},
    ...[-1,0,1,100,101,1.5].map(top_k=>({query:'q',top_k})),
    ...[-1,0,1,1200,65536,65537,1.5].map(max_bundle_bytes=>({query:'q',max_bundle_bytes})),
    ...['',' ','x\n','\u0085','\ud800','😀'.repeat(100),'😀'.repeat(101),' valid '].map(name=>({query:'q',client:{name,version:'1'}})),
    {query:'q',client:{name:'a',version:'1',extra:true}},{query:'q',include_related:'true'},{query:'q',mode:'invalid'},{query:'q',include_pending:true}];
  for(const value of cases)expect(recallSchema.safeParse(value).success,JSON.stringify(value)).toBe(recall(value));
  expect(Object.keys(doc.paths).some(path=>path.includes('/rendered')||path.includes('/authority/'))).toBe(false);
  expect(Object.keys(doc.components.schemas).some(name=>/Delivery|Authority|Legacy|Structured/.test(name))).toBe(false);
  expect(doc.paths['/v1/recall'].post.parameters[0].schema.enum).toEqual(['bundle_v3']);
});
it('describes exactly the ready-to-use server bundle and JSON response without a ledger',()=>{
  const validate=ajv.getSchema('#/components/schemas/RecallBundleResponse');
  expect(validate(formatRecallBundle([],0))).toBe(true);
  expect(validate({schema_version:'persistio.recall_bundle.v3',bundle:'',delivery:{}})).toBe(false);
  const json=ajv.getSchema('#/components/schemas/RecallJsonResponse');
  expect(json({memories:[],related_memories:[],raw_chunks:[],evidence_chunks:[]})).toBe(true);
});
