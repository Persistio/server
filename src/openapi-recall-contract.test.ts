import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { parse } from 'yaml';
import { it, expect } from 'vitest';
import { recallSchema } from './routes/recall';
import { renderedDeliverySchema } from './services/memory-observability';

const require = createRequire(import.meta.url);
const ajv = new (require('ajv/dist/2020'))({ strict: true });
require('ajv-formats')(ajv);
const doc = parse(readFileSync(new URL('../../../openapi.yaml', import.meta.url), 'utf8'));
for (const [name, schema] of Object.entries(doc.components.schemas)) ajv.addSchema(schema, `#/components/schemas/${name}`);
const recall = ajv.compile(doc.paths['/v1/recall'].post.requestBody.content['application/json'].schema);
const ack = ajv.getSchema('#/components/schemas/RenderedDeliveryRequest');

it('matches recall/client request boundaries and documents the authenticated handshake', () => {
  const cases: unknown[] = [{query:'q'}, {}, {query:''}, ...[-1,0,1,100,101,1.5].map(top_k=>({query:'q',top_k})),
    ...['', ' ', 'x\n', '\u0085', '\ud800', '😀'.repeat(100), '😀'.repeat(101), ' valid '].map(name=>({query:'q',client:{name,version:'1'}})),
    {query:'q',client:{name:'a',version:'1',extra:true}}, {query:'q',include_related:'true'}, {query:'q',mode:'invalid'}];
  for(const value of cases) expect(recallSchema.safeParse(value).success,JSON.stringify(value)).toBe(recall(value));
  const route=doc.paths['/v1/recall/{deliveryId}/rendered'].post;
  expect(route.security.length).toBeGreaterThan(0);
  for(const status of ['200','400','401','403','404','500']) expect(route.responses[status]).toBeDefined();
});

it('matches expressible ACK syntax, retaining runtime-only partition/token relations', () => {
  const base={rendered_ids:[],dropped:[],token_budget:100,rendered_tokens:10,truncated:false,render_target:'tool_response'};
  for(const value of [base,{}, {...base,extra:1}, {...base,render_target:'prompt'}, {...base,truncated:'false'},
    ...[-1,0,0.5,2147483647,2147483648].map(token_budget=>({...base,token_budget,rendered_tokens:0})),
    {...base,rendered_ids:['not-uuid']}, {...base,dropped:[{id:'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',reason:'invalid',extra:true}]}]) {
    expect(renderedDeliverySchema.safeParse(value).success,JSON.stringify(value)).toBe(ack(value));
  }
  expect(ack({...base,rendered_tokens:101})).toBe(true);
  expect(renderedDeliverySchema.safeParse({...base,rendered_tokens:101}).success).toBe(false);
});
