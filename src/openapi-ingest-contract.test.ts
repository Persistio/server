import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';
import { ingestSchema, bulkIngestSchema } from './routes/ingest';

const require = createRequire(import.meta.url);
const Ajv = require('ajv/dist/2020');
const addFormats = require('ajv-formats');
const document = parse(readFileSync(new URL('../../../openapi.yaml', import.meta.url), 'utf8'));
const ajv = new Ajv({ strict: true });
addFormats(ajv);
// Register component IDs separately: validate as JSON Schema 2020-12, not the
// legacy OpenAPI nullable extension accepted by Ajv's default compatibility mode.
for (const [name, schema] of Object.entries(document.components.schemas)) {
  ajv.addSchema(schema, `#/components/schemas/${name}`);
}
const validate = ajv.getSchema('#/components/schemas/IngestChunk');
const base = { role: 'user', content: 'payload', timestamp: '2026-06-01T00:00:00Z' };
const provenance = { actor_type: 'human', authorship: 'original', trigger_type: 'direct', artifact_type: 'message', cadence: 'one_off' };

describe('OpenAPI ingest contract', () => {
  it('uses one chunk contract for both normal and bulk ingest', () => {
    expect(document.openapi).toBe('3.1.0');
    expect(document.components.schemas.CaptureProvenance.properties.payload_author.properties.is_user)
      .toEqual({ type: ['boolean', 'null'] });
    for (const path of ['/v1/ingest', '/v1/ingest/bulk']) {
      expect(document.paths[path].post.requestBody.content['application/json'].schema.properties.chunks.items)
        .toEqual({ $ref: '#/components/schemas/IngestChunk' });
    }
    expect(document.components.schemas.IngestReceipt.required.sort()).toEqual(['accepted', 'chunks', 'inserted', 'replayed']);
  });
  it('matches runtime acceptance for canonical source identity and nested provenance boundaries', () => {
    const cases = [base, { ...base, source_event: { namespace: 'capture', id: 'event' } },
      ...[-1, 0, 1.5, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER + 1].map(ordinal => ({ ...base, source_event: { namespace: 'n', id: 'i', ordinal } })),
      ...['', ' ', '\u00a0', '\u2003', ' valid ', 'n'.repeat(256), 'n'.repeat(257), 'bad\u0000id', '\tid'].map(namespace => ({ ...base, source_event: { namespace, id: 'i' } })),
      ...['', ' ', '\u00a0', '\u2003', ' valid ', 'i'.repeat(512), 'i'.repeat(513), 'bad\u0007id', 'id\n'].map(id => ({ ...base, source_event: { namespace: 'n', id } })),
      { ...base, source_event: { namespace: 'n', id: 'i', extra: true } },
      { ...base, provenance }, { ...base, provenance: { ...provenance, semantic_block_reason: 'forged' } },
      ...[null, true, false, 'true'].map(is_user => ({ ...base, provenance: { ...provenance,
        payload_author: { actor_type: 'human', authorship: 'original', is_user } } })),
      { ...base, provenance: { ...provenance, transport: { initiator_actor_type: 'agent', receiver_actor_type: 'agent' } } },
      { ...base, provenance: { ...provenance, transport: { initiator_actor_type: 'invalid', receiver_actor_type: 'agent' } } },
      ...['a'.repeat(64), 'A'.repeat(64), 'a'.repeat(63)].map(dataset_sha256 => ({ ...base, provenance: { ...provenance,
        import: { importer: 'test', importer_version: '2', import_job_id: 'job', dataset_sha256, original_timestamp: base.timestamp } } }))
    ];
    for (const chunk of cases) {
      const documented = validate(chunk);
      for (const schema of [ingestSchema, bulkIngestSchema]) {
        expect(schema.safeParse({ session_id: 'session', chunks: [chunk] }).success, JSON.stringify(chunk)).toBe(documented);
      }
    }
  });
  it('applies the same whitespace, control and length boundary to every provenance identity field', () => {
    const complete = { ...base, source_event: { namespace: 'n', id: 'i', message_id: 'm' }, provenance: { ...provenance,
      transport: { initiator_actor_type: 'import', receiver_actor_type: 'agent', initiator_id: 'job', receiver_id: 'r',
        source_session_id: 'session', source_channel: 'internal', source_tool: 'importer' },
      import: { importer: 'test', importer_version: '2', import_job_id: 'job', dataset_sha256: 'a'.repeat(64), original_timestamp: base.timestamp }
    } };
    const fields = [
      ['source_event', 'namespace'], ['source_event', 'id'], ['source_event', 'message_id'],
      ...['initiator_id', 'receiver_id', 'source_session_id', 'source_channel', 'source_tool'].map(key => ['provenance', 'transport', key]),
      ...['importer', 'importer_version', 'import_job_id'].map(key => ['provenance', 'import', key])
    ];
    for (const field of fields) {
      const limit = field.at(-1) === 'namespace' ? 256 : 512;
      for (const [value, accepted] of [['   ', false], ['\u00a0\u2003', false], [' valid ', true], ['\tvalid', false],
        ['valid\n', false], ['x\u0085y', false], ['x\u009fy', false], ['\ud800', false],
        ['x'.repeat(limit), true], ['x'.repeat(limit + 1), false], [` ${'x'.repeat(limit)} `, false],
        ['😀'.repeat(limit), true], ['😀'.repeat(limit + 1), false], ['e\u0301'.repeat(limit / 2), true],
        ['e\u0301'.repeat(limit / 2 + 1), false]] as const) {
        const chunk = structuredClone(complete);
        let target: Record<string, any> = chunk;
        for (const key of field.slice(0, -1)) target = target[key];
        target[field.at(-1)!] = value;
        expect(validate(chunk), `${field.join('.')} schema ${JSON.stringify(value)}`).toBe(accepted);
        for (const schema of [ingestSchema, bulkIngestSchema]) {
          expect(schema.safeParse({ session_id: 'session', chunks: [chunk] }).success).toBe(accepted);
        }
      }
    }
  });
  it('uses the same explicit RFC 3339 subset for both timestamp fields', () => {
    const examples = [
      ['2026-06-01T00:00:00Z', true], ['2026-06-01T00:00:00.123456Z', true],
      ['2026-06-01T00:00:00+02:30', true], ['2026-06-01T00:00:00-00:00', true],
      ['2024-02-29T00:00:00Z', true], ['2025-02-29T00:00:00Z', false],
      ['2026-06-01t00:00:00z', false], ['2026-06-01T00:00:00z', false],
      ['2026-06-01 00:00:00Z', false], ['2026-06-01T00:00Z', false],
      ['2026-06-01T00:00:00+0230', false], ['2026-06-01T00:00:00+02', false],
      ['2026-06-01T00:00:00+24:00', false], ['2026-06-01T00:00:00+02:60', false],
      ['2026-06-01T24:00:00Z', false], ['2026-06-01T23:59:60Z', false],
      ['2026-06-01T00:00:00', false], ['2026-06-31T00:00:00Z', false],
      [`2026-06-01T00:00:00.${'1'.repeat(50)}Z`, false]
    ] as const;
    for (const [timestamp, accepted] of examples) {
      const variants = [{ ...base, timestamp }, { ...base, provenance: { ...provenance,
        import: { importer: 'test', importer_version: '2', import_job_id: 'job', dataset_sha256: 'a'.repeat(64), original_timestamp: timestamp }
      } }];
      for (const chunk of variants) {
        expect(validate(chunk), timestamp).toBe(accepted);
        for (const schema of [ingestSchema, bulkIngestSchema]) {
          expect(schema.safeParse({ session_id: 'session', chunks: [chunk] }).success, timestamp).toBe(accepted);
        }
      }
    }
  });
});
