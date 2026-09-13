import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';

import { createMemoryShape, updateMemoryShape } from './routes/memories';

type SchemaObject = {
  anyOf?: Array<{ required?: string[] }>;
  properties?: Record<string, unknown>;
  required?: string[];
};

type OpenApiDocument = {
  paths: Record<string, Record<string, {
    parameters?: Array<{ in: string; name: string }>;
    requestBody?: {
      content?: {
        'application/json'?: { schema?: SchemaObject };
      };
    };
  }>>;
};

const openApiPath = fileURLToPath(new URL('../../../openapi.yaml', import.meta.url));
const document = parse(readFileSync(openApiPath, 'utf8')) as OpenApiDocument;

function requestSchema(path: string, method: string): SchemaObject {
  const schema = document.paths[path]?.[method]?.requestBody?.content?.['application/json']?.schema;
  if (!schema) throw new Error(`Missing OpenAPI request schema for ${method.toUpperCase()} ${path}`);
  return schema;
}

function queryParameters(path: string): string[] {
  return (document.paths[path]?.get?.parameters ?? [])
    .filter((parameter) => parameter.in === 'query')
    .map((parameter) => parameter.name)
    .sort();
}

describe('OpenAPI memory contract', () => {
  it('documents every accepted create and update field', () => {
    const create = requestSchema('/v1/memories', 'post');
    const update = requestSchema('/v1/memories/{id}', 'patch');

    expect(Object.keys(create.properties ?? {}).sort()).toEqual(Object.keys(createMemoryShape).sort());
    expect(create.required?.sort()).toEqual(['data', 'scope', 'subject']);
    expect(Object.keys(update.properties ?? {}).sort()).toEqual(Object.keys(updateMemoryShape).sort());
    expect(update.anyOf?.flatMap((schema) => schema.required ?? []).sort()).toEqual(
      Object.keys(updateMemoryShape).filter((field) => field !== 'scope_change_reason').sort()
    );
  });

  it('documents the complete memory browsing surface', () => {
    expect(queryParameters('/v1/memories')).toEqual([
      'archived', 'category', 'filter', 'include_children', 'limit', 'offset', 'q', 'sort', 'subject'
    ]);
    expect(queryParameters('/v1/memories/subjects')).toEqual([
      'archived', 'limit', 'offset', 'q', 'sort'
    ]);
    expect(document.paths['/v1/memories/graph']?.get).toBeDefined();
    expect(queryParameters('/admin/vaults/{id}/memories')).toEqual([
      'archived', 'category', 'filter', 'include_children', 'limit', 'offset', 'q', 'sort', 'subject'
    ]);
  });
});
