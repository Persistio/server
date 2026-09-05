import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

import {
  buildPlatformEvent,
  curatorRunCompletedEventType,
  memoryArchivedEventType,
  memoryCreatedEventType,
  usagePeriodClosedEventType,
  type PlatformEvent
} from './platform-event';

type JsonSchema = {
  additionalProperties?: boolean | JsonSchema;
  const?: unknown;
  enum?: unknown[];
  format?: string;
  minimum?: number;
  minLength?: number;
  pattern?: string;
  properties?: Record<string, JsonSchema>;
  required?: string[];
  type?: string | string[];
};

const fixtures: PlatformEvent<string>[] = [
  buildPlatformEvent({
    category: 'usage',
    data: {
      account_id: 'workspace-1',
      actor: { id: null, type: 'system' },
      counts: { ingest_events: 12 },
      limits: { ingest_events_per_month: 100 },
      period: '2026-05',
      plan_id: 'unlimited',
      platform_vault_id: 'vault-1',
      sensitivity: 'metadata_only',
      summary: 'Usage period 2026-05 closed',
      usage: { ingest_events: 12 },
      vault_id: 'vault-1',
      workspace_id: 'workspace-1'
    },
    id: 'evt-usage',
    occurredAt: '2026-06-01T00:00:03.000Z',
    subject: 'vault/vault-1',
    type: usagePeriodClosedEventType,
    vaultId: 'vault-1',
    workspaceId: 'workspace-1'
  }),
  buildPlatformEvent({
    data: {
      actor: { id: null, type: 'worker' },
      candidates_processed: 8,
      curator_input_tokens: 900,
      curator_output_tokens: 90,
      curator_requests: 1,
      platform_vault_id: 'vault-1',
      sensitivity: 'metadata_only',
      summary: 'Curator run complete',
      vault_id: 'vault-1',
      workspace_id: 'workspace-1'
    },
    id: 'evt-curator',
    occurredAt: '2026-06-01T00:00:03.000Z',
    subject: 'vault/vault-1',
    type: curatorRunCompletedEventType,
    vaultId: 'vault-1',
    workspaceId: 'workspace-1'
  }),
  buildPlatformEvent({
    data: {
      actor: { id: 'api-key-1', type: 'api_key' },
      memory_id: 'memory-1',
      platform_vault_id: 'vault-1',
      sensitivity: 'metadata_only',
      source: 'api',
      summary: 'New memory added',
      vault_id: 'vault-1',
      workspace_id: 'workspace-1'
    },
    id: 'evt-memory-created',
    occurredAt: '2026-06-01T00:00:03.000Z',
    subject: 'vault/vault-1/memory/memory-1',
    type: memoryCreatedEventType,
    vaultId: 'vault-1',
    workspaceId: 'workspace-1'
  }),
  buildPlatformEvent({
    data: {
      actor: { id: 'api-key-1', type: 'api_key' },
      memory_id: 'memory-1',
      platform_vault_id: 'vault-1',
      sensitivity: 'metadata_only',
      source: 'api',
      summary: 'Memory archived',
      vault_id: 'vault-1',
      workspace_id: 'workspace-1'
    },
    id: 'evt-memory-archived',
    occurredAt: '2026-06-01T00:00:03.000Z',
    subject: 'vault/vault-1/memory/memory-1',
    type: memoryArchivedEventType,
    vaultId: 'vault-1',
    workspaceId: 'workspace-1'
  })
];

describe('platform event schemas', () => {
  it.each(fixtures)('validates emitted %s fixtures against published JSON Schemas', (fixture) => {
    const schema = loadEventSchema(fixture.type);
    const errors = validateSchema(fixture, schema);
    expect(errors).toEqual([]);
  });
});

function loadEventSchema(eventType: string): JsonSchema {
  const schemaPath = resolve(__dirname, '../../../../schemas/events', `${eventType}.json`);
  return JSON.parse(readFileSync(schemaPath, 'utf8')) as JsonSchema;
}

function validateSchema(value: unknown, schema: JsonSchema, path = '$'): string[] {
  const errors: string[] = [];
  if ('const' in schema && value !== schema.const) {
    errors.push(`${path} expected const ${String(schema.const)}`);
  }
  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${path} expected one of ${schema.enum.join(', ')}`);
  }
  if (schema.type && !matchesType(value, schema.type)) {
    errors.push(`${path} expected type ${Array.isArray(schema.type) ? schema.type.join('|') : schema.type}`);
    return errors;
  }
  if (typeof value === 'string') {
    if (schema.minLength !== undefined && value.length < schema.minLength) {
      errors.push(`${path} expected minLength ${schema.minLength}`);
    }
    if (schema.pattern && !new RegExp(schema.pattern).test(value)) {
      errors.push(`${path} expected pattern ${schema.pattern}`);
    }
    if (schema.format === 'date-time' && Number.isNaN(Date.parse(value))) {
      errors.push(`${path} expected date-time`);
    }
  }
  if (typeof value === 'number' && schema.minimum !== undefined && value < schema.minimum) {
    errors.push(`${path} expected minimum ${schema.minimum}`);
  }
  if (isRecord(value)) {
    for (const key of schema.required ?? []) {
      if (!(key in value)) {
        errors.push(`${path}.${key} is required`);
      }
    }
    for (const [key, propertySchema] of Object.entries(schema.properties ?? {})) {
      if (key in value) {
        errors.push(...validateSchema(value[key], propertySchema, `${path}.${key}`));
      }
    }
    if (isRecord(schema.additionalProperties)) {
      const knownProperties = new Set(Object.keys(schema.properties ?? {}));
      for (const [key, propertyValue] of Object.entries(value)) {
        if (!knownProperties.has(key)) {
          errors.push(...validateSchema(propertyValue, schema.additionalProperties, `${path}.${key}`));
        }
      }
    }
  }

  return errors;
}

function matchesType(value: unknown, type: string | string[]): boolean {
  const types = Array.isArray(type) ? type : [type];
  return types.some((candidate) => {
    if (candidate === 'null') return value === null;
    if (candidate === 'array') return Array.isArray(value);
    if (candidate === 'object') return isRecord(value);
    if (candidate === 'number') return typeof value === 'number' && Number.isFinite(value);
    if (candidate === 'string') return typeof value === 'string';
    return false;
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
