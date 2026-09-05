export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];

export interface JsonObject {
  [key: string]: JsonValue;
}

export const platformEventSource = 'io.persistio.platform';
export const platformEventSchemaBaseUrl = 'https://schemas.persistio.io/events';

export const usagePeriodClosedEventType = 'io.persistio.usage_period.closed.v1';
export const memoryCreatedEventType = 'io.persistio.memory.created.v1';
export const memoryArchivedEventType = 'io.persistio.memory.archived.v1';
export const curatorRunCompletedEventType = 'io.persistio.curator.run.completed.v1';

export const legacyUsagePeriodClosedEventType = 'vault.usage_period.closed';

export type KnownPlatformEventType =
  | typeof curatorRunCompletedEventType
  | typeof memoryArchivedEventType
  | typeof memoryCreatedEventType
  | typeof usagePeriodClosedEventType;

export type PlatformEventStatus = 'pending' | 'delivered' | 'failed' | 'dead';
export type PlatformEventCategory = 'activity' | 'operational' | 'security' | 'usage';
export type PlatformEventSeverity = 'error' | 'info' | 'notice' | 'warning';

export type VaultUsagePeriodUsageField =
  | 'ingest_events'
  | 'memory_adds'
  | 'searches'
  | 'curator_runs'
  | 'curator_requests'
  | 'curator_input_tokens'
  | 'curator_output_tokens'
  | 'curator_candidates_processed'
  | 'curator_candidates_deferred';

export type VaultUsagePeriodLimitField =
  | 'ingest_events_per_month'
  | 'memory_adds_per_month'
  | 'searches_per_month'
  | 'curator_runs_per_month'
  | 'curator_requests_per_month'
  | 'curator_tokens_per_month';

export interface PlatformActor {
  id: string | null;
  type: 'api_key' | 'system' | 'user' | 'worker';
}

export interface PlatformActivityCounts {
  [key: string]: number;
}

export interface PlatformActivityData {
  actor?: PlatformActor;
  counts?: PlatformActivityCounts;
  sensitivity: 'metadata_only';
  summary: string;
  vault_id?: string;
  workspace_id: string;
}

export interface VaultUsagePeriodClosedPayload extends PlatformActivityData {
  account_id: string | null;
  limits: Partial<Record<VaultUsagePeriodLimitField, number>>;
  period: string;
  plan_id: string;
  platform_vault_id: string;
  usage: Partial<Record<VaultUsagePeriodUsageField, number>>;
  vault_id: string;
}

export interface CuratorRunCompletedPayload extends PlatformActivityData {
  candidates_processed: number;
  curator_input_tokens: number;
  curator_output_tokens: number;
  curator_requests: number;
  platform_vault_id: string;
  vault_id: string;
}

export interface MemoryCreatedPayload extends PlatformActivityData {
  memory_id: string;
  platform_vault_id: string;
  source: 'api' | 'extraction_worker' | 'system';
  vault_id: string;
}

export interface MemoryArchivedPayload extends PlatformActivityData {
  memory_id: string;
  platform_vault_id: string;
  source: 'api' | 'curation_worker' | 'system';
  vault_id: string;
}

export interface PlatformEventPayloads {
  [curatorRunCompletedEventType]: CuratorRunCompletedPayload;
  [memoryArchivedEventType]: MemoryArchivedPayload;
  [memoryCreatedEventType]: MemoryCreatedPayload;
  [usagePeriodClosedEventType]: VaultUsagePeriodClosedPayload;
}

export type PlatformEventPayload<TEventType extends string> =
  TEventType extends keyof PlatformEventPayloads
    ? PlatformEventPayloads[TEventType]
    : JsonObject;

export interface PlatformEvent<TEventType extends string = KnownPlatformEventType> {
  category: PlatformEventCategory;
  data: PlatformEventPayload<TEventType>;
  datacontenttype: 'application/json';
  dataschema: string;
  id: string;
  severity: PlatformEventSeverity;
  source: string;
  specversion: '1.0';
  subject: string;
  time: string;
  type: TEventType;
  vaultid?: string;
  workspaceid: string;
}

export interface BuildPlatformEventInput<TEventType extends string> {
  category?: PlatformEventCategory;
  data: PlatformEventPayload<TEventType>;
  id: string;
  occurredAt: Date | string;
  severity?: PlatformEventSeverity;
  subject: string;
  type: TEventType;
  vaultId?: string | null;
  workspaceId: string;
}

export function buildPlatformEvent<TEventType extends string>(
  input: BuildPlatformEventInput<TEventType>
): PlatformEvent<TEventType> {
  const event: PlatformEvent<TEventType> = {
    category: input.category ?? 'activity',
    data: input.data,
    datacontenttype: 'application/json',
    dataschema: `${platformEventSchemaBaseUrl}/${input.type}.json`,
    id: input.id,
    severity: input.severity ?? 'info',
    source: platformEventSource,
    specversion: '1.0',
    subject: input.subject,
    time: input.occurredAt instanceof Date ? input.occurredAt.toISOString() : new Date(input.occurredAt).toISOString(),
    type: input.type,
    workspaceid: input.workspaceId
  };

  if (input.vaultId) {
    event.vaultid = input.vaultId;
  }

  return event;
}

export function schemaUrlForEventType(eventType: string): string {
  return `${platformEventSchemaBaseUrl}/${eventType}.json`;
}
