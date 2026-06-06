export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];

export interface JsonObject {
  [key: string]: JsonValue;
}

export type KnownPlatformEventType = 'vault.usage_period.closed';
export type PlatformEventStatus = 'pending' | 'delivered' | 'failed' | 'dead';

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

export interface VaultUsagePeriodClosedPayload extends JsonObject {
  platform_vault_id: string;
  account_id: string | null;
  period: string;
  plan_id: string;
  usage: Partial<Record<VaultUsagePeriodUsageField, number>>;
  limits: Partial<Record<VaultUsagePeriodLimitField, number>>;
}

export interface PlatformEventPayloads {
  'vault.usage_period.closed': VaultUsagePeriodClosedPayload;
}

export type PlatformEventPayload<TEventType extends string> =
  TEventType extends keyof PlatformEventPayloads
    ? PlatformEventPayloads[TEventType]
    : JsonObject;

export interface PlatformEvent<TEventType extends string = KnownPlatformEventType> {
  event_id: string;
  event_type: TEventType;
  schema_version: number;
  occurred_at: string;
  subject: string;
  payload: PlatformEventPayload<TEventType>;
}
