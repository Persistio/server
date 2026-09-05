import type { AppConfig } from '../config';
import {
  DEFAULT_TOP_VAULT_LIMIT,
  DEFAULT_TOP_VAULT_METRIC
} from './analytics-defaults';
import type {
  CustomerAnalyticsQueryInfo,
  TopVaultMetric,
  TopVaultMetricPoint,
  WorkspaceMetricPoint
} from './customer-analytics';

export interface WorkspaceSnapshotInput {
  from: Date;
  grain: 'day' | 'hour';
  to: Date;
  workspaceId: string;
}

export interface VaultSnapshotInput extends WorkspaceSnapshotInput {
  vaultId: string;
}

export interface TopVaultSnapshotInput {
  from: Date;
  limit: number;
  metric: TopVaultMetric;
  to: Date;
  workspaceId: string;
}

export interface SnapshotResult<T> {
  items: T[];
  query: CustomerAnalyticsQueryInfo;
}

export interface AnalyticsSnapshotCache {
  getTopVaults(input: TopVaultSnapshotInput): Promise<SnapshotResult<TopVaultMetricPoint> | null>;
  getVaultMetrics(input: VaultSnapshotInput): Promise<SnapshotResult<WorkspaceMetricPoint> | null>;
  getWorkspaceSummary(input: WorkspaceSnapshotInput): Promise<SnapshotResult<WorkspaceMetricPoint> | null>;
  setTopVaults(input: TopVaultSnapshotInput, result: SnapshotResult<TopVaultMetricPoint>): Promise<void>;
  setVaultMetrics(input: VaultSnapshotInput, result: SnapshotResult<WorkspaceMetricPoint>): Promise<void>;
  setWorkspaceSummary(input: WorkspaceSnapshotInput, result: SnapshotResult<WorkspaceMetricPoint>): Promise<void>;
}

interface FirestoreDocumentReferenceLike {
  get(): Promise<FirestoreDocumentSnapshotLike>;
  set(data: Record<string, unknown>): Promise<unknown>;
}

interface FirestoreDocumentSnapshotLike {
  exists: boolean;
  data(): Record<string, unknown> | undefined;
}

interface FirestoreLike {
  collection(name: string): {
    doc(id: string): FirestoreDocumentReferenceLike;
  };
}

type SnapshotKind = 'top_vaults_default_30d' | 'vault_metrics_default_30d' | 'workspace_summary_default_30d';

interface SnapshotDocument<T> {
  cache_key_version: 1;
  expires_at: Date | string | { toDate?: () => Date; toMillis?: () => number };
  from: string;
  grain: 'day' | 'hour';
  items: T[];
  kind: SnapshotKind;
  query: CustomerAnalyticsQueryInfo;
  refreshed_at: string;
  to: string;
  vault_id?: string;
  workspace_id: string;
}

const targetMaxSnapshotBytes = 200 * 1024;

export function createAnalyticsSnapshotCache(config: AppConfig): AnalyticsSnapshotCache | null {
  if (!config.ANALYTICS_FIRESTORE_SNAPSHOT_ENABLED) {
    return null;
  }

  const projectId = sanitizeFirestoreProjectId(
    config.ANALYTICS_FIRESTORE_PROJECT_ID ||
    config.ANALYTICS_BIGQUERY_PROJECT_ID ||
    config.GCP_PUBSUB_PROJECT_ID ||
    config.VERTEX_PROJECT_ID
  );
  const databaseId = sanitizeFirestoreDatabaseId(config.ANALYTICS_FIRESTORE_DATABASE_ID);
  const collectionId = sanitizeFirestoreCollectionId(config.ANALYTICS_FIRESTORE_SNAPSHOT_COLLECTION);

  return new FirestoreAnalyticsSnapshotCache({
    collectionId,
    databaseId,
    firestore: createFirestoreClient(projectId, databaseId),
    ttlSeconds: config.ANALYTICS_FIRESTORE_SNAPSHOT_TTL_SECONDS
  });
}

export class FirestoreAnalyticsSnapshotCache implements AnalyticsSnapshotCache {
  private readonly collectionId: string;
  private readonly firestore: FirestoreLike;
  private readonly ttlMs: number;

  constructor(input: { collectionId: string; databaseId?: string; firestore: FirestoreLike; ttlSeconds: number }) {
    this.collectionId = input.collectionId;
    this.firestore = input.firestore;
    this.ttlMs = input.ttlSeconds * 1000;
  }

  async getWorkspaceSummary(input: WorkspaceSnapshotInput): Promise<SnapshotResult<WorkspaceMetricPoint> | null> {
    if (!isDefaultDailySnapshotRange(input)) return null;
    const snapshot = await this.read<WorkspaceMetricPoint>(workspaceSummarySnapshotId(input), 'workspace_summary_default_30d');
    return snapshot ? toSnapshotResult(snapshot) : null;
  }

  async setWorkspaceSummary(input: WorkspaceSnapshotInput, result: SnapshotResult<WorkspaceMetricPoint>): Promise<void> {
    if (!isDefaultDailySnapshotRange(input)) return;
    await this.write(workspaceSummarySnapshotId(input), 'workspace_summary_default_30d', input, result);
  }

  async getVaultMetrics(input: VaultSnapshotInput): Promise<SnapshotResult<WorkspaceMetricPoint> | null> {
    if (!isDefaultDailySnapshotRange(input)) return null;
    const snapshot = await this.read<WorkspaceMetricPoint>(vaultMetricsSnapshotId(input), 'vault_metrics_default_30d');
    if (!snapshot || !matchesSnapshotInput(snapshot, input)) return null;
    return snapshot ? toSnapshotResult(snapshot) : null;
  }

  async setVaultMetrics(input: VaultSnapshotInput, result: SnapshotResult<WorkspaceMetricPoint>): Promise<void> {
    if (!isDefaultDailySnapshotRange(input)) return;
    await this.write(vaultMetricsSnapshotId(input), 'vault_metrics_default_30d', input, result);
  }

  async getTopVaults(input: TopVaultSnapshotInput): Promise<SnapshotResult<TopVaultMetricPoint> | null> {
    if (!isDefaultTopVaultSnapshot(input)) return null;
    const snapshot = await this.read<TopVaultMetricPoint>(topVaultsSnapshotId(input), 'top_vaults_default_30d');
    return snapshot ? toSnapshotResult(snapshot) : null;
  }

  async setTopVaults(input: TopVaultSnapshotInput, result: SnapshotResult<TopVaultMetricPoint>): Promise<void> {
    if (!isDefaultTopVaultSnapshot(input)) return;
    await this.write(topVaultsSnapshotId(input), 'top_vaults_default_30d', { ...input, grain: 'day' }, result);
  }

  private async read<T>(id: string, kind: SnapshotKind): Promise<SnapshotDocument<T> | null> {
    const snapshot = await this.firestore.collection(this.collectionId).doc(id).get();
    if (!snapshot.exists) return null;

    const data = snapshot.data();
    if (!isSnapshotDocument<T>(data, kind)) return null;
    const expiresAt = snapshotExpiryMs(data.expires_at);
    if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) return null;

    return data;
  }

  private async write<T>(
    id: string,
    kind: SnapshotKind,
    input: WorkspaceSnapshotInput & { vaultId?: string },
    result: SnapshotResult<T>
  ): Promise<void> {
    const refreshedAt = new Date();
    const expiresAt = new Date(refreshedAt.getTime() + this.ttlMs);
    const data = {
      cache_key_version: 1,
      expires_at: expiresAt,
      from: input.from.toISOString(),
      grain: input.grain,
      items: result.items,
      kind,
      query: result.query,
      refreshed_at: refreshedAt.toISOString(),
      to: input.to.toISOString(),
      ...(input.vaultId ? { vault_id: input.vaultId } : {}),
      workspace_id: input.workspaceId
    };
    if (Buffer.byteLength(JSON.stringify(data)) > targetMaxSnapshotBytes) return;
    await this.firestore.collection(this.collectionId).doc(id).set(data);
  }
}

function createFirestoreClient(projectId: string, databaseId: string): FirestoreLike {
  // Loaded lazily so non-analytics roles do not initialize Firestore clients.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { Firestore } = require('@google-cloud/firestore') as typeof import('@google-cloud/firestore');
  return new Firestore({ databaseId, projectId }) as unknown as FirestoreLike;
}

function toSnapshotResult<T>(snapshot: SnapshotDocument<T>): SnapshotResult<T> {
  return {
    items: snapshot.items,
    query: {
      ...snapshot.query,
      cache_hit: true,
      cache_refreshed_at: snapshot.refreshed_at
    } as CustomerAnalyticsQueryInfo
  };
}

function isSnapshotDocument<T>(data: unknown, kind: SnapshotKind): data is SnapshotDocument<T> {
  return typeof data === 'object' &&
    data !== null &&
    (data as { cache_key_version?: unknown }).cache_key_version === 1 &&
    (data as { kind?: unknown }).kind === kind &&
    isSnapshotExpiry((data as { expires_at?: unknown }).expires_at) &&
    typeof (data as { refreshed_at?: unknown }).refreshed_at === 'string' &&
    Array.isArray((data as { items?: unknown }).items) &&
    typeof (data as { query?: unknown }).query === 'object' &&
    (data as { query?: unknown }).query !== null;
}

function isDefaultDailySnapshotRange(input: WorkspaceSnapshotInput): boolean {
  return input.grain === 'day' &&
    isUtcDayBoundary(input.from) &&
    isUtcDayBoundary(input.to) &&
    input.to.getTime() - input.from.getTime() === 30 * 24 * 60 * 60 * 1000;
}

function isDefaultTopVaultSnapshot(input: TopVaultSnapshotInput): boolean {
  return input.limit === DEFAULT_TOP_VAULT_LIMIT &&
    input.metric === DEFAULT_TOP_VAULT_METRIC &&
    isDefaultDailySnapshotRange({ ...input, grain: 'day' });
}

function matchesSnapshotInput(snapshot: SnapshotDocument<unknown>, input: WorkspaceSnapshotInput & { vaultId?: string }): boolean {
  return snapshot.from === input.from.toISOString() &&
    snapshot.to === input.to.toISOString() &&
    snapshot.grain === input.grain &&
    snapshot.workspace_id === input.workspaceId &&
    (!input.vaultId || snapshot.vault_id === input.vaultId);
}

function snapshotExpiryMs(value: SnapshotDocument<unknown>['expires_at']): number {
  if (value instanceof Date) return value.getTime();
  if (typeof value === 'string') return new Date(value).getTime();
  if (typeof value.toMillis === 'function') return value.toMillis();
  if (typeof value.toDate === 'function') return value.toDate().getTime();
  return Number.NaN;
}

function isSnapshotExpiry(value: unknown): value is SnapshotDocument<unknown>['expires_at'] {
  if (value instanceof Date || typeof value === 'string') return true;
  return typeof value === 'object' &&
    value !== null &&
    (typeof (value as { toDate?: unknown }).toDate === 'function' ||
      typeof (value as { toMillis?: unknown }).toMillis === 'function');
}

function isUtcDayBoundary(value: Date): boolean {
  return value.getUTCHours() === 0 &&
    value.getUTCMinutes() === 0 &&
    value.getUTCSeconds() === 0 &&
    value.getUTCMilliseconds() === 0;
}

function workspaceSummarySnapshotId(input: WorkspaceSnapshotInput): string {
  return [
    'v1',
    'workspace_summary_default_30d',
    input.workspaceId.toLowerCase(),
    dateKey(input.from),
    dateKey(input.to)
  ].join('__');
}

function topVaultsSnapshotId(input: TopVaultSnapshotInput): string {
  return [
    'v1',
    'top_vaults_default_30d',
    input.workspaceId.toLowerCase(),
    dateKey(input.from),
    dateKey(input.to),
    input.metric,
    String(input.limit)
  ].join('__');
}

function vaultMetricsSnapshotId(input: VaultSnapshotInput): string {
  return [
    'v1',
    'vault_metrics_default_30d',
    input.workspaceId.toLowerCase(),
    input.vaultId.toLowerCase()
  ].join('__');
}

function dateKey(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function sanitizeFirestoreProjectId(value: string): string {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error('ANALYTICS_FIRESTORE_PROJECT_ID must contain only letters, numbers, underscores, or dashes');
  }
  return value;
}

function sanitizeFirestoreDatabaseId(value: string): string {
  if (value === '(default)') return value;
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error('ANALYTICS_FIRESTORE_DATABASE_ID must be "(default)" or contain only letters, numbers, underscores, or dashes');
  }
  return value;
}

function sanitizeFirestoreCollectionId(value: string): string {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error('ANALYTICS_FIRESTORE_SNAPSHOT_COLLECTION must contain only letters, numbers, underscores, or dashes');
  }
  return value;
}
