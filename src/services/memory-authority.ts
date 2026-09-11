export const MEMORY_AUTHORITY_STATES = ['untrusted', 'proposed', 'approved', 'revoked'] as const;
export function approvalEventExistsSql(memoryAlias: string): string {
  return `EXISTS (
    SELECT 1
    FROM memory_authority_events authority_event
    WHERE authority_event.vault_id = ${memoryAlias}.vault_id
      AND authority_event.memory_id = ${memoryAlias}.id
      AND authority_event.event_type = 'approve'
      AND authority_event.new_state = 'approved'
      AND authority_event.new_version = ${memoryAlias}.authority_version
  )`;
}

export function legacyMigrationEventExistsSql(memoryAlias: string): string {
  return `EXISTS (
    SELECT 1
    FROM memory_authority_events migration_event
    WHERE migration_event.vault_id = ${memoryAlias}.vault_id
      AND migration_event.memory_id = ${memoryAlias}.id
      AND migration_event.event_type = 'migration'
      AND migration_event.source = 'migration'
      AND migration_event.actor_type = 'system'
      AND migration_event.new_state = 'proposed'
      AND migration_event.new_version = ${memoryAlias}.authority_version
      AND migration_event.snapshot IS NOT NULL
      AND migration_event.snapshot->>'type' = 'user_rule'
      AND migration_event.snapshot->>'scope' = 'global'
      AND migration_event.snapshot->>'status' = 'active'
      AND migration_event.snapshot->>'archived_at' IS NULL
  )`;
}

export function activeRevocationExistsSql(memoryAlias: string): string {
  return `EXISTS (
    SELECT 1
    FROM memory_authority_events revocation_event
    WHERE revocation_event.vault_id = ${memoryAlias}.vault_id
      AND revocation_event.memory_id = ${memoryAlias}.id
      AND revocation_event.event_type = 'revoke'
      AND NOT EXISTS (
        SELECT 1
        FROM memory_authority_events later_approval
        WHERE later_approval.vault_id = ${memoryAlias}.vault_id
          AND later_approval.memory_id = ${memoryAlias}.id
          AND later_approval.event_type = 'approve'
          AND later_approval.new_state = 'approved'
          AND later_approval.new_version > revocation_event.new_version
          AND later_approval.new_version <= ${memoryAlias}.authority_version
      )
  )`;
}

export function memoryAuthorityPredicateSql(memoryAlias: string, globalPolicyParameter: string): string {
  const hasApprovalEvent = approvalEventExistsSql(memoryAlias);
  const hasActiveRevocation = activeRevocationExistsSql(memoryAlias);
  const hasLegacyMigrationEvent = legacyMigrationEventExistsSql(memoryAlias);
  return `(
    (
      ${memoryAlias}.type IS NOT DISTINCT FROM 'user_rule'
      AND ${memoryAlias}.scope IS NOT DISTINCT FROM 'global'
      AND (
        (${globalPolicyParameter}::text = 'legacy'
          AND NOT ${hasActiveRevocation}
          AND (
            (${memoryAlias}.authority_state = 'approved' AND ${hasApprovalEvent})
            OR (${memoryAlias}.authority_state = 'proposed' AND ${hasLegacyMigrationEvent})
          )
        )
        OR (${globalPolicyParameter}::text = 'approved_only' AND NOT ${hasActiveRevocation}
          AND ${memoryAlias}.authority_state = 'approved' AND ${hasApprovalEvent})
      )
    )
    OR (
      NOT (
        ${memoryAlias}.type IS NOT DISTINCT FROM 'user_rule'
        AND ${memoryAlias}.scope IS NOT DISTINCT FROM 'global'
      )
      AND (
        NOT ${memoryAlias}.authority_required
        OR (${memoryAlias}.authority_state = 'approved' AND ${hasApprovalEvent})
      )
    )
  )`;
}
export type MemoryAuthorityState = typeof MEMORY_AUTHORITY_STATES[number];

export const GLOBAL_RULE_POLICIES = ['off', 'approved_only', 'legacy'] as const;
export type GlobalRulePolicy = typeof GLOBAL_RULE_POLICIES[number];

export const BEHAVIORAL_MEMORY_TYPES = [
  'user_preference',
  'user_rule',
  'task_pattern',
  'workflow',
  'constraint'
] as const;

export type BehavioralMemoryType = typeof BEHAVIORAL_MEMORY_TYPES[number];

export function isBehavioralMemoryType(type: string | null | undefined): type is BehavioralMemoryType {
  return typeof type === 'string' && BEHAVIORAL_MEMORY_TYPES.includes(type as BehavioralMemoryType);
}

export function isAuthorityRecallable(
  type: string | null | undefined,
  authorityState: MemoryAuthorityState | string | null | undefined,
  hasValidApprovalEvent: boolean,
  authorityRequired = isBehavioralMemoryType(type)
): boolean {
  return !authorityRequired || (authorityState === 'approved' && hasValidApprovalEvent);
}

export function isGlobalRuleRecallable(
  policy: GlobalRulePolicy,
  authorityState: MemoryAuthorityState | string | null | undefined,
  hasValidApprovalEvent: boolean,
  hasActiveRevocation = authorityState === 'revoked',
  hasCurrentLegacyMigrationEvent = false
): boolean {
  if (policy === 'off') return false;
  if (authorityState === 'revoked' || hasActiveRevocation) return false;
  if (authorityState === 'approved' && hasValidApprovalEvent) return true;
  return policy === 'legacy' && authorityState === 'proposed' && hasCurrentLegacyMigrationEvent;
}

export function isMemoryRecallable(
  type: string | null | undefined,
  scope: string | null | undefined,
  authorityState: MemoryAuthorityState | string | null | undefined,
  hasValidApprovalEvent: boolean,
  globalRulePolicy: GlobalRulePolicy,
  authorityRequired = isBehavioralMemoryType(type),
  hasActiveRevocation = authorityState === 'revoked',
  hasCurrentLegacyMigrationEvent = false
): boolean {
  if (type === 'user_rule' && scope === 'global') {
    return isGlobalRuleRecallable(
      globalRulePolicy,
      authorityState,
      hasValidApprovalEvent,
      hasActiveRevocation,
      hasCurrentLegacyMigrationEvent
    );
  }
  return isAuthorityRecallable(type, authorityState, hasValidApprovalEvent, authorityRequired);
}
