import type { Client, PoolClient } from 'pg';

import { query, withTransaction } from '../db/client';
import { getConfig } from '../config';
import { decryptForVault, MemoryCiphertextError, type VaultEncryptionContext } from './crypto';
import type { ExtractorService } from './extractor';
import { memoryValidityPredicateSql } from './memory-validity';
import { memoryAuthorityPredicateSql, type GlobalRulePolicy } from './memory-authority';
import { memoryPolicyEventCounter } from './observability-effects';

type ConflictDecision = 'supersede_old' | 'needs_review' | 'merge' | 'discard_new';
const VALID_DECISIONS: ConflictDecision[] = ['supersede_old', 'discard_new', 'needs_review', 'merge'];
type ScanClient = Client | PoolClient;
const DATABASE_UTC_DATE = "(statement_timestamp() AT TIME ZONE 'UTC')::date";
interface MemoryCandidateRow extends VaultEncryptionContext {
  memory_id: string;
  data: string;
  status: string;
  similarity: number;
  scope: string;
  scope_key: string | null;
  row_version: string;
  authority_eligible: boolean;
  source_timestamp: string | null;
  valid_from: string | null;
  valid_until: string | null;
  created_at: string;
}

export interface ContradictionScanOptions {
  client?: ScanClient;
  budget?: { remaining: number };
  maxArbitrations?: number;
  globalRulePolicy?: GlobalRulePolicy;
}
export interface ContradictionScanResult {
  completedMemoryIds: string[];
  deferredMemoryIds: string[];
}

/** Static applicability shared by input selection and commit-time validation. */
function eligibleSql(alias: string, date: string): string {
  return `${alias}.status = 'active' AND ${alias}.archived_at IS NULL
    AND ((${alias}.scope = 'global' AND ${alias}.scope_key IS NULL)
      OR (${alias}.scope IN ('project', 'task', 'session') AND ${alias}.scope_key IS NOT NULL))
    AND ${alias}.sensitivity <> 'restricted'
    AND ${alias}.confidence > 0 AND ${alias}.confidence <= 1
    AND CASE WHEN ${alias}.evidence ? 'policy_rejections' THEN
      CASE WHEN jsonb_typeof(${alias}.evidence -> 'policy_rejections') = 'array'
        THEN jsonb_array_length(${alias}.evidence -> 'policy_rejections') = 0
        ELSE false END
      ELSE true END
    AND (${alias}.source_timestamp IS NULL OR ${alias}.source_timestamp <= statement_timestamp() + interval '5 minutes')
    AND ${memoryValidityPredicateSql(alias, date)}`;
}

function sameValidityWindow(first: MemoryCandidateRow, second: MemoryCandidateRow): boolean {
  return first.valid_from === second.valid_from && first.valid_until === second.valid_until;
}

export async function scanForContradictions(
  vaultId: string,
  newMemoryIds: string[],
  extractor: ExtractorService,
  options: ContradictionScanOptions = {}
): Promise<ContradictionScanResult> {
  const config = getConfig();
  const policy = options.globalRulePolicy ?? config.GLOBAL_RULE_POLICY;
  const result: ContradictionScanResult = { completedMemoryIds: [], deferredMemoryIds: [] };
  const budget = options.budget ?? { remaining: config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH };
  const execute = options.client ? options.client.query.bind(options.client) : query;
  const transaction = async <T>(run: (client: ScanClient) => Promise<T>): Promise<T> => {
    const freshSnapshotTransaction = async (client: ScanClient) => {
      // Authority events can change without updating the memory's xmin. A fresh
      // statement snapshot after lock acquisition is required, even if the
      // connection's default isolation level is stronger than READ COMMITTED.
      await client.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
      return run(client);
    };
    if (!options.client) return withTransaction(freshSnapshotTransaction);
    // The scheduler's advisory-lock connection also owns every decision commit.
    // A lost connection cannot commit stale work through another pooled client.
    await options.client.query('BEGIN');
    try {
      const value = await freshSnapshotTransaction(options.client);
      await options.client.query('COMMIT');
      return value;
    } catch (error) {
      await options.client.query('ROLLBACK');
      throw error;
    }
  };

  for (const memoryId of newMemoryIds) {
    const startingBudget = budget.remaining;
    const memoryLimit = options.maxArbitrations ?? config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH;
    if (!config.CONTRADICTION_SCAN_ENABLED || budget.remaining <= 0) {
      result.deferredMemoryIds.push(memoryId);
      continue;
    }
    const currentResult = await execute<MemoryCandidateRow>(
      `SELECT m.id AS memory_id, m.data, m.status, m.scope, m.scope_key, m.xmin::text AS row_version,
              m.source_timestamp::text, m.valid_from::text, m.valid_until::text, m.created_at::text,
              1.0 AS similarity, ${memoryAuthorityPredicateSql('m', '$3')} AS authority_eligible,
              v.id, v.encrypted_dek, v.vault_encryption_enabled
       FROM memories m JOIN vaults v ON v.id = m.vault_id
       WHERE m.vault_id = $1 AND m.id = $2 AND ${eligibleSql('m', DATABASE_UTC_DATE)}`,
      [vaultId, memoryId, policy]
    );
    const current = currentResult.rows[0];
    if (!current || current.status !== 'active') {
      result.completedMemoryIds.push(memoryId);
      continue;
    }
    if (!current.authority_eligible) {
      result.deferredMemoryIds.push(memoryId);
      continue;
    }
    let currentFact: string;
    try {
      currentFact = await decryptForVault(current, current.data);
    } catch (error) {
      if (!(error instanceof MemoryCiphertextError)) throw error;
      await quarantineCiphertext(current, vaultId, execute);
      result.deferredMemoryIds.push(memoryId);
      continue;
    }

    const limit = Math.min(budget.remaining, memoryLimit) + 1;
    const candidates = await execute<MemoryCandidateRow>(
      `SELECT m.id AS memory_id, m.data, m.status, m.scope, m.scope_key, m.xmin::text AS row_version,
              m.source_timestamp::text, m.valid_from::text, m.valid_until::text, m.created_at::text,
              1 - (m.embedding <=> current.embedding) AS similarity,
              v.id, v.encrypted_dek, v.vault_encryption_enabled
       FROM memories current JOIN memories m ON m.vault_id = current.vault_id
       JOIN vaults v ON v.id = m.vault_id
       WHERE current.id = $2 AND current.vault_id = $1 AND current.xmin::text = $4
         AND ${eligibleSql('current', DATABASE_UTC_DATE)} AND ${memoryAuthorityPredicateSql('current', '$5')}
         AND m.id <> current.id AND m.scope = current.scope
         AND m.scope_key IS NOT DISTINCT FROM current.scope_key
         AND ${eligibleSql('m', DATABASE_UTC_DATE)} AND ${memoryAuthorityPredicateSql('m', '$5')}
         AND current.embedding IS NOT NULL AND m.embedding IS NOT NULL
         AND 1 - (m.embedding <=> current.embedding) > $3
       ORDER BY similarity DESC, m.id
       LIMIT $6`,
      [vaultId, memoryId, config.CONTRADICTION_SCAN_MIN_SIMILARITY,
        current.row_version, policy, limit]
    );
    if (!candidates.rows.length) {
      const unchanged = await execute(
        'SELECT 1 FROM memories WHERE vault_id = $1 AND id = $2 AND xmin::text = $3',
        [vaultId, memoryId, current.row_version]
      );
      if (unchanged.rowCount !== 1) {
        result.deferredMemoryIds.push(memoryId);
        continue;
      }
    }

    let complete = candidates.rows.length < limit;
    for (const candidate of candidates.rows) {
      if (budget.remaining <= 0 || startingBudget - budget.remaining >= memoryLimit) { complete = false; break; }
      if (candidate.status !== 'active') continue;
      let candidateFact: string;
      try {
        candidateFact = await decryptForVault(candidate, candidate.data);
      } catch (error) {
        if (!(error instanceof MemoryCiphertextError)) throw error;
        // Attribute the defect to the offending row. It must not poison the
        // current memory or hide good candidates behind repeated retries.
        await quarantineCiphertext(candidate, vaultId, execute);
        complete = false;
        continue;
      }
      let decision: ConflictDecision;
      if (!sameValidityWindow(current, candidate)) {
        // Current overlap does not authorize retiring either record's entire
        // temporal scope. Keep both records and their bounds for explicit review,
        // including exact text matches whose intended horizons still differ.
        decision = 'needs_review';
      } else if (candidateFact === currentFact) {
        decision = 'merge';
      } else {
        // Recheck before spending; both inputs are locked and checked again at commit.
        const unchanged = await execute(
          `SELECT 1 FROM memories WHERE vault_id = $1
           AND ((id = $2 AND xmin::text = $4) OR (id = $3 AND xmin::text = $5))`,
          [vaultId, memoryId, candidate.memory_id, current.row_version, candidate.row_version]
        );
        if (unchanged.rowCount !== 2) { complete = false; break; }
        budget.remaining -= 1;
        // Scheduling order conveys no chronology. Explicit neutral pair context
        // prevents a backfilled old reminder from masquerading as newer evidence.
        const temporalContext = (memory: MemoryCandidateRow) => ({
          sourceTimestamp: memory.source_timestamp,
          validFrom: memory.valid_from,
          validUntil: memory.valid_until,
          createdAt: memory.created_at
        });
        decision = await extractor.arbitrateConflict(candidateFact, currentFact, vaultId, {
          existing: temporalContext(candidate), incoming: temporalContext(current)
        });
      }
      if (!VALID_DECISIONS.includes(decision)) {
        throw new Error(`Invalid contradiction arbitration decision: ${String(decision)}`);
      }
      await transaction(async client => {
        const eligibleInputsSql = `SELECT m.id FROM memories m
           WHERE m.vault_id = $1
             AND ((m.id = $2 AND m.xmin::text = $4) OR (m.id = $3 AND m.xmin::text = $5))
             AND ${eligibleSql('m', DATABASE_UTC_DATE)}
             AND ${memoryAuthorityPredicateSql('m', '$6')}`;
        const inputParameters = [vaultId, memoryId, candidate.memory_id,
          current.row_version, candidate.row_version, policy];
        const locked = await client.query(`${eligibleInputsSql} ORDER BY m.id FOR UPDATE OF m`, inputParameters);
        if (locked.rowCount !== 2) throw new Error('Contradiction inputs changed after arbitration');
        // A lock wait may outlive a committed revocation or UTC date boundary.
        // Re-evaluate after the locks with a new snapshot and statement clock.
        // Deferred authority-event reconciliation must acquire these same row
        // locks, so later event transactions cannot commit before this decision.
        const applicable = await client.query(eligibleInputsSql, inputParameters);
        if (applicable.rowCount !== 2) throw new Error('Contradiction inputs changed after arbitration');
        await applyDecision(client, vaultId, current, candidate, decision);
        await client.query(
          `INSERT INTO contradiction_scan_log (vault_id, memory_id_a, memory_id_b, decision, similarity)
           VALUES ($1, $2, $3, $4, $5)`,
          [vaultId, memoryId, candidate.memory_id, decision, candidate.similarity]
        );
      });
      if (decision !== 'supersede_old') { complete = true; break; }
    }
    (complete ? result.completedMemoryIds : result.deferredMemoryIds).push(memoryId);
  }
  return result;
}

async function quarantineCiphertext(
  memory: MemoryCandidateRow,
  vaultId: string,
  execute: typeof query
): Promise<void> {
  const changed = await execute(
    `UPDATE memories SET status = 'needs_review', updated_at = now()
     WHERE vault_id = $1 AND id = $2 AND xmin::text = $3 AND status = 'active' AND archived_at IS NULL`,
    [vaultId, memory.memory_id, memory.row_version]
  );
  if (changed.rowCount === 1) memoryPolicyEventCounter.add(1, {
    event: 'quarantine', source: 'contradiction_scanner', reason: 'invalid_ciphertext'
  });
}

async function applyDecision(
  client: ScanClient,
  vaultId: string,
  current: MemoryCandidateRow,
  candidate: MemoryCandidateRow,
  decision: ConflictDecision
): Promise<void> {
  // Both exact revisions and applicability were checked under ordered row locks.
  const targetIds = decision === 'needs_review' ? [current.memory_id, candidate.memory_id]
    : [decision === 'supersede_old' ? candidate.memory_id : current.memory_id];
  const status = decision === 'needs_review' ? 'needs_review' : decision === 'merge' ? 'superseded' : 'contradicted';
  const updated = await client.query(
    'UPDATE memories SET status = $3, updated_at = now() WHERE vault_id = $1 AND id = ANY($2::uuid[])',
    [vaultId, targetIds, status]
  );
  if (updated.rowCount !== targetIds.length) throw new Error('Contradiction target changed after arbitration');
  if (decision === 'merge') {
    const strengthened = await client.query(
      `UPDATE memories SET confidence = LEAST(confidence + 0.1, 1), updated_at = now()
       WHERE vault_id = $1 AND id = $2`,
      [vaultId, candidate.memory_id]
    );
    if (strengthened.rowCount !== 1) throw new Error('Contradiction merge target changed after arbitration');
  }
}
