import type { PoolClient } from 'pg';

import { query, withTransaction } from '../db/client';
import { getConfig } from '../config';
import { decryptForVault, type VaultEncryptionContext } from './crypto';
import { ExtractorService } from './extractor';

type ConflictDecision = 'supersede_old' | 'needs_review' | 'merge' | 'discard_new';
const VALID_DECISIONS: ConflictDecision[] = ['supersede_old', 'discard_new', 'needs_review', 'merge'];

interface MemoryCandidateRow extends VaultEncryptionContext {
  memory_id: string;
  data: string;
  polarity: 'positive' | 'negative' | 'neutral';
  status: 'active' | 'superseded' | 'contradicted' | 'needs_review' | 'candidate';
  similarity: number;
}

export async function scanForContradictions(
  vaultId: string,
  newMemoryIds: string[],
  extractor: ExtractorService
): Promise<void> {
  const config = getConfig();
  if (!config.CONTRADICTION_SCAN_ENABLED || newMemoryIds.length === 0) {
    return;
  }

  const seenPairs = new Set<string>();
  let arbitrations = 0;

  for (const memoryId of newMemoryIds) {
    if (arbitrations >= config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH) {
      break;
    }

    const memoryResult = await query<MemoryCandidateRow>(
      `SELECT m.id AS memory_id, m.data, m.polarity, m.status, 1.0 AS similarity,
              v.id, v.encrypted_dek, v.vault_encryption_enabled
       FROM memories AS m
       JOIN vaults AS v
         ON v.id = m.vault_id
       WHERE m.vault_id = $1
         AND m.id = $2
         AND m.archived_at IS NULL
       LIMIT 1`,
      [vaultId, memoryId]
    );
    const current = memoryResult.rows[0];
    if (!current) {
      continue;
    }

    const candidatesResult = await query<MemoryCandidateRow>(
      `SELECT m.id AS memory_id, m.data, m.polarity, m.status,
              1 - (m.embedding <=> current.embedding) AS similarity,
              v.id, v.encrypted_dek, v.vault_encryption_enabled
       FROM memories AS current
       JOIN memories AS m
         ON m.vault_id = current.vault_id
       JOIN vaults AS v
         ON v.id = m.vault_id
       WHERE current.id = $2
         AND current.vault_id = $1
         AND current.embedding IS NOT NULL
         AND m.id <> current.id
         AND m.archived_at IS NULL
         AND m.embedding IS NOT NULL
         AND 1 - (m.embedding <=> current.embedding) > $3
       ORDER BY similarity DESC
       LIMIT 10`,
      [vaultId, memoryId, config.CONTRADICTION_SCAN_MIN_SIMILARITY]
    );

    const currentFact = await decryptForVault(current, current.data);

    for (const candidate of candidatesResult.rows) {
      if (arbitrations >= config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH) {
        break;
      }

      const pairKey = [current.memory_id, candidate.memory_id].sort().join(':');
      if (seenPairs.has(pairKey)) {
        continue;
      }

      const candidateFact = await decryptForVault(candidate, candidate.data);

      // Only skip if texts are identical — polarity alone is not a contradiction signal
      // (two neutral facts can still contradict each other: "User lives in London" vs "User lives in New York")
      if (currentFact === candidateFact) {
        continue;
      }

      seenPairs.add(pairKey);
      const decision = await extractor.arbitrateConflict(candidateFact, currentFact);
      const safeDecision: ConflictDecision = VALID_DECISIONS.includes(decision) ? decision : 'needs_review';
      await withTransaction(async (client) => {
        await applyDecision(client, vaultId, current.memory_id, candidate.memory_id, safeDecision);
        await logDecision(client, vaultId, current.memory_id, candidate.memory_id, safeDecision, candidate.similarity);
      });
      arbitrations += 1;
    }
  }
}

async function applyDecision(
  client: PoolClient,
  vaultId: string,
  currentMemoryId: string,
  candidateMemoryId: string,
  decision: ConflictDecision
): Promise<void> {
  if (decision === 'supersede_old') {
    await client.query(
      `UPDATE memories
       SET status = 'contradicted',
           updated_at = now()
       WHERE vault_id = $1
         AND id = $2
         AND archived_at IS NULL`,
      [vaultId, candidateMemoryId]
    );
    return;
  }

  if (decision === 'discard_new') {
    await client.query(
      `UPDATE memories
       SET status = 'contradicted',
           updated_at = now()
       WHERE vault_id = $1
         AND id = $2
         AND archived_at IS NULL`,
      [vaultId, currentMemoryId]
    );
    return;
  }

  if (decision === 'needs_review') {
    await client.query(
      `UPDATE memories
       SET status = 'needs_review',
           updated_at = now()
       WHERE vault_id = $1
         AND id = ANY($2::uuid[])
         AND archived_at IS NULL`,
      [vaultId, [currentMemoryId, candidateMemoryId]]
    );
    return;
  }

  // merge: existing candidate is confirmed/strengthened — supersede current, boost candidate confidence
  if (decision === 'merge') {
    await client.query(
      `UPDATE memories
       SET status = 'superseded',
           updated_at = now()
       WHERE vault_id = $1
         AND id = $2
         AND archived_at IS NULL`,
      [vaultId, currentMemoryId]
    );
    await client.query(
      `UPDATE memories
       SET confidence = LEAST(confidence + 1, 100),
           updated_at = now()
       WHERE vault_id = $1
         AND id = $2
         AND archived_at IS NULL`,
      [vaultId, candidateMemoryId]
    );
  }
}

async function logDecision(
  client: PoolClient,
  vaultId: string,
  currentMemoryId: string,
  candidateMemoryId: string,
  decision: ConflictDecision,
  similarity: number
): Promise<void> {
  await client.query(
    `INSERT INTO contradiction_scan_log (vault_id, memory_id_a, memory_id_b, decision, similarity)
     VALUES ($1, $2, $3, $4, $5)`,
    [vaultId, currentMemoryId, candidateMemoryId, decision, similarity]
  );
}
