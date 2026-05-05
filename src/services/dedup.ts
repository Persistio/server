import crypto from 'node:crypto';
import type { QueryResult, QueryResultRow } from 'pg';

import { query } from '../db/client';
import {
  computeSubjectHmac,
  decryptForVault,
  encryptForVault,
  encryptSubjectForVault,
  isVaultEncryptionActive,
  type VaultEncryptionContext
} from './crypto';
import { ExtractorService } from './extractor';
import { canCreateMemory, checkQuota, incrementUsage } from './usage';
import { withSpan } from '../telemetry';

export interface DedupInput {
  vaultId: string;
  fact: string;
  subject: string;
  embedding: number[];
  sourceChunks: string[];
}

interface MemoryRow {
  id: string;
  data: string;
  confidence: number;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
}

export type DedupResult =
  | { action: 'skipped'; memoryId?: string }
  | { action: 'updated'; memoryId: string }
  | { action: 'inserted'; memoryId: string }
  | { action: 'conflict'; memoryId: string };

interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<T>>;
}

export async function deduplicateMemory(
  input: DedupInput,
  db: Queryable = { query },
  extractor?: ExtractorService
): Promise<DedupResult> {
  return withSpan('memory.deduplicate', {
    'vault.id': input.vaultId,
    'memory.subject': input.subject,
    'memory.source_chunks_count': input.sourceChunks.length
  }, async (span) => {
    const hash = crypto.createHash('md5').update(input.fact).digest('hex');
    const vaultResult = await db.query<VaultEncryptionContext>(
      `SELECT id, encrypted_dek, vault_encryption_enabled
       FROM vaults
       WHERE id = $1
       LIMIT 1`,
      [input.vaultId]
    );
    const vault = vaultResult.rows[0];

    const exactMatch = await db.query<{ id: string }>(
      `SELECT id
       FROM memories
       WHERE vault_id = $1 AND hash = $2 AND archived_at IS NULL
       LIMIT 1`,
      [input.vaultId, hash]
    );

    if (exactMatch.rowCount) {
      span.setAttribute('dedup.result', 'skipped');
      return {
        action: 'skipped',
        memoryId: exactMatch.rows[0].id
      };
    }

    const subjectMatchTarget = isVaultEncryptionActive(vault) && vault.encrypted_dek
      ? computeSubjectHmac(input.subject, await unwrapVaultDek(vault))
      : input.subject;
    const subjectMatchColumn = isVaultEncryptionActive(vault) ? 'm.subject_hmac' : 'm.subject';
    const subjectMatches = await db.query<(MemoryRow & { similarity: number })>(
      `SELECT m.id, m.data, m.confidence, v.encrypted_dek, v.vault_encryption_enabled,
              1 - (m.embedding <=> $3::vector) AS similarity
       FROM memories AS m
       JOIN vaults AS v
         ON v.id = m.vault_id
       WHERE m.vault_id = $1
         AND ${subjectMatchColumn} = $2
         AND m.archived_at IS NULL
         AND m.embedding IS NOT NULL
       ORDER BY similarity DESC`,
      [input.vaultId, subjectMatchTarget, JSON.stringify(input.embedding)]
    );

    const bestMatch = subjectMatches.rows[0];
    if (bestMatch) {
      span.setAttribute('dedup.best_similarity', bestMatch.similarity);
    }

    if (bestMatch && bestMatch.similarity > 0.90) {
      const storedFact = await encryptForVault(getVaultContext(bestMatch, input.vaultId), input.fact);
      await db.query(
        `UPDATE memories
         SET data = $2, hash = $3, embedding = $4::vector,
             source_chunks = $5::uuid[], updated_at = now(), confidence = confidence + 1
         WHERE id = $1`,
        [bestMatch.id, storedFact, hash, JSON.stringify(input.embedding), input.sourceChunks]
      );
      span.setAttribute('dedup.result', 'updated');
      return { action: 'updated', memoryId: bestMatch.id };
    }

    if (bestMatch && bestMatch.similarity >= 0.80) {
      const bestMatchVault = getVaultContext(bestMatch, input.vaultId);
      const existingFact = await decryptForVault(bestMatchVault, bestMatch.data);
      const decision = extractor
        ? await extractor.arbitrateConflict(existingFact, input.fact)
        : 'keep_both';
      span.setAttribute('dedup.conflict_decision', decision);

      if (decision === 'update') {
        const storedFact = await encryptForVault(bestMatchVault, input.fact);
        await db.query(
          `UPDATE memories
           SET data = $2, hash = $3, embedding = $4::vector,
               source_chunks = $5::uuid[], updated_at = now(), confidence = confidence + 1
           WHERE id = $1`,
          [bestMatch.id, storedFact, hash, JSON.stringify(input.embedding), input.sourceChunks]
        );
        span.setAttribute('dedup.result', 'updated');
        return { action: 'updated', memoryId: bestMatch.id };
      }

      if (decision === 'discard_new') {
        span.setAttribute('dedup.result', 'skipped');
        return { action: 'skipped', memoryId: bestMatch.id };
      }

      if (!(await canCreateMemory(input.vaultId))) {
        span.setAttribute('dedup.result', 'skipped');
        span.setAttribute('dedup.reason', 'memories_max');
        return { action: 'skipped', memoryId: bestMatch.id };
      }

      await checkQuota(input.vaultId, 'memory_adds');
      const storedFact = await encryptForVault(bestMatchVault, input.fact);
      const encryptedSubject = await encryptSubjectForVault(bestMatchVault, input.subject);
      const inserted = await db.query<{ id: string }>(
        `INSERT INTO memories (
           vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding, source_chunks
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::uuid[])
         RETURNING id`,
        [
          input.vaultId,
          storedFact,
          isVaultEncryptionActive(bestMatchVault) ? '' : input.subject,
          encryptedSubject?.encrypted ?? null,
          encryptedSubject?.hmac ?? null,
          hash,
          JSON.stringify(input.embedding),
          input.sourceChunks
        ]
      );
      await incrementUsage(input.vaultId, 'memory_adds');
      span.setAttribute('dedup.result', 'inserted');
      return { action: 'inserted', memoryId: inserted.rows[0].id };
    }

    if (!(await canCreateMemory(input.vaultId))) {
      span.setAttribute('dedup.result', 'skipped');
      span.setAttribute('dedup.reason', 'memories_max');
      return { action: 'skipped' };
    }

    await checkQuota(input.vaultId, 'memory_adds');
    const storedFact = await encryptForVault(vault, input.fact);
    const encryptedSubject = await encryptSubjectForVault(vault, input.subject);
    const inserted = await db.query<{ id: string }>(
      `INSERT INTO memories (
         vault_id, data, subject, subject_encrypted, subject_hmac, hash, embedding, source_chunks
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8::uuid[])
       RETURNING id`,
      [
        input.vaultId,
        storedFact,
        isVaultEncryptionActive(vault) ? '' : input.subject,
        encryptedSubject?.encrypted ?? null,
        encryptedSubject?.hmac ?? null,
        hash,
        JSON.stringify(input.embedding),
        input.sourceChunks
      ]
    );
    await incrementUsage(input.vaultId, 'memory_adds');

    span.setAttribute('dedup.result', 'inserted');
    return {
      action: 'inserted',
      memoryId: inserted.rows[0].id
    };
  });
}

function getVaultContext(row: MemoryRow, vaultId: string): VaultEncryptionContext {
  return {
    id: vaultId,
    encrypted_dek: row.encrypted_dek,
    vault_encryption_enabled: row.vault_encryption_enabled
  };
}

async function unwrapVaultDek(vault: VaultEncryptionContext): Promise<Buffer> {
  if (!vault.encrypted_dek) {
    throw new Error(`Vault ${vault.id} is missing encrypted_dek`);
  }

  const { unwrapDek } = await import('./crypto');
  return unwrapDek(vault.encrypted_dek);
}
