import { query } from '../db/client';
import type { QueryResult, QueryResultRow } from 'pg';
import { computeSubjectHmac, isVaultEncryptionActive, unwrapDek, type VaultEncryptionContext } from './crypto';
import type { MemoryScope } from './memory-scope';
import { memoryValidityPredicateSql, toDateOnly } from './memory-validity';

interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<T>>;
}

export function normaliseSubject(subject: string): string {
  return subject
    .toLowerCase()
    .replace(/^the\s+/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

export interface VaultSubject {
  canonical: string;
  aliases: string[];
  embedding: number[] | null;
}

export async function resolveCanonical(
  vaultId: string,
  subject: string,
  scope: MemoryScope,
  scopeKey: string | null,
  db: Queryable = { query }
): Promise<string | null> {
  const normalisedSubject = normaliseSubject(subject);
  const result = await db.query<{ canonical: string }>(
    `SELECT canonical
     FROM entity_aliases
     WHERE vault_id = $1
       AND alias = $2
       AND scope = $3
       AND scope_key IS NOT DISTINCT FROM $4::text
     LIMIT 1`,
    [vaultId, normalisedSubject, scope, scopeKey]
  );

  return result.rows[0]?.canonical ?? null;
}

export async function getVaultSubjectList(
  vaultId: string,
  topN: number,
  recentN: number,
  scope: MemoryScope,
  scopeKey: string | null
): Promise<VaultSubject[]> {
  const subjectListDate = toDateOnly(new Date());
  if (!subjectListDate) {
    throw new Error('Unable to derive a valid subject-list date');
  }
  const vaultResult = await query<VaultEncryptionContext>(
    `SELECT id, encrypted_dek, vault_encryption_enabled
     FROM vaults
     WHERE id = $1
     LIMIT 1`,
    [vaultId]
  );
  const vault = vaultResult.rows[0];
  if (!vault) {
    return [];
  }

  if (isVaultEncryptionActive(vault)) {
    return getEncryptedVaultSubjectList(vault, topN, recentN, scope, scopeKey, subjectListDate);
  }

  // Top N by memory count
  const topResult = await query<{ subject: string }>(
    `SELECT m.subject, COUNT(m.id) AS cnt
     FROM memories m
     WHERE m.vault_id = $1
       AND m.scope = $4
       AND m.scope_key IS NOT DISTINCT FROM $5::text
       AND m.archived_at IS NULL
       AND m.status = 'active'
       AND m.sensitivity <> 'restricted'
       AND m.confidence > 0 AND m.confidence <= 1
       AND (m.source_timestamp IS NULL OR m.source_timestamp <= now() + interval '5 minutes')
       AND ${memoryValidityPredicateSql('m', '$3')}
       AND m.subject <> ''
     GROUP BY m.subject
     ORDER BY cnt DESC
     LIMIT $2`,
    [vaultId, topN, subjectListDate, scope, scopeKey]
  );

  // Recent N by latest memory activity
  const recentResult = await query<{ subject: string }>(
    `SELECT m.subject
     FROM memories m
     WHERE m.vault_id = $1
       AND m.scope = $4
       AND m.scope_key IS NOT DISTINCT FROM $5::text
       AND m.archived_at IS NULL
       AND m.status = 'active'
       AND m.sensitivity <> 'restricted'
       AND m.confidence > 0 AND m.confidence <= 1
       AND (m.source_timestamp IS NULL OR m.source_timestamp <= now() + interval '5 minutes')
       AND ${memoryValidityPredicateSql('m', '$3')}
       AND m.subject <> ''
     GROUP BY m.subject
     ORDER BY MAX(COALESCE(m.updated_at, m.created_at)) DESC NULLS LAST
     LIMIT $2`,
    [vaultId, recentN, subjectListDate, scope, scopeKey]
  );

  // Deduplicate
  const seen = new Set<string>();
  const canonicals: string[] = [];
  for (const row of [...topResult.rows, ...recentResult.rows]) {
    const canonical = normaliseSubject(row.subject);
    if (!seen.has(canonical)) {
      seen.add(canonical);
      canonicals.push(canonical);
    }
  }

  return hydrateVaultSubjects(vaultId, canonicals, scope, scopeKey);
}

async function getEncryptedVaultSubjectList(
  vault: VaultEncryptionContext,
  topN: number,
  recentN: number,
  scope: MemoryScope,
  scopeKey: string | null,
  subjectListDate: string
): Promise<VaultSubject[]> {
  if (!vault.encrypted_dek) {
    return [];
  }

  const aliasResult = await query<{ canonical: string; alias: string }>(
    `SELECT canonical, alias
     FROM entity_aliases
     WHERE vault_id = $1
       AND scope = $2
       AND scope_key IS NOT DISTINCT FROM $3::text`,
    [vault.id, scope, scopeKey]
  );
  if (aliasResult.rows.length === 0) {
    return [];
  }

  const dek = await unwrapDek(vault.encrypted_dek);
  const hmacs: string[] = [];
  const hmacCanonicals: string[] = [];
  for (const row of aliasResult.rows) {
    hmacs.push(computeSubjectHmac(row.alias, dek));
    hmacCanonicals.push(row.canonical);
  }

  const topResult = await query<{ canonical: string }>(
    `WITH known_subjects AS (
       SELECT subject_hmac, canonical
       FROM UNNEST($2::text[], $3::text[]) AS t(subject_hmac, canonical)
     )
     SELECT ks.canonical, COUNT(m.id) AS cnt
     FROM memories m
     JOIN known_subjects ks ON ks.subject_hmac = m.subject_hmac
     WHERE m.vault_id = $1
       AND m.scope = $6
       AND m.scope_key IS NOT DISTINCT FROM $7::text
       AND m.archived_at IS NULL
       AND m.status = 'active'
       AND m.sensitivity <> 'restricted'
       AND m.confidence > 0 AND m.confidence <= 1
       AND (m.source_timestamp IS NULL OR m.source_timestamp <= now() + interval '5 minutes')
       AND ${memoryValidityPredicateSql('m', '$5')}
     GROUP BY ks.canonical
     ORDER BY cnt DESC
     LIMIT $4`,
    [vault.id, hmacs, hmacCanonicals, topN, subjectListDate, scope, scopeKey]
  );

  const recentResult = await query<{ canonical: string }>(
    `WITH known_subjects AS (
       SELECT subject_hmac, canonical
       FROM UNNEST($2::text[], $3::text[]) AS t(subject_hmac, canonical)
     )
     SELECT ks.canonical
     FROM memories m
     JOIN known_subjects ks ON ks.subject_hmac = m.subject_hmac
     WHERE m.vault_id = $1
       AND m.scope = $6
       AND m.scope_key IS NOT DISTINCT FROM $7::text
       AND m.archived_at IS NULL
       AND m.status = 'active'
       AND m.sensitivity <> 'restricted'
       AND m.confidence > 0 AND m.confidence <= 1
       AND (m.source_timestamp IS NULL OR m.source_timestamp <= now() + interval '5 minutes')
       AND ${memoryValidityPredicateSql('m', '$5')}
     GROUP BY ks.canonical
     ORDER BY MAX(COALESCE(m.updated_at, m.created_at)) DESC NULLS LAST
     LIMIT $4`,
    [vault.id, hmacs, hmacCanonicals, recentN, subjectListDate, scope, scopeKey]
  );

  return hydrateVaultSubjects(vault.id, [
    ...topResult.rows.map((row) => row.canonical),
    ...recentResult.rows.map((row) => row.canonical)
  ], scope, scopeKey);
}

async function hydrateVaultSubjects(
  vaultId: string,
  canonicals: string[],
  scope: MemoryScope,
  scopeKey: string | null
): Promise<VaultSubject[]> {
  const uniqueCanonicals = Array.from(new Set(canonicals));
  if (uniqueCanonicals.length === 0) {
    return [];
  }

  const aliasLookupResult = await query<{ canonical: string; alias: string }>(
    `SELECT canonical, alias
     FROM entity_aliases
     WHERE vault_id = $1
       AND scope = $3
       AND scope_key IS NOT DISTINCT FROM $4::text
       AND (
         canonical = ANY($2::text[])
         OR alias = ANY($2::text[])
       )`,
    [vaultId, uniqueCanonicals, scope, scopeKey]
  );

  const canonicalByAlias = new Map(aliasLookupResult.rows.map((row) => [row.alias, row.canonical]));
  const resolvedCanonicals = uniqueCanonicals.map((canonical) => canonicalByAlias.get(canonical) ?? canonical);
  const uniqueResolvedCanonicals = Array.from(new Set(resolvedCanonicals));

  const aliasResult = await query<{ canonical: string; alias: string; embedding: string | null }>(
    `SELECT canonical, alias, embedding::text AS embedding
     FROM entity_aliases
     WHERE vault_id = $1
       AND scope = $3
       AND scope_key IS NOT DISTINCT FROM $4::text
       AND canonical = ANY($2::text[])`,
    [vaultId, uniqueResolvedCanonicals, scope, scopeKey]
  );

  const aliasRowsByCanonical = new Map<string, { alias: string; embedding: string | null }[]>();
  for (const row of aliasResult.rows) {
    const rows = aliasRowsByCanonical.get(row.canonical) ?? [];
    rows.push({ alias: row.alias, embedding: row.embedding });
    aliasRowsByCanonical.set(row.canonical, rows);
  }

  return uniqueResolvedCanonicals.map((canonical) => {
    const rows = aliasRowsByCanonical.get(canonical) ?? [];
    const aliases = rows
      .map((row) => row.alias)
      .filter((alias) => alias !== canonical);

    const canonicalRow = rows.find((row) => row.alias === canonical);
    let embedding: number[] | null = null;
    if (canonicalRow?.embedding) {
      embedding = parseEmbedding(canonicalRow.embedding);
    }

    return { canonical, aliases, embedding };
  });
}

function normaliseForMatching(s: string): string {
  return s
    .toLowerCase()
    .replace(/`[^`]+`/g, ' ')
    .replace(/'s\b/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function levenshtein(a: string, b: string): number {
  const m = a.length;
  const n = b.length;
  const dp: number[][] = Array.from({ length: m + 1 }, (_, i) =>
    Array.from({ length: n + 1 }, (_, j) => (i === 0 ? j : j === 0 ? i : 0))
  );
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] =
        a[i - 1] === b[j - 1]
          ? dp[i - 1][j - 1]
          : 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
    }
  }
  return dp[m][n];
}

export function resolveSubjectTier1(
  subject: string,
  subjectList: VaultSubject[],
  maxDistance: number
): string | null {
  const norm = normaliseForMatching(subject);
  const effectiveMax = Math.min(maxDistance, Math.floor(norm.length * 0.4));
  for (const vs of subjectList) {
    const normCanonical = normaliseForMatching(vs.canonical);
    if (norm === normCanonical || levenshtein(norm, normCanonical) <= effectiveMax) {
      return vs.canonical;
    }
    for (const alias of vs.aliases) {
      const normAlias = normaliseForMatching(alias);
      if (norm === normAlias || levenshtein(norm, normAlias) <= effectiveMax) {
        return vs.canonical;
      }
    }
  }
  return null;
}

function cosineSimilarity(a: number[], b: number[]): number {
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    magA += a[i] * a[i];
    magB += b[i] * b[i];
  }
  const denom = Math.sqrt(magA) * Math.sqrt(magB);
  return denom === 0 ? 0 : dot / denom;
}

export function resolveSubjectTier2(
  subjectEmbedding: number[],
  subjectList: VaultSubject[],
  highThreshold: number,
  lowThreshold: number
): { canonical: string; confidence: 'high' | 'ambiguous' } | null {
  let bestSim = -1;
  let bestCanonical = '';
  for (const vs of subjectList) {
    if (!vs.embedding) continue;
    const sim = cosineSimilarity(subjectEmbedding, vs.embedding);
    if (sim > bestSim) {
      bestSim = sim;
      bestCanonical = vs.canonical;
    }
  }
  if (bestCanonical === '') return null;
  if (bestSim >= highThreshold) return { canonical: bestCanonical, confidence: 'high' };
  if (bestSim >= lowThreshold) return { canonical: bestCanonical, confidence: 'ambiguous' };
  return null;
}

export async function storeCanonicalEmbedding(
  vaultId: string,
  canonical: string,
  embedding: number[],
  scope: MemoryScope,
  scopeKey: string | null,
  db: Queryable = { query }
): Promise<void> {
  const normalisedCanonical = normaliseSubject(canonical);
  await db.query(
    `INSERT INTO entity_aliases (vault_id, alias, canonical, embedding, scope, scope_key)
     VALUES ($1, $2, $2, $3::vector, $4, $5)
     ON CONFLICT (vault_id, scope, scope_key, alias)
     DO UPDATE SET embedding = EXCLUDED.embedding`,
    [vaultId, normalisedCanonical, JSON.stringify(embedding), scope, scopeKey]
  );
}

export async function storeSubjectAlias(
  vaultId: string,
  alias: string,
  canonical: string,
  scope: MemoryScope,
  scopeKey: string | null,
  db: Queryable = { query }
): Promise<void> {
  const normalisedAlias = normaliseSubject(alias);
  const normalisedCanonical = normaliseSubject(canonical);
  if (!normalisedAlias || !normalisedCanonical || normalisedAlias === normalisedCanonical) {
    return;
  }

  await db.query(
    `INSERT INTO entity_aliases (vault_id, alias, canonical, scope, scope_key)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (vault_id, scope, scope_key, alias)
     DO UPDATE SET canonical = EXCLUDED.canonical`,
    [vaultId, normalisedAlias, normalisedCanonical, scope, scopeKey]
  );
}

function parseEmbedding(value: string): number[] | null {
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map(Number) : null;
  } catch {
    return null;
  }
}
