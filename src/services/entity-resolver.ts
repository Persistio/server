import { query } from '../db/client';

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

export async function resolveCanonical(vaultId: string, subject: string): Promise<string | null> {
  const normalisedSubject = normaliseSubject(subject);
  const result = await query<{ canonical: string }>(
    `SELECT canonical
     FROM entity_aliases
     WHERE vault_id = $1
       AND alias = $2
     LIMIT 1`,
    [vaultId, normalisedSubject]
  );

  return result.rows[0]?.canonical ?? null;
}

export async function getVaultSubjectList(
  vaultId: string,
  topN: number,
  recentN: number
): Promise<VaultSubject[]> {
  // Top N by memory count
  const topResult = await query<{ canonical: string }>(
    `SELECT ea.canonical, COUNT(m.id) AS cnt
     FROM memories m
     JOIN entity_aliases ea ON ea.vault_id = m.vault_id AND ea.canonical = m.subject_hmac
     WHERE m.vault_id = $1 AND m.archived_at IS NULL
     GROUP BY ea.canonical
     ORDER BY cnt DESC
     LIMIT $2`,
    [vaultId, topN]
  );

  // Recent N by latest memory activity
  const recentResult = await query<{ canonical: string }>(
    `SELECT ea.canonical
     FROM memories m
     JOIN entity_aliases ea ON ea.vault_id = m.vault_id AND ea.canonical = m.subject_hmac
     WHERE m.vault_id = $1 AND m.archived_at IS NULL
     GROUP BY ea.canonical
     ORDER BY MAX(COALESCE(m.updated_at, m.created_at)) DESC NULLS LAST
     LIMIT $2`,
    [vaultId, recentN]
  );

  // Deduplicate
  const seen = new Set<string>();
  const canonicals: string[] = [];
  for (const row of [...topResult.rows, ...recentResult.rows]) {
    if (!seen.has(row.canonical)) {
      seen.add(row.canonical);
      canonicals.push(row.canonical);
    }
  }

  if (canonicals.length === 0) {
    return [];
  }

  const aliasResult = await query<{ canonical: string; alias: string; embedding: string | null }>(
    `SELECT canonical, alias, embedding::text AS embedding
     FROM entity_aliases
     WHERE vault_id = $1
       AND canonical = ANY($2::text[])`,
    [vaultId, canonicals]
  );

  const aliasRowsByCanonical = new Map<string, { alias: string; embedding: string | null }[]>();
  for (const row of aliasResult.rows) {
    const rows = aliasRowsByCanonical.get(row.canonical) ?? [];
    rows.push({ alias: row.alias, embedding: row.embedding });
    aliasRowsByCanonical.set(row.canonical, rows);
  }

  return canonicals.map((canonical) => {
    const rows = aliasRowsByCanonical.get(canonical) ?? [];
    const aliases = rows
      .map((row) => row.alias)
      .filter((alias) => alias !== canonical);

    const canonicalRow = rows.find((row) => row.alias === canonical);
    let embedding: number[] | null = null;
    if (canonicalRow?.embedding) {
      try {
        embedding = JSON.parse(canonicalRow.embedding) as number[];
      } catch {
        embedding = null;
      }
    }

    return { canonical, aliases, embedding };
  });
}

function normaliseForMatching(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
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
  embedding: number[]
): Promise<void> {
  await query(
    `INSERT INTO entity_aliases (vault_id, alias, canonical, embedding)
     VALUES ($1, $2, $2, $3::vector)
     ON CONFLICT (vault_id, alias)
     DO UPDATE SET embedding = EXCLUDED.embedding`,
    [vaultId, canonical, JSON.stringify(embedding)]
  );
}
