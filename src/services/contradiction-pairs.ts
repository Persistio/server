/** SQL fragments take server-owned aliases only, never caller text. History is a
 * discovery hint; only current exact revisions may describe recall uncertainty.
 * Neither predicate grants mutation authority or changes the send allowance. */
export function contradictionPairSql(log: string, a: string, b: string, revisions = true): string {
  const rev = (side: string, memory: string) => revisions ? ` AND ${log}.revision_${side}=${memory}.revision::bigint` : '';
  return `(${log}.vault_id=${a}.vault_id AND ${b}.vault_id=${a}.vault_id AND (
    (${log}.memory_id_a=${a}.id${rev('a',a)} AND ${log}.memory_id_b=${b}.id${rev('b',b)}) OR
    (${log}.memory_id_b=${a}.id${rev('b',a)} AND ${log}.memory_id_a=${b}.id${rev('a',b)})))`;
}
export function unresolvedPairSql(a: string, b: string, current = true): string {
  return `EXISTS(SELECT 1 FROM contradiction_scan_log unresolved
    WHERE ${contradictionPairSql('unresolved',a,b,current)}
      AND unresolved.decision='keep_both' AND unresolved.send_attempt IS NULL
      AND unresolved.revision_a IS NOT NULL AND unresolved.revision_b IS NOT NULL)
    AND NOT EXISTS(SELECT 1 FROM contradiction_scan_log resolved
      WHERE ${contradictionPairSql('resolved',a,b,current)} AND resolved.send_attempt IS NULL
        AND resolved.decision IN ('merge','discard_new','supersede_old'))`;
}
