/**
 * Capacity reserves space for every unarchived status, including future-dated
 * memories. An inclusive end date releases capacity on the following UTC day,
 * without waiting for the archival job. This is not an active inventory filter.
 */
export function memoryCapacityPredicateSql(alias: string): string {
  return `(${alias}.archived_at IS NULL
    AND (${alias}.valid_until IS NULL OR ${alias}.valid_until >= (now() AT TIME ZONE 'UTC')::date))`;
}
