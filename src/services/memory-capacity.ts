/**
 * Every retained unarchived memory occupies capacity. Applicability ending does
 * not erase historical knowledge or release its storage allocation.
 */
export function memoryCapacityPredicateSql(alias: string): string {
  return `(${alias}.archived_at IS NULL)`;
}
