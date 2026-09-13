import { createOperationalLogger } from '../operational-metadata';
const operationalLog=createOperationalLogger('contradiction-activation');
import { Client } from 'pg';

import { getConfig } from '../config';
import { AiBudgetDeferredError } from './usage';
import { CircuitBreakerOpenError } from './ai-resilience';
import { scanForContradictions } from './contradiction-scanner';
import type { ExtractorService } from './extractor';

const MAX_VAULTS_PER_TICK = 20;
const RETRY_BASE_MS = 60_000;
const MAX_RETRY_MS = 60 * 60_000;

interface ScheduledMemory {
  memory_id: string;
  generation: string;
  failures: number;
}

/** Only the periodic extraction loop calls this. Manual jobs cannot reset its budget. */
export async function drainDueContradictionActivations(extractor: ExtractorService): Promise<void> {
  const config = getConfig();
  if (!config.CONTRADICTION_SCAN_ENABLED || config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH <= 0) return;

  // AI accounting uses the normal pool. A dedicated connection avoids holding
  // its only connection during a model call when DB_POOL_MAX is one.
  const client = new Client({
    connectionString: config.DATABASE_URL,
    application_name: 'persistio:contradiction-activation',
    connectionTimeoutMillis: config.DB_POOL_CONNECTION_TIMEOUT_MS
  });
  // A server-side disconnect can arrive while the model is running and no query
  // is active. Handle pg's error event without replacing the failed connection;
  // every subsequent read/write must fail on this same lock-owning client.
  client.on('error', (error: Error) => {
    operationalLog.warn(JSON.stringify({ level: 40, msg: 'contradiction activation connection lost',
      error_type: error.name }));
  });
  const budget = { remaining: config.CONTRADICTION_MAX_ARBITRATIONS_PER_BATCH };
  try {
    await client.connect();
    const vaults = await client.query<{ vault_id: string; revision: string }>(
      `SELECT vault_id, revision::text FROM memory_contradiction_pending_vaults
       WHERE pending_count > 0 AND next_visit_at <= now()
       ORDER BY next_visit_at, vault_id LIMIT $1`,
      [MAX_VAULTS_PER_TICK]
    );
    for (const { vault_id: vaultId, revision } of vaults.rows) {
      if (budget.remaining <= 0) break;
      const lockKey = `persistio:contradiction-activation:${vaultId}`;
      const lock = await client.query<{ locked: boolean }>(
        'SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked', [lockKey]
      );
      if (!lock.rows[0]?.locked) {
        // A group of busy oldest vaults must not permanently occupy the bounded
        // shortlist. Let other pending heads precede them on the next tick.
        await client.query(
          `UPDATE memory_contradiction_pending_vaults SET next_visit_at = now() + interval '1 second'
           WHERE vault_id = $1 AND revision = $2::bigint `, [vaultId, revision]
        );
        continue;
      }
      try {
        // One memory per selected vault gives independent vaults a turn even
        // when a single tenant has a large backlog. Partial scans stay durable.
        const due = await client.query<ScheduledMemory>(
          `SELECT memory_id, generation, failures FROM memory_contradiction_schedule
           WHERE vault_id = $1 AND available_at <= now()
           ORDER BY available_at, memory_id LIMIT 1`, [vaultId]
        );
        const memory = due.rows[0];
        if (memory) {
          try {
            const result = await scanForContradictions(vaultId, [memory.memory_id], extractor, {
              client, budget, maxArbitrations: 1
            });
            if (result.completedMemoryIds.includes(memory.memory_id)) {
              await client.query(
                'DELETE FROM memory_contradiction_schedule WHERE memory_id = $1 AND generation = $2',
                [memory.memory_id, memory.generation]
              );
            } else {
              await deferMemory(client, memory, RETRY_BASE_MS, false);
            }
          } catch (error) {
            // Every failure remains retryable. Provider outages and malformed
            // neighboring rows must never permanently discard this reminder.
            const delay = error instanceof AiBudgetDeferredError
              ? Math.max(RETRY_BASE_MS, error.availableAt.getTime() - Date.now())
              : error instanceof CircuitBreakerOpenError
                ? Math.max(RETRY_BASE_MS, error.retryAfterMs)
                : Math.min(MAX_RETRY_MS, RETRY_BASE_MS * 2 ** memory.failures);
            await deferMemory(client, memory, delay, true);
            operationalLog.warn(JSON.stringify({ level: 40, msg: 'contradiction activation deferred',
              vault_id: vaultId,
              error_type: error instanceof Error ? error.name : 'unknown' }));
          }
        }
        await scheduleNextVaultVisit(client, vaultId);
      } finally {
        await client.query('SELECT pg_advisory_unlock(hashtextextended($1, 0))', [lockKey]);
      }
    }
  } finally {
    await client.end();
  }
}

async function deferMemory(client: Client, memory: ScheduledMemory, delayMs: number, failed: boolean): Promise<void> {
  await client.query(
    `UPDATE memory_contradiction_schedule
     SET available_at = now() + ($3::bigint * interval '1 millisecond'),
         failures = CASE WHEN $4 THEN LEAST(failures + 1, 10) ELSE 0 END
     WHERE memory_id = $1 AND generation = $2`,
    [memory.memory_id, memory.generation, delayMs, failed]
  );
}

async function scheduleNextVaultVisit(client: Client, vaultId: string): Promise<void> {
  // Read the revision before computing the next date. Any concurrent enqueue,
  // replacement or removal invalidates this update and preserves its wakeup.
  const state = await client.query<{ revision: string }>(
    'SELECT revision::text FROM memory_contradiction_pending_vaults WHERE vault_id = $1', [vaultId]
  );
  if (!state.rows[0]) return;
  const next = await client.query<{ available_at: Date }>(
    `SELECT available_at FROM memory_contradiction_schedule
     WHERE vault_id = $1
     ORDER BY available_at, memory_id LIMIT 1`, [vaultId]
  );
  await client.query(
    `UPDATE memory_contradiction_pending_vaults
     SET next_visit_at = GREATEST(now() + interval '1 second', COALESCE($3::timestamptz, now()))
     WHERE vault_id = $1 AND revision = $2::bigint`,
    [vaultId, state.rows[0].revision, next.rows[0]?.available_at ?? null]
  );
}
