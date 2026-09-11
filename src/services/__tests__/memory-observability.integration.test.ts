import crypto from 'node:crypto';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Pool, type PoolClient } from 'pg';

const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;
const describeWithPostgres = describe.skipIf(!databaseUrl);

describeWithPostgres('immutable memory observability (PostgreSQL)', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaultId = crypto.randomUUID();
  const segmentId = crypto.randomUUID();
  const memoryId = crypto.randomUUID();
  const deliveryId = crypto.randomUUID();
  let client: PoolClient;

  beforeAll(async () => {
    client = await pool.connect();
    const migration = await client.query<{ applied: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE filename = '052_memory_observability.sql') AS applied`
    );
    if (!migration.rows[0]?.applied) throw new Error('Test database must be migrated through 052_memory_observability.sql');
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO vaults (id, name, api_key_hash) VALUES ($1, $2, $3)`,
      [vaultId, `observability-${vaultId}`, crypto.randomUUID()]
    );
    await client.query(
      `INSERT INTO segments (id, vault_id, session_id, chunk_ids) VALUES ($1, $2, 'observability', '{}'::uuid[])`,
      [segmentId, vaultId]
    );
  });

  afterAll(async () => {
    await client.query('ROLLBACK');
    client.release();
    await pool.end();
  });

  it('records every substantive transition without retaining memory plaintext', async () => {
    await client.query(
      `INSERT INTO memories (
         id, vault_id, data, subject, hash, scope, status, type, evidence, source_segment_id
       ) VALUES ($1, $2, 'sensitive value', 'sensitive subject', $3, 'global', 'candidate', 'system_fact', '{}'::jsonb, $4)`,
      [memoryId, vaultId, crypto.randomUUID(), segmentId]
    );
    await client.query(`UPDATE memories SET confidence = 0.8 WHERE id = $1`, [memoryId]);
    await client.query(`UPDATE memories SET recall_count = 1, last_recalled = now(), updated_at = now() WHERE id = $1`, [memoryId]);

    const events = await client.query<{
      event_type: string;
      changed_fields: string[];
      source: string;
      database_application: string;
      statement_hash: string;
      after_state: Record<string, unknown> | null;
    }>(
      `SELECT event_type, changed_fields, source, database_application, statement_hash, after_state
       FROM memory_mutation_events WHERE vault_id = $1 AND memory_id = $2 ORDER BY occurred_at, id`,
      [vaultId, memoryId]
    );
    expect(events.rows.map((event) => event.event_type)).toEqual(['create', 'decay']);
    expect(events.rows.every((event) => event.source === 'database')).toBe(true);
    expect(events.rows.every((event) => event.database_application === 'unknown')).toBe(true);
    expect(events.rows.every((event) => /^[0-9a-f]{64}$/.test(event.statement_hash))).toBe(true);
    expect(events.rows[0].after_state).toHaveProperty('data_hash');
    expect(events.rows[0].after_state).toHaveProperty('subject_hash');
    expect(events.rows[0].after_state).not.toHaveProperty('data');
    expect(events.rows[0].after_state).not.toHaveProperty('subject');
    expect(events.rows[0].after_state).not.toHaveProperty('evidence');
    expect(events.rows[1].changed_fields).toContain('confidence');
  });

  it('reconstructs an exact selected, returned, and rendered delivery', async () => {
    await client.query('SAVEPOINT invalid_delivery_count');
    await expect(client.query(
      `INSERT INTO memory_delivery_runs (
         id, vault_id, query_hash, response_format, mode, top_k, min_similarity,
         global_rule_policy, include_global_rules_requested, include_global_rules_effective,
         selected_count, global_selected_count
       ) VALUES ($1, $2, $3, 'bundle_v2', 'agent', 10, 0.5, 'approved_only', false, false, 0, 1)`,
      [crypto.randomUUID(), vaultId, 'b'.repeat(64)]
    )).rejects.toThrow();
    await client.query('ROLLBACK TO SAVEPOINT invalid_delivery_count');

    await client.query(
      `INSERT INTO memory_delivery_runs (
         id, vault_id, query_hash, response_format, mode, client_name, client_version,
         session_id, trigger_type, top_k, min_similarity, global_rule_policy,
         include_global_rules_requested, include_global_rules_effective,
         selected_count, global_selected_count
       ) VALUES ($1, $2, $3, 'bundle_v2', 'agent', 'test-client', '1.0.0',
                 'session', 'direct', 10, 0.5, 'legacy', true, true, 1, 1)`,
      [deliveryId, vaultId, 'c'.repeat(64)]
    );
    for (const stage of ['selected', 'returned']) {
      await client.query(
        `INSERT INTO memory_delivery_events (
           delivery_id, vault_id, memory_id, authority_version, stage, section,
           memory_type, retrieval_reason, scope, authority_state, authority_required,
           authority_approval_valid, similarity
         ) VALUES ($1, $2, $3, 1, $4, 'historical_facts', 'user_rule',
                   'global_behavioral', 'global', 'proposed', true, false, 0.75)`,
        [deliveryId, vaultId, memoryId, stage]
      );
    }
    await client.query('SAVEPOINT missing_render_target');
    await expect(client.query(
      `INSERT INTO memory_delivery_events (
         delivery_id, vault_id, memory_id, authority_version, stage, section,
         memory_type, retrieval_reason, scope, authority_state, authority_required,
         authority_approval_valid, similarity, token_budget, rendered_tokens, truncated
       ) VALUES ($1, $2, $3, 1, 'rendered', 'historical_facts', 'user_rule',
                 'global_behavioral', 'global', 'proposed', true, false, 0.75, 100, 75, false)`,
      [deliveryId, vaultId, memoryId]
    )).rejects.toThrow();
    await client.query('ROLLBACK TO SAVEPOINT missing_render_target');
    await client.query(
      `INSERT INTO memory_delivery_events (
         delivery_id, vault_id, memory_id, authority_version, stage, section,
         memory_type, retrieval_reason, scope, authority_state, authority_required,
         authority_approval_valid, similarity, render_target, token_budget, rendered_tokens, truncated
       ) VALUES ($1, $2, $3, 1, 'rendered', 'historical_facts', 'user_rule',
                 'global_behavioral', 'global', 'proposed', true, false, 0.75,
                 'prompt_context', 100, 75, false)`,
      [deliveryId, vaultId, memoryId]
    );

    const timeline = await client.query<{ stage: string; render_target: string | null }>(
      `SELECT stage, render_target FROM memory_delivery_events
       WHERE delivery_id = $1 ORDER BY occurred_at, stage`, [deliveryId]
    );
    expect(timeline.rows.map((row) => row.stage).sort()).toEqual(['rendered', 'returned', 'selected']);
    expect(timeline.rows.find((row) => row.stage === 'rendered')?.render_target).toBe('prompt_context');
  });

  it('preserves curation and mutation evidence after source and vault deletion', async () => {
    const destructiveAuditFks = await client.query<{ table_name: string; constraint_name: string }>(
      `SELECT table_name, constraint_name
       FROM information_schema.table_constraints
       WHERE constraint_type='FOREIGN KEY'
         AND table_name=ANY($1::text[])`,
      [[
        'memory_authority_events', 'memory_scope_change_log', 'contradiction_scan_log',
        'curation_action_log', 'curation_review_runs', 'curation_dead_letter',
        'extraction_dead_letter'
      ]]
    );
    expect(destructiveAuditFks.rows).toEqual([]);
    const referenceGuards = await client.query<{ event_object_table: string }>(
      `SELECT DISTINCT event_object_table
       FROM information_schema.triggers
       WHERE trigger_name LIKE '%_reference_guard'
         AND event_object_table=ANY($1::text[])
       ORDER BY event_object_table`,
      [[
        'contradiction_scan_log', 'curation_action_log', 'curation_review_runs',
        'curation_dead_letter', 'extraction_dead_letter'
      ]]
    );
    expect(referenceGuards.rows.map((row) => row.event_object_table)).toEqual([
      'contradiction_scan_log', 'curation_action_log', 'curation_dead_letter',
      'curation_review_runs', 'extraction_dead_letter'
    ]);
    await client.query('SAVEPOINT invalid_retained_audit_reference');
    await expect(client.query(
      `INSERT INTO contradiction_scan_log (
         vault_id, memory_id_a, memory_id_b, decision, similarity
       ) VALUES ($1, $2, $3, 'needs_review', 0.95)`,
      [vaultId, memoryId, crypto.randomUUID()]
    )).rejects.toThrow(/must belong to vault/);
    await client.query('ROLLBACK TO SAVEPOINT invalid_retained_audit_reference');

    const action = await client.query<{ id: string }>(
      `INSERT INTO curation_action_log (
         vault_id, segment_id, action_type, memory_id, raw_curator_response, applied_at
       ) VALUES ($1, $2, 'update', $3, '{}'::jsonb, now()) RETURNING id`,
      [vaultId, segmentId, memoryId]
    );
    const review = await client.query<{ id: string }>(
      `INSERT INTO curation_review_runs (
         vault_id, segment_id, model, schema_version, prompt_version, prompt_hash,
         validation_status, raw_response
       ) VALUES ($1, $2, 'test', 'v1', 'v1', $3, 'valid', '{}'::jsonb) RETURNING id`,
      [vaultId, segmentId, 'a'.repeat(64)]
    );
    const scopeChange = await client.query<{ id: string }>(
      `INSERT INTO memory_scope_change_log (
         vault_id, memory_id, old_scope, new_scope, actor_type, source, reason
       ) VALUES ($1, $2, 'session', 'global', 'system', 'system', 'retention fixture')
       RETURNING id`,
      [vaultId, memoryId]
    );
    const authority = await client.query<{ id: string }>(
      `INSERT INTO memory_authority_events (
         vault_id, memory_id, event_type, old_state, new_state, old_version,
         new_version, actor_type, source, reason
       ) VALUES ($1, $2, 'invalidate', 'proposed', 'proposed', 1, 2,
                 'system', 'system', 'retention fixture')
       RETURNING id`,
      [vaultId, memoryId]
    );
    const contradiction = await client.query<{ id: string }>(
      `INSERT INTO contradiction_scan_log (
         vault_id, memory_id_a, memory_id_b, decision, similarity
       ) VALUES ($1, $2, $2, 'needs_review', 0.95) RETURNING id`,
      [vaultId, memoryId]
    );
    const curationDeadLetter = await client.query<{ id: string }>(
      `INSERT INTO curation_dead_letter (
         vault_id, segment_id, retry_count, last_error
       ) VALUES ($1, $2, 3, 'retention fixture') RETURNING id`,
      [vaultId, segmentId]
    );
    const extractionDeadLetter = await client.query<{ id: string }>(
      `INSERT INTO extraction_dead_letter (
         vault_id, segment_id, retry_count, last_error
       ) VALUES ($1, $2, 3, 'retention fixture') RETURNING id`,
      [vaultId, segmentId]
    );
    await client.query(
      `UPDATE curation_review_runs SET before_state = '{"locked":true}'::jsonb
       WHERE id = $1 AND validation_status = 'valid'`,
      [review.rows[0].id]
    );
    await client.query(
      `UPDATE curation_review_runs
       SET validation_status = 'applied', after_state = '{"applied":true}'::jsonb, applied_at = now()
       WHERE id = $1 AND validation_status = 'valid'`,
      [review.rows[0].id]
    );
    await client.query('SAVEPOINT tamper_terminal_review');
    await expect(client.query(
      `UPDATE curation_review_runs SET raw_response = '{"tampered":true}'::jsonb WHERE id = $1`,
      [review.rows[0].id]
    )).rejects.toThrow(/not permitted/);
    await client.query('ROLLBACK TO SAVEPOINT tamper_terminal_review');

    await client.query(`DELETE FROM segments WHERE id = $1`, [segmentId]);
    for (const [table, id] of [
      ['curation_action_log', action.rows[0].id],
      ['curation_review_runs', review.rows[0].id],
      ['curation_dead_letter', curationDeadLetter.rows[0].id],
      ['extraction_dead_letter', extractionDeadLetter.rows[0].id]
    ]) {
      expect((await client.query(`SELECT 1 FROM ${table} WHERE id = $1`, [id])).rowCount).toBe(1);
    }

    await client.query(`DELETE FROM vaults WHERE id = $1`, [vaultId]);
    expect((await client.query(
      `SELECT 1 FROM memory_mutation_events WHERE vault_id = $1 AND memory_id = $2`, [vaultId, memoryId]
    )).rowCount).toBeGreaterThan(0);
    expect((await client.query(`SELECT 1 FROM memory_delivery_runs WHERE id = $1`, [deliveryId])).rowCount).toBe(1);
    expect((await client.query(`SELECT 1 FROM memory_delivery_events WHERE delivery_id = $1`, [deliveryId])).rowCount).toBe(3);
    for (const [table, id] of [
      ['memory_scope_change_log', scopeChange.rows[0].id],
      ['memory_authority_events', authority.rows[0].id],
      ['contradiction_scan_log', contradiction.rows[0].id],
      ['curation_action_log', action.rows[0].id],
      ['curation_review_runs', review.rows[0].id],
      ['curation_dead_letter', curationDeadLetter.rows[0].id],
      ['extraction_dead_letter', extractionDeadLetter.rows[0].id]
    ]) {
      expect((await client.query(`SELECT 1 FROM ${table} WHERE id = $1`, [id])).rowCount).toBe(1);
    }
    await expect(client.query(
      `DELETE FROM memory_mutation_events WHERE vault_id = $1 AND memory_id = $2`, [vaultId, memoryId]
    )).rejects.toThrow(/append-only/);
  });
});
