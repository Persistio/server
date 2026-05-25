import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

import { getConfig, getConfiguredEmbeddingDimensions } from '../config';
import { query } from '../db/client';
import { createApiKey, requireAdminAuth } from '../middleware/auth';
import { generateAndWrapDek } from '../services/crypto';

const createVaultSchema = z.object({
  name: z.string().min(1),
  purpose: z.string().min(1).max(500).optional(),
  plan: z.string().min(1).optional()
}).strict();

const updateVaultSchema = z.object({
  name: z.string().min(1).optional(),
  purpose: z.string().min(1).max(500).nullable().optional(),
  plan: z.string().min(1).optional(),
  status: z.enum(['active', 'disabled']).optional()
}).strict().refine((value) => value.name !== undefined || value.purpose !== undefined || value.plan !== undefined || value.status !== undefined, {
  message: 'At least one field must be provided'
});

const createPlanSchema = z.object({
  id: z.string().min(1),
  limits: z.record(z.unknown()).default({})
}).strict();

const updatePlanSchema = z.object({
  limits: z.record(z.unknown())
}).strict();

async function upsertPlan(planId: string, limits: Record<string, unknown>): Promise<{ id: string; limits: Record<string, unknown> }> {
  const result = await query<{ id: string; limits: Record<string, unknown> }>(
    `INSERT INTO plans (id, limits)
     VALUES ($1, $2::jsonb)
     ON CONFLICT (id) DO UPDATE SET
       limits = EXCLUDED.limits
     RETURNING id, limits`,
    [planId, JSON.stringify(limits)]
  );

  return result.rows[0];
}

async function planExists(planId: string): Promise<boolean> {
  const result = await query<{ id: string }>(
    `SELECT id
     FROM plans
     WHERE id = $1
     LIMIT 1`,
    [planId]
  );

  return Boolean(result.rowCount);
}

export async function registerAdminRoutes(app: FastifyInstance) {
  app.get('/admin/plans', { preHandler: requireAdminAuth }, async () => {
    const result = await query<{ id: string; limits: Record<string, unknown> }>(
      `SELECT id, limits
       FROM plans
       ORDER BY id ASC`
    );

    return { items: result.rows };
  });

  app.get('/admin/plans/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().min(1) }).parse(request.params);
    const result = await query<{ id: string; limits: Record<string, unknown> }>(
      `SELECT id, limits
       FROM plans
       WHERE id = $1
       LIMIT 1`,
      [id]
    );

    if (!result.rowCount) return reply.code(404).send({ error: 'Plan not found' });
    return result.rows[0];
  });

  app.post('/admin/plans', { preHandler: requireAdminAuth }, async (request, reply) => {
    const parsedBody = createPlanSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    const plan = await upsertPlan(body.id, body.limits);
    return reply.code(200).send(plan);
  });

  app.patch('/admin/plans/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().min(1) }).parse(request.params);
    const parsedBody = updatePlanSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    const result = await query<{ id: string; limits: Record<string, unknown> }>(
      `UPDATE plans
       SET limits = $2::jsonb
       WHERE id = $1
       RETURNING id, limits`,
      [id, JSON.stringify(body.limits)]
    );

    if (!result.rowCount) return reply.code(404).send({ error: 'Plan not found' });
    return result.rows[0];
  });

  app.delete('/admin/plans/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().min(1) }).parse(request.params);
    const usage = await query<{ count: string }>(
      `SELECT COUNT(*)::text AS count
       FROM vaults
       WHERE plan_id = $1`,
      [id]
    );

    if (Number(usage.rows[0]?.count ?? 0) > 0) {
      return reply.code(409).send({ error: 'Plan is in use' });
    }

    const result = await query<{ id: string }>(
      `DELETE FROM plans
       WHERE id = $1
       RETURNING id`,
      [id]
    );

    if (!result.rowCount) return reply.code(404).send({ error: 'Plan not found' });
    return { id: result.rows[0].id, deleted: true };
  });

  app.post('/admin/vaults', { preHandler: requireAdminAuth }, async (request, reply) => {
    const parsedBody = createVaultSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    const apiKey = createApiKey();
    const planId = body.plan ?? 'unlimited';
    if (!(await planExists(planId))) {
      return reply.code(404).send({ error: 'Plan not found' });
    }
    const encryptedDek = getConfig().ENCRYPTION_ENABLED
      ? (await generateAndWrapDek()).encryptedDek
      : null;

    const result = await query<{ id: string; name: string; purpose: string | null; plan_id: string; status: string; created_at: string }>(
      `INSERT INTO vaults (name, purpose, plan_id, api_key_hash, settings, encrypted_dek, vault_encryption_enabled)
       VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
       RETURNING id, name, purpose, plan_id, status, created_at`,
      [
        body.name,
        body.purpose ?? null,
        planId,
        apiKey.hash,
        JSON.stringify({
          embedding_dimensions: getConfiguredEmbeddingDimensions()
        }),
        encryptedDek,
        encryptedDek !== null
      ]
    );

    return reply.code(201).send({
      id: result.rows[0].id,
      name: result.rows[0].name,
      purpose: result.rows[0].purpose,
      plan: result.rows[0].plan_id,
      status: result.rows[0].status,
      api_key: apiKey.rawKey
    });
  });

  app.post('/admin/vaults/:id/rotate-key', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const apiKey = createApiKey();
    const result = await query<{ id: string }>(
      `UPDATE vaults SET api_key_hash = $1 WHERE id = $2 RETURNING id`,
      [apiKey.hash, id]
    );
    if (!result.rowCount) return reply.code(404).send({ error: 'Vault not found' });
    return reply.code(200).send({ id: result.rows[0].id, api_key: apiKey.rawKey });
  });

  app.patch('/admin/vaults/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const parsedBody = updateVaultSchema.safeParse(request.body);
    if (!parsedBody.success) return reply.code(400).send({ error: 'Invalid request body' });

    const body = parsedBody.data;
    if (body.plan && !(await planExists(body.plan))) {
      return reply.code(404).send({ error: 'Plan not found' });
    }
    const result = await query(
      `UPDATE vaults
       SET name = COALESCE($2, name),
           purpose = CASE
             WHEN $3::boolean THEN $4
             ELSE purpose
           END,
           plan_id = COALESCE($5, plan_id),
           status = COALESCE($6, status)
       WHERE id = $1
       RETURNING id, name, purpose, created_at, settings, plan_id, status, account_id, vault_encryption_enabled`,
      [id, body.name ?? null, body.purpose !== undefined, body.purpose ?? null, body.plan ?? null, body.status ?? null]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Vault not found' });
    }

    return result.rows[0];
  });

  app.delete('/admin/vaults/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query(
      `DELETE FROM vaults
       WHERE id = $1
       RETURNING id`,
      [params.id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Vault not found' });
    }

    return { id: result.rows[0].id, deleted: true };
  });

  app.get('/admin/vaults/:id', { preHandler: requireAdminAuth }, async (request, reply) => {
    const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query(
      `SELECT id, name, purpose, created_at, settings, plan_id, status, account_id, vault_encryption_enabled
       FROM vaults
       WHERE id = $1`,
      [id]
    );

    if (!result.rowCount) {
      return reply.code(404).send({ error: 'Vault not found' });
    }

    return result.rows[0];
  });

  app.get('/admin/vaults', { preHandler: requireAdminAuth }, async () => {
    const result = await query(
      `SELECT id, name, purpose, created_at, settings, plan_id, status, account_id, vault_encryption_enabled
       FROM vaults
       ORDER BY created_at DESC`
    );
    return { items: result.rows };
  });
}
