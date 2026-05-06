import crypto from 'node:crypto';
import type { FastifyReply, FastifyRequest } from 'fastify';

import { getConfig } from '../config';
import { query } from '../db/client';

export interface VaultContext {
  id: string;
  name: string;
  purpose: string | null;
  settings: Record<string, unknown>;
  plan_id: string;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
}

declare module 'fastify' {
  interface FastifyRequest {
    vault: VaultContext;
  }
}

function hashKey(rawKey: string): string {
  return crypto.createHash('sha256').update(rawKey).digest('hex');
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

function getBearerToken(request: FastifyRequest): string | undefined {
  const authorization = request.headers.authorization;
  if (!authorization) {
    return undefined;
  }

  const match = authorization.match(/^Bearer\s+(.+)$/i);
  return match?.[1];
}

export async function requireVaultAuth(request: FastifyRequest, reply: FastifyReply) {
  const rawKey = getBearerToken(request);

  if (!rawKey) {
    return reply.code(401).send({ error: 'Unauthorized' });
  }

  const apiKeyHash = hashKey(rawKey);
  const result = await query<VaultContext>(
    `SELECT id, name, purpose, settings, plan_id, encrypted_dek, vault_encryption_enabled
     FROM vaults
     WHERE api_key_hash = $1
     LIMIT 1`,
    [apiKeyHash]
  );

  if (!result.rowCount) {
    return reply.code(401).send({ error: 'Unauthorized' });
  }

  request.vault = result.rows[0];
}

export async function requireAdminAuth(request: FastifyRequest, reply: FastifyReply) {
  const config = getConfig();
  const headerKey = request.headers['x-admin-key'];
  const candidate = typeof headerKey === 'string' ? headerKey : getBearerToken(request);

  if (!candidate || !timingSafeEqual(candidate, config.ADMIN_API_KEY)) {
    return reply.code(401).send({ error: 'Unauthorized' });
  }
}

export function createApiKey() {
  const rawKey = crypto.randomBytes(24).toString('hex');
  return {
    rawKey,
    hash: hashKey(rawKey)
  };
}
