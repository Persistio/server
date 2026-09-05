import crypto from 'node:crypto';
import type { FastifyReply, FastifyRequest } from 'fastify';
import type { Token as Auth0Token } from '@auth0/auth0-fastify-api';

import { getConfig } from '../config';
import { parsePlatformOAuthClientPolicies, type OAuthRoutePermission } from '../oauth-client-policies';

export interface VaultContext {
  id: string;
  name: string;
  purpose: string | null;
  settings: Record<string, unknown>;
  plan_id: string;
  status: string;
  account_id: string | null;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
}

export type PlatformAuthMethod = 'api_key' | 'oauth';
export type PlatformActorType = 'service' | 'user' | 'system';

export interface PlatformAuthActor {
  type: PlatformActorType;
  id: string | null;
}

export interface PlatformAuthContext {
  method: PlatformAuthMethod;
  subject: string;
  client_id: string | null;
  scopes: string[];
  account_id: string | null;
  vault_id: string | null;
  actor: PlatformAuthActor | null;
}

declare module 'fastify' {
  interface FastifyRequest {
    auth: PlatformAuthContext;
    vault: VaultContext;
  }
}

declare module '@auth0/auth0-fastify-api' {
  interface Token {
    permissions?: unknown;
    client_id?: string;
    azp?: string;
    account_id?: string;
    vault_id?: string;
    [claim: string]: unknown;
  }
}

interface AuthenticateOptions {
  adminScopes?: string[];
  allowAdminApiKey?: boolean;
  allowVaultApiKey?: boolean;
  allowOAuth?: boolean;
}

interface OAuthClientPolicy {
  clientId: string;
  allowedScopes: Set<string>;
  allowDelegatedHeaders: boolean;
  requireAccountContext: boolean;
  allowGlobalAccess: boolean;
  routePermissions: Set<OAuthRoutePermission>;
}

const accountIdClaim = 'https://persistio.ai/account_id';
const vaultIdClaim = 'https://persistio.ai/vault_id';
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const delegatedHeaderNames = [
  'x-persistio-actor-type',
  'x-persistio-actor-id',
  'x-persistio-account-id'
] as const;
function hashKey(rawKey: string): string {
  return crypto.createHash('sha256').update(rawKey).digest('hex');
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

function getBearerToken(request: FastifyRequest): string | undefined {
  const authorization = request.headers.authorization;
  if (!authorization) return undefined;
  const match = authorization.match(/^Bearer\s+(.+)$/i);
  return match?.[1];
}

function isJwtCredential(token: string): boolean {
  return token.split('.').length === 3;
}

function getHeaderString(request: FastifyRequest, name: string): string | null {
  const value = request.headers[name.toLowerCase()];
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function hasDelegatedHeaders(request: FastifyRequest): boolean {
  return delegatedHeaderNames.some((name) => getHeaderString(request, name));
}

function normalizeUuid(value: string | null, label: string): string | null {
  if (!value) return null;
  if (!uuidPattern.test(value)) {
    throw new Error(`${label} must be a UUID`);
  }
  return value.toLowerCase();
}

function getStringClaim(payload: Auth0Token, claim: string): string | null {
  const value = payload[claim];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function getClientId(payload: Auth0Token): string | null {
  return payload.client_id ?? payload.azp ?? null;
}

function getScopes(payload: Auth0Token): string[] {
  const scopes = new Set<string>();
  if (typeof payload.scope === 'string') {
    for (const scope of payload.scope.split(/\s+/)) {
      if (scope) scopes.add(scope);
    }
  }

  if (Array.isArray(payload.permissions)) {
    for (const permission of payload.permissions) {
      if (typeof permission === 'string' && permission.length > 0) {
        scopes.add(permission);
      }
    }
  }

  return [...scopes];
}

function hasRequiredScope(auth: PlatformAuthContext, requiredScopes: string[] | undefined): boolean {
  if (!requiredScopes?.length) return true;
  return requiredScopes.some((scope) => auth.scopes.includes(scope));
}

function routePermissionForScopes(requiredScopes: string[] | undefined): OAuthRoutePermission {
  if (!requiredScopes?.length) return 'admin';
  if (requiredScopes.some((scope) => scope.startsWith('platform:plans:'))) return 'plans';
  if (requiredScopes.some((scope) => scope.startsWith('platform:vaults:') || scope.startsWith('platform:vault_keys:'))) return 'vaults';
  if (requiredScopes.some((scope) => scope.startsWith('platform:analytics:'))) return 'analytics';
  return 'admin';
}

function routePermissionsForAllowedScopes(scopes: Set<string>): Set<OAuthRoutePermission> {
  const permissions = new Set<OAuthRoutePermission>();
  for (const scope of scopes) {
    if (scope === 'platform:admin') permissions.add('admin');
    if (scope.startsWith('platform:plans:')) permissions.add('plans');
    if (scope.startsWith('platform:vaults:') || scope.startsWith('platform:vault_keys:')) permissions.add('vaults');
    if (scope.startsWith('platform:analytics:')) permissions.add('analytics');
  }
  return permissions;
}

function buildClientPolicies(): Map<string, OAuthClientPolicy> {
  const policies = new Map<string, OAuthClientPolicy>();

  for (const policyConfig of parsePlatformOAuthClientPolicies(getConfig().PLATFORM_OAUTH_CLIENT_POLICIES)) {
    const allowedScopes = new Set(policyConfig.allowed_scopes);
    policies.set(policyConfig.client_id, {
      clientId: policyConfig.client_id,
      allowedScopes,
      allowDelegatedHeaders: policyConfig.allow_delegation,
      requireAccountContext: policyConfig.require_account_context,
      allowGlobalAccess: policyConfig.allow_global_access,
      routePermissions: routePermissionsForAllowedScopes(allowedScopes)
    });
  }

  return policies;
}

function getClientPolicy(auth: PlatformAuthContext): OAuthClientPolicy {
  if (!auth.client_id) {
    throw new Error('Missing OAuth client id');
  }

  const policy = buildClientPolicies().get(auth.client_id);
  if (!policy) {
    throw new Error('OAuth client is not allowed');
  }

  return policy;
}

function enforceClientPolicy(request: FastifyRequest, auth: PlatformAuthContext, requiredScopes: string[] | undefined) {
  const policy = getClientPolicy(auth);
  const routePermission = routePermissionForScopes(requiredScopes);

  if (!policy.routePermissions.has(routePermission) && !policy.routePermissions.has('admin')) {
    throw new Error('OAuth client is not allowed for route');
  }

  for (const scope of auth.scopes) {
    if (!policy.allowedScopes.has(scope)) {
      throw new Error('OAuth client scope is not allowed');
    }
  }

  if (!hasRequiredScope(auth, requiredScopes)) {
    throw new Error('Missing required scope');
  }

  if (hasDelegatedHeaders(request) && !policy.allowDelegatedHeaders) {
    throw new Error('OAuth client cannot send delegated headers');
  }

  const accountScopedRoute = routePermission === 'vaults' || routePermission === 'analytics';
  if (policy.requireAccountContext && accountScopedRoute && !auth.account_id) {
    throw new Error('OAuth client requires delegated account context');
  }

  if (!policy.allowGlobalAccess && accountScopedRoute && !auth.account_id) {
    throw new Error('OAuth client cannot make global account-less calls');
  }
}

function requestedRoute(request: FastifyRequest): string {
  return request.routeOptions?.url ?? request.url;
}

function denialReason(error: unknown): string {
  if (!(error instanceof Error)) return 'unknown';

  switch (error.message) {
    case 'Missing credentials':
      return 'missing_credentials';
    case 'OAuth is disabled':
      return 'oauth_disabled';
    case 'OAuth verifier is not configured':
      return 'oauth_verifier_not_configured';
    case 'OAuth is not allowed for this route':
      return 'oauth_not_allowed_for_route';
    case 'Missing subject':
      return 'missing_subject';
    case 'Missing OAuth client id':
      return 'missing_oauth_client_id';
    case 'OAuth client is not allowed':
      return 'oauth_client_not_allowed';
    case 'OAuth client is not allowed for route':
      return 'oauth_client_route_not_allowed';
    case 'OAuth client scope is not allowed':
      return 'oauth_scope_not_allowed';
    case 'Missing required scope':
      return 'missing_required_scope';
    case 'OAuth client cannot send delegated headers':
      return 'delegation_not_allowed';
    case 'OAuth client requires delegated account context':
      return 'delegated_account_required';
    case 'OAuth client cannot make global account-less calls':
      return 'global_access_not_allowed';
    case 'Delegated account does not match token account':
      return 'delegated_account_mismatch';
    case 'Invalid admin API key':
      return 'invalid_admin_api_key';
    case 'Invalid vault API key':
      return 'invalid_vault_api_key';
    case 'Invalid actor type':
      return 'invalid_actor_type';
    default:
      if (error.message.endsWith('must be a UUID')) return 'invalid_uuid';
      return 'auth_denied';
  }
}

function logAuthDecision(
  request: FastifyRequest,
  outcome: 'allowed' | 'denied',
  input: {
    auth?: PlatformAuthContext | null;
    reason?: string;
    requiredScopes?: string[];
  } = {}
) {
  request.log[outcome === 'allowed' ? 'info' : 'warn']({
    event: 'platform_auth_decision',
    route: requestedRoute(request),
    method: request.method,
    outcome,
    reason: input.reason,
    required_scopes: input.requiredScopes ?? [],
    auth_method: input.auth?.method ?? null,
    subject: input.auth?.subject ?? null,
    client_id: input.auth?.client_id ?? null,
    scopes: input.auth?.scopes ?? [],
    account_id: input.auth?.account_id ?? null,
    vault_id: input.auth?.vault_id ?? null,
    actor_type: input.auth?.actor?.type ?? null,
    actor_id: input.auth?.actor?.id ?? null
  }, 'platform auth decision');
}

function getActor(request: FastifyRequest): PlatformAuthActor | null {
  const actorType = getHeaderString(request, 'x-persistio-actor-type');
  const actorId = getHeaderString(request, 'x-persistio-actor-id');
  if (!actorType && !actorId) return null;
  if (actorType !== 'service' && actorType !== 'user' && actorType !== 'system') {
    throw new Error('Invalid actor type');
  }

  return {
    type: actorType,
    id: actorId
  };
}

function getDelegatedAccountId(request: FastifyRequest, tokenAccountId: string | null): string | null {
  const tokenAccountUuid = normalizeUuid(tokenAccountId, 'Token account ID');
  const headerAccountId = normalizeUuid(getHeaderString(request, 'x-persistio-account-id'), 'Delegated account ID');
  if (tokenAccountUuid && headerAccountId && tokenAccountUuid !== headerAccountId) {
    throw new Error('Delegated account does not match token account');
  }

  return tokenAccountUuid ?? headerAccountId;
}

function getDelegatedVaultId(payload: Auth0Token): string | null {
  const tokenVaultId = getStringClaim(payload, 'vault_id') ?? getStringClaim(payload, vaultIdClaim);
  return normalizeUuid(tokenVaultId, 'Token vault ID');
}

function buildOAuthAuthContext(request: FastifyRequest, payload: Auth0Token): PlatformAuthContext {
  const tokenAccountId = getStringClaim(payload, 'account_id') ?? getStringClaim(payload, accountIdClaim);
  if (!payload.sub) {
    throw new Error('Missing subject');
  }

  return {
    method: 'oauth',
    subject: payload.sub,
    client_id: getClientId(payload),
    scopes: getScopes(payload),
    account_id: getDelegatedAccountId(request, tokenAccountId),
    vault_id: getDelegatedVaultId(payload),
    actor: getActor(request)
  };
}

async function getVaultByApiKey(rawKey: string): Promise<VaultContext | null> {
  const { query } = await import('../db/client');
  const apiKeyHash = hashKey(rawKey);
  const result = await query<VaultContext>(
    `SELECT id, name, purpose, settings, plan_id, status, account_id, encrypted_dek, vault_encryption_enabled
     FROM vaults
     WHERE api_key_hash = $1
       AND status = 'active'
     LIMIT 1`,
    [apiKeyHash]
  );

  return result.rows[0] ?? null;
}

async function authenticateOAuth(
  request: FastifyRequest,
  reply: FastifyReply,
  requiredScopes: string[] | undefined
): Promise<PlatformAuthContext | null> {
  const config = getConfig();
  if (config.PLATFORM_AUTH_MODE === 'api_key') {
    throw new Error('OAuth is disabled');
  }

  if (!request.server.requireAuth) {
    throw new Error('OAuth verifier is not configured');
  }

  const verifier = request.server.requireAuth();
  await verifier(request, reply);
  if (reply.sent) {
    logAuthDecision(request, 'denied', {
      reason: 'oauth_verifier_rejected',
      requiredScopes
    });
    return null;
  }

  const auth = buildOAuthAuthContext(request, request.user);
  enforceClientPolicy(request, auth, requiredScopes);
  return auth;
}

async function authenticateRequest(
  request: FastifyRequest,
  reply: FastifyReply,
  options: AuthenticateOptions
): Promise<{ auth: PlatformAuthContext; vault?: VaultContext } | null> {
  const config = getConfig();
  const bearerToken = getBearerToken(request);
  const headerKey = request.headers['x-admin-key'];
  const headerAdminKey = typeof headerKey === 'string' ? headerKey : undefined;
  const bearerIsJwt = bearerToken ? isJwtCredential(bearerToken) : false;
  const bearerAdminKey = bearerToken && !bearerIsJwt && options.allowAdminApiKey !== false
    ? bearerToken
    : undefined;
  const adminKey = headerAdminKey ?? bearerAdminKey;
  const token = bearerToken ?? adminKey;

  if (!token) {
    throw new Error('Missing credentials');
  }

  if (bearerToken && bearerIsJwt) {
    if (options.allowOAuth === false) {
      throw new Error('OAuth is not allowed for this route');
    }

    const auth = await authenticateOAuth(request, reply, options.adminScopes);
    return auth ? { auth } : null;
  }

  if (adminKey) {
    const adminKeyAllowed = options.allowAdminApiKey !== false && config.PLATFORM_AUTH_MODE !== 'oauth';
    if (!adminKeyAllowed || !timingSafeEqual(adminKey, config.ADMIN_API_KEY)) {
      throw new Error('Invalid admin API key');
    }

    return {
      auth: {
        method: 'api_key',
        subject: 'admin_api_key',
        client_id: null,
        scopes: ['platform:admin'],
        account_id: null,
        vault_id: null,
        actor: null
      }
    };
  }

  if (!bearerToken) {
    throw new Error('Missing bearer token');
  }

  const vaultKeyAllowed = options.allowVaultApiKey !== false;
  if (!vaultKeyAllowed) {
    throw new Error('Vault API keys are disabled');
  }

  const vault = await getVaultByApiKey(bearerToken);
  if (!vault) {
    throw new Error('Invalid vault API key');
  }

  return {
    auth: {
      method: 'api_key',
      subject: `vault:${vault.id}`,
      client_id: null,
      scopes: ['vault:access'],
      account_id: vault.account_id,
      vault_id: vault.id,
      actor: null
    },
    vault
  };
}

async function requireVaultApiKey(request: FastifyRequest, reply: FastifyReply) {
  try {
    const result = await authenticateRequest(request, reply, {
      allowAdminApiKey: false,
      allowOAuth: false,
      allowVaultApiKey: true
    });
    if (!result?.vault) {
      logAuthDecision(request, 'denied', { reason: 'vault_context_missing' });
      return reply.code(401).send({ error: 'Unauthorized' });
    }
    request.auth = result.auth;
    request.vault = result.vault;
  } catch (error) {
    logAuthDecision(request, 'denied', { reason: denialReason(error) });
    return reply.code(401).send({ error: 'Unauthorized' });
  }
}

export async function requireVaultAuth(request: FastifyRequest, reply: FastifyReply) {
  return requireVaultApiKey(request, reply);
}

export const requireVaultReadAuth = requireVaultApiKey;
export const requireVaultWriteAuth = requireVaultApiKey;
export const requireVaultStatsAuth = requireVaultApiKey;
export const requireAnyVaultAuth = requireVaultApiKey;

export function requireAdminScope(scopes: string | string[]) {
  const requiredScopes = Array.isArray(scopes) ? scopes : [scopes];
  return async (request: FastifyRequest, reply: FastifyReply) => {
    try {
      const result = await authenticateRequest(request, reply, {
        allowAdminApiKey: true,
        allowVaultApiKey: false,
        allowOAuth: true,
        adminScopes: requiredScopes
      });
      if (!result) return;
      request.auth = result.auth;
      logAuthDecision(request, 'allowed', {
        auth: result.auth,
        requiredScopes
      });
    } catch (error) {
      logAuthDecision(request, 'denied', {
        reason: denialReason(error),
        requiredScopes
      });
      return reply.code(401).send({ error: 'Unauthorized' });
    }
  };
}

export async function requireAdminAuth(request: FastifyRequest, reply: FastifyReply) {
  return requireAdminScope('platform:admin')(request, reply);
}

export function getAuthAccountId(request: FastifyRequest): string | null {
  return request.auth?.account_id ?? null;
}

export function ensureRequestedAccountAccess(request: FastifyRequest, accountId: string | null): boolean {
  const authAccountId = getAuthAccountId(request);
  return !authAccountId || authAccountId === accountId;
}

export function resetAuthCachesForTests() {
  // Auth0 verifier caches are owned by the registered SDK instance.
}

export function createApiKey() {
  const rawKey = crypto.randomBytes(24).toString('hex');
  return {
    rawKey,
    hash: hashKey(rawKey)
  };
}
