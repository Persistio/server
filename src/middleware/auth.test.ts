import crypto from 'node:crypto';

import Fastify from 'fastify';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { queryMock } = vi.hoisted(() => ({
  queryMock: vi.fn()
}));

vi.mock('../db/client', () => ({
  query: queryMock
}));

const originalEnv = process.env;
const originalFetch = globalThis.fetch;
const appWorkerClientId = 'test-app-worker-client-id';
const reportingClientId = 'test-reporting-client-id';

const appWorkerPolicy = {
  client_id: appWorkerClientId,
  allowed_scopes: [
    'platform:vaults:create',
    'platform:vaults:read',
    'platform:vaults:update',
    'platform:vaults:delete',
    'platform:vault_keys:rotate',
    'platform:vaults:stats:read',
    'platform:plans:read',
    'platform:plans:write',
    'platform:analytics:read'
  ],
  allow_delegation: true,
  require_account_context: true,
  allow_global_access: false
};

const reportingPolicy = {
  client_id: reportingClientId,
  allowed_scopes: ['platform:plans:read'],
  allow_delegation: false,
  require_account_context: false,
  allow_global_access: true
};

function oauthClientPolicies(...policies: Array<typeof appWorkerPolicy | typeof reportingPolicy>): string {
  return JSON.stringify(policies);
}

function baseEnv(overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  return {
    ADMIN_API_KEY: 'test-admin-key',
    DATABASE_URL: 'postgres://example.com/test',
    OPENAI_API_KEY: 'test-openai-key',
    ...overrides
  };
}

function encodeJwtPart(value: unknown): string {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}

function createJwt(input: {
  audience?: string;
  clientId?: string | null;
  exp?: number;
  issuer?: string;
  kid?: string;
  nbf?: number;
  privateKey: crypto.KeyObject;
  payload?: Record<string, unknown>;
  scope?: string;
  sub?: string | null;
}) {
  const header = encodeJwtPart({ alg: 'RS256', kid: input.kid ?? 'test-key', typ: 'JWT' });
  const now = Math.floor(Date.now() / 1000);
  const payload = encodeJwtPart({
    iss: input.issuer ?? 'https://auth.persistio.test/',
    aud: input.audience ?? 'https://api.persistio.test',
    ...(input.sub === null ? {} : { sub: input.sub ?? 'client-subject' }),
    ...(input.clientId === null ? {} : { client_id: input.clientId ?? appWorkerClientId }),
    scope: input.scope ?? 'platform:vaults:create',
    iat: now,
    exp: input.exp ?? now + 3600,
    ...(input.nbf === undefined ? {} : { nbf: input.nbf }),
    ...input.payload
  });
  const signer = crypto.createSign('RSA-SHA256');
  signer.update(`${header}.${payload}`);
  signer.end();
  const signature = signer.sign(input.privateKey).toString('base64url');
  return `${header}.${payload}.${signature}`;
}

async function buildAuthApp(
  env: NodeJS.ProcessEnv,
  mode: 'adminVaultCreate' | 'adminPlanRead' | 'adminVaultStatsRead' | 'analyticsRead' | 'vaultRead' = 'adminVaultCreate',
  options: Parameters<typeof Fastify>[0] = {}
) {
  vi.resetModules();
  process.env = baseEnv(env);
  if (
    (process.env.PLATFORM_OAUTH_ISSUER || process.env.PLATFORM_OAUTH_AUDIENCE) &&
    !process.env.PLATFORM_OAUTH_CLIENT_POLICIES
  ) {
    process.env.PLATFORM_OAUTH_CLIENT_POLICIES = oauthClientPolicies(appWorkerPolicy);
  }
  const auth = await import('./auth');
  const { getConfig } = await import('../config');
  const { registerPlatformOAuth } = await import('../oauth');
  const app = Fastify(options);
  await registerPlatformOAuth(app, getConfig());

  if (mode === 'adminPlanRead') {
    app.get('/test', { preHandler: auth.requireAdminScope('platform:plans:read') }, async (request) => ({
      auth: request.auth
    }));
  } else if (mode === 'adminVaultStatsRead') {
    app.get('/test', { preHandler: auth.requireAdminScope('platform:vaults:stats:read') }, async (request) => ({
      auth: request.auth
    }));
  } else if (mode === 'analyticsRead') {
    app.get('/test', { preHandler: auth.requireAdminScope('platform:analytics:read') }, async (request) => ({
      auth: request.auth
    }));
  } else if (mode === 'vaultRead') {
    app.get('/test', { preHandler: auth.requireVaultReadAuth }, async (request) => ({
      auth: request.auth,
      vault: request.vault
    }));
  } else {
    app.get('/test', { preHandler: auth.requireAdminScope('platform:vaults:create') }, async (request) => ({
      auth: request.auth
    }));
  }

  return { app, auth };
}

function activeVault() {
  return {
    id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3',
    name: 'Vault',
    purpose: null,
    settings: {},
    plan_id: 'unlimited',
    status: 'active',
    account_id: 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef',
    encrypted_dek: null,
    vault_encryption_enabled: false
  };
}

describe('platform auth middleware', () => {
  let privateKey: crypto.KeyObject;
  let publicJwk: crypto.JsonWebKey;

  beforeEach(() => {
    const keyPair = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
    privateKey = keyPair.privateKey;
    publicJwk = keyPair.publicKey.export({ format: 'jwk' }) as crypto.JsonWebKey;
    queryMock.mockReset();
    globalThis.fetch = vi.fn(async (input) => {
      const url = String(input);
      if (url.endsWith('/.well-known/openid-configuration')) {
        return Response.json({
          issuer: 'https://auth.persistio.test/',
          jwks_uri: 'https://auth.persistio.test/.well-known/jwks.json'
        });
      }
      if (url.endsWith('/.well-known/jwks.json')) {
        return Response.json({
          keys: [{ ...publicJwk, kid: 'test-key', alg: 'RS256', use: 'sig' }]
        });
      }
      return new Response('not found', { status: 404 });
    }) as typeof fetch;
  });

  afterEach(() => {
    process.env = originalEnv;
    globalThis.fetch = originalFetch;
    vi.resetModules();
  });

  it('normalizes legacy admin API key auth in api_key mode', async () => {
    const { app } = await buildAuthApp({ PLATFORM_AUTH_MODE: 'api_key' });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      method: 'api_key',
      subject: 'admin_api_key',
      scopes: ['platform:admin']
    });

    await app.close();
  });

  it('accepts the legacy admin API key as a bearer token in dual mode', async () => {
    const { app } = await buildAuthApp({ PLATFORM_AUTH_MODE: 'dual' });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: 'Bearer test-admin-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      method: 'api_key',
      subject: 'admin_api_key',
      scopes: ['platform:admin']
    });

    await app.close();
  });

  it('rejects missing credentials', async () => {
    const { app } = await buildAuthApp({ PLATFORM_AUTH_MODE: 'dual' });

    const response = await app.inject({ method: 'GET', url: '/test' });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('uses Auth0 Fastify API validation for scoped OAuth client credentials tokens', async () => {
    const token = createJwt({ privateKey, scope: 'platform:vaults:create' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      method: 'oauth',
      subject: 'client-subject',
      client_id: 'test-app-worker-client-id',
      account_id: 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef',
      scopes: ['platform:vaults:create']
    });

    await app.close();
  });

  it('rejects expired OAuth tokens through the Auth0 verifier', async () => {
    const token = createJwt({
      privateKey,
      exp: Math.floor(Date.now() / 1000) - 60,
      scope: 'platform:vaults:create'
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);
    expect(response.headers['www-authenticate']).toContain('invalid_token');

    await app.close();
  });

  it('rejects OAuth tokens with a future not-before claim', async () => {
    const token = createJwt({
      privateKey,
      nbf: Math.floor(Date.now() / 1000) + 3600,
      scope: 'platform:vaults:create'
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects OAuth tokens with the wrong issuer', async () => {
    const token = createJwt({
      privateKey,
      issuer: 'https://evil.persistio.test/',
      scope: 'platform:vaults:create'
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects OAuth tokens with the wrong audience', async () => {
    const token = createJwt({
      privateKey,
      audience: 'https://wrong-audience.test',
      scope: 'platform:vaults:create'
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects OAuth tokens missing a subject', async () => {
    const token = createJwt({ privateKey, sub: null, scope: 'platform:vaults:create' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects OAuth tokens missing a client id', async () => {
    const token = createJwt({ privateKey, clientId: null, scope: 'platform:vaults:create' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects OAuth tokens missing the required route scope', async () => {
    const token = createJwt({ privateKey, scope: 'platform:vaults:read' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects disallowed OAuth clients', async () => {
    const token = createJwt({ privateKey, clientId: 'rogue-client', scope: 'platform:vaults:create' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects allowed clients with no client-policy route permission', async () => {
    const token = createJwt({ privateKey, clientId: 'test-reporting-client-id', scope: 'platform:vaults:create' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy, reportingPolicy)
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('requires delegated account context for App Worker vault lifecycle calls', async () => {
    const token = createJwt({ privateKey, scope: 'platform:vaults:create' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: `Bearer ${token}` }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('requires delegated account context for analytics routes', async () => {
    const token = createJwt({ privateKey, scope: 'platform:analytics:read' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    }, 'analyticsRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: `Bearer ${token}` }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('accepts the App Worker OAuth client for delegated analytics routes', async () => {
    const token = createJwt({ privateKey, scope: 'platform:analytics:read' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    }, 'analyticsRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      account_id: 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef',
      client_id: 'test-app-worker-client-id',
      scopes: ['platform:analytics:read']
    });

    await app.close();
  });

  it('allows App Worker plan routes without account context', async () => {
    const token = createJwt({ privateKey, scope: 'platform:plans:read' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    }, 'adminPlanRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: `Bearer ${token}` }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      client_id: 'test-app-worker-client-id',
      scopes: ['platform:plans:read']
    });

    await app.close();
  });

  it('rejects delegated account mismatch', async () => {
    const token = createJwt({
      privateKey,
      scope: 'platform:vaults:create',
      payload: { account_id: 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef' }
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'aaaaaaaa-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects non-delegation-capable clients that send delegated headers', async () => {
    const token = createJwt({
      privateKey,
      clientId: 'test-reporting-client-id',
      scope: 'platform:plans:read'
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy, reportingPolicy)
    }, 'adminPlanRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects the legacy admin key in oauth mode', async () => {
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { 'x-admin-key': 'test-admin-key' }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('does not fall back to x-admin-key when a bearer JWT is invalid', async () => {
    const token = createJwt({
      privateKey,
      audience: 'https://wrong-audience.test',
      scope: 'platform:vaults:create'
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-admin-key': 'test-admin-key',
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);

    await app.close();
  });

  it('rejects OAuth tokens on vault-scoped customer routes', async () => {
    const token = createJwt({
      privateKey,
      scope: 'platform:vaults:read',
      payload: { vault_id: 'dff718f2-9d97-43b2-a3cc-a14099ed42c3' }
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test'
    }, 'vaultRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(401);
    expect(queryMock).not.toHaveBeenCalled();

    await app.close();
  });

  it('accepts the App Worker OAuth client for delegated vault stats admin routes', async () => {
    const token = createJwt({
      privateKey,
      scope: 'platform:vaults:stats:read'
    });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    }, 'adminVaultStatsRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: {
        authorization: `Bearer ${token}`,
        'x-persistio-account-id': 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
      }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      client_id: 'test-app-worker-client-id',
      scopes: ['platform:vaults:stats:read'],
      account_id: 'd934b9cf-05b2-476d-a7b8-ef6c36b9f3ef'
    });

    await app.close();
  });

  it('emits structured auth audit logs without credentials for control-plane requests', async () => {
    const logs: string[] = [];
    const token = createJwt({ privateKey, scope: 'platform:plans:read' });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'dual',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    }, 'adminPlanRead', {
      logger: {
        level: 'info',
        stream: {
          write: (line: string) => logs.push(line)
        }
      }
    });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: `Bearer ${token}` }
    });

    expect(response.statusCode).toBe(200);
    const authLog = logs
      .map((line) => JSON.parse(line))
      .find((entry) => entry.event === 'platform_auth_decision');
    expect(authLog).toMatchObject({
      route: '/test',
      method: 'GET',
      outcome: 'allowed',
      auth_method: 'oauth',
      client_id: appWorkerClientId,
      scopes: ['platform:plans:read']
    });
    expect(JSON.stringify(authLog)).not.toContain(token);
    expect(JSON.stringify(authLog)).not.toContain('client-secret');
    expect(JSON.stringify(authLog)).not.toContain('test-vault-key');

    await app.close();
  });

  it('still accepts vault API keys on vault-scoped routes in dual mode', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [activeVault()] });
    const { app } = await buildAuthApp({ PLATFORM_AUTH_MODE: 'dual' }, 'vaultRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      method: 'api_key',
      subject: 'vault:dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      scopes: ['vault:access']
    });

    await app.close();
  });

  it('keeps vault API keys valid on customer direct vault routes in oauth mode', async () => {
    queryMock.mockResolvedValueOnce({ rowCount: 1, rows: [activeVault()] });
    const { app } = await buildAuthApp({
      PLATFORM_AUTH_MODE: 'oauth',
      PLATFORM_OAUTH_ISSUER: 'https://auth.persistio.test/',
      PLATFORM_OAUTH_AUDIENCE: 'https://api.persistio.test',
      PLATFORM_OAUTH_CLIENT_POLICIES: oauthClientPolicies(appWorkerPolicy)
    }, 'vaultRead');

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().auth).toMatchObject({
      method: 'api_key',
      subject: 'vault:dff718f2-9d97-43b2-a3cc-a14099ed42c3',
      scopes: ['vault:access']
    });

    await app.close();
  });

  it('does not allow a vault API key to satisfy an admin guard', async () => {
    const { app } = await buildAuthApp({ PLATFORM_AUTH_MODE: 'dual' });

    const response = await app.inject({
      method: 'GET',
      url: '/test',
      headers: { authorization: 'Bearer test-vault-key' }
    });

    expect(response.statusCode).toBe(401);
    expect(queryMock).not.toHaveBeenCalled();

    await app.close();
  });
});
