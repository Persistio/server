import { generateKeyPairSync, sign } from 'node:crypto';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createWorkerAuthenticator } from '../auth';
import { requestWorkerConfig } from '../config';

export const env = {
  REQUEST_WORKER_ENABLED: 'true', REQUEST_WORKER_AUDIENCE: 'https://worker-example.run.app',
  REQUEST_WORKER_INVOKER_EMAIL: 'tasks@example-project.iam.gserviceaccount.com',
  REQUEST_WORKER_INVOKER_SUBJECT: '123456789012345678901'
};
const config = requestWorkerConfig(env);
const keys = generateKeyPairSync('rsa', { modulusLength: 2048 });
function token(overrides: Record<string, unknown> = {}, invalidSignature = false) {
  const now = Math.floor(Date.now() / 1000);
  const body = [ { alg: 'RS256', typ: 'JWT', kid: 'fixture' }, {
    iss: 'https://accounts.google.com', aud: config.REQUEST_WORKER_AUDIENCE,
    email: config.REQUEST_WORKER_INVOKER_EMAIL, email_verified: true,
    sub: config.REQUEST_WORKER_INVOKER_SUBJECT, iat: now, exp: now + 300, ...overrides
  } ].map(value => Buffer.from(JSON.stringify(value)).toString('base64url')).join('.');
  return 'Bearer ' + body + '.' + (invalidSignature ? Buffer.from('forged') : sign('RSA-SHA256', Buffer.from(body), keys.privateKey)).toString('base64url');
}
afterEach(() => vi.unstubAllGlobals());
describe('private worker configuration and cryptographic authentication', () => {
  it('is opt-in, binds the service origin and leaves task-deadline cleanup margin', () => {
    expect(() => requestWorkerConfig({ ...env, REQUEST_WORKER_ENABLED: undefined })).toThrow();
    for (const audience of ['http://worker-example.run.app', 'https://worker-example.run.app/path', 'https://worker-example.run.app/', 'https://worker-example.run.app.evil.test', 'https://user@worker-example.run.app']) {
      expect(() => requestWorkerConfig({ ...env, REQUEST_WORKER_AUDIENCE: audience })).toThrow();
    }
    expect(() => requestWorkerConfig({ ...env, REQUEST_WORKER_DEADLINE_MS: '600000' })).toThrow();
    expect(config.REQUEST_WORKER_DEADLINE_MS).toBe(120000);
  });
  it('verifies actual RSA signatures and all required claims; never trusts unsigned task metadata', async () => {
    const fetcher = vi.fn().mockImplementation(async () => new Response(JSON.stringify({ fixture: keys.publicKey.export({ type: 'spki', format: 'pem' }) }), { headers: { 'cache-control': 'max-age=600' } }));
    vi.stubGlobal('fetch', fetcher);
    const authenticate = createWorkerAuthenticator(config);
    expect(await authenticate(token())).toBe(true);
    for (const claims of [
      { aud: 'https://other.run.app' }, { iss: 'https://evil.test' }, { email: 'other@example-project.iam.gserviceaccount.com' },
      { sub: '999999999999999999999' }, { email_verified: false }, { email_verified: 'true' },
      { exp: Math.floor(Date.now() / 1000) - 601 }, { iat: Math.floor(Date.now() / 1000) + 601 },
      { exp: Math.floor(Date.now() / 1000) + 7200 }
    ]) expect(await authenticate(token(claims))).toBe(false);
    expect(await authenticate(token({}, true))).toBe(false);
    expect(await authenticate('Bearer ' + token().slice(7).split('.').slice(0, 2).join('.') + '.')).toBe(false);
    expect(await authenticate(undefined)).toBe(false);
    expect(await authenticate('test-admin-key')).toBe(false);
    expect(await authenticate('Bearer ' + 'a'.repeat(8192))).toBe(false);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it('fails closed on unavailable or malformed signing certificates', async () => {
    for (const response of [new Response('', { status: 503 }), new Response('not json'), new Response(JSON.stringify({ fixture: {} }))]) {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
      expect(await createWorkerAuthenticator(config)(token())).toBe(false);
    }
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('timeout')));
    expect(await createWorkerAuthenticator(config)(token())).toBe(false);
  });
});
