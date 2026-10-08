import { OAuth2Client, type Certificates } from 'google-auth-library';
import type { RequestWorkerConfig } from './config';

/** Fixed Google certificate endpoint, bounded fetch, no task-header/API-key bypass. */
export function createWorkerAuthenticator(config: RequestWorkerConfig) {
  const verifier = new OAuth2Client();
  let cached: { certificates: Certificates; expires: number } | undefined;
  return async (authorization: string | undefined): Promise<boolean> => {
    if (!authorization || authorization.length > 8192 || !/^Bearer [A-Za-z0-9_.-]+$/.test(authorization)) return false;
    try {
      if (!cached || cached.expires <= Date.now()) {
        const response = await fetch('https://www.googleapis.com/oauth2/v1/certs', {
          signal: AbortSignal.timeout(3000), redirect: 'error'
        });
        if (!response.ok) return false;
        const body = await response.text();
        if (body.length > 64 * 1024) return false;
        const certificates: unknown = JSON.parse(body);
        if (!certificates || typeof certificates !== 'object' || Array.isArray(certificates) ||
            Object.values(certificates).some(value => typeof value !== 'string')) return false;
        const maxAge = /max-age=(\d+)/.exec(response.headers.get('cache-control') ?? '');
        cached = { certificates: certificates as Certificates,
          expires: Date.now() + Math.min(600, Number(maxAge?.[1] ?? 60)) * 1000 };
      }
      const ticket = await verifier.verifySignedJwtWithCertsAsync(
        authorization.slice(7), cached.certificates, config.REQUEST_WORKER_AUDIENCE,
        ['accounts.google.com', 'https://accounts.google.com'], 3600
      );
      const claims = ticket.getPayload();
      return claims?.email_verified === true && claims.email === config.REQUEST_WORKER_INVOKER_EMAIL &&
        claims.sub === config.REQUEST_WORKER_INVOKER_SUBJECT;
    } catch {
      return false;
    }
  };
}
