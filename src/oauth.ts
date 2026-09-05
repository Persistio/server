import fastifyAuth0Api from '@auth0/auth0-fastify-api';
import type { FastifyInstance } from 'fastify';

import type { AppConfig } from './config';

export async function registerPlatformOAuth(app: FastifyInstance, config: AppConfig) {
  if (config.PLATFORM_AUTH_MODE === 'api_key') {
    return;
  }

  if (!config.PLATFORM_OAUTH_ISSUER || !config.PLATFORM_OAUTH_AUDIENCE) {
    return;
  }

  await app.register(fastifyAuth0Api, {
    audience: config.PLATFORM_OAUTH_AUDIENCE,
    domains: [issuerToDomain(config.PLATFORM_OAUTH_ISSUER)],
    dpop: { mode: 'disabled' }
  });
}

function issuerToDomain(issuer: string): string {
  const url = new URL(issuer);
  return url.hostname;
}
