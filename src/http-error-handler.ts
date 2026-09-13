import type { FastifyInstance } from 'fastify';
import { safeErrorCode } from './operational-metadata';
import { applyRateLimitHeaders, QuotaExceededError } from './services/usage';

/** Registered by production and HTTP integration fixtures, never duplicated there. */
export function registerPlatformErrorHandler(app: FastifyInstance): void {
  app.setErrorHandler((error, request, reply) => {
    if (error instanceof QuotaExceededError) {
      applyRateLimitHeaders(reply, error.headers);
      reply.code(error.statusCode).send({error: error.message});
      return;
    }
    const code = safeErrorCode(error);
    request.log.error({error_code: code, route: request.routeOptions.url ?? '/unmatched'}, 'platform_event');
    const errorStatus = error && typeof error === 'object' && 'statusCode' in error ? error.statusCode : undefined;
    const status = typeof errorStatus === 'number' && Number.isInteger(errorStatus) && errorStatus >= 400 && errorStatus <= 599 ? errorStatus
      : code === 'invalid_request' ? 400 : 500;
    reply.code(status).send({error: status < 500 ? 'Invalid request' : 'Internal server error', code});
  });
}
