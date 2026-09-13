import Fastify from 'fastify';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { registerPlatformErrorHandler } from './http-error-handler';
import { operationalLoggerOptions } from './operational-metadata';
import { QuotaExceededError } from './services/usage';
import { RawChunkWorkPool } from './services/raw-chunk-work-pool';

async function response(fail: () => unknown, path = '/failure?secret=canary') {
  const logs: string[] = [];
  const app = Fastify({disableRequestLogging: true, logger: {
    ...operationalLoggerOptions(), stream: {write: (line: string) => { logs.push(line); }}
  }});
  registerPlatformErrorHandler(app);
  app.get('/failure', async () => { await fail(); return {}; });
  try { return {result: await app.inject(path), logs}; }
  finally { await app.close(); }
}

describe('production HTTP error contract', () => {
  it.each([400,401,403,404,409,413,429,500,502,503,504,599])('preserves %i without exposing upstream contents', async statusCode => {
    const {result,logs} = await response(() => {throw Object.assign(new Error('canary upstream payload'), {
      statusCode, code: 'canary', cause: new Error('canary'), headers: {authorization: 'canary'}
    });});
    expect(result.statusCode).toBe(statusCode);
    expect(result.body).not.toContain('canary');
    expect(JSON.stringify(result.headers)).not.toContain('canary');
    expect(logs.join('')).not.toContain('canary');
  });
  it.each([NaN,Infinity,-1,200,399,400.5,503.5,600,'503',undefined])('normalizes invalid status %s', async statusCode => {
    const {result} = await response(() => {throw Object.assign(new Error('private'), {statusCode});});
    expect(result.statusCode).toBe(500);
    expect(result.body).not.toContain('private');
  });
  it('retains schema 400 and trusted quota 429 with retry headers', async () => {
    expect((await response(() => z.string().parse(1))).result.statusCode).toBe(400);
    const {result} = await response(() => {throw new QuotaExceededError('memory_adds quota exceeded', {
      limit: 1, remaining: 0, resetAtEpochSeconds: 2000000000, retryAfterSeconds: 60
    });});
    expect(result.statusCode).toBe(429);
    expect(result.json()).toEqual({error: 'memory_adds quota exceeded'});
    expect(result.headers).toMatchObject({'x-ratelimit-limit':'1','x-ratelimit-remaining':'0','retry-after':'60'});
  });
  it('does not trust an upstream error that merely uses the quota error name', async () => {
    const {result} = await response(() => {throw Object.assign(new Error('canary'), {
      name: 'QuotaExceededError', statusCode: 429, headers: {limit: 'canary'}
    });});
    expect(result.statusCode).toBe(429);
    expect(result.body+JSON.stringify(result.headers)).not.toContain('canary');
  });
  it('preserves actual provider pool saturation and deadline errors', async () => {
    expect((await response(() => new RawChunkWorkPool(0).map([1], async n => n))).result.statusCode).toBe(503);
    let release!: () => void;
    const blocked = new Promise<void>(resolve => {release = resolve;});
    try {
      expect((await response(() => new RawChunkWorkPool(1).map([1], async () => blocked, 1))).result.statusCode).toBe(503);
    } finally { release(); }
  });
});
