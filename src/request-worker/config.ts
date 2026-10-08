import { z } from 'zod';

const schema = z.object({
  REQUEST_WORKER_ENABLED: z.literal('true'),
  REQUEST_WORKER_ADMISSION_ENABLED: z.enum(['true', 'false']).optional(),
  REQUEST_WORKER_AUDIENCE: z.string().url().refine(value => {
    const u = new URL(value);
    return u.protocol === 'https:' && u.hostname.endsWith('.run.app') && !u.port &&
      !u.username && !u.password && u.pathname === '/' && !u.search && !u.hash && value === u.origin;
  }),
  REQUEST_WORKER_INVOKER_EMAIL: z.string().regex(/^[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com$/),
  REQUEST_WORKER_INVOKER_SUBJECT: z.string().regex(/^[0-9]{10,30}$/),
  REQUEST_WORKER_DEADLINE_MS: z.coerce.number().int().min(10_000).max(540_000).default(120_000)
});
export type RequestWorkerConfig = z.infer<typeof schema>;
export function requestWorkerConfig(env: NodeJS.ProcessEnv = process.env): RequestWorkerConfig {
  return schema.parse(env);
}
