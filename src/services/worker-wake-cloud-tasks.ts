import { createHash } from 'node:crypto';
import { GoogleAuth } from 'google-auth-library';
import { workerWakeSchema, type WakeTransport, type WorkerWake } from './worker-wake-types';

interface CloudTasksOptions {
  enabled?: boolean;
  queue?: string;
  workerOrigin?: string;
  invokerEmail?: string;
  dispatchDeadlineSeconds?: number;
}
const API = 'https://cloudtasks.googleapis.com/v2/';
const REQUEST_TIMEOUT_MS = 5_000;

/** No client discovery/network activity at import or construction. The future host
 * explicitly opts in and owns IAM/private ingress; headers are not authentication. */
export class CloudTasksWakeTransport implements WakeTransport {
  private auth: GoogleAuth | undefined;
  private readonly config: { queue: string; origin: string; email: string; deadline: number } | undefined;

  constructor(options: CloudTasksOptions = {}) {
    if (!options.enabled) return;
    const { queue, workerOrigin, invokerEmail } = options;
    const deadline = options.dispatchDeadlineSeconds ?? 600;
    if (!queue || !/^projects\/[a-zA-Z0-9:.-]+\/locations\/[a-z0-9-]+\/queues\/[a-zA-Z0-9-]{1,100}$/.test(queue)) {
      throw new Error('Invalid worker task queue');
    }
    const url = new URL(workerOrigin ?? '');
    if (url.protocol !== 'https:' || !url.hostname.endsWith('.run.app') || url.port ||
        url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
      throw new Error('Worker target must be a fixed HTTPS run.app origin');
    }
    if (!invokerEmail || !/^[a-zA-Z0-9-]+@[a-zA-Z0-9-]+\.iam\.gserviceaccount\.com$/.test(invokerEmail)) {
      throw new Error('Invalid worker task invoker');
    }
    if (!Number.isSafeInteger(deadline) || deadline < 15 || deadline > 1800) throw new Error('Invalid task deadline');
    this.config = { queue, origin: url.origin, email: invokerEmail, deadline };
  }

  async create(value: WorkerWake, dueAt: Date): Promise<'created' | 'exists'> {
    const config = this.requireConfig();
    const wake = workerWakeSchema.parse(value);
    if (!Number.isFinite(dueAt.getTime())) throw new Error('Invalid task due time');
    const status = await this.request(config.queue + '/tasks', 'POST', {
      task: {
        name: this.taskName(wake),
        scheduleTime: dueAt.toISOString(),
        dispatchDeadline: config.deadline + 's',
        httpRequest: {
          httpMethod: 'POST',
          url: config.origin + '/internal/worker/wake',
          headers: { 'Content-Type': 'application/json' },
          oidcToken: { serviceAccountEmail: config.email, audience: config.origin },
          body: Buffer.from(JSON.stringify(wake)).toString('base64')
        }
      }
    });
    if (status >= 200 && status < 300) return 'created';
    if (status === 409) return 'exists';
    throw new Error('Worker task creation failed');
  }

  async exists(value: WorkerWake): Promise<boolean> {
    const config = this.requireConfig();
    const status = await this.request(this.taskName(workerWakeSchema.parse(value)), 'GET');
    if (status === 200) return true;
    if (status !== 404) throw new Error('Worker task inspection failed');
    // A queue-level 404 is not proof a retained task name can safely be replaced.
    if (await this.request(config.queue, 'GET') !== 200) throw new Error('Worker task queue unavailable');
    return false;
  }

  private requireConfig() {
    if (!this.config) throw new Error('Worker task transport disabled');
    return this.config;
  }

  private taskName(wake: WorkerWake): string {
    const id = createHash('sha256').update(wake.kind + ':' + wake.token).digest('hex');
    return this.requireConfig().queue + '/tasks/' + id;
  }

  private async request(path: string, method: 'GET' | 'POST', body?: unknown): Promise<number> {
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(new Error('Worker task request timed out')), REQUEST_TIMEOUT_MS);
    const interrupted = new Promise<never>((_, reject) => {
      abort.signal.addEventListener('abort', () => reject(abort.signal.reason), { once: true });
    });
    try {
      this.auth ??= new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-tasks'] });
      // Credential discovery may not honour AbortSignal. Race this READ-ONLY step;
      // after timeout the continuation cannot issue an external task request.
      const headers = await Promise.race([this.auth.getRequestHeaders(API + path), interrupted]);
      abort.signal.throwIfAborted();
      const response = await fetch(API + path, {
        method, headers: { ...headers, 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: abort.signal, redirect: 'error'
      });
      // We need only the status; do not retain or log payload/error bodies.
      await response.body?.cancel();
      return response.status;
    } finally {
      clearTimeout(timer);
    }
  }
}
