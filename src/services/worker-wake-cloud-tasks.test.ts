import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { CloudTasksWakeTransport } from './worker-wake-cloud-tasks';
import type { WorkerWake } from './worker-wake-types';

const auth = vi.hoisted(() => ({ headers: vi.fn(), construct: vi.fn() }));
vi.mock('google-auth-library', () => ({
  GoogleAuth: class {
    constructor(options: unknown) { auth.construct(options); }
    getRequestHeaders = auth.headers;
  }
}));
const options = {
  enabled: true, queue: 'projects/project/locations/europe-west2/queues/worker',
  workerOrigin: 'https://worker-xyz.run.app', invokerEmail: 'worker@project.iam.gserviceaccount.com'
};
const wake: WorkerWake = { version: 1, kind: 'extraction', generation: '9007199254740993', token: 'd1b29137-df55-43d0-ab98-ec6c3a002832' };
const fetchMock = vi.fn();
beforeEach(() => {
  vi.clearAllMocks();
  auth.headers.mockResolvedValue({ Authorization: 'Bearer fixture' });
  fetchMock.mockReset().mockImplementation(async () => new Response(null, { status: 200 }));
  vi.stubGlobal('fetch', fetchMock);
});
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });

it('is inert by default and rejects calls without discovering credentials', async () => {
  const transport = new CloudTasksWakeTransport();
  await expect(transport.create(wake, new Date())).rejects.toThrow('disabled');
  expect(auth.construct).not.toHaveBeenCalled();
  expect(fetchMock).not.toHaveBeenCalled();
});
it('sends only the strict envelope to a fixed authenticated worker target and stable hashed name', async () => {
  const transport = new CloudTasksWakeTransport(options);
  expect(auth.construct).not.toHaveBeenCalled();
  const due = new Date('2026-09-27T10:00:00Z');
  await transport.create(wake, due);
  await transport.create(wake, due);
  const [url, request] = fetchMock.mock.calls[0];
  expect(url).toBe('https://cloudtasks.googleapis.com/v2/' + options.queue + '/tasks');
  expect(request.headers.Authorization).toBe('Bearer fixture');
  expect(request.redirect).toBe('error');
  const task = JSON.parse(request.body).task;
  expect(task.name).toMatch(/\/tasks\/[a-f0-9]{64}$/);
  expect(JSON.parse(fetchMock.mock.calls[1][1].body).task.name).toBe(task.name);
  expect(task.scheduleTime).toBe(due.toISOString());
  expect(task.dispatchDeadline).toBe('600s');
  expect(task.httpRequest.url).toBe(options.workerOrigin + '/internal/worker/wake');
  expect(task.httpRequest.oidcToken).toEqual({ serviceAccountEmail: options.invokerEmail, audience: options.workerOrigin });
  expect(JSON.parse(Buffer.from(task.httpRequest.body, 'base64').toString())).toEqual(wake);
});
it('distinguishes retained name from business completion', async () => {
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 409 }));
  expect(await new CloudTasksWakeTransport(options).create(wake, new Date())).toBe('exists');
});
it.each([403, 404, 429, 503])('does not treat queue failure %s as positive task absence', async status => {
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 404 }));
  fetchMock.mockResolvedValueOnce(new Response(null, { status }));
  await expect(new CloudTasksWakeTransport(options).exists(wake)).rejects.toThrow('queue unavailable');
  expect(fetchMock).toHaveBeenCalledTimes(2);
});
it('confirms the queue exists before accepting a missing task', async () => {
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 404 }));
  expect(await new CloudTasksWakeTransport(options).exists(wake)).toBe(false);
  expect(fetchMock.mock.calls[1][0]).toBe('https://cloudtasks.googleapis.com/v2/' + options.queue);
});
it('does not retry transient task-inspection failures', async () => {
  fetchMock.mockResolvedValueOnce(new Response(null, { status: 503 }));
  await expect(new CloudTasksWakeTransport(options).exists(wake)).rejects.toThrow('inspection failed');
  expect(fetchMock).toHaveBeenCalledOnce();
});
it('bounds credential discovery; a late result cannot create a task', async () => {
  vi.useFakeTimers();
  let release!: (headers: object) => void;
  auth.headers.mockReturnValue(new Promise(resolve => { release = resolve; }));
  const pending = new CloudTasksWakeTransport(options).create(wake, new Date());
  const assertion = expect(pending).rejects.toThrow('timed out');
  await vi.advanceTimersByTimeAsync(5000);
  await assertion;
  release({ Authorization: 'Bearer late' });
  await Promise.resolve();
  expect(fetchMock).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
});
it('cancels slow HTTP I/O with no hidden retry', async () => {
  vi.useFakeTimers();
  fetchMock.mockImplementation((_url, request) => new Promise((_, reject) =>
    request.signal.addEventListener('abort', () => reject(request.signal.reason))));
  const pending = new CloudTasksWakeTransport(options).create(wake, new Date());
  const assertion = expect(pending).rejects.toThrow('timed out');
  await vi.advanceTimersByTimeAsync(5000);
  await assertion;
  expect(fetchMock).toHaveBeenCalledOnce();
});
it.each(['http://worker.run.app', 'https://evil.example', 'https://worker.run.app/other', 'https://u:p@worker.run.app', 'https://worker.run.app/?token=secret'])('rejects unsafe target %s', workerOrigin => {
  expect(() => new CloudTasksWakeTransport({ ...options, workerOrigin })).toThrow();
});
it('rejects unknown versions, extra payload fields and invalid generations before network I/O', async () => {
  const transport = new CloudTasksWakeTransport(options);
  for (const value of [{ ...wake, version: 2 }, { ...wake, vaultId: 'private' }, { ...wake, generation: '9223372036854775808' }]) {
    await expect(transport.create(value as WorkerWake, new Date())).rejects.toThrow();
  }
  expect(fetchMock).not.toHaveBeenCalled();
});
