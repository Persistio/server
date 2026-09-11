import { isMainThread, workerData } from 'node:worker_threads';

export function memoryTelemetryComponent(mainThread = isMainThread, data: unknown = workerData): string {
  if (mainThread) return 'main';
  const component = data && typeof data === 'object' ? (data as { component?: unknown }).component : undefined;
  return component === 'extraction' || component === 'curation' ? component : '';
}

// A worker preload runs before Node sets argv[1], so process.command cannot
// identify it. The same fixed workerData contract already identifies DB pools.
// Register LAST: OTEL_RESOURCE_ATTRIBUTES must not override producer identity.
export const memoryProducerDetector = {
  detect: () => ({ attributes: { 'persistio.component': memoryTelemetryComponent() } })
};
