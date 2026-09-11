import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const sdk = vi.hoisted(() => ({ start: vi.fn(), shutdown: vi.fn(), count: 0 }));
vi.mock('@opentelemetry/sdk-node', () => ({ NodeSDK: class {
  constructor() { sdk.count++; }
  start = sdk.start;
  shutdown = sdk.shutdown;
} }));
vi.mock('@opentelemetry/auto-instrumentations-node', () => ({ getNodeAutoInstrumentations: () => [] }));
vi.mock('@opentelemetry/sdk-metrics', async importOriginal => ({
  ...await importOriginal<typeof import('@opentelemetry/sdk-metrics')>(), PeriodicExportingMetricReader: class {}
}));
vi.mock('@azure/monitor-opentelemetry-exporter', () => ({ AzureMonitorMetricExporter: class {}, AzureMonitorTraceExporter: class {} }));
vi.mock('@opentelemetry/exporter-metrics-otlp-http', () => ({ OTLPMetricExporter: class { selectAggregationTemporality() { return 1; } } }));
vi.mock('@opentelemetry/exporter-trace-otlp-http', () => ({ OTLPTraceExporter: class {} }));

describe('telemetry shutdown exposes the actual SDK lifetime, without a host timeout', () => {
  beforeEach(() => { vi.resetModules(); vi.useFakeTimers(); sdk.count = 0; sdk.start.mockReset(); sdk.shutdown.mockReset(); });
  afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });
  it('does nothing when disabled and shares concurrent shutdown before permitting reinitialization', async () => {
    const telemetry = await import('./azure-monitor');
    await telemetry.shutdownTelemetry();
    expect(sdk.shutdown).not.toHaveBeenCalled();
    let finish!: () => void;
    sdk.shutdown.mockImplementation(() => new Promise<void>(resolve => { finish = resolve; }));
    telemetry.useOtlpTelemetry();
    const first = telemetry.shutdownTelemetry();
    expect(telemetry.shutdownTelemetry()).toBe(first);
    telemetry.useAzureMonitor({ azureMonitorExporterOptions: { connectionString: 'test' } });
    await vi.advanceTimersByTimeAsync(0);
    expect(sdk.count).toBe(1);
    finish(); await first;
    expect(sdk.shutdown).toHaveBeenCalledTimes(1);
    telemetry.useOtlpTelemetry(); expect(sdk.count).toBe(2);
  });
  it.each(['reject', 'throw'] as const)('propagates %s without printing payload or reinitializing an uncertain SDK', async failure => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const telemetry = await import('./azure-monitor');
    const secret = new Error('PRIVATE_EXPORT_PAYLOAD');
    sdk.shutdown.mockImplementation(() => {
      if (failure === 'throw') throw secret;
      if (failure === 'reject') return Promise.reject(secret);
    });
    telemetry.useOtlpTelemetry();
    await expect(telemetry.shutdownTelemetry()).rejects.toBe(secret);
    telemetry.useOtlpTelemetry(); expect(sdk.count).toBe(1);
    await expect(telemetry.shutdownTelemetry()).rejects.toBe(secret);
    expect(sdk.shutdown).toHaveBeenCalledTimes(1);
    expect(warn).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
  it.each(['azure', 'otlp'])('%s stays pending beyond five seconds and blocks another SDK until actual completion', async provider => {
    const telemetry = await import('./azure-monitor');
    let finish!: () => void;
    sdk.shutdown.mockImplementation(() => new Promise<void>(r => { finish = r; }));
    if (provider === 'azure') telemetry.useAzureMonitor({ azureMonitorExporterOptions: { connectionString: 'test' } });
    else telemetry.useOtlpTelemetry();
    const done = vi.fn(); const operation = telemetry.shutdownTelemetry().then(done);
    await vi.advanceTimersByTimeAsync(6000);
    expect(done).not.toHaveBeenCalled();
    telemetry.useOtlpTelemetry(); expect(sdk.count).toBe(1);
    finish(); await operation; expect(done).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });
  it('collects fresh observable gauges without changing counter temporality', async () => {
    const { SnapshotOtlpMetricExporter } = await import('./azure-monitor');
    const { InstrumentType, AggregationTemporality } = await import('@opentelemetry/sdk-metrics');
    const exporter = new SnapshotOtlpMetricExporter();
    expect(exporter.selectAggregationTemporality(InstrumentType.OBSERVABLE_GAUGE)).toBe(AggregationTemporality.DELTA);
    expect(exporter.selectAggregationTemporality(InstrumentType.COUNTER)).toBe(1);
  });
});
