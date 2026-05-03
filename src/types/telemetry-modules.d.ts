declare module '@azure/monitor-opentelemetry' {
  export interface AzureMonitorOpenTelemetryOptions {
    azureMonitorExporterOptions?: { connectionString?: string };
    enableLiveMetrics?: boolean;
    instrumentationOptions?: Record<string, { enabled: boolean }>;
  }

  export function useAzureMonitor(options?: AzureMonitorOpenTelemetryOptions): void;
  export function shutdownAzureMonitor(): Promise<void>;
}

declare module '@opentelemetry/api' {
  export interface Span {
    setAttribute(key: string, value: string | number | boolean): void;
    setStatus(status: { code: number; message?: string }): void;
    recordException(error: Error): void;
    end(): void;
  }

  export interface Tracer {
    startSpan(name: string, options?: Record<string, unknown>): Span;
  }

  export interface Histogram {
    record(value: number, attributes?: Record<string, string>): void;
  }

  export interface Counter {
    add(value: number, attributes?: Record<string, string>): void;
  }

  export interface ObservableResult {
    observe(value: number, attributes?: Record<string, string>): void;
  }

  export interface ObservableGauge {
    addCallback(callback: (result: ObservableResult) => void | Promise<void>): void;
  }

  export interface Meter {
    createHistogram(name: string, options?: Record<string, unknown>): Histogram;
    createCounter(name: string, options?: Record<string, unknown>): Counter;
    createObservableGauge(name: string, options?: Record<string, unknown>): ObservableGauge;
  }

  export const context: {
    active(): unknown;
    with<T>(activeContext: unknown, fn: () => T): T;
  };

  export const metrics: {
    getMeter(name: string, version: string): Meter;
  };

  export const trace: {
    getTracer(name: string, version: string): Tracer;
    getActiveSpan(): { spanContext(): { traceId: string } } | undefined;
    setSpan(activeContext: unknown, span: Span): unknown;
  };

  export const SpanStatusCode: {
    OK: number;
    ERROR: number;
  };
}
