import { context, metrics, SpanStatusCode, trace } from '@opentelemetry/api';

export interface SpanLike {
  setAttribute(key: string, value: string | number | boolean): void;
  setStatus(status: { code: number; message?: string }): void;
  recordException(error: Error): void;
  end(): void;
}

export interface HistogramLike {
  record(value: number, attributes?: Record<string, string>): void;
}

export interface CounterLike {
  add(value: number, attributes?: Record<string, string>): void;
}

export interface ObservableResultLike {
  observe(value: number, attributes?: Record<string, string>): void;
}

export interface ObservableGaugeLike {
  addCallback(callback: (result: ObservableResultLike) => void | Promise<void>): void;
}

export interface MeterLike {
  createHistogram(name: string, options?: Record<string, unknown>): HistogramLike;
  createCounter(name: string, options?: Record<string, unknown>): CounterLike;
  createObservableGauge(name: string, options?: Record<string, unknown>): ObservableGaugeLike;
}

const noopSpan: SpanLike = {
  setAttribute() {},
  setStatus() {},
  recordException() {},
  end() {}
};

export function getTracer() {
  return trace.getTracer('persistio-server', '0.1.0');
}

export const meter = metrics.getMeter('persistio-server', '0.1.0');

export function getTraceId() {
  return trace.getActiveSpan()?.spanContext().traceId;
}

export function getSpanAttributes(record: Record<string, unknown>) {
  const traceId = getTraceId();
  return traceId ? { ...record, trace_id: traceId } : record;
}

export async function withSpan<T>(
  name: string,
  attributes: Record<string, string | number | boolean | undefined>,
  fn: (span: SpanLike) => Promise<T>
): Promise<T> {
  const tracer = getTracer();
  const filteredAttributes = Object.fromEntries(
    Object.entries(attributes).filter((entry): entry is [string, string | number | boolean] => entry[1] !== undefined)
  );
  const span = tracer?.startSpan(name, { attributes: filteredAttributes }) ?? noopSpan;
  const spanContext = trace.setSpan(context.active(), span);
  const runInContext = (fn: () => Promise<T>) => context.with(spanContext, fn);

  const run = async () => {
    try {
      const result = await fn(span);
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (error) {
      span.recordException(error instanceof Error ? error : new Error(String(error)));
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: error instanceof Error ? error.message : String(error)
      });
      throw error;
    } finally {
      span.end();
    }
  };
  return runInContext(() => run());
}
