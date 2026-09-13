import { AzureMonitorMetricExporter, AzureMonitorTraceExporter } from '@azure/monitor-opentelemetry-exporter';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { AggregationTemporality, InstrumentType, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { envDetector, processDetector, hostDetector } from '@opentelemetry/resources';
import { memoryProducerDetector } from './memory-telemetry-resource';

export interface OpenTelemetryInstrumentationOptions {
  azureSdk?: { enabled: boolean };
  http?: { enabled: boolean };
  postgreSql?: { enabled: boolean };
}

export interface AzureMonitorOpenTelemetryOptions {
  azureMonitorExporterOptions?: { connectionString?: string };
  enableLiveMetrics?: boolean;
  instrumentationOptions?: OpenTelemetryInstrumentationOptions;
  serviceName?: string;
}

export interface OtlpOpenTelemetryOptions {
  endpoint?: string;
  metricEndpoint?: string;
  traceEndpoint?: string;
  instrumentationOptions?: OpenTelemetryInstrumentationOptions;
  serviceName?: string;
}

let sdk: NodeSDK | undefined;
let shutdownPromise: Promise<void> | undefined;

const DEFAULT_OTLP_HTTP_ENDPOINT = 'http://localhost:4318';
const isEnabled = (value?: { enabled: boolean }) => value?.enabled !== false;

// Do not retain unobserved health states between snapshots. The exported type
// remains GAUGE; cumulative counters and the Azure exporter are unchanged.
export class SnapshotOtlpMetricExporter extends OTLPMetricExporter {
  override selectAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality {
    return instrumentType === InstrumentType.OBSERVABLE_GAUGE
      ? AggregationTemporality.DELTA : super.selectAggregationTemporality(instrumentType);
  }
}

export function useAzureMonitor(options: AzureMonitorOpenTelemetryOptions = {}) {
  if (sdk || shutdownPromise) {
    return;
  }

  const connectionString = options.azureMonitorExporterOptions?.connectionString;
  if (!connectionString) {
    return;
  }

  const traceExporter = new AzureMonitorTraceExporter({ connectionString });
  const metricReader = new PeriodicExportingMetricReader({
    exporter: new AzureMonitorMetricExporter({ connectionString })
  });

  sdk = new NodeSDK({
    serviceName: options.serviceName ?? 'persistio-server',
    resourceDetectors: [memoryProducerDetector],
    traceExporter,
    metricReaders: [metricReader],
    // Automatic HTTP/DB/SDK spans can export URLs, statements and raw provider
    // exceptions. Only the explicit allowlisted platform spans are exported.
    instrumentations: []
  });

  sdk.start();
}

export function useOtlpTelemetry(options: OtlpOpenTelemetryOptions = {}) {
  if (sdk || shutdownPromise) {
    return;
  }

  const endpoint = options.endpoint ?? DEFAULT_OTLP_HTTP_ENDPOINT;
  const traceExporter = new OTLPTraceExporter({
    url: options.traceEndpoint ?? buildOtlpEndpoint(endpoint, 'v1/traces')
  });
  const metricReader = new PeriodicExportingMetricReader({
    exporter: new SnapshotOtlpMetricExporter({
      url: options.metricEndpoint ?? buildOtlpEndpoint(endpoint, 'v1/metrics')
    })
  });

  sdk = new NodeSDK({
    serviceName: options.serviceName ?? 'persistio-server',
    resourceDetectors: [memoryProducerDetector],
    traceExporter,
    metricReaders: [metricReader],
    // Automatic HTTP/DB/SDK spans can export URLs, statements and raw provider
    // exceptions. Only the explicit allowlisted platform spans are exported.
    instrumentations: []
  });

  sdk.start();
}

export function shutdownTelemetry(): Promise<void> {
  if (shutdownPromise) return shutdownPromise;
  if (!sdk) return Promise.resolve();
  const current = sdk;
  sdk = undefined;
  // No platform timeout here. Callers may bound their wait, not this SDK's
  // lifetime. A rejected SDK may still have components running: do not reinit.
  shutdownPromise = Promise.resolve().then(() => current.shutdown()).then(() => {
    shutdownPromise = undefined;
  });
  return shutdownPromise;
}

export const shutdownAzureMonitor = shutdownTelemetry;

function buildOtlpEndpoint(endpoint: string, path: 'v1/metrics' | 'v1/traces'): string {
  return `${endpoint.replace(/\/+$/, '')}/${path}`;
}
