import { AzureMonitorMetricExporter, AzureMonitorTraceExporter } from '@azure/monitor-opentelemetry-exporter';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { NodeSDK } from '@opentelemetry/sdk-node';

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

const DEFAULT_OTLP_HTTP_ENDPOINT = 'http://localhost:4318';
const isEnabled = (value?: { enabled: boolean }) => value?.enabled !== false;

export function useAzureMonitor(options: AzureMonitorOpenTelemetryOptions = {}) {
  if (sdk) {
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
  const instrumentationConfig: Record<string, { enabled: boolean }> = {
    '@opentelemetry/instrumentation-azure-sdk': {
      enabled: isEnabled(options.instrumentationOptions?.azureSdk)
    },
    '@opentelemetry/instrumentation-http': {
      enabled: isEnabled(options.instrumentationOptions?.http)
    },
    '@opentelemetry/instrumentation-pg': {
      enabled: isEnabled(options.instrumentationOptions?.postgreSql)
    }
  };

  sdk = new NodeSDK({
    serviceName: options.serviceName ?? 'persistio-server',
    traceExporter,
    metricReaders: [metricReader],
    instrumentations: [getNodeAutoInstrumentations(instrumentationConfig)]
  });

  sdk.start();
}

export function useOtlpTelemetry(options: OtlpOpenTelemetryOptions = {}) {
  if (sdk) {
    return;
  }

  const endpoint = options.endpoint ?? DEFAULT_OTLP_HTTP_ENDPOINT;
  const traceExporter = new OTLPTraceExporter({
    url: options.traceEndpoint ?? buildOtlpEndpoint(endpoint, 'v1/traces')
  });
  const metricReader = new PeriodicExportingMetricReader({
    exporter: new OTLPMetricExporter({
      url: options.metricEndpoint ?? buildOtlpEndpoint(endpoint, 'v1/metrics')
    })
  });
  const instrumentationConfig: Record<string, { enabled: boolean }> = {
    '@opentelemetry/instrumentation-azure-sdk': {
      enabled: isEnabled(options.instrumentationOptions?.azureSdk)
    },
    '@opentelemetry/instrumentation-http': {
      enabled: isEnabled(options.instrumentationOptions?.http)
    },
    '@opentelemetry/instrumentation-pg': {
      enabled: isEnabled(options.instrumentationOptions?.postgreSql)
    }
  };

  sdk = new NodeSDK({
    serviceName: options.serviceName ?? 'persistio-server',
    traceExporter,
    metricReaders: [metricReader],
    instrumentations: [getNodeAutoInstrumentations(instrumentationConfig)]
  });

  sdk.start();
}

export async function shutdownTelemetry() {
  if (!sdk) {
    return;
  }

  const current = sdk;
  sdk = undefined;
  await current.shutdown();
}

export const shutdownAzureMonitor = shutdownTelemetry;

function buildOtlpEndpoint(endpoint: string, path: 'v1/metrics' | 'v1/traces'): string {
  return `${endpoint.replace(/\/+$/, '')}/${path}`;
}
