import { AzureMonitorMetricExporter, AzureMonitorTraceExporter } from '@azure/monitor-opentelemetry-exporter';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';

export interface AzureMonitorOpenTelemetryOptions {
  azureMonitorExporterOptions?: { connectionString?: string };
  enableLiveMetrics?: boolean;
  instrumentationOptions?: Record<string, { enabled: boolean }>;
}

let sdk: NodeSDK | undefined;

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
    serviceName: 'persistio-server',
    traceExporter,
    metricReaders: [metricReader],
    instrumentations: [
      getNodeAutoInstrumentations(instrumentationConfig)
    ]
  });

  sdk.start();
}

export async function shutdownAzureMonitor() {
  if (!sdk) {
    return;
  }

  const current = sdk;
  sdk = undefined;
  await current.shutdown();
}
