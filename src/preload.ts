// IMPORTANT: This file must have no imports before telemetry is initialised.
// It is loaded via node --require before the application entrypoints.
import { useAzureMonitor, useOtlpTelemetry } from './azure-monitor';

const connectionString = process.env.APPLICATIONINSIGHTS_CONNECTION_STRING;
const provider = getTelemetryProvider();

switch (provider) {
  case 'azure_monitor':
    if (connectionString) {
      useAzureMonitor({
        azureMonitorExporterOptions: { connectionString },
        serviceName: process.env.OTEL_SERVICE_NAME || 'persistio-server',
        instrumentationOptions: {
          http: { enabled: true },
          azureSdk: { enabled: true },
          postgreSql: { enabled: true }
        }
      });
      console.log('[persistio] OpenTelemetry: Azure Monitor initialised');
    } else {
      console.log('[persistio] OpenTelemetry: APPLICATIONINSIGHTS_CONNECTION_STRING not set, telemetry disabled');
    }
    break;
  case 'gcp_otlp':
    useOtlpTelemetry({
      endpoint: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318',
      traceEndpoint: process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || undefined,
      metricEndpoint: process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT || undefined,
      serviceName: process.env.OTEL_SERVICE_NAME || process.env.K_SERVICE || 'persistio-server',
      instrumentationOptions: {
        http: { enabled: true },
        azureSdk: { enabled: false },
        postgreSql: { enabled: true }
      }
    });
    console.log('[persistio] OpenTelemetry: OTLP HTTP initialised');
    break;
  case 'none':
    console.log('[persistio] OpenTelemetry: telemetry disabled');
    break;
  default:
    console.log(`[persistio] OpenTelemetry: unknown TELEMETRY_PROVIDER=${provider}, telemetry disabled`);
}

function getTelemetryProvider(): string {
  if (process.env.TELEMETRY_PROVIDER) {
    return process.env.TELEMETRY_PROVIDER;
  }

  return connectionString ? 'azure_monitor' : 'none';
}
