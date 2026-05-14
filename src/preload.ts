// IMPORTANT: This file must have no imports before useAzureMonitor() is called.
// It is loaded via node --require before the application entrypoints.
import { useAzureMonitor } from './azure-monitor';

const connectionString = process.env.APPLICATIONINSIGHTS_CONNECTION_STRING;

if (connectionString) {
  useAzureMonitor({
    azureMonitorExporterOptions: { connectionString },
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
