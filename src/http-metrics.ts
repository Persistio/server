import { meter } from './telemetry';

export const httpRequestDurationHistogram = meter.createHistogram('persistio.http.request.duration', {
  description: 'Request latency by route and status',
  unit: 'ms'
});
