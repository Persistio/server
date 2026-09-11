import { describe, expect, it } from 'vitest';
import { memoryTelemetryComponent } from './memory-telemetry-resource';

describe('fixed telemetry producer identities', () => {
  it('identifies main independently of worker data', () => {
    expect(memoryTelemetryComponent(true, { component: 'curation' })).toBe('main');
  });
  it('accepts only the existing two worker-launch components', () => {
    expect(memoryTelemetryComponent(false, { component: 'extraction' })).toBe('extraction');
    expect(memoryTelemetryComponent(false, { component: 'curation' })).toBe('curation');
    for (const data of [null, undefined, '', 'extraction', {}, { component: 'main' }, { component: 1 }, { component: 'unknown' }]) {
      expect(memoryTelemetryComponent(false, data)).toBe('');
    }
  });
});
