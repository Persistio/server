import { beforeEach, describe, expect, it, vi } from 'vitest';

import { CircuitBreakerOpenError, ServiceCircuitBreaker, isAuthFailureError, isRateLimitError } from '../ai-resilience';

beforeEach(() => {
  process.env.DATABASE_URL ??= 'postgres://example.com/test';
  process.env.ADMIN_API_KEY ??= 'test-admin-key';
  process.env.OPENAI_API_KEY ??= 'test-openai-key';
});

describe('ai resilience helpers', () => {
  it('detects auth and rate limit errors by status code', () => {
    expect(isAuthFailureError({ status: 401 })).toBe(true);
    expect(isAuthFailureError({ status: 403 })).toBe(true);
    expect(isAuthFailureError({ status: 429 })).toBe(false);
    expect(isRateLimitError({ status: 429 })).toBe(true);
    expect(isRateLimitError({ status: 500 })).toBe(false);
  });

  it('opens after consecutive auth failures and blocks requests until probe time', () => {
    vi.useFakeTimers();
    const breaker = new ServiceCircuitBreaker('extractor');

    breaker.onFailure({ status: 401 });
    breaker.onFailure({ status: 403 });
    const result = breaker.onFailure({ status: 401 });

    expect(result.opened).toBe(true);
    expect(() => breaker.beforeRequest()).toThrow(CircuitBreakerOpenError);

    vi.advanceTimersByTime(300000);
    expect(() => breaker.beforeRequest()).not.toThrow();
    breaker.onSuccess();
    expect(() => breaker.beforeRequest()).not.toThrow();
    vi.useRealTimers();
  });
});
