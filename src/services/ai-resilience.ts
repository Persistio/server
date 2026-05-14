import { getConfig } from '../config';

export class CircuitBreakerOpenError extends Error {
  readonly statusCode = 503;
  readonly retryAfterMs: number;
  readonly service: string;

  constructor(service: string, retryAfterMs: number) {
    super(`${service} circuit breaker is open`);
    this.name = 'CircuitBreakerOpenError';
    this.service = service;
    this.retryAfterMs = retryAfterMs;
  }
}

export function isRateLimitError(error: unknown): error is { status?: number } {
  return Boolean(error && typeof error === 'object' && 'status' in error && (error as { status?: number }).status === 429);
}

export function isAuthFailureError(error: unknown): error is { status?: number } {
  return Boolean(
    error
      && typeof error === 'object'
      && 'status' in error
      && ((error as { status?: number }).status === 401 || (error as { status?: number }).status === 403)
  );
}

interface CircuitBreakerState {
  consecutiveFailures: number;
  currentProbeIntervalMs: number;
  halfOpenProbeInFlight: boolean;
  nextProbeAt: number | null;
  state: 'closed' | 'open' | 'half-open';
}

export class ServiceCircuitBreaker {
  private readonly failureThreshold: number;
  private readonly initialProbeIntervalMs: number;
  private readonly maxProbeIntervalMs: number;
  private readonly service: string;
  private readonly state: CircuitBreakerState;

  constructor(service: string) {
    const config = getConfig();
    this.service = service;
    this.failureThreshold = config.CIRCUIT_BREAKER_THRESHOLD;
    this.initialProbeIntervalMs = config.CIRCUIT_BREAKER_PROBE_INTERVAL_MS;
    this.maxProbeIntervalMs = config.CIRCUIT_BREAKER_MAX_PROBE_INTERVAL_MS;
    this.state = {
      consecutiveFailures: 0,
      currentProbeIntervalMs: this.initialProbeIntervalMs,
      halfOpenProbeInFlight: false,
      nextProbeAt: null,
      state: 'closed'
    };
  }

  beforeRequest(now = Date.now()) {
    if (this.state.state === 'closed') {
      return;
    }

    if (this.state.state === 'open') {
      if (this.state.nextProbeAt && now >= this.state.nextProbeAt) {
        this.state.state = 'half-open';
        this.state.halfOpenProbeInFlight = true;
        return;
      }

      throw new CircuitBreakerOpenError(this.service, Math.max(0, (this.state.nextProbeAt ?? now) - now));
    }

    if (this.state.halfOpenProbeInFlight) {
      throw new CircuitBreakerOpenError(this.service, this.state.currentProbeIntervalMs);
    }

    this.state.halfOpenProbeInFlight = true;
  }

  onSuccess() {
    this.state.consecutiveFailures = 0;
    this.state.currentProbeIntervalMs = this.initialProbeIntervalMs;
    this.state.halfOpenProbeInFlight = false;
    this.state.nextProbeAt = null;
    this.state.state = 'closed';
  }

  onFailure(error: unknown): { opened: boolean; nextProbeAt: number | null } {
    if (!isAuthFailureError(error)) {
      if (this.state.state === 'half-open') {
        this.state.halfOpenProbeInFlight = false;
      }
      return { opened: false, nextProbeAt: this.state.nextProbeAt };
    }

    if (this.state.state === 'half-open') {
      this.state.currentProbeIntervalMs = Math.min(this.maxProbeIntervalMs, this.state.currentProbeIntervalMs * 2);
      this.trip();
      return { opened: true, nextProbeAt: this.state.nextProbeAt };
    }

    this.state.consecutiveFailures += 1;
    if (this.state.consecutiveFailures >= this.failureThreshold) {
      this.trip();
      return { opened: true, nextProbeAt: this.state.nextProbeAt };
    }

    return { opened: false, nextProbeAt: this.state.nextProbeAt };
  }

  getSnapshot() {
    return {
      ...this.state,
      service: this.service
    };
  }

  private trip() {
    this.state.consecutiveFailures = 0;
    this.state.halfOpenProbeInFlight = false;
    this.state.nextProbeAt = Date.now() + this.state.currentProbeIntervalMs;
    this.state.state = 'open';
  }
}
