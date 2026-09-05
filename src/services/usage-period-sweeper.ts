import { closeStaleUsagePeriods, type UsagePeriodSweepResult } from './usage';

export interface UsagePeriodSweeperLogger {
  error?(details: unknown, message?: string): void;
  info?(details: unknown, message?: string): void;
  warn?(details: unknown, message?: string): void;
}

export interface UsagePeriodSweeperOptions {
  batchSize: number;
  intervalMs: number;
  logger?: UsagePeriodSweeperLogger;
  sweep?: (batchSize: number) => Promise<UsagePeriodSweepResult>;
}

export class UsagePeriodSweeper {
  private activeSweep: Promise<UsagePeriodSweepResult> | undefined;
  private readonly batchSize: number;
  private readonly intervalMs: number;
  private readonly logger?: UsagePeriodSweeperLogger;
  private readonly sweep: (batchSize: number) => Promise<UsagePeriodSweepResult>;
  private timer: NodeJS.Timeout | undefined;

  constructor(options: UsagePeriodSweeperOptions) {
    this.batchSize = options.batchSize;
    this.intervalMs = options.intervalMs;
    this.logger = options.logger;
    this.sweep = options.sweep ?? closeStaleUsagePeriods;
  }

  start(): void {
    if (this.timer) return;

    this.timer = setInterval(() => {
      void this.sweepDuePeriods().catch((error: unknown) => {
        this.logger?.error?.({ err: error }, 'Usage period sweep failed');
      });
    }, this.intervalMs);

    void this.sweepDuePeriods().catch((error: unknown) => {
      this.logger?.error?.({ err: error }, 'Usage period sweep failed');
    });
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }

    if (this.activeSweep) {
      try {
        await this.activeSweep;
      } catch (error) {
        this.logger?.warn?.({ err: error }, 'Active usage period sweep failed during shutdown');
      }
    }
  }

  async sweepDuePeriods(): Promise<UsagePeriodSweepResult & { skipped: boolean }> {
    if (this.activeSweep) {
      return {
        closed: 0,
        currentPeriod: '',
        selected: 0,
        skipped: true
      };
    }

    const sweep = this.sweep(this.batchSize);
    this.activeSweep = sweep;

    try {
      const result = await sweep;
      if (result.selected > 0 || result.closed > 0) {
        this.logger?.info?.(result, 'Usage period sweep completed');
      }
      return { ...result, skipped: false };
    } finally {
      if (this.activeSweep === sweep) {
        this.activeSweep = undefined;
      }
    }
  }
}
