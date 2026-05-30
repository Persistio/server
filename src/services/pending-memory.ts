export const PENDING_RECALL_WINDOW_MS = 48 * 60 * 60 * 1000;

export function pendingRecallCutoff(now = new Date()): Date {
  return new Date(now.getTime() - PENDING_RECALL_WINDOW_MS);
}
