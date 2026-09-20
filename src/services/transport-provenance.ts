import crypto from 'node:crypto';

export interface SourceEventIdentity {
  namespace: string;
  id: string;
  message_id?: string;
  ordinal: number;
}

export function sourceEventKey(vaultId: string, event: SourceEventIdentity): string {
  return crypto.createHash('sha256')
    .update(JSON.stringify(event.ordinal === 0
      ? [vaultId, event.namespace, event.id]
      : [vaultId, event.namespace, event.id, event.ordinal]))
    .digest('hex');
}
