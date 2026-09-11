import crypto from 'node:crypto';

export interface SourceEventIdentity {
  namespace: string;
  id: string;
  message_id?: string;
  ordinal: number;
}

export interface InterSessionEnvelope {
  source_session_id: string;
  source_channel: string;
  source_tool: string;
  is_user: boolean | null;
}

const INTER_SESSION_PREFIX = '[Inter-session message]';

export function parseInterSessionEnvelope(content: string): InterSessionEnvelope | null {
  const header = content.trimStart().split(/\r?\n/u, 1)[0];
  if (!header.startsWith(INTER_SESSION_PREFIX)) return null;
  const fields = parseFields(header.slice(INTER_SESSION_PREFIX.length));
  const isUser = fields.get('isUser');
  return {
    source_session_id: fields.get('sourceSession') ?? 'unknown',
    source_channel: fields.get('sourceChannel') ?? 'unknown',
    source_tool: fields.get('sourceTool') ?? 'unknown',
    is_user: isUser === 'true' ? true : isUser === 'false' ? false : null
  };
}

function parseFields(value: string): Map<string, string> {
  const fields = new Map<string, string>();
  const duplicates = new Set<string>();
  for (const token of value.trim().split(/\s+/u)) {
    const separator = token.indexOf('=');
    if (separator <= 0 || separator === token.length - 1) continue;
    const key = token.slice(0, separator);
    if (fields.has(key) || duplicates.has(key)) {
      fields.delete(key);
      duplicates.add(key);
      continue;
    }
    fields.set(key, token.slice(separator + 1));
  }
  return fields;
}

export function sourceEventKey(vaultId: string, event: SourceEventIdentity): string {
  return crypto.createHash('sha256')
    .update(JSON.stringify(event.ordinal === 0
      ? [vaultId, event.namespace, event.id]
      : [vaultId, event.namespace, event.id, event.ordinal]))
    .digest('hex');
}
