export function formatConversationForExtraction(
  chunks: Array<{ role: string; decryptedContent: string; created_at: string }>
): string {
  return chunks
    .map((chunk) => {
      const timestamp = new Date(chunk.created_at);
      const prefix = Number.isFinite(timestamp.getTime())
        ? `[${timestamp.toISOString()}] `
        : '';
      return `${prefix}${chunk.role}: ${chunk.decryptedContent}`;
    })
    .join('\n');
}
