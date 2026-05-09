export function sanitizePromptData(value: string): string {
  return value
    .replace(/\[.*?\]/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/[^\x20-\x7E]/g, ' ')
    .replace(/[\r\n]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 500);
}

export function scrubMemoryForCurator(data: string): string {
  let out = data.replace(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g, '[internal host]');
  out = out.replace(
    /(?<![\/\w])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?![\/\w])/gi,
    '[id]'
  );
  out = out.replace(/(?<!\w):(\d{2,5})(?!\d)(?![\/\w])/g, ':[port]');
  out = out.replace(/\b[A-Za-z0-9+=_\-]{20,}\b/g, (m) => isSensitiveToken(m) ? '[redacted]' : m);
  return out;
}

function isSensitiveToken(s: string): boolean {
  if (/^[0-9a-f]{20,}$/i.test(s)) {
    return true;
  }

  return isHighEntropy(s);
}

function isHighEntropy(s: string): boolean {
  const counts = new Map<string, number>();
  for (const c of s) {
    counts.set(c, (counts.get(c) ?? 0) + 1);
  }

  let entropy = 0;
  for (const n of counts.values()) {
    const p = n / s.length;
    entropy -= p * Math.log2(p);
  }

  return entropy >= 4.5;
}
