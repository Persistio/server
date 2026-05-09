const PRIVATE_KEY_HEADER_PATTERN = /-----BEGIN .* PRIVATE KEY-----/;
const API_KEY_PATTERN = /(?:api[_-]?key|apikey|secret|token|password|passwd|pwd)\s*[:=]\s*\S+/i;
const BEARER_TOKEN_PATTERN = /\bBearer\s+[A-Za-z0-9+/=_.\-]{8,}/i;
const HIGH_ENTROPY_TOKEN_PATTERN = /(?<!\S)[A-Za-z0-9+/=_-]{20,}(?!\S)/g;

export type SecretPatternMatch =
  | 'private_key_header'
  | 'api_key_assignment'
  | 'bearer-token'
  | 'high_entropy_token';

function calculateShannonEntropy(value: string): number {
  const counts = new Map<string, number>();
  for (const char of value) {
    counts.set(char, (counts.get(char) ?? 0) + 1);
  }

  let entropy = 0;
  for (const count of counts.values()) {
    const probability = count / value.length;
    entropy -= probability * Math.log2(probability);
  }

  return entropy;
}

function isHighEntropyToken(value: string): boolean {
  if (value.length < 20) {
    return false;
  }

  const alphabetChars = value.match(/[A-Fa-f0-9+/=_-]/g) ?? [];
  if (alphabetChars.length / value.length <= 0.8) {
    return false;
  }

  return calculateShannonEntropy(value) >= 4.5;
}

export function matchSecretPattern(value: string): SecretPatternMatch | null {
  if (PRIVATE_KEY_HEADER_PATTERN.test(value)) {
    return 'private_key_header';
  }

  if (API_KEY_PATTERN.test(value)) {
    return 'api_key_assignment';
  }

  if (BEARER_TOKEN_PATTERN.test(value)) {
    return 'bearer-token';
  }

  const tokens = value.match(HIGH_ENTROPY_TOKEN_PATTERN) ?? [];
  if (tokens.some((token) => isHighEntropyToken(token))) {
    return 'high_entropy_token';
  }

  return null;
}
