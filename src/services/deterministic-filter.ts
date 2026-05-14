import crypto from 'node:crypto';

import type { ExtractedFact } from './extractor';
import { matchSecretPattern } from '../utils/secret-filter';

export type CandidateDropReason =
  | 'empty'
  | 'duplicate'
  | 'secret_like'
  | 'low_salience'
  | 'implementation_detail';

export interface FilteredCandidate<TFact extends ExtractedFact = ExtractedFact> {
  fact: TFact;
  normalizedFact: string;
  hash: string;
}

export interface DroppedCandidate<TFact extends ExtractedFact = ExtractedFact> {
  fact: TFact;
  reason: CandidateDropReason;
}

export interface CandidateFilterResult<TFact extends ExtractedFact = ExtractedFact> {
  accepted: Array<FilteredCandidate<TFact>>;
  dropped: Array<DroppedCandidate<TFact>>;
}

const secretPatterns = [
  /\b(?:sk|pk|rk|ghp|gho|ghu|ghs|github_pat|xox[baprs])-[-A-Za-z0-9_]{16,}\b/i,
  /\b(?:postgres|postgresql|mysql|mongodb|redis):\/\/[^@\s]+:[^@\s]+@[^/\s]+/i
];

const lowSaliencePatterns = [
  /^(?:ok|okay|sure|thanks|thank you|great|nice|sounds good|got it|done|cool)[.!]*$/i,
  /^(?:the )?(?:user|assistant) (?:said|asked|replied|responded|mentioned) (?:ok|okay|thanks|thank you|sure|yes|no)[.!]*$/i,
  /\b(?:hello|hi|hey|good morning|good afternoon|good evening)\b/i
];

const implementationDetailPatterns = [
  /\b(?:line|column) \d+\b/i,
  /\b(?:stack trace|traceback|console\.log|debug log|temporary debug|scratch file)\b/i,
  /\b(?:npm install|npm run|node_modules|dist\/|\.ts:\d+|\.js:\d+)\b/i,
  /\b(?:renamed|moved|edited|opened|read) (?:the )?file\b/i
];

export function filterMemoryCandidates<TFact extends ExtractedFact>(candidates: TFact[]): CandidateFilterResult<TFact> {
  const accepted: Array<FilteredCandidate<TFact>> = [];
  const dropped: Array<DroppedCandidate<TFact>> = [];
  const seen = new Set<string>();

  for (const fact of candidates) {
    const normalizedFact = normalizeCandidateText(fact.fact);
    const normalizedSubject = normalizeCandidateText(fact.subject);

    if (!normalizedFact || !normalizedSubject) {
      dropped.push({ fact, reason: 'empty' });
      continue;
    }

    const combined = `${fact.fact}\n${fact.subject}`;
    if (isSecretLike(combined)) {
      dropped.push({ fact, reason: 'secret_like' });
      continue;
    }

    const hashInput = `${normalizedSubject}:${normalizedFact}`;
    const hash = crypto.createHash('sha256').update(hashInput).digest('hex');
    if (seen.has(hash)) {
      dropped.push({ fact, reason: 'duplicate' });
      continue;
    }
    seen.add(hash);

    if (isImplementationDetail(normalizedFact)) {
      dropped.push({ fact, reason: 'implementation_detail' });
      continue;
    }

    if (isLowSalience(fact, normalizedFact)) {
      dropped.push({ fact, reason: 'low_salience' });
      continue;
    }

    accepted.push({
      fact: {
        ...fact,
        fact: fact.fact.trim(),
        subject: fact.subject.trim()
      } as TFact,
      normalizedFact,
      hash
    });
  }

  return { accepted, dropped };
}

export function normalizeCandidateText(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isSecretLike(value: string): boolean {
  // The worker already runs matchSecretPattern on fact text before this filter.
  // Reusing it here over fact + subject catches subject-line leaks without adding
  // another broad high-entropy heuristic that would drop benign IDs such as SHAs.
  return Boolean(matchSecretPattern(value)) || secretPatterns.some((pattern) => pattern.test(value));
}

function isLowSalience(fact: ExtractedFact, normalizedFact: string): boolean {
  if (normalizedFact.length < 8) {
    return true;
  }

  if (lowSaliencePatterns.some((pattern) => pattern.test(normalizedFact))) {
    return true;
  }

  return fact.salience < 0.2 && fact.type === null;
}

function isImplementationDetail(normalizedFact: string): boolean {
  return implementationDetailPatterns.some((pattern) => pattern.test(normalizedFact));
}
