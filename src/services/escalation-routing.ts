import type { DedupInput } from './dedup';

export type EscalationReason =
  | 'behavioral_signal'
  | 'low_extraction_confidence'
  | 'strong_semantic_overlap'
  | 'possible_conflict'
  | 'possible_supersession'
  | 'existing_needs_review'
  | 'existing_candidate'
  | 'low_existing_confidence';

export interface EscalationMatchContext {
  similarity: number;
  confidence: number;
  status: 'active' | 'candidate' | 'superseded' | 'contradicted' | 'needs_review';
  type: DedupInput['type'];
  polarity: DedupInput['polarity'];
  volatility: DedupInput['volatility'];
  score: number;
  salience: number;
}

export interface EscalationDecision {
  escalate: boolean;
  reasons: EscalationReason[];
}

export type MissingEscalatorDecision = 'needs_review' | 'keep_both';

const behavioralTypes = new Set<DedupInput['type']>([
  'user_preference',
  'user_rule',
  'task_pattern',
  'workflow'
]);

const supersessionTypes = new Set<DedupInput['type']>([
  'constraint',
  'decision',
  'project',
  'system_fact'
]);

export function decideEscalation(input: DedupInput, match: EscalationMatchContext): EscalationDecision {
  const reasons = new Set<EscalationReason>();

  if (behavioralTypes.has(input.type) || behavioralTypes.has(match.type)) {
    reasons.add('behavioral_signal');
  }

  // This is intentionally about the newly extracted candidate. Existing memory
  // uncertainty is tracked separately as low_existing_confidence.
  if (input.score <= 6 || input.salience < 0.45) {
    reasons.add('low_extraction_confidence');
  }

  if (match.similarity >= 0.88) {
    reasons.add('strong_semantic_overlap');
  }

  if (
    input.polarity !== 'neutral' &&
    match.polarity !== 'neutral' &&
    input.polarity !== match.polarity
  ) {
    reasons.add('possible_conflict');
  }

  if (
    input.status === 'superseded' ||
    input.status === 'contradicted' ||
    input.status === 'needs_review' ||
    input.volatility === 'high' ||
    (match.similarity >= 0.84 && (supersessionTypes.has(input.type) || supersessionTypes.has(match.type)))
  ) {
    reasons.add('possible_supersession');
  }

  if (match.status === 'needs_review') {
    reasons.add('existing_needs_review');
  }

  if (match.status === 'candidate') {
    reasons.add('existing_candidate');
  }

  if (match.confidence < 0.6) {
    reasons.add('low_existing_confidence');
  }

  return {
    escalate: reasons.size > 0,
    reasons: Array.from(reasons)
  };
}

export function defaultDecisionWithoutEscalator(escalate: boolean): MissingEscalatorDecision {
  return escalate ? 'needs_review' : 'keep_both';
}
