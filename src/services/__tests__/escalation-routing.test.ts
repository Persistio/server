import { describe, expect, it } from 'vitest';

import { decideEscalation, defaultDecisionWithoutEscalator, type EscalationMatchContext } from '../escalation-routing';
import type { DedupInput } from '../dedup';

function input(overrides: Partial<DedupInput> = {}): DedupInput {
  return {
    vaultId: 'vault-1',
    fact: 'Persistio stores raw chunks before extracting memories.',
    score: 8,
    subject: 'Persistio',
    embedding: [0.1, 0.2],
    sourceChunks: ['00000000-0000-0000-0000-000000000001'],
    salience: 0.7,
    sensitivity: 'low',
    type: 'domain_knowledge',
    scope: 'project',
    polarity: 'neutral',
    status: 'active',
    volatility: 'low',
    evidence: null,
    validFrom: null,
    validUntil: null,
    sourceSegmentId: null,
    ...overrides
  };
}

function match(overrides: Partial<EscalationMatchContext> = {}): EscalationMatchContext {
  return {
    similarity: 0.82,
    confidence: 1,
    status: 'active',
    type: 'domain_knowledge',
    polarity: 'neutral',
    volatility: 'low',
    score: 8,
    salience: 0.7,
    ...overrides
  };
}

describe('decideEscalation', () => {
  it('keeps routine moderate overlap on the cheap path', () => {
    expect(decideEscalation(input(), match())).toEqual({
      escalate: false,
      reasons: []
    });
  });

  it('escalates behavioral memories', () => {
    expect(decideEscalation(
      input({ type: 'user_preference' }),
      match()
    )).toEqual({
      escalate: true,
      reasons: ['behavioral_signal']
    });
  });

  it('escalates low-confidence extraction results', () => {
    expect(decideEscalation(
      input({ score: 6, salience: 0.8 }),
      match()
    ).reasons).toContain('low_extraction_confidence');

    expect(decideEscalation(
      input({ score: 9, salience: 0.3 }),
      match()
    ).reasons).toContain('low_extraction_confidence');
  });

  it('escalates strong overlap and possible conflict', () => {
    const decision = decideEscalation(
      input({ polarity: 'positive' }),
      match({ similarity: 0.89, polarity: 'negative' })
    );

    expect(decision.escalate).toBe(true);
    expect(decision.reasons).toContain('strong_semantic_overlap');
    expect(decision.reasons).toContain('possible_conflict');
  });

  it('escalates supersession-prone and already ambiguous memories', () => {
    const decision = decideEscalation(
      input({ type: 'decision' }),
      match({ similarity: 0.85, status: 'needs_review', confidence: 0.5 })
    );

    expect(decision.escalate).toBe(true);
    expect(decision.reasons).toContain('possible_supersession');
    expect(decision.reasons).toContain('existing_needs_review');
    expect(decision.reasons).toContain('low_existing_confidence');
  });

  it('separates existing candidate and low existing confidence reasons', () => {
    const decision = decideEscalation(
      input(),
      match({ status: 'candidate', confidence: 0.5 })
    );

    expect(decision.escalate).toBe(true);
    expect(decision.reasons).toContain('existing_candidate');
    expect(decision.reasons).toContain('low_existing_confidence');
    expect(decision.reasons).not.toContain('existing_needs_review');
  });
});

describe('defaultDecisionWithoutEscalator', () => {
  it('preserves the safe needs_review default when escalation is required but no extractor is available', () => {
    expect(defaultDecisionWithoutEscalator(true)).toBe('needs_review');
  });

  it('keeps routine cheap-path overlap without escalation', () => {
    expect(defaultDecisionWithoutEscalator(false)).toBe('keep_both');
  });
});
