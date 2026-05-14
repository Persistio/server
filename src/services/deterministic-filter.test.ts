import assert from 'node:assert/strict';
import { describe, it } from 'vitest';

import { filterMemoryCandidates, normalizeCandidateText } from './deterministic-filter';
import type { ExtractedFact } from './extractor';

function candidate(overrides: Pick<ExtractedFact, 'fact' | 'subject'> & Partial<ExtractedFact>): ExtractedFact {
  return {
    score: 7,
    salience: 0.7,
    sensitivity: 'low',
    type: 'project',
    scope: 'project',
    polarity: 'neutral',
    status: 'active',
    volatility: 'low',
    evidence: null,
    valid_from: null,
    valid_until: null,
    ...overrides
  };
}

describe('filterMemoryCandidates', () => {
  it('accepts durable preference and project facts', () => {
    const result = filterMemoryCandidates([
      candidate({ fact: 'Chris prefers PostgreSQL for Persistio storage.', subject: 'Chris', type: 'user_preference' }),
      candidate({ fact: 'Persistio uses pgvector for semantic search.', subject: 'Persistio' })
    ]);

    assert.equal(result.accepted.length, 2);
    assert.equal(result.dropped.length, 0);
    assert.equal(result.accepted[0].fact.fact, 'Chris prefers PostgreSQL for Persistio storage.');
    assert.equal(result.accepted[0].fact.type, 'user_preference');
  });

  it('drops empty candidates', () => {
    const result = filterMemoryCandidates([
      candidate({ fact: '   ', subject: 'Chris' }),
      candidate({ fact: 'Chris prefers dark mode.', subject: '   ' })
    ]);

    assert.deepEqual(result.dropped.map((item) => item.reason), ['empty', 'empty']);
    assert.equal(result.accepted.length, 0);
  });

  it('drops exact duplicates after normalization', () => {
    const result = filterMemoryCandidates([
      candidate({ fact: 'Chris prefers dark mode.', subject: 'Chris' }),
      candidate({ fact: ' chris prefers dark mode ', subject: 'Chris' })
    ]);

    assert.equal(result.accepted.length, 1);
    assert.equal(result.dropped.length, 1);
    assert.equal(result.dropped[0].reason, 'duplicate');
  });

  it('drops secret-like candidates', () => {
    const result = filterMemoryCandidates([
      candidate({ fact: 'The API key is sk-abc12345678901234567890123456789.', subject: 'OpenAI' }),
      candidate({ fact: 'The database URL is postgresql://user:password@example.com/persistio.', subject: 'Persistio' })
    ]);

    assert.deepEqual(result.dropped.map((item) => item.reason), ['secret_like', 'secret_like']);
    assert.equal(result.accepted.length, 0);
  });

  it('does not treat benign high-entropy identifiers as secrets', () => {
    const result = filterMemoryCandidates([
      candidate({
        fact: 'Persistio build 0123456789abcdef0123456789abcdef01234567 failed in CI.',
        subject: 'Persistio CI'
      })
    ]);

    assert.equal(result.accepted.length, 1);
    assert.equal(result.dropped.length, 0);
  });

  it('drops secret-like candidates from the subject line', () => {
    const result = filterMemoryCandidates([
      candidate({
        fact: 'The key belongs to the deployment environment.',
        subject: 'api_key=sk-abc12345678901234567890123456789'
      })
    ]);

    assert.equal(result.accepted.length, 0);
    assert.equal(result.dropped[0].reason, 'secret_like');
  });

  it('drops low salience conversational filler', () => {
    const result = filterMemoryCandidates([
      candidate({ fact: 'Thanks.', subject: 'Chris' }),
      candidate({ fact: 'The user said okay.', subject: 'Chris' })
    ]);

    assert.deepEqual(result.dropped.map((item) => item.reason), ['low_salience', 'low_salience']);
    assert.equal(result.accepted.length, 0);
  });

  it('keeps specific memories without requiring durable-keyword matches', () => {
    const result = filterMemoryCandidates([
      candidate({
        fact: 'Chris lives in London.',
        subject: 'Chris',
        type: 'system_fact'
      }),
      candidate({
        fact: 'The demo is on Friday.',
        subject: 'Persistio demo',
        type: 'project'
      })
    ]);

    assert.equal(result.accepted.length, 2);
    assert.equal(result.dropped.length, 0);
  });

  it('drops very low salience untyped candidates', () => {
    const result = filterMemoryCandidates([
      candidate({
        fact: 'There was a brief aside.',
        subject: 'Conversation',
        salience: 0.1,
        type: null
      })
    ]);

    assert.equal(result.accepted.length, 0);
    assert.equal(result.dropped[0].reason, 'low_salience');
  });

  it('drops implementation details without durable value', () => {
    const result = filterMemoryCandidates([
      candidate({ fact: 'The stack trace mentioned line 42.', subject: 'Debug session' }),
      candidate({ fact: 'The assistant opened the file.', subject: 'Workspace' })
    ]);

    assert.deepEqual(result.dropped.map((item) => item.reason), ['implementation_detail', 'implementation_detail']);
    assert.equal(result.accepted.length, 0);
  });
});

describe('normalizeCandidateText', () => {
  it('normalizes casing, punctuation, and whitespace', () => {
    assert.equal(normalizeCandidateText('  Chris PREFERS: Postgres!!  '), 'chris prefers postgres');
  });
});
