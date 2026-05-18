import { describe, expect, it } from 'vitest';

import { composeRecallRows } from './recall';

type Row = Parameters<typeof composeRecallRows>[0][number];

function row(id: string, source: Row['source']): Row {
  return {
    id,
    data: id,
    subject: id,
    categories: [],
    confidence: 1,
    score: 8,
    salience: '0.80',
    sensitivity: 'low',
    type: source === 'behavioral' ? 'user_rule' : 'system_fact',
    scope: 'global',
    polarity: 'neutral',
    status: 'active',
    valid_from: null,
    valid_until: null,
    similarity: source === 'behavioral' ? 1 : 0.9,
    source,
    created_at: '2026-05-16T00:00:00.000Z',
    updated_at: '2026-05-16T00:00:00.000Z',
    recall_count: 0,
    last_recalled: null
  };
}

describe('composeRecallRows', () => {
  const rows = [
    row('behavior-1', 'behavioral'),
    row('behavior-2', 'behavioral'),
    row('behavior-3', 'behavioral'),
    row('semantic-1', 'semantic'),
    row('semantic-2', 'semantic'),
    row('semantic-3', 'semantic'),
    row('semantic-4', 'semantic')
  ];

  it('preserves behavioral injection in agent mode', () => {
    expect(composeRecallRows(rows, 5, 'agent').map((item) => item.id)).toEqual([
      'behavior-1',
      'behavior-2',
      'behavior-3',
      'semantic-1',
      'semantic-2'
    ]);
  });

  it('respects top_k when behavioral injection exceeds the requested budget', () => {
    expect(composeRecallRows(rows, 2, 'agent').map((item) => item.id)).toEqual([
      'behavior-1',
      'behavior-2'
    ]);
  });

  it('keeps the full top_k budget for semantic and graph context in factual mode', () => {
    expect(composeRecallRows(rows, 3, 'factual').map((item) => item.id)).toEqual([
      'semantic-1',
      'semantic-2',
      'semantic-3'
    ]);
  });
});
