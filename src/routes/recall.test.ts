import { describe, expect, it } from 'vitest';

import { buildRecallBundle, composeRecallRows } from './recall';

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
    type: source === 'global_behavioral' ? 'user_rule' : 'system_fact',
    scope: 'global',
    polarity: 'neutral',
    status: 'active',
    valid_from: null,
    valid_until: null,
    similarity: source === 'global_behavioral' ? 0 : 0.9,
    source,
    created_at: '2026-05-16T00:00:00.000Z',
    updated_at: '2026-05-16T00:00:00.000Z',
    recall_count: 0,
    last_recalled: null
  };
}

describe('composeRecallRows', () => {
  const rows = [
    row('global-rule-1', 'global_behavioral'),
    row('global-rule-2', 'global_behavioral'),
    row('global-rule-3', 'global_behavioral'),
    row('semantic-1', 'semantic'),
    row('semantic-2', 'semantic'),
    row('semantic-3', 'semantic'),
    row('semantic-4', 'semantic')
  ];

  it('keeps the top_k budget for query-relevant rows in agent mode', () => {
    expect(composeRecallRows(rows, 5, 'agent').map((item) => item.id)).toEqual([
      'semantic-1',
      'semantic-2',
      'semantic-3',
      'semantic-4'
    ]);
  });

  it('does not let global rules consume the requested query budget', () => {
    expect(composeRecallRows(rows, 2, 'agent').map((item) => item.id)).toEqual([
      'semantic-1',
      'semantic-2'
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

describe('buildRecallBundle', () => {
  it('separates global rules from query-relevant bundle sections', () => {
    const globalRule = { ...row('global-rule', 'global_behavioral'), data: 'Always ask before destructive actions.' };
    const relevantRule = {
      ...row('relevant-rule', 'semantic'),
      type: 'user_rule',
      data: 'When discussing Project Atlas, prefer low-cost infrastructure.',
      similarity: 0.95
    };

    expect(buildRecallBundle([relevantRule], [globalRule]).bundle).toMatchObject({
      global_user_rules: ['Always ask before destructive actions.'],
      user_rules: ['When discussing Project Atlas, prefer low-cost infrastructure.']
    });
  });

  it('orders global rules by salience and recency', () => {
    const older = {
      ...row('older-rule', 'global_behavioral'),
      data: 'older',
      created_at: '2026-05-16T00:00:00.000Z'
    };
    const newer = {
      ...row('newer-rule', 'global_behavioral'),
      data: 'newer',
      created_at: '2026-05-17T00:00:00.000Z'
    };

    expect(buildRecallBundle([], [older, newer]).bundle.global_user_rules).toEqual([
      'newer',
      'older'
    ]);
  });

  it('orders query-relevant rows by similarity before salience', () => {
    const moreRelevant = {
      ...row('more-relevant', 'semantic'),
      type: 'user_rule',
      data: 'more relevant',
      similarity: 0.95,
      salience: '0.60'
    };
    const moreSalient = {
      ...row('more-salient', 'semantic'),
      type: 'user_rule',
      data: 'more salient',
      similarity: 0.75,
      salience: '0.90'
    };

    expect(buildRecallBundle([moreSalient, moreRelevant]).bundle.user_rules).toEqual([
      'more relevant',
      'more salient'
    ]);
  });
});
