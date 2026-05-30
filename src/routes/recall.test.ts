import { describe, expect, it } from 'vitest';

import {
  buildRecallBundle,
  combineSemanticCandidateRows,
  composeRecallRows,
  composeRelatedRecallRows,
  recallCandidateLimit
} from './recall';

type Row = Parameters<typeof composeRecallRows>[0][number];

function row(id: string, source: Row['source'], overrides: Partial<Row> = {}): Row {
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
    source_timestamp: '2026-05-15T12:00:00.000Z',
    similarity: source === 'global_behavioral' ? 0 : 0.9,
    source,
    created_at: '2026-05-16T00:00:00.000Z',
    updated_at: '2026-05-16T00:00:00.000Z',
    recall_count: 0,
    last_recalled: null,
    ...overrides
  };
}

describe('recallCandidateLimit', () => {
  it('overfetches enough rows to filter weak matches before applying top_k', () => {
    expect(recallCandidateLimit(3)).toBe(25);
    expect(recallCandidateLimit(10)).toBe(40);
    expect(recallCandidateLimit(100)).toBe(400);
  });
});

describe('combineSemanticCandidateRows', () => {
  it('keeps the full active candidate pool before adding fresh pending candidates', () => {
    const activeRows = Array.from({ length: recallCandidateLimit(5) }, (_, index) => row(`active-${index}`, 'semantic', {
      similarity: 0.70 - (index * 0.001)
    }));
    const pendingRows = Array.from({ length: recallCandidateLimit(5) }, (_, index) => row(`pending-${index}`, 'semantic', {
      status: 'candidate',
      similarity: 0.95 - (index * 0.001),
      source_timestamp: '2026-05-29T00:00:00.000Z',
      created_at: '2026-05-29T00:00:00.000Z'
    }));

    const combinedRows = combineSemanticCandidateRows(activeRows, pendingRows);

    expect(combinedRows).toHaveLength(activeRows.length + pendingRows.length);
    expect(combinedRows.slice(0, activeRows.length).map((item) => item.id)).toEqual(
      activeRows.map((item) => item.id)
    );
    expect(combinedRows.slice(activeRows.length).map((item) => item.id)).toEqual(
      pendingRows.map((item) => item.id)
    );
  });

  it('deduplicates pending candidates behind the active row for the same memory', () => {
    const activeRow = row('same-memory', 'semantic', {
      status: 'active',
      similarity: 0.80
    });
    const pendingRow = row('same-memory', 'semantic', {
      status: 'candidate',
      similarity: 0.95,
      source_timestamp: '2026-05-29T00:00:00.000Z',
      created_at: '2026-05-29T00:00:00.000Z'
    });

    expect(combineSemanticCandidateRows([activeRow], [pendingRow])).toEqual([
      activeRow
    ]);
  });
});

describe('composeRecallRows', () => {
  const now = new Date('2026-05-30T00:00:00.000Z');
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

  it('promotes behavioral memory types in agent mode when semantic scores are close', () => {
    const candidates = [
      row('factual-higher-similarity', 'semantic', { type: 'system_fact', similarity: 0.79 }),
      row('domain-higher-similarity', 'semantic', { type: 'domain_knowledge', similarity: 0.78 }),
      row('rule-close-match', 'semantic', { type: 'user_rule', similarity: 0.74 }),
      row('preference-close-match', 'semantic', { type: 'user_preference', similarity: 0.73 })
    ];

    expect(composeRecallRows(candidates, 2, 'agent', 0.30).map((item) => item.id)).toEqual([
      'rule-close-match',
      'preference-close-match'
    ]);
  });

  it('promotes factual memory types in factual mode when semantic scores are close', () => {
    const candidates = [
      row('rule-higher-similarity', 'semantic', { type: 'user_rule', similarity: 0.79 }),
      row('preference-higher-similarity', 'semantic', { type: 'user_preference', similarity: 0.78 }),
      row('fact-close-match', 'semantic', { type: 'system_fact', similarity: 0.74 }),
      row('domain-close-match', 'semantic', { type: 'domain_knowledge', similarity: 0.73 })
    ];

    expect(composeRecallRows(candidates, 2, 'factual', 0.30).map((item) => item.id)).toEqual([
      'fact-close-match',
      'domain-close-match'
    ]);
  });

  it('promotes recent memories when semantic scores are close', () => {
    const candidates = [
      row('older-slightly-higher-similarity', 'semantic', {
        similarity: 0.82,
        source_timestamp: '2026-04-20T00:00:00.000Z',
        updated_at: '2026-04-20T00:00:00.000Z',
        created_at: '2026-04-20T00:00:00.000Z'
      }),
      row('recent-close-match', 'semantic', {
        similarity: 0.79,
        source_timestamp: '2026-05-29T00:00:00.000Z',
        updated_at: '2026-05-29T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      })
    ];

    expect(composeRecallRows(candidates, 1, 'factual', 0.30, now).map((item) => item.id)).toEqual([
      'recent-close-match'
    ]);
  });

  it('does not let recency overcome substantially stronger semantic matches', () => {
    const candidates = [
      row('older-much-higher-similarity', 'semantic', {
        similarity: 0.88,
        source_timestamp: '2026-04-20T00:00:00.000Z',
        updated_at: '2026-04-20T00:00:00.000Z',
        created_at: '2026-04-20T00:00:00.000Z'
      }),
      row('recent-weaker-match', 'semantic', {
        similarity: 0.79,
        source_timestamp: '2026-05-29T00:00:00.000Z',
        updated_at: '2026-05-29T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      })
    ];

    expect(composeRecallRows(candidates, 1, 'factual', 0.30, now).map((item) => item.id)).toEqual([
      'older-much-higher-similarity'
    ]);
  });

  it('uses source_timestamp before created_at so backfilled old memories do not look fresh', () => {
    const candidates = [
      row('old-source-new-row', 'semantic', {
        similarity: 0.80,
        source_timestamp: '2026-04-20T00:00:00.000Z',
        updated_at: '2026-05-29T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      }),
      row('recent-source', 'semantic', {
        similarity: 0.79,
        source_timestamp: '2026-05-29T00:00:00.000Z',
        updated_at: '2026-05-29T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      })
    ];

    expect(composeRecallRows(candidates, 1, 'factual', 0.30, now).map((item) => item.id)).toEqual([
      'recent-source'
    ]);
  });

  it('excludes graph rows from direct mode-ranked recall rows', () => {
    const candidates = [
      row('semantic-low', 'semantic', { type: 'system_fact', similarity: 0.36 }),
      row('graph-behavioral', 'graph', { type: 'user_rule', similarity: 0.50 })
    ];

    expect(composeRecallRows(candidates, 1, 'agent', 0.30).map((item) => item.id)).toEqual([
      'semantic-low'
    ]);
  });

  it('drops semantic rows below the requested quality floor', () => {
    const candidates = [
      row('strong', 'semantic', { similarity: 0.82 }),
      row('weak', 'semantic', { similarity: 0.39 }),
      row('neighbor', 'graph', { similarity: 0.5 })
    ];

    expect(composeRecallRows(candidates, 5, 'agent', 0.45).map((item) => item.id)).toEqual([
      'strong'
    ]);
  });

  it('returns fewer than top_k when semantic matches do not clear the quality floor', () => {
    const candidates = [
      row('weak-1', 'semantic', { similarity: 0.44 }),
      row('weak-2', 'semantic', { similarity: 0.30 })
    ];

    expect(composeRecallRows(candidates, 5, 'factual', 0.45)).toEqual([]);
  });

  it('excludes candidate memories unless pending recall is enabled', () => {
    const candidates = [
      row('active', 'semantic', { similarity: 0.80 }),
      row('pending', 'semantic', {
        status: 'candidate',
        similarity: 0.95,
        source_timestamp: '2026-05-29T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      })
    ];

    expect(composeRecallRows(candidates, 5, 'agent', 0.30, now).map((item) => item.id)).toEqual([
      'active'
    ]);
    expect(composeRecallRows(candidates, 5, 'agent', 0.30, now, true).map((item) => item.id)).toEqual([
      'pending',
      'active'
    ]);
  });

  it('only includes fresh candidate memories when pending recall is enabled', () => {
    const candidates = [
      row('fresh-pending', 'semantic', {
        status: 'candidate',
        similarity: 0.80,
        source_timestamp: '2026-05-29T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      }),
      row('stale-pending', 'semantic', {
        status: 'candidate',
        similarity: 0.95,
        source_timestamp: '2026-05-20T00:00:00.000Z',
        created_at: '2026-05-20T00:00:00.000Z'
      })
    ];

    expect(composeRecallRows(candidates, 5, 'agent', 0.30, now, true).map((item) => item.id)).toEqual([
      'fresh-pending'
    ]);
  });

  it('uses source_timestamp before created_at when deciding candidate freshness', () => {
    const candidates = [
      row('old-source-new-candidate-row', 'semantic', {
        status: 'candidate',
        similarity: 0.95,
        source_timestamp: '2026-05-20T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      }),
      row('fresh-source-candidate', 'semantic', {
        status: 'candidate',
        similarity: 0.80,
        source_timestamp: '2026-05-29T00:00:00.000Z',
        created_at: '2026-05-29T00:00:00.000Z'
      })
    ];

    expect(composeRecallRows(candidates, 5, 'agent', 0.30, now, true).map((item) => item.id)).toEqual([
      'fresh-source-candidate'
    ]);
  });
});

describe('composeRelatedRecallRows', () => {
  it('returns graph rows separately without filling direct top_k slots', () => {
    const directRows = [
      row('direct', 'semantic', { similarity: 0.91 })
    ];
    const graphRows = [
      row('related-1', 'graph', { similarity: null }),
      row('related-2', 'graph', { similarity: null })
    ];

    expect(composeRecallRows([...directRows, ...graphRows], 5, 'agent', 0.30).map((item) => item.id)).toEqual([
      'direct'
    ]);
    expect(composeRelatedRecallRows(graphRows, directRows).map((item) => item.id)).toEqual([
      'related-1',
      'related-2'
    ]);
  });

  it('deduplicates graph rows against direct rows and applies the related limit', () => {
    const directRows = [
      row('direct', 'semantic', { similarity: 0.91 })
    ];
    const graphRows = [
      row('direct', 'graph', { similarity: null }),
      row('related-1', 'graph', { similarity: null }),
      row('related-1', 'graph', { similarity: null }),
      row('related-2', 'graph', { similarity: null })
    ];

    expect(composeRelatedRecallRows(graphRows, directRows, 1).map((item) => item.id)).toEqual([
      'related-1'
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

  it('preserves ranked query-relevant row order inside bundle sections', () => {
    const firstRanked = {
      ...row('first-ranked', 'semantic'),
      type: 'user_rule',
      data: 'first ranked',
      similarity: 0.75,
      salience: '0.60'
    };
    const secondRanked = {
      ...row('second-ranked', 'semantic'),
      type: 'user_rule',
      data: 'second ranked',
      similarity: 0.95,
      salience: '0.90'
    };

    expect(buildRecallBundle([firstRanked, secondRanked]).bundle.user_rules).toEqual([
      'first ranked',
      'second ranked'
    ]);
  });

  it('preserves recency-ranked row order in bundle sections', () => {
    const now = new Date('2026-05-30T00:00:00.000Z');
    const candidates = [
      row('older-slightly-higher-similarity', 'semantic', {
        type: 'user_rule',
        data: 'older',
        similarity: 0.82,
        source_timestamp: '2026-04-20T00:00:00.000Z'
      }),
      row('recent-close-match', 'semantic', {
        type: 'user_rule',
        data: 'recent',
        similarity: 0.79,
        source_timestamp: '2026-05-29T00:00:00.000Z'
      })
    ];

    const rankedRows = composeRecallRows(candidates, 2, 'agent', 0.30, now);

    expect(rankedRows.map((item) => item.id)).toEqual([
      'recent-close-match',
      'older-slightly-higher-similarity'
    ]);
    expect(buildRecallBundle(rankedRows).bundle.user_rules).toEqual([
      'recent',
      'older'
    ]);
  });
});
