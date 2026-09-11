import { beforeEach, describe, expect, it, vi } from 'vitest';
const mocks = vi.hoisted(() => ({ embed: vi.fn() }));
vi.mock('../../config', () => ({ getConfig: () => ({ STORAGE_EMBEDDING_DIMENSIONS: 2 }) }));
vi.mock('../embedder', () => ({ getEmbedder: () => ({ embed: mocks.embed }) }));
import { prepareCuratorWrites } from '../curator-write-preparation';

describe('bound curator write preparation', () => {
  const actions = { nodes_to_create: [{ statement: 'same', subject: 'child' }, { statement: 'parent', subject: 'parent' }],
    nodes_to_update: [{ statement: 'same', subject: 'child' }] } as never;
  beforeEach(() => { mocks.embed.mockReset().mockResolvedValueOnce([1, 0]).mockResolvedValueOnce([0, 1]).mockResolvedValueOnce([0.5, 0.5]); });
  it('binds action kind/index/text/subject independently of graph application order', async () => {
    const prepared = await prepareCuratorWrites(actions, 'vault', () => {});
    expect(prepared.vector('create', 1, 'parent', 'parent')).toEqual([0, 1]);
    expect(prepared.vector('create', 0, 'same', 'child')).toEqual([1, 0]);
    expect(prepared.vector('update', 0, 'same', 'child')).toEqual([0.5, 0.5]);
    for (const args of [['create', 1, 'same', 'child'], ['create', 0, 'changed', 'child'], ['update', 0, 'same', null], ['update', 2, 'same', 'child']] as const) {
      expect(() => prepared.vector(...args)).toThrow('does not match');
    }
    const modified = prepared.vector('create', 0, 'same', 'child'); modified[0] = 100;
    expect(prepared.vector('create', 0, 'same', 'child')).toEqual([1, 0]);
    expect(mocks.embed).toHaveBeenCalledTimes(3);
  });
  it.each([[1], [1, 2, 3], [NaN, 0], [Infinity, 0]])('rejects invalid prepared vector %j without apply fallback', async vector => {
    mocks.embed.mockReset().mockResolvedValue(vector);
    await expect(prepareCuratorWrites(actions, 'vault', () => {})).rejects.toThrow('Invalid prepared');
    expect(mocks.embed).toHaveBeenCalledOnce();
  });
  it('does not start another provider stage after known lease loss', async () => {
    let calls = 0;
    await expect(prepareCuratorWrites(actions, 'vault', () => { if (++calls === 2) throw new Error('lost'); })).rejects.toThrow('lost');
    expect(mocks.embed).toHaveBeenCalledOnce();
  });
});
