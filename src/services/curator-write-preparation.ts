import { getConfig } from '../config';
import type { CuratorResult } from './curator';
import { getEmbedder } from './embedder';

export interface PreparedCuratorWrites {
  vector(kind: 'create' | 'update', index: number, fact: string, subject: string | null): number[];
}

/** Bound, attempt-local artifacts: no provider is reachable through the result. */
export async function prepareCuratorWrites(
  actions: CuratorResult, vaultId: string, assertNotLost: () => void
): Promise<PreparedCuratorWrites> {
  const vectors = new Map<string, number[]>();
  const dimensions = getConfig().STORAGE_EMBEDDING_DIMENSIONS;
  const key = (kind: string, index: number, fact: string, subject: string | null) =>
    JSON.stringify([kind, index, fact, subject]);
  for (const kind of ['create', 'update'] as const) {
    const nodes = kind === 'create' ? actions.nodes_to_create : actions.nodes_to_update;
    for (const [index, node] of nodes.entries()) {
      assertNotLost();
      const vector = await getEmbedder().embed(node.statement, {
        vaultId, modelRole: 'embedding', source: 'curation_worker', inputType: 'document'
      });
      if (vector.length !== dimensions || !vector.every(Number.isFinite)) {
        throw new Error('Invalid prepared curator embedding');
      }
      vectors.set(key(kind, index, node.statement, node.subject ?? null), [...vector]);
    }
  }
  return Object.freeze({
    vector(kind: 'create' | 'update', index: number, fact: string, subject: string | null) {
      const vector = vectors.get(key(kind, index, fact, subject));
      if (!vector) throw new Error('Prepared curator write does not match action');
      return [...vector];
    }
  });
}
