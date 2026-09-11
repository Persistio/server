import type { CuratorAliasMaps, CuratorMemory, CuratorResult } from './curator';
import { intersectValidityWindows } from './memory-validity';

export type CuratorNodeRef = { kind: 'existing'; id: string } | { kind: 'created'; index: number };
export interface CompiledCuratorGraph {
  creationOrder: number[];
  parents: Array<CuratorNodeRef | null>;
  edges: Array<{ from: CuratorNodeRef; to: CuratorNodeRef }>;
}
type Binding = Pick<CuratorMemory, 'scope' | 'scope_key'>;
type Node = Binding & { ref: CuratorNodeRef; subject: string };
const key = (ref: CuratorNodeRef) => ref.kind === 'existing' ? `existing:${ref.id}` : `created:${ref.index}`;
const sameBinding = (a: Binding, b: Binding) => a.scope === b.scope && a.scope_key === b.scope_key;
const window = (memory: CuratorMemory) => ({ valid_from: memory.valid_from ?? null, valid_until: memory.valid_until ?? null });

/** Compile only schema/disposition-validated plans. No database lookups or UUID fallback. */
export function compileCuratorGraph(
  plan: CuratorResult, candidates: CuratorMemory[], active: CuratorMemory[], aliases: CuratorAliasMaps
): CompiledCuratorGraph {
  const inputs = new Map([...candidates, ...active].map(memory => [memory.id, memory]));
  const byAlias = (alias: string) => {
    const memory = inputs.get(aliases.aliasToId.get(alias) ?? '');
    if (!memory) throw new Error('Unknown reviewed memory alias');
    return memory;
  };
  const archived = new Set(plan.nodes_to_archive.map(action => byAlias(action.id).id));
  const promoted = new Set(plan.promoted_candidates.map(action => byAlias(action.id).id));
  const updates = new Map(plan.nodes_to_update.map(action => [byAlias(action.id).id, action]));
  const nodes: Node[] = active.filter(memory => !archived.has(memory.id)).map(memory => ({
    ...memory, ref: { kind: 'existing', id: memory.id }, subject: updates.get(memory.id)?.subject ?? memory.subject
  }));
  nodes.push(...candidates.filter(memory => promoted.has(memory.id)).map(memory => ({
    ...memory, ref: { kind: 'existing' as const, id: memory.id }
  })));
  const created = plan.nodes_to_create.map((action, index): Node => {
    const sources = action.consumed_candidate_ids.map(byAlias);
    if (!sources.length || sources.some(source => !sameBinding(source, sources[0])) || action.scope !== sources[0].scope) {
      throw new Error('Created node has inconsistent applicability');
    }
    intersectValidityWindows(sources.map(window));
    return { ref: { kind: 'created', index }, subject: action.subject, scope: action.scope, scope_key: sources[0].scope_key };
  });
  for (const action of plan.nodes_to_update) {
    intersectValidityWindows([byAlias(action.id), ...action.consumed_candidate_ids.map(byAlias)].map(window));
  }
  nodes.push(...created);
  const existing = new Map(nodes.flatMap(node => node.ref.kind === 'existing' ? [[node.ref.id, node] as const] : []));
  const renamed = active.filter(memory => updates.get(memory.id)?.subject !== undefined
    && updates.get(memory.id)!.subject !== memory.subject);
  const resolve = (text: string, binding?: Binding): Node => {
    if (/^[CM][1-9][0-9]*$/.test(text)) {
      const node = existing.get(aliases.aliasToId.get(text) ?? '');
      if (!node) throw new Error('Graph alias does not name a final surviving node');
      if (nodes.some(other => other.subject === text && key(other.ref) !== key(node.ref)
        && (!binding || sameBinding(other, binding)))) throw new Error('Ambiguous alias-shaped subject reference');
      if (binding && !sameBinding(node, binding)) throw new Error('Graph reference crosses applicability binding');
      return node;
    }
    if (renamed.some(memory => memory.subject === text && (!binding || sameBinding(memory, binding)))) {
      throw new Error('Graph reference uses a renamed old subject; use an alias');
    }
    const matches = nodes.filter(node => node.subject === text && (!binding || sameBinding(node, binding)));
    if (matches.length !== 1) throw new Error('Graph subject must name one unambiguous final surviving node');
    return matches[0];
  };
  const parents = plan.nodes_to_create.map((action, index) => {
    if (!action.parent_subject) return null;
    const parent = resolve(action.parent_subject, created[index]);
    if (key(parent.ref) === key(created[index].ref)) throw new Error('Graph parent cannot reference itself');
    return parent.ref;
  });
  const creationOrder: number[] = [];
  const visited = new Set<number>();
  const visiting = new Set<number>();
  // Iterative dependency traversal avoids stack growth with provider-sized arrays.
  for (let start = 0; start < created.length; start++) {
    const chain: number[] = [];
    let index: number | null = start;
    while (index !== null && !visited.has(index)) {
      if (visiting.has(index)) throw new Error('Graph parent dependencies contain a cycle');
      visiting.add(index);
      chain.push(index);
      const parent: CuratorNodeRef | null = parents[index];
      index = parent?.kind === 'created' ? parent.index : null;
    }
    for (const child of chain.reverse()) {
      visiting.delete(child);
      visited.add(child);
      creationOrder.push(child);
    }
  }
  const seenEdges = new Set<string>();
  const edges = plan.edges_to_create.map(action => {
    const from = resolve(action.from_subject);
    const to = resolve(action.to_subject, from);
    if (!sameBinding(from, to)) throw new Error('Graph edge crosses applicability binding');
    if (key(from.ref) === key(to.ref)) throw new Error('Graph edge cannot reference itself');
    const edgeKey = JSON.stringify([key(from.ref), key(to.ref), action.type]);
    if (seenEdges.has(edgeKey)) throw new Error('Duplicate graph edge');
    seenEdges.add(edgeKey);
    return { from: from.ref, to: to.ref };
  });
  return { creationOrder, parents, edges };
}
