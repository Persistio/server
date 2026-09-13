import type { CuratorAliasMaps, CuratorMemory, CuratorResult } from './curator';

export type CuratorNodeRef = { kind: 'existing'; id: string } | { kind: 'created'; index: number };
export interface CompiledCuratorGraph {
  creationOrder: number[];
  parents: Array<CuratorNodeRef | null>;
  edges: Array<{ from: CuratorNodeRef; to: CuratorNodeRef }>;
}
type Binding = Pick<CuratorMemory,'scope'|'scope_key'>;
type Node = Binding & { ref: CuratorNodeRef; alias: string; parentId: string | null };
const key = (ref: CuratorNodeRef) => ref.kind === 'existing' ? ref.id : `new:${ref.index}`;
const sameBinding = (a: Binding,b: Binding) => a.scope === b.scope && a.scope_key === b.scope_key;

/** Compile only fully validated dispositions; database incident graph is checked at apply. */
export function compileCuratorGraph(plan: CuratorResult, targets: CuratorMemory[], context: CuratorMemory[], aliases: CuratorAliasMaps): CompiledCuratorGraph {
  const inputs = new Map([...targets,...context].map(m => [aliases.idToAlias.get(m.id)!,m]));
  const memory = (alias: string) => {
    const m = inputs.get(alias);
    if (!m) throw new Error('Unknown reviewed memory');
    return m;
  };
  const archived = new Set(plan.archive.map(a => a.id));
  const replacements = new Map<string,string>();
  plan.consolidate.forEach(a => a.sources.forEach(id => replacements.set(id,a.id)));
  const changes = new Map(plan.scope_changes.map(a => [a.id,a]));
  const nodes = new Map<string,Node>();
  for (const [alias,m] of inputs) {
    if (archived.has(alias) || replacements.has(alias)) continue;
    const binding = changes.get(alias) ?? m;
    nodes.set(alias,{alias,ref:{kind:'existing',id:m.id},scope:binding.scope,scope_key:binding.scope_key,parentId:m.parent_id});
  }
  plan.consolidate.forEach((a,index) => {
    const first = memory(a.sources[0]);
    const binding = changes.get(a.id) ?? first;
    nodes.set(a.id,{alias:a.id,ref:{kind:'created',index},scope:binding.scope,scope_key:binding.scope_key,parentId:null});
  });
  const resolve = (alias: string): Node => {
    const node = nodes.get(alias);
    if (!node) throw new Error('Graph endpoint is not a final surviving node');
    return node;
  };
  const parentAliases = new Map<string,string>();
  const inheritedParents = plan.consolidate.map(a => {
    const parents = new Set(a.sources.map(memory).map(m => m.parent_id).filter((id): id is string => id !== null));
    // Internal source-to-source parents disappear on consolidation; every
    // external parent must agree. Never silently choose one of two parents.
    const external = [...parents].filter(id => !a.sources.some(s => memory(s).id === id));
    if (external.length > 1) throw new Error('Consolidation has conflicting parents');
    if (!external.length) return null;
    const alias = aliases.idToAlias.get(external[0]);
    if (!alias) throw new Error('Consolidation parent was not reviewed');
    return replacements.get(alias) ?? alias;
  });
  for (const [alias,node] of nodes) {
    const parentAlias = node.ref.kind === 'created' ? inheritedParents[node.ref.index]
      : node.parentId ? aliases.idToAlias.get(node.parentId) : null;
    if (!parentAlias) continue; // Unseen existing parents are validated under DB locks.
    const finalParent = replacements.get(parentAlias) ?? parentAlias;
    if (archived.has(finalParent)) throw new Error('Plan archives a surviving node parent');
    const parent = resolve(finalParent);
    if (alias === parent.alias || !sameBinding(node,parent)) throw new Error('Invalid parent binding');
    parentAliases.set(alias,parent.alias);
  }
  const edges = plan.edges.map(a => {
    const from = resolve(a.from), to = resolve(a.to);
    if (!sameBinding(from,to) || key(from.ref) === key(to.ref)) throw new Error('Invalid graph edge binding');
    return {from:from.ref,to:to.ref};
  });
  const adjacency = new Map<string,string[]>();
  for (const [child,parent] of parentAliases) adjacency.set(child,[parent]);
  for (const edge of plan.edges.filter(e => e.type === 'part_of')) adjacency.set(edge.from,[...(adjacency.get(edge.from) ?? []),edge.to]);
  const done = new Set<string>();
  const visiting = new Set<string>();
  const visit = (id: string) => {
    if (visiting.has(id)) throw new Error('Curator hierarchy cycle');
    if (done.has(id)) return;
    visiting.add(id);
    for (const parent of adjacency.get(id) ?? []) visit(parent);
    visiting.delete(id); done.add(id);
  };
  for (const id of nodes.keys()) visit(id);
  const creationOrder: number[] = [];
  const createdDone = new Set<number>();
  const order = (index: number) => {
    if (createdDone.has(index)) return;
    const parent = inheritedParents[index] ? resolve(inheritedParents[index]!) : null;
    if (parent?.ref.kind === 'created') order(parent.ref.index);
    createdDone.add(index); creationOrder.push(index);
  };
  plan.consolidate.forEach((_,i) => order(i));
  return {creationOrder,parents:inheritedParents.map(alias => alias ? resolve(alias).ref : null),edges};
}
