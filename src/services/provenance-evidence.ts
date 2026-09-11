import type { z } from 'zod';
import type { captureProvenanceSchema } from './ingest-provenance-schema';
import type { InterSessionEnvelope } from './transport-provenance';

type CaptureProvenance = z.infer<typeof captureProvenanceSchema>;
// Priority is fixed, so combining restrictions is commutative and associative.
type Restriction = 0 | 1 | 2 | 3;
export interface ProvenanceEvidence {
  readonly restriction: Restriction;
  readonly allDirectHumanOriginal: boolean;
}
export const INITIAL_EVIDENCE: ProvenanceEvidence = Object.freeze({ restriction: 0, allDirectHumanOriginal: true });
const HUMAN_AUTHORSHIP = new Set(['original', 'imported', 'transcribed']);
const GENERATED_ACTORS = new Set(['agent', 'assistant', 'tool', 'system']);
const REASONS = [null,
  'contradictory or unverified source authorship is not eligible for automatic semantic memory mutation',
  'unverifiable or generated transported payload is not eligible for automatic semantic memory mutation',
  'malformed stored provenance is not eligible for automatic semantic memory mutation'
] as const;

export function evidenceBlockReason(evidence: ProvenanceEvidence): string | null {
  return REASONS[evidence.restriction];
}

export function combineProvenanceEvidence(a: ProvenanceEvidence, b: ProvenanceEvidence): ProvenanceEvidence {
  return { restriction: Math.max(a.restriction, b.restriction) as Restriction,
    allDirectHumanOriginal: a.allDirectHumanOriginal && b.allDirectHumanOriginal };
}

/** Input is either schema-validated raw evidence, or explicit absence/invalidity.
 * Never evaluate normalized display identities or aggregate away raw claims first.
 * Author classes, rather than literal actor equality, permit agent/assistant cron
 * aliases without confusing delivery actors with the human payload author.
 */
export function evaluateProvenanceEvidence(
  source: CaptureProvenance | null,
  invalid: boolean,
  envelope: InterSessionEnvelope | null
): ProvenanceEvidence {
  const blocked = (restriction: Restriction): ProvenanceEvidence => ({ restriction, allDirectHumanOriginal: false });
  if (invalid) return blocked(3);
  if (!source) return blocked(envelope && envelope.is_user !== true ? 2 : 0);

  const author = source.payload_author;
  const transported = Boolean(envelope || source.transport || source.provenance_basis?.includes('transport_envelope'));
  const delivered = transported || source.import !== undefined;
  const ordinaryImport = !envelope && !source.provenance_basis?.includes('transport_envelope')
    && source.transport?.initiator_actor_type === 'import'
    && source.transport.source_channel === undefined && source.transport.source_tool === undefined
    && source.actor_type === 'import' && source.authorship === 'imported'
    && source.trigger_type === 'backfill' && source.cadence === 'batch' && source.import !== undefined
    && author?.actor_type === 'import' && author.authorship === 'imported' && author.is_user === null;
  if (ordinaryImport) return blocked(0);

  const primaryHuman = source.actor_type === 'human' && HUMAN_AUTHORSHIP.has(source.authorship);
  const primaryGenerated = GENERATED_ACTORS.has(source.actor_type) && source.authorship === 'generated';
  const explicitHuman = author?.actor_type === 'human' && HUMAN_AUTHORSHIP.has(author.authorship) && author.is_user === true;
  const explicitGenerated = author !== undefined && GENERATED_ACTORS.has(author.actor_type)
    && author.authorship === 'generated' && author.is_user === false;
  const consistent = author === undefined ? primaryHuman || primaryGenerated
    : (primaryHuman && explicitHuman) || (primaryGenerated && explicitGenerated);
  if (!consistent) return blocked(transported ? 2 : 1);

  // A corroborating header may supply absent human source evidence, but cannot
  // overwrite a supplied unknown/false/contradictory author. Imports also require
  // positive author evidence unless they are the exact ordinary carrier above.
  const verifiedHuman = primaryHuman && (explicitHuman || (author === undefined && envelope?.is_user === true));
  if (delivered && (!verifiedHuman || (envelope !== null && envelope.is_user !== true))) return blocked(2);

  return { restriction: 0, allDirectHumanOriginal: primaryHuman && explicitHuman
    && source.authorship === 'original' && author?.authorship === 'original'
    && !delivered && ['direct', 'api'].includes(source.trigger_type)
    && source.cadence === 'one_off' && !source.source_class?.startsWith('agent_') };
}
