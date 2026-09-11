import { z } from 'zod';
import { provenanceIdentitySchema } from './provenance-identity';

// Explicit RFC 3339 subset shared with OpenAPI: uppercase T/Z, seconds,
// colon-separated offsets, and no leap seconds (Date/PostgreSQL interoperability).
export const ingestTimestampSchema = z.string().max(64).pipe(z.string().datetime({ offset: true }).regex(
  /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/
));
const provenanceBasisLimit = z.unknown().superRefine((value, ctx) => {
  if (Array.isArray(value) && value.length > 8) ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'Too many provenance basis values', fatal: true });
});
const provenanceActorSchema = z.enum(['human', 'assistant', 'agent', 'tool', 'system', 'import', 'unknown']);
const provenanceAuthorshipSchema = z.enum(['original', 'generated', 'transcribed', 'imported', 'mixed', 'unknown']);

export const captureProvenanceSchema = z.object({
    source_class: z.enum([
      'agent_cron',
      'agent_hook',
      'agent_slack',
      'agent_subagent',
      'agent_other',
      'thread_conversation',
      'direct_or_import',
      'unknown'
    ]).optional(),
    actor_type: provenanceActorSchema,
    trigger_type: z.enum(['direct', 'delegated', 'scheduled', 'event', 'backfill', 'api', 'unknown']),
    artifact_type: z.enum(['message', 'conversation', 'tool_result', 'status', 'observation', 'log', 'summary', 'document', 'unknown']),
    authorship: provenanceAuthorshipSchema,
    cadence: z.enum(['one_off', 'recurring', 'batch', 'unknown']),
    provenance_confidence: z.number().min(0).max(1).optional(),
    provenance_basis: provenanceBasisLimit.pipe(z.array(z.enum([
      'session_id_prefix',
      'agent_trigger',
      'integration_marker',
      'thread_session_shape',
      'session_id_shape',
      'role_counts',
      'plugin_capture',
      'api_provenance',
      'api_provenance_aggregate',
      'transport_envelope',
      'fallback'
    ])).max(8)).optional(),
    payload_author: z.object({
      actor_type: provenanceActorSchema,
      authorship: provenanceAuthorshipSchema,
      is_user: z.boolean().nullable()
    }).strict().optional(),
    transport: z.object({
      initiator_actor_type: provenanceActorSchema,
      initiator_id: provenanceIdentitySchema().optional(),
      receiver_actor_type: provenanceActorSchema,
      receiver_id: provenanceIdentitySchema().optional(),
      source_session_id: provenanceIdentitySchema().optional(),
      source_channel: provenanceIdentitySchema().optional(),
      source_tool: provenanceIdentitySchema().optional()
    }).strict().optional(),
    import: z.object({
      importer: provenanceIdentitySchema(),
      importer_version: provenanceIdentitySchema(),
      dataset_sha256: z.string().regex(/^[0-9a-f]{64}$/),
      import_job_id: provenanceIdentitySchema(),
      original_timestamp: ingestTimestampSchema
    }).strict().optional()
}).strict();
