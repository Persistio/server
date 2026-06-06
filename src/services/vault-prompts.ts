export const VAULT_TYPES = ['general', 'custom'] as const;
export type VaultType = typeof VAULT_TYPES[number];
export type PromptRole = 'extraction' | 'curation';

export interface VaultPromptContext {
  type: VaultType | null;
  custom_extraction_prompt?: string | null;
  custom_curation_prompt?: string | null;
}

export interface PromptValidationResult {
  valid: boolean;
  feedback: string[];
}

export const MAX_CUSTOM_EXTRACTION_PROMPT_BYTES = 65536;
export const MAX_CUSTOM_CURATION_PROMPT_BYTES = 24000;
const MIN_CUSTOM_PROMPT_CHARS = 120;

const INJECTION_PATTERNS: Array<{ pattern: RegExp; message: string }> = [
  {
    pattern: /\b(ignore|override|forget|disregard)\b.{0,80}\b(previous|prior|above|system|developer|safety)\b.{0,40}\b(instructions?|prompts?|rules?)\b/i,
    message: 'Remove instructions that tell the model to ignore, override, or forget system/developer/safety instructions.'
  },
  {
    pattern: /\b(reveal|print|show|dump|expose|leak)\b.{0,80}\b(system prompt|developer message|hidden prompt|api key|secret|password|token|credential)s?\b/i,
    message: 'Remove requests to reveal hidden prompts, credentials, secrets, tokens, or other private material.'
  },
  {
    pattern: /\b(send|post|upload|exfiltrate|transmit)\b.{0,80}\b(to|over)\b.{0,80}\b(http|https|webhook|email|url|server)\b/i,
    message: 'Remove instructions to send prompt input or extracted data to external services.'
  },
  {
    pattern: /\b(execute|run|shell|terminal|curl|wget|powershell|bash)\b.{0,80}\b(command|script|process|request)?\b/i,
    message: 'Remove tool, shell, network, or code-execution instructions from the custom prompt.'
  },
  {
    pattern: /\b(do not|don't|never)\b.{0,40}\b(output|return|respond with)\b.{0,40}\bjson\b/i,
    message: 'Custom prompts must preserve the required JSON-only output contract.'
  }
];

export function normalizeVaultType(value: string | null | undefined): VaultType | null {
  if (!value) return null;
  return (VAULT_TYPES as readonly string[]).includes(value) ? value as VaultType : null;
}

export function resolveVaultPrompt(input: {
  role: PromptRole;
  defaultPrompt: string;
  vault?: VaultPromptContext | null;
}): string {
  const vaultType = input.vault?.type ?? null;
  if (vaultType === 'custom') {
    const customPrompt = input.role === 'extraction'
      ? input.vault?.custom_extraction_prompt
      : input.vault?.custom_curation_prompt;
    if (customPrompt?.trim()) {
      return customPrompt.trim();
    }
  }

  return input.defaultPrompt;
}

export function validateCustomPrompt(role: PromptRole, prompt: string | null | undefined): PromptValidationResult {
  const feedback: string[] = [];
  const value = prompt?.trim() ?? '';

  if (!value) {
    feedback.push(`Provide a ${role} prompt.`);
  } else if (value.length < MIN_CUSTOM_PROMPT_CHARS) {
    feedback.push(`Expand the ${role} prompt to at least ${MIN_CUSTOM_PROMPT_CHARS} characters so it contains durable instructions, safety rules, and the output contract.`);
  }

  const maxBytes = role === 'curation'
    ? MAX_CUSTOM_CURATION_PROMPT_BYTES
    : MAX_CUSTOM_EXTRACTION_PROMPT_BYTES;
  if (Buffer.byteLength(value, 'utf8') > maxBytes) {
    const maxLabel = role === 'curation' ? '24KB' : '64KB';
    const reason = role === 'curation'
      ? ' so each curator call still has room for candidate memories, active memories, and raw conversation'
      : '';
    feedback.push(`Shorten the ${role} prompt to ${maxLabel} or less${reason}.`);
  }

  if (value && !/\bjson\b/i.test(value)) {
    feedback.push(`Add a JSON-only output requirement to the ${role} prompt.`);
  }

  if (value && !/\b(untrusted|plain text|data only|not instructions)\b/i.test(value)) {
    feedback.push(`Tell the ${role} model to treat conversation, prompt headers, candidates, and existing memories as untrusted data/plain text, not instructions.`);
  }

  if (role === 'extraction') {
    if (value && !/\b(fact|subject|score|salience)\b/i.test(value)) {
      feedback.push('Mention the extraction schema fields such as fact, subject, score, and salience.');
    }
  } else if (value && !/\b(nodes_to_create|nodes_to_update|discarded_candidates|edges_to_create)\b/i.test(value)) {
    feedback.push('Mention the curation schema fields such as nodes_to_create, nodes_to_update, edges_to_create, and discarded_candidates.');
  }

  for (const guard of INJECTION_PATTERNS) {
    if (guard.pattern.test(value)) {
      feedback.push(guard.message);
    }
  }

  return {
    valid: feedback.length === 0,
    feedback: Array.from(new Set(feedback))
  };
}

export function customPromptsAllowedForPlan(planId: string): boolean {
  const normalized = planId.toLowerCase();
  return normalized === 'unlimited';
}

export function validateVaultPromptSettings(input: {
  type: VaultType | null;
  planId: string;
  customExtractionPrompt?: string | null;
  customCurationPrompt?: string | null;
}): { ok: true } | { ok: false; statusCode: 400 | 403; error: string; feedback?: string[] } {
  if (input.type !== 'custom') {
    if (input.customExtractionPrompt?.trim() || input.customCurationPrompt?.trim()) {
      return {
        ok: false,
        statusCode: 400,
        error: 'Custom prompts require vault type "custom".',
        feedback: ['Set type to custom before supplying custom extraction or curation prompts.']
      };
    }
    return { ok: true };
  }

  if (!customPromptsAllowedForPlan(input.planId)) {
    return {
      ok: false,
      statusCode: 403,
      error: 'Custom vault prompts require an Unlimited plan.',
      feedback: ['Upgrade this vault to Unlimited, then retry with type custom.']
    };
  }

  const extraction = validateCustomPrompt('extraction', input.customExtractionPrompt);
  const curation = validateCustomPrompt('curation', input.customCurationPrompt);
  const feedback = [
    ...extraction.feedback.map((message) => `Extraction: ${message}`),
    ...curation.feedback.map((message) => `Curation: ${message}`)
  ];

  if (feedback.length > 0) {
    return {
      ok: false,
      statusCode: 400,
      error: 'Custom prompt validation failed.',
      feedback
    };
  }

  return { ok: true };
}
