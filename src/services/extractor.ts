import OpenAI from 'openai';

import { getConfig } from '../config';

export interface ExtractedFact {
  fact: string;
  subject: string;
}

const systemPrompt = `You are a memory extraction engine. Extract durable, searchable facts from the conversation chunk below.

Rules:
- Extract DECISIONS, PREFERENCES, PROBLEMS, FAILURES, TECHNICAL DETAILS, and PEOPLE/PROJECT facts
- Include bugs, errors, and rejected options — these are valuable context
- Write facts as short, definitive statements: "User prefers dark mode" not "User is considering dark mode"
- Be specific: extract exact values, names, and numbers where mentioned
- Subject must be the specific entity the fact is about (a person, project, tool, or concept) — avoid vague subjects like "the project" or "the user"
- One fact per distinct piece of information — do not bundle multiple facts into one
- Skip conversational filler, acknowledgements, and speculative thinking
- Output ONLY valid JSON: [{"fact":"...","subject":"..."}]`;

export class ExtractorService {
  private readonly client: OpenAI;
  private readonly model: string;

  constructor() {
    const config = getConfig();
    this.client = new OpenAI({
      apiKey: config.EXTRACTOR_API_KEY,
      baseURL: config.EXTRACTOR_BASE_URL
    });
    this.model = config.EXTRACTOR_MODEL;
  }

  async arbitrateConflict(existingFact: string, newFact: string): Promise<'update' | 'keep_both' | 'discard_new'> {
    const response = await this.client.chat.completions.create({
      model: this.model,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content: 'You are a memory conflict resolver. Given two related facts, decide what to do. Respond with ONLY one of: UPDATE (replace the old fact with the new one), KEEP_BOTH (both facts are complementary and should coexist), DISCARD_NEW (the old fact is still accurate and the new one adds nothing).'
        },
        {
          role: 'user',
          content: `Existing fact: "${existingFact}"\n\nNew fact: "${newFact}"\n\nWhat should we do?`
        }
      ]
    });

    const usage = response.usage;
    if (usage) {
      console.log(JSON.stringify({
        level: 30,
        msg: 'arbitration token usage',
        model: this.model,
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens
      }));
    }

    const raw = response.choices[0]?.message?.content?.trim().toUpperCase() ?? '';
    if (raw.includes('UPDATE')) return 'update';
    if (raw.includes('KEEP_BOTH')) return 'keep_both';
    return 'discard_new';
  }

  async extractFacts(conversation: string): Promise<ExtractedFact[]> {
    const response = await this.client.chat.completions.create({
      model: this.model,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content: systemPrompt
        },
        {
          role: 'user',
          content: conversation
        }
      ]
    });

    const usage = response.usage;
    if (usage) {
      console.log(JSON.stringify({
        level: 30,
        msg: 'extractor token usage',
        model: this.model,
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens
      }));
    }

    const raw = response.choices[0]?.message?.content ?? '[]';
    // Strip markdown code fences if the LLM wrapped the response
    const content = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '').trim();
    const parsed = JSON.parse(content) as unknown;

    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .filter((item): item is ExtractedFact => {
        return Boolean(
          item &&
          typeof item === 'object' &&
          typeof (item as ExtractedFact).fact === 'string' &&
          typeof (item as ExtractedFact).subject === 'string'
        );
      })
      .map((item) => ({
        fact: item.fact.trim(),
        subject: item.subject.trim()
      }))
      .filter((item) => item.fact && item.subject);
  }
}
