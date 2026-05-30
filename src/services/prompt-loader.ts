import fs from 'node:fs';
import path from 'node:path';

interface PromptCacheEntry {
  expiresAt: number;
  value: string;
}

export class PromptLoader {
  private readonly promptFile: string;
  private readonly promptsDir: string;
  private readonly fallback: string;
  private readonly label: string;
  private readonly ttlMs: number;
  private cache: PromptCacheEntry | null = null;

  constructor(input: {
    promptFile: string;
    promptsDir: string;
    fallback: string;
    label: string;
    ttlMs?: number;
  }) {
    this.promptFile = input.promptFile;
    this.promptsDir = input.promptsDir;
    this.fallback = input.fallback;
    this.label = input.label;
    this.ttlMs = input.ttlMs ?? 60_000;
  }

  getPrompt(): string {
    const now = Date.now();
    if (this.cache && this.cache.expiresAt > now) {
      return this.cache.value;
    }

    const value = this.loadPrompt();
    this.cache = {
      value,
      expiresAt: now + this.ttlMs
    };
    return value;
  }

  private loadPrompt(): string {
    if (!this.promptFile) {
      return this.fallback;
    }

    const allowedDir = fs.existsSync(this.promptsDir)
      ? fs.realpathSync(path.resolve(this.promptsDir))
      : path.resolve(this.promptsDir);
    const promptPath = this.resolvePromptPath();
    let resolved: string;
    try {
      resolved = fs.realpathSync(promptPath);
    } catch {
      console.warn(`[${this.label}] Could not resolve prompt file path, falling back to default`);
      return this.fallback;
    }

    if (!resolved.startsWith(allowedDir + path.sep)) {
      console.warn(`[${this.label}] Prompt file is outside allowed directory, ignoring`);
      return this.fallback;
    }

    try {
      const content = fs.readFileSync(resolved, 'utf8').trim();
      if (Buffer.byteLength(content, 'utf8') > 65536) {
        console.warn(`[${this.label}] Prompt file exceeds 64KB limit, falling back to default`);
        return this.fallback;
      }
      return content;
    } catch {
      console.warn(`[${this.label}] Failed to read prompt file, falling back to default`);
      return this.fallback;
    }
  }

  private resolvePromptPath(): string {
    if (path.isAbsolute(this.promptFile)) {
      return this.promptFile;
    }

    const fromPromptsDir = path.resolve(this.promptsDir, this.promptFile);
    if (fs.existsSync(fromPromptsDir)) {
      return fromPromptsDir;
    }

    const basenameFromPromptsDir = path.resolve(this.promptsDir, path.basename(this.promptFile));
    if (fs.existsSync(basenameFromPromptsDir)) {
      return basenameFromPromptsDir;
    }

    return path.resolve(this.promptFile);
  }
}
