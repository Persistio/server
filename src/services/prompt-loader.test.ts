import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { describe, expect, it } from 'vitest';

import { PromptLoader } from './prompt-loader';

describe('PromptLoader', () => {
  it('loads relative prompt files from PROMPTS_DIR', () => {
    const promptsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-prompts-'));
    fs.writeFileSync(path.join(promptsDir, 'curator.txt'), 'mounted curator prompt', 'utf8');

    const loader = new PromptLoader({
      promptFile: 'curator.txt',
      promptsDir,
      fallback: 'fallback',
      label: 'curator',
      ttlMs: 1
    });

    expect(loader.getPrompt()).toBe('mounted curator prompt');
  });

  it('allows legacy prompts/name.txt defaults to resolve under PROMPTS_DIR', () => {
    const promptsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-prompts-'));
    fs.writeFileSync(path.join(promptsDir, 'curator.txt'), 'public curator prompt', 'utf8');

    const loader = new PromptLoader({
      promptFile: 'prompts/curator.txt',
      promptsDir,
      fallback: 'fallback',
      label: 'curator',
      ttlMs: 1
    });

    expect(loader.getPrompt()).toBe('public curator prompt');
  });

  it('does not load prompt files outside PROMPTS_DIR', () => {
    const promptsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-prompts-'));
    const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-private-prompts-'));
    const outsidePrompt = path.join(outsideDir, 'curator.txt');
    fs.writeFileSync(outsidePrompt, 'private curator prompt', 'utf8');

    const loader = new PromptLoader({
      promptFile: outsidePrompt,
      promptsDir,
      fallback: 'fallback',
      label: 'curator',
      ttlMs: 1
    });

    expect(loader.getPrompt()).toBe('fallback');
  });
});
