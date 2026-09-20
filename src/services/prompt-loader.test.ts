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

describe('PromptLoader packaging and confinement regressions', () => {
  it('preserves an explicit mounted file ahead of the legacy basename', () => {
    const promptsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-mount-'));
    try {
      fs.mkdirSync(path.join(promptsDir, 'custom'));
      fs.writeFileSync(path.join(promptsDir, 'curator.txt'), 'canonical basename');
      fs.writeFileSync(path.join(promptsDir, 'custom', 'curator.txt'), 'synthetic mounted override');
      const loader = new PromptLoader({ promptFile: 'custom/curator.txt', promptsDir, fallback: 'fallback', label: 'test' });
      expect(loader.getPrompt()).toBe('synthetic mounted override');
    } finally { fs.rmSync(promptsDir, { recursive: true, force: true }); }
  });

  it('keeps the existing fallback for missing files without reading a sibling directory', () => {
    const promptsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-missing-'));
    try {
      expect(new PromptLoader({ promptFile: 'missing.txt', promptsDir, fallback: 'fallback', label: 'test' }).getPrompt()).toBe('fallback');
      expect(new PromptLoader({ promptFile: '', promptsDir, fallback: 'fallback', label: 'test' }).getPrompt()).toBe('fallback');
    } finally { fs.rmSync(promptsDir, { recursive: true, force: true }); }
  });

  it('rejects a mounted symlink escaping the allowed prompt directory', () => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-confinement-'));
    const promptsDir = path.join(parent, 'allowed');
    try {
      fs.mkdirSync(promptsDir);
      fs.writeFileSync(path.join(parent, 'outside.txt'), 'synthetic outside sentinel');
      fs.symlinkSync(path.join(parent, 'outside.txt'), path.join(promptsDir, 'curator.txt'));
      expect(new PromptLoader({ promptFile: 'curator.txt', promptsDir, fallback: 'fallback', label: 'test' }).getPrompt()).toBe('fallback');
    } finally { fs.rmSync(parent, { recursive: true, force: true }); }
  });

  it('keeps the file-size fallback boundary for custom mounts', () => {
    const promptsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'persistio-size-'));
    try {
      fs.writeFileSync(path.join(promptsDir, 'curator.txt'), 'x'.repeat(65537));
      expect(new PromptLoader({ promptFile: 'curator.txt', promptsDir, fallback: 'fallback', label: 'test' }).getPrompt()).toBe('fallback');
    } finally { fs.rmSync(promptsDir, { recursive: true, force: true }); }
  });
});
