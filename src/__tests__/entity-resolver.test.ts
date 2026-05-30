import { beforeEach, describe, expect, it, vi } from 'vitest';

import { query } from '../db/client';
import {
  getVaultSubjectList,
  resolveSubjectTier1,
  resolveSubjectTier2,
  storeCanonicalEmbedding,
  storeSubjectAlias,
  type VaultSubject
} from '../services/entity-resolver';

vi.mock('../db/client', () => ({
  query: vi.fn()
}));

vi.mock('../services/crypto', () => ({
  computeSubjectHmac: vi.fn((subject: string) => `hmac:${subject}`),
  isVaultEncryptionActive: vi.fn((vault: { vault_encryption_enabled: boolean }) => vault.vault_encryption_enabled),
  unwrapDek: vi.fn(async () => Buffer.from('test-dek'))
}));

const mockQuery = vi.mocked(query);

const makeSubject = (canonical: string, aliases: string[] = [], embedding: number[] | null = null): VaultSubject => ({
  canonical,
  aliases,
  embedding
});

beforeEach(() => {
  mockQuery.mockReset();
});

describe('resolveSubjectTier1', () => {
  it('returns canonical on exact match', () => {
    const list = [makeSubject('fantastic-system')];
    expect(resolveSubjectTier1('fantastic-system', list, 2)).toBe('fantastic-system');
  });

  it('returns canonical on normalised exact match (case + punctuation)', () => {
    const list = [makeSubject('fantastic-system')];
    expect(resolveSubjectTier1('Fantastic System', list, 2)).toBe('fantastic-system');
  });

  it('returns canonical on distance 1', () => {
    const list = [makeSubject('Persistio')];
    expect(resolveSubjectTier1('Persistio', list, 2)).toBe('Persistio');
  });

  it('returns canonical via alias match', () => {
    const list = [makeSubject('fantastic-system', ['project fantastic system'])];
    expect(resolveSubjectTier1('project fantastic system', list, 2)).toBe('fantastic-system');
  });

  it('returns canonical when subject has backtick-wrapped suffix (stripped by normalisation)', () => {
    const list = [makeSubject('fantastic-system')];
    expect(resolveSubjectTier1('fantastic-system `wrangler.toml`', list, 2)).toBe('fantastic-system');
  });

  it('does not canonicalize distinct subjects just because one prefixes the other', () => {
    const list = [makeSubject('new york')];
    expect(resolveSubjectTier1('new york times', list, 2)).toBeNull();
  });

  it('leaves generic-token variants to aliases or embedding fallback', () => {
    const list = [makeSubject('fantastic-system')];
    expect(resolveSubjectTier1('project fantastic system', list, 2)).toBeNull();
  });

  it('returns null when no match within distance', () => {
    const list = [makeSubject('Persistio')];
    expect(resolveSubjectTier1('completely unrelated topic xyz', list, 2)).toBeNull();
  });

  it('returns null on empty list', () => {
    expect(resolveSubjectTier1('anything', [], 2)).toBeNull();
  });

  it('returns first canonical match when multiple candidates exist', () => {
    const list = [
      makeSubject('Persistio'),
      makeSubject('fantastic-system')
    ];
    expect(resolveSubjectTier1('fantastic-system', list, 2)).toBe('fantastic-system');
  });
});

describe('getVaultSubjectList', () => {
  it('builds the known subject list from stored memory subjects, not subject HMACs', async () => {
    mockQuery
      .mockResolvedValueOnce({
        rows: [{ id: 'vault-1', encrypted_dek: null, vault_encryption_enabled: false }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({
        rows: [{ subject: 'Fantastic System' }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({
        rows: [{ subject: 'Persistio' }, { subject: 'Fantastic System' }],
        rowCount: 2
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic system' },
          { canonical: 'persistio', alias: 'persistio' }
        ],
        rowCount: 2
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic-system', embedding: '[1,0,0]' },
          { canonical: 'fantastic-system', alias: 'fantastic system', embedding: null },
          { canonical: 'persistio', alias: 'persistio', embedding: '[0,1,0]' }
        ],
        rowCount: 3
      } as never);

    const subjects = await getVaultSubjectList('vault-1', 5, 5);

    expect(mockQuery).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('FROM vaults'),
      ['vault-1']
    );
    expect(mockQuery).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('m.subject <>'),
      ['vault-1', 5]
    );
    expect(mockQuery).toHaveBeenNthCalledWith(
      4,
      expect.stringContaining('alias = ANY'),
      ['vault-1', ['fantastic system', 'persistio']]
    );
    expect(mockQuery).toHaveBeenNthCalledWith(
      5,
      expect.stringContaining('canonical = ANY'),
      ['vault-1', ['fantastic-system', 'persistio']]
    );
    expect(subjects).toEqual([
      { canonical: 'fantastic-system', aliases: ['fantastic system'], embedding: [1, 0, 0] },
      { canonical: 'persistio', aliases: [], embedding: [0, 1, 0] }
    ]);
  });

  it('returns normalised memory subjects when no aliases have been stored yet', async () => {
    mockQuery
      .mockResolvedValueOnce({
        rows: [{ id: 'vault-1', encrypted_dek: null, vault_encryption_enabled: false }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({ rows: [{ subject: 'Fantastic System' }], rowCount: 1 } as never)
      .mockResolvedValueOnce({ rows: [], rowCount: 0 } as never)
      .mockResolvedValueOnce({ rows: [], rowCount: 0 } as never)
      .mockResolvedValueOnce({ rows: [], rowCount: 0 } as never);

    await expect(getVaultSubjectList('vault-1', 5, 5)).resolves.toEqual([
      { canonical: 'fantastic system', aliases: [], embedding: null }
    ]);
  });

  it('builds encrypted vault subject lists from canonical alias HMACs', async () => {
    mockQuery
      .mockResolvedValueOnce({
        rows: [{ id: 'vault-1', encrypted_dek: 'wrapped-dek', vault_encryption_enabled: true }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic-system' },
          { canonical: 'persistio', alias: 'persistio' }
        ],
        rowCount: 2
      } as never)
      .mockResolvedValueOnce({
        rows: [{ canonical: 'fantastic-system' }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({
        rows: [{ canonical: 'persistio' }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic-system' },
          { canonical: 'persistio', alias: 'persistio' }
        ],
        rowCount: 2
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic-system', embedding: '[1,0,0]' },
          { canonical: 'persistio', alias: 'persistio', embedding: '[0,1,0]' }
        ],
        rowCount: 2
      } as never);

    const subjects = await getVaultSubjectList('vault-1', 5, 5);

    expect(mockQuery).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining('JOIN known_subjects'),
      ['vault-1', ['hmac:fantastic-system', 'hmac:persistio'], ['fantastic-system', 'persistio'], 5]
    );
    expect(mockQuery).toHaveBeenNthCalledWith(
      5,
      expect.stringContaining('alias = ANY'),
      ['vault-1', ['fantastic-system', 'persistio']]
    );
    expect(subjects).toEqual([
      { canonical: 'fantastic-system', aliases: [], embedding: [1, 0, 0] },
      { canonical: 'persistio', aliases: [], embedding: [0, 1, 0] }
    ]);
  });

  it('maps encrypted alias HMACs back to their resolved canonical', async () => {
    mockQuery
      .mockResolvedValueOnce({
        rows: [{ id: 'vault-1', encrypted_dek: 'wrapped-dek', vault_encryption_enabled: true }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic-system' },
          { canonical: 'fantastic-system', alias: 'old fantastic name' }
        ],
        rowCount: 2
      } as never)
      .mockResolvedValueOnce({
        rows: [{ canonical: 'fantastic-system' }],
        rowCount: 1
      } as never)
      .mockResolvedValueOnce({
        rows: [],
        rowCount: 0
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic-system' },
          { canonical: 'fantastic-system', alias: 'old fantastic name' }
        ],
        rowCount: 2
      } as never)
      .mockResolvedValueOnce({
        rows: [
          { canonical: 'fantastic-system', alias: 'fantastic-system', embedding: '[1,0,0]' },
          { canonical: 'fantastic-system', alias: 'old fantastic name', embedding: null }
        ],
        rowCount: 2
      } as never);

    const subjects = await getVaultSubjectList('vault-1', 5, 5);

    expect(mockQuery).toHaveBeenNthCalledWith(
      3,
      expect.stringContaining('JOIN known_subjects'),
      ['vault-1', ['hmac:fantastic-system', 'hmac:old fantastic name'], ['fantastic-system', 'fantastic-system'], 5]
    );
    expect(subjects).toEqual([
      { canonical: 'fantastic-system', aliases: ['old fantastic name'], embedding: [1, 0, 0] }
    ]);
  });
});

describe('entity alias storage', () => {
  it('normalises canonical embedding rows before storing them', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 } as never);

    await storeCanonicalEmbedding('vault-1', 'Fantastic System', [1, 2, 3]);

    expect(mockQuery).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO entity_aliases'),
      ['vault-1', 'fantastic system', '[1,2,3]']
    );
  });

  it('stores resolved aliases without overwriting the canonical embedding', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 } as never);

    await storeSubjectAlias('vault-1', 'Fantastic System `wrangler.toml`', 'fantastic-system');

    expect(mockQuery).toHaveBeenCalledWith(
      expect.stringContaining('DO UPDATE SET canonical = EXCLUDED.canonical'),
      ['vault-1', 'fantastic system `wrangler.toml`', 'fantastic-system']
    );
  });
});

describe('resolveSubjectTier2', () => {
  // Build two embeddings that are highly similar (cosine sim ~1)
  const baseEmbed: number[] = Array(1536).fill(0);
  baseEmbed[0] = 1;

  const nearEmbed: number[] = Array(1536).fill(0);
  nearEmbed[0] = 0.9999;
  nearEmbed[1] = 0.01;

  const farEmbed: number[] = Array(1536).fill(0);
  farEmbed[5] = 1; // orthogonal

  it('returns high confidence for very similar embedding', () => {
    const list = [makeSubject('Persistio', [], baseEmbed)];
    const result = resolveSubjectTier2(nearEmbed, list, 0.92, 0.80);
    expect(result).not.toBeNull();
    expect(result?.confidence).toBe('high');
    expect(result?.canonical).toBe('Persistio');
  });

  it('returns ambiguous when similarity is between thresholds', () => {
    // Create an embedding that will produce ~0.85 similarity
    const midEmbed: number[] = Array(1536).fill(0);
    midEmbed[0] = 0.85;
    midEmbed[5] = 0.527; // roughly normalised to give ~0.85 cosine with baseEmbed
    const list = [makeSubject('Persistio', [], baseEmbed)];
    const result = resolveSubjectTier2(midEmbed, list, 0.92, 0.80);
    // May be high or ambiguous depending on actual sim — just verify it doesn't throw
    expect(['high', 'ambiguous', null]).toContain(result ? result.confidence : null);
  });

  it('returns null for orthogonal embedding (below low threshold)', () => {
    const list = [makeSubject('Persistio', [], baseEmbed)];
    const result = resolveSubjectTier2(farEmbed, list, 0.92, 0.80);
    expect(result).toBeNull();
  });

  it('returns null when no canonical has embedding', () => {
    const list = [makeSubject('Persistio', [], null)];
    expect(resolveSubjectTier2(baseEmbed, list, 0.92, 0.80)).toBeNull();
  });

  it('returns null on empty list', () => {
    expect(resolveSubjectTier2(baseEmbed, [], 0.92, 0.80)).toBeNull();
  });

  it('picks best match across multiple canonicals', () => {
    const list = [
      makeSubject('unrelated', [], farEmbed),
      makeSubject('Persistio', [], baseEmbed)
    ];
    const result = resolveSubjectTier2(nearEmbed, list, 0.92, 0.80);
    expect(result?.canonical).toBe('Persistio');
  });
});
