import { describe, expect, it } from 'vitest';

import { matchSecretPattern } from '../utils/secret-filter';
import { scrubMemoryForCurator } from '../utils/sanitize';

describe('matchSecretPattern', () => {
  it('catches bearer tokens in conversation content before embedding', () => {
    expect(matchSecretPattern('Authorization: Bearer abc123testtoken')).toBe('bearer-token');
  });

  it('catches standalone bearer tokens', () => {
    expect(matchSecretPattern('Bearer abc123testtoken')).toBe('bearer-token');
  });
});

describe('scrubMemoryForCurator', () => {
  it('replaces IPv4 addresses', () => {
    expect(scrubMemoryForCurator('host at 192.168.1.64 port')).toBe('host at [internal host] port');
  });

  it('replaces bare UUIDs', () => {
    expect(scrubMemoryForCurator('vault 962cbf4d-f27e-4ca5-8b1d-3ac93bb3d8c4 exists')).toBe('vault [id] exists');
  });

  it('does not replace UUIDs in URL paths', () => {
    expect(scrubMemoryForCurator('/vaults/962cbf4d-f27e-4ca5-8b1d-3ac93bb3d8c4/memories')).toContain('962cbf4d');
  });

  it('replaces isolated ports', () => {
    expect(scrubMemoryForCurator('listening on :4827')).toBe('listening on :[port]');
  });

  it('replaces high-entropy tokens', () => {
    expect(scrubMemoryForCurator('key aa3d9aa0fbfecb5542858e44c738ad4e4e683ced15550719 set')).toBe('key [redacted] set');
  });
});
