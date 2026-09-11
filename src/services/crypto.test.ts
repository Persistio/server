import { describe, expect, it, vi } from 'vitest';

const kmsMock = vi.hoisted(() => ({
  encrypt: vi.fn(async () => [{ ciphertext: Buffer.from('wrapped-dek') }]),
  decrypt: vi.fn(async () => [{ plaintext: Buffer.from('plain-dek') }])
}));

vi.mock('../config', () => ({
  getConfig: () => ({
    KEY_PROVIDER: 'gcp_kms',
    GCP_KMS_KEY_NAME: 'projects/persistio/locations/europe-west2/keyRings/persistio/cryptoKeys/vault-dek',
    ENCRYPTION_ENABLED: true
  })
}));

vi.mock('@google-cloud/kms', () => ({
  KeyManagementServiceClient: class {
    encrypt = kmsMock.encrypt;
    decrypt = kmsMock.decrypt;
  }
}));

import { decryptForVault, encryptField, generateAndWrapDek, initCryptoClient, MemoryCiphertextError, unwrapDek, prepareVaultCrypto } from './crypto';

describe('GCP KMS crypto provider', () => {
  it('prepares the exact wrapped key independently of the vault-ID cache and never unwraps during apply', async () => {
    await initCryptoClient();
    const oldKey = Buffer.alloc(32, 8), newKey = Buffer.alloc(32, 9);
    const vault = { id: 'prepared-identity', encrypted_dek: 'b2xk', vault_encryption_enabled: true };
    kmsMock.decrypt.mockResolvedValueOnce([{ plaintext: oldKey }]);
    await decryptForVault(vault, encryptField('old cache entry', oldKey));
    const current = { ...vault, encrypted_dek: 'bmV3' };
    kmsMock.decrypt.mockResolvedValueOnce([{ plaintext: newKey }]);
    const prepared = await prepareVaultCrypto(current);
    const calls = kmsMock.decrypt.mock.calls.length;
    const client = { query: vi.fn(async () => ({ rows: [current] })) };
    await prepared.assertCurrent(client as never);
    const ciphertext = prepared.encrypt(current, 'private fact');
    expect(ciphertext).not.toContain('private fact');
    expect(prepared.decrypt(current, ciphertext)).toBe('private fact');
    expect(prepared.subject(current, 'topic')?.hmac).toBe(prepared.subjectMatch(current, 'topic'));
    expect(client.query.mock.calls[0][0]).toContain('FOR SHARE');
    expect(kmsMock.decrypt.mock.calls.length).toBe(calls);
    expect(() => prepared.encrypt(vault, 'wrong key')).toThrow('identity changed');
    for (const changed of [{ ...current, id: 'other' }, vault, { ...current, vault_encryption_enabled: false }]) {
      await expect(prepared.assertCurrent({ query: async () => ({ rows: [changed] }) } as never)).rejects.toThrow('identity changed');
    }
  });
  it('prepares plaintext vaults without KMS and rejects enabling encryption before apply', async () => {
    const vault = { id: 'plaintext', encrypted_dek: null, vault_encryption_enabled: false };
    const count = kmsMock.decrypt.mock.calls.length;
    const prepared = await prepareVaultCrypto(vault);
    expect(prepared.encrypt(vault, 'plain')).toBe('plain');
    expect(prepared.subject(vault, 'topic')).toBeNull();
    expect(kmsMock.decrypt.mock.calls.length).toBe(count);
    await expect(prepared.assertCurrent({ query: async () => ({ rows: [{ ...vault, vault_encryption_enabled: true }] }) } as never)).rejects.toThrow('identity changed');
  });
  it('distinguishes corrupt ciphertext from key-provider failures', async () => {
    await initCryptoClient();
    const dek = Buffer.alloc(32, 7);
    kmsMock.decrypt.mockResolvedValueOnce([{ plaintext: dek }]);
    const vault = { id: 'crypto-boundary-test', encrypted_dek: 'd3JhcHBlZA==', vault_encryption_enabled: true };
    await expect(decryptForVault(vault, encryptField('valid memory', dek))).resolves.toBe('valid memory');
    await expect(decryptForVault(vault, 'malformed')).rejects.toBeInstanceOf(MemoryCiphertextError);
    const outage = new Error('KMS unavailable');
    kmsMock.decrypt.mockRejectedValueOnce(outage);
    await expect(decryptForVault({ ...vault, id: 'kms-outage-test' }, 'malformed')).rejects.toBe(outage);
  });
  it('wraps and unwraps DEKs using the configured Cloud KMS key', async () => {
    await initCryptoClient();

    const wrapped = await generateAndWrapDek();
    const unwrapped = await unwrapDek(Buffer.from('wrapped-dek').toString('base64'));

    expect(wrapped.encryptedDek).toBe(Buffer.from('wrapped-dek').toString('base64'));
    expect(unwrapped.toString('utf8')).toBe('plain-dek');
    expect(kmsMock.encrypt).toHaveBeenCalledWith(expect.objectContaining({
      name: 'projects/persistio/locations/europe-west2/keyRings/persistio/cryptoKeys/vault-dek',
      plaintext: expect.any(Buffer)
    }));
    expect(kmsMock.decrypt).toHaveBeenCalledWith({
      name: 'projects/persistio/locations/europe-west2/keyRings/persistio/cryptoKeys/vault-dek',
      ciphertext: Buffer.from('wrapped-dek')
    });
  });
});
