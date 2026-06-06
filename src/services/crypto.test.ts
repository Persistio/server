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

import { generateAndWrapDek, initCryptoClient, unwrapDek } from './crypto';

describe('GCP KMS crypto provider', () => {
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
