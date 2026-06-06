import crypto from 'node:crypto';

import { ManagedIdentityCredential } from '@azure/identity';
import { CryptographyClient, KeyClient } from '@azure/keyvault-keys';
import { KeyManagementServiceClient } from '@google-cloud/kms';

import { getConfig } from '../config';

const DEK_CACHE_TTL_MS = 5 * 60 * 1000;

const dekCache = new Map<string, { dek: Buffer; expiresAt: number }>();
// In-flight deduplication map: prevents cache stampede under concurrent decryption (Fix 1).
const dekInflight = new Map<string, Promise<Buffer>>();
let keyEncryptionProvider: KeyEncryptionProvider | null = null;

export interface VaultEncryptionContext {
  id: string;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
}

interface KeyEncryptionProvider {
  init(): Promise<void>;
  wrapDek(dek: Buffer): Promise<string>;
  unwrapDek(encryptedDek: string): Promise<Buffer>;
}

// Keep-alive options: reuse TCP/TLS connections across Key Vault calls.
// `keepAliveOptions` is part of ExtendedClientOptions, which KeyClientOptions
// and CryptographyClientOptions both inherit via ExtendedCommonClientOptions.
// This avoids the TS2353 error that `pipelineOptions` caused in PR #126.
const KV_KEEP_ALIVE = { keepAliveOptions: { enable: true } } as const;

class AzureKeyVaultProvider implements KeyEncryptionProvider {
  private readonly credential = new ManagedIdentityCredential();
  private cryptoClient: CryptographyClient | null = null;

  async init(): Promise<void> {
    const { KEY_VAULT_URI, KEK_KEY_NAME } = getConfig();
    const keyClient = new KeyClient(KEY_VAULT_URI, this.credential, KV_KEEP_ALIVE);
    const key = await keyClient.getKey(KEK_KEY_NAME);
    this.cryptoClient = new CryptographyClient(key, this.credential, KV_KEEP_ALIVE);
    console.log('[persistio] Azure Key Vault crypto provider initialised');
  }

  async wrapDek(dek: Buffer): Promise<string> {
    const result = await this.getCryptographyClient().wrapKey('RSA-OAEP-256', dek);
    return Buffer.from(result.result).toString('base64');
  }

  async unwrapDek(encryptedDek: string): Promise<Buffer> {
    const result = await this.getCryptographyClient().unwrapKey('RSA-OAEP-256', Buffer.from(encryptedDek, 'base64'));
    return Buffer.from(result.result);
  }

  private getCryptographyClient(): CryptographyClient {
    if (!this.cryptoClient) {
      throw new Error('Azure Key Vault crypto provider has not been initialised');
    }

    return this.cryptoClient;
  }
}

class GcpKmsProvider implements KeyEncryptionProvider {
  private readonly client: KeyManagementServiceClient;
  private readonly keyName: string;

  constructor() {
    const config = getConfig();
    this.client = new KeyManagementServiceClient();
    this.keyName = config.GCP_KMS_KEY_NAME;
  }

  async init(): Promise<void> {
    if (!this.keyName) {
      throw new Error('GCP_KMS_KEY_NAME is required for GCP KMS crypto provider');
    }
    console.log('[persistio] GCP Cloud KMS crypto provider initialised');
  }

  async wrapDek(dek: Buffer): Promise<string> {
    const [result] = await this.client.encrypt({
      name: this.keyName,
      plaintext: dek
    });
    return Buffer.from(result.ciphertext ?? new Uint8Array()).toString('base64');
  }

  async unwrapDek(encryptedDek: string): Promise<Buffer> {
    const [result] = await this.client.decrypt({
      name: this.keyName,
      ciphertext: Buffer.from(encryptedDek, 'base64')
    });
    return Buffer.from(result.plaintext ?? new Uint8Array());
  }
}

export async function initCryptoClient(): Promise<void> {
  keyEncryptionProvider = createKeyEncryptionProvider();
  await keyEncryptionProvider.init();
}

export async function generateAndWrapDek(): Promise<{ encryptedDek: string }> {
  const dek = crypto.randomBytes(32);
  return { encryptedDek: await getKeyEncryptionProvider().wrapDek(dek) };
}

export async function unwrapDek(encryptedDek: string): Promise<Buffer> {
  return getKeyEncryptionProvider().unwrapDek(encryptedDek);
}

export function encryptField(plaintext: string, dek: Buffer): string {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', dek, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, encrypted]).toString('base64');
}

export function decryptField(ciphertext: string, dek: Buffer): string {
  const buf = Buffer.from(ciphertext, 'base64');
  const iv = buf.subarray(0, 12);
  const tag = buf.subarray(12, 28);
  const encrypted = buf.subarray(28);
  const decipher = crypto.createDecipheriv('aes-256-gcm', dek, iv, { authTagLength: 16 });
  decipher.setAuthTag(tag);
  return decipher.update(encrypted, undefined, 'utf8') + decipher.final('utf8');
}

export function isVaultEncryptionActive(vault: VaultEncryptionContext): boolean {
  return getConfig().ENCRYPTION_ENABLED && vault.vault_encryption_enabled;
}

export function computeSubjectHmac(subject: string, dek: Buffer): string {
  return crypto.createHmac('sha256', dek).update(subject).digest('hex');
}

export async function encryptForVault(vault: VaultEncryptionContext, plaintext: string): Promise<string> {
  if (!isVaultEncryptionActive(vault)) {
    return plaintext;
  }

  const dek = await getVaultDek(vault);
  return encryptField(plaintext, dek);
}

export async function decryptForVault(vault: VaultEncryptionContext, ciphertext: string): Promise<string> {
  if (!isVaultEncryptionActive(vault)) {
    return ciphertext;
  }

  const dek = await getVaultDek(vault);
  return decryptField(ciphertext, dek);
}

export async function encryptSubjectForVault(
  vault: VaultEncryptionContext,
  subject: string
): Promise<{ encrypted: string; hmac: string } | null> {
  if (!isVaultEncryptionActive(vault)) {
    return null;
  }

  const dek = await getVaultDek(vault);
  return {
    encrypted: encryptField(subject, dek),
    hmac: computeSubjectHmac(subject, dek)
  };
}

async function getVaultDek(vault: VaultEncryptionContext): Promise<Buffer> {
  if (!vault.encrypted_dek) {
    throw new Error(`Vault ${vault.id} is missing encrypted_dek`);
  }

  // Fast path: valid cached DEK.
  const cached = dekCache.get(vault.id);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.dek;
  }

  // In-flight deduplication (Fix 1): if an unwrapKey call is already in progress
  // for this vault, await it rather than issuing a parallel request.
  const inflight = dekInflight.get(vault.id);
  if (inflight) {
    return inflight;
  }

  const encryptedDek = vault.encrypted_dek;
  const promise = unwrapDek(encryptedDek)
    .then((dek) => {
      dekCache.set(vault.id, {
        dek,
        expiresAt: Date.now() + DEK_CACHE_TTL_MS
      });
      return dek;
    })
    .finally(() => {
      dekInflight.delete(vault.id);
    });

  dekInflight.set(vault.id, promise);
  return promise;
}

function createKeyEncryptionProvider(): KeyEncryptionProvider {
  const config = getConfig();
  return config.KEY_PROVIDER === 'gcp_kms'
    ? new GcpKmsProvider()
    : new AzureKeyVaultProvider();
}

function getKeyEncryptionProvider(): KeyEncryptionProvider {
  if (!keyEncryptionProvider) {
    throw new Error('Key encryption provider has not been initialised');
  }

  return keyEncryptionProvider;
}
