import crypto from 'node:crypto';

import { ManagedIdentityCredential } from '@azure/identity';
import { CryptographyClient, KeyClient } from '@azure/keyvault-keys';

import { getConfig } from '../config';

const DEK_CACHE_TTL_MS = 5 * 60 * 1000;
const credential = new ManagedIdentityCredential();
let cryptoClient: CryptographyClient | null = null;

const dekCache = new Map<string, { dek: Buffer; expiresAt: number }>();

export interface VaultEncryptionContext {
  id: string;
  encrypted_dek: string | null;
  vault_encryption_enabled: boolean;
}

export async function initCryptoClient(): Promise<void> {
  const { KEY_VAULT_URI, KEK_KEY_NAME } = getConfig();
  const keyClient = new KeyClient(KEY_VAULT_URI, credential);
  const key = await keyClient.getKey(KEK_KEY_NAME);
  cryptoClient = new CryptographyClient(key, credential);
  console.log('[persistio] Key Vault crypto client initialised');
}

export async function generateAndWrapDek(): Promise<{ encryptedDek: string }> {
  const dek = crypto.randomBytes(32);
  const client = getCryptographyClient();
  const result = await client.wrapKey('RSA-OAEP-256', dek);
  return { encryptedDek: Buffer.from(result.result).toString('base64') };
}

export async function unwrapDek(encryptedDek: string): Promise<Buffer> {
  const client = getCryptographyClient();
  const result = await client.unwrapKey('RSA-OAEP-256', Buffer.from(encryptedDek, 'base64'));
  return Buffer.from(result.result);
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

  const cached = dekCache.get(vault.id);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.dek;
  }

  const dek = await unwrapDek(vault.encrypted_dek);
  dekCache.set(vault.id, {
    dek,
    expiresAt: Date.now() + DEK_CACHE_TTL_MS
  });
  return dek;
}

function getCryptographyClient(): CryptographyClient {
  if (!cryptoClient) {
    throw new Error('Key Vault crypto client has not been initialised');
  }

  return cryptoClient;
}
