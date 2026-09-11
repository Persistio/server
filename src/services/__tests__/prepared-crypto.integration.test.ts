import crypto from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
const kms = vi.hoisted(() => ({ unwrap: vi.fn() }));
vi.mock('@google-cloud/kms', () => ({ KeyManagementServiceClient: class { decrypt = kms.unwrap; } }));
const databaseUrl = process.env.PERSISTIO_TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)('prepared crypto database binding', () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const vaults: string[] = [];
  let db: typeof import('../../db/client'), crypt: typeof import('../crypto');
  beforeAll(async () => {
    process.env.DATABASE_URL = databaseUrl;
    db = await import('../../db/client'); await db.runMigrations();
    const config = (await import('../../config')).getConfig();
    config.ENCRYPTION_ENABLED = true; config.KEY_PROVIDER = 'gcp_kms';
    config.GCP_KMS_KEY_NAME = 'projects/test/locations/global/keyRings/test/cryptoKeys/test';
    crypt = await import('../crypto'); await crypt.initCryptoClient();
  });
  afterEach(async () => { kms.unwrap.mockReset(); await pool.query('DELETE FROM vaults WHERE id=ANY($1::uuid[])', [vaults]); vaults.length = 0; });
  afterAll(async () => { await pool.end(); await db?.closePool(); });
  async function fixture() {
    const id = crypto.randomUUID(); vaults.push(id);
    const vault = { id, encrypted_dek: Buffer.from('wrapped-test-key').toString('base64'), vault_encryption_enabled: true };
    await pool.query('INSERT INTO vaults(id,name,api_key_hash,encrypted_dek,vault_encryption_enabled) VALUES ($1,$2,$3,$4,true)', [id, 'prepared-crypto-test', crypto.randomUUID(), vault.encrypted_dek]);
    return vault;
  }
  it('holds no vault lock during KMS, then holds identity stable through encrypted SQL commit without another unwrap', async () => {
    const vault = await fixture();
    kms.unwrap.mockImplementation(async () => {
      const probe = await pool.connect();
      try { await probe.query('BEGIN'); await probe.query('SELECT * FROM vaults WHERE id=$1 FOR UPDATE NOWAIT', [vault.id]); }
      finally { await probe.query('ROLLBACK'); probe.release(); }
      return [{ plaintext: Buffer.alloc(32, 7) }];
    });
    const prepared = await crypt.prepareVaultCrypto(vault);
    const ciphertext = prepared.encrypt(vault, 'private worker fact');
    await db.withTransaction(async client => {
      await prepared.assertCurrent(client);
      const probe = await pool.connect();
      try {
        await probe.query('BEGIN');
        await expect(probe.query('SELECT * FROM vaults WHERE id=$1 FOR UPDATE NOWAIT', [vault.id])).rejects.toMatchObject({ code: '55P03' });
      } finally { await probe.query('ROLLBACK'); probe.release(); }
      await client.query("INSERT INTO memories(vault_id,data,subject,hash,scope) VALUES ($1,$2,'','test','global')", [vault.id, ciphertext]);
      expect(prepared.decrypt(vault, ciphertext)).toBe('private worker fact');
      expect(kms.unwrap).toHaveBeenCalledOnce();
    });
    expect((await pool.query('SELECT data FROM memories WHERE vault_id=$1', [vault.id])).rows[0].data).toBe(ciphertext);
    expect(ciphertext).not.toContain('private');
  });
  it('rejects changed encryption identity without writing or asking KMS again', async () => {
    const vault = await fixture(); kms.unwrap.mockResolvedValue([{ plaintext: Buffer.alloc(32, 7) }]);
    const prepared = await crypt.prepareVaultCrypto(vault);
    await pool.query('UPDATE vaults SET encrypted_dek=$2 WHERE id=$1', [vault.id, Buffer.from('rotated').toString('base64')]);
    await expect(db.withTransaction(async client => {
      await prepared.assertCurrent(client);
      await client.query("INSERT INTO memories(vault_id,data,subject,hash,scope) VALUES ($1,'bad','','test','global')", [vault.id]);
    })).rejects.toThrow('identity changed');
    expect(kms.unwrap).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT 1 FROM memories WHERE vault_id=$1', [vault.id])).rowCount).toBe(0);
  });
});
