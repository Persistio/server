/**
 * Live integration tests for the deterministic extraction filter (PR #89).
 *
 * Each test:
 *   1. Creates a fresh isolated vault.
 *   2. Ingests a conversation engineered to contain a specific noise category.
 *   3. Triggers extraction immediately via POST /v1/extract.
 *   4. Polls GET /v1/jobs/:id until the job completes.
 *   5. Asserts on the memories that were (or were not) stored.
 *
 * Requires a running Persistio server. Set env vars before running:
 *   PERSISTIO_LIVE=true
 *   PERSISTIO_BASE_URL=http://localhost:4827   (default)
 *   ADMIN_API_KEY=test-admin-key               (must match server config)
 *
 * Run:
 *   PERSISTIO_LIVE=true npm test --workspace @persistio/server -- --reporter=verbose deterministic-filter.live
 */
import assert from 'node:assert/strict';
import { describe, it, beforeAll, afterAll } from 'vitest';

const describeLive = process.env.PERSISTIO_LIVE ? describe : describe.skip;

const BASE_URL = process.env.PERSISTIO_BASE_URL ?? 'http://localhost:4827';
const ADMIN_KEY = process.env.ADMIN_API_KEY ?? 'test-admin-key';
const JOB_POLL_INTERVAL_MS = 500;
const JOB_TIMEOUT_MS = 60_000;
const TEST_TIMEOUT_MS = 90_000;

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

async function createVault(name: string): Promise<{ id: string; api_key: string }> {
  const res = await fetch(`${BASE_URL}/admin/vaults`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${ADMIN_KEY}` },
    body: JSON.stringify({ name })
  });
  assert.equal(res.status, 201, `createVault ${name} failed: ${res.status}`);
  return res.json() as Promise<{ id: string; api_key: string }>;
}

async function deleteVault(vaultId: string): Promise<void> {
  await fetch(`${BASE_URL}/admin/vaults/${vaultId}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${ADMIN_KEY}` }
  });
}

async function ingest(apiKey: string, sessionId: string, chunks: Array<{ role: string; content: string }>): Promise<void> {
  const now = new Date().toISOString();
  const res = await fetch(`${BASE_URL}/v1/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({
      session_id: sessionId,
      chunks: chunks.map((c) => ({ ...c, timestamp: now }))
    })
  });
  assert.equal(res.status, 202, `ingest failed: ${res.status} ${await res.text()}`);
}

async function triggerExtraction(apiKey: string): Promise<string> {
  const res = await fetch(`${BASE_URL}/v1/extract`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}` }
  });
  assert.equal(res.status, 202, `triggerExtraction failed: ${res.status}`);
  const body = await res.json() as { job_id: string };
  return body.job_id;
}

async function waitForJob(apiKey: string, jobId: string): Promise<{ status: string; error?: string }> {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const res = await fetch(`${BASE_URL}/v1/jobs/${jobId}`, {
      headers: { Authorization: `Bearer ${apiKey}` }
    });
    assert.equal(res.status, 200, `waitForJob poll failed: ${res.status}`);
    const body = await res.json() as { status: string; error?: string };
    if (body.status === 'completed' || body.status === 'failed') {
      return body;
    }
    await new Promise((r) => setTimeout(r, JOB_POLL_INTERVAL_MS));
  }
  throw new Error(`Job ${jobId} did not complete within ${JOB_TIMEOUT_MS}ms`);
}

async function listMemories(apiKey: string): Promise<Array<{ data: string; subject: string; type: string | null; status: string }>> {
  const res = await fetch(`${BASE_URL}/v1/memories`, {
    headers: { Authorization: `Bearer ${apiKey}` }
  });
  assert.equal(res.status, 200);
  const body = await res.json() as { items: Array<{ data: string; subject: string; type: string | null; status: string }> };
  return body.items;
}

async function runExtractionTest(vaultName: string, chunks: Array<{ role: string; content: string }>): Promise<{
  memories: Array<{ data: string; subject: string; type: string | null; status: string }>;
  vault: { id: string; api_key: string };
  jobStatus: string;
}> {
  const vault = await createVault(vaultName);
  await ingest(vault.api_key, `${vaultName}-session`, chunks);
  const jobId = await triggerExtraction(vault.api_key);
  const job = await waitForJob(vault.api_key, jobId);
  const memories = await listMemories(vault.api_key);
  return { memories, vault, jobStatus: job.status };
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

const createdVaultIds: string[] = [];

describeLive('Deterministic extraction filter — live tests', () => {
  beforeAll(async () => {
    // Smoke-check server is reachable
    const res = await fetch(`${BASE_URL}/health`);
    assert.equal(res.status, 200, `Server not reachable at ${BASE_URL}`);
    const body = await res.json() as { status: string; db: string };
    assert.equal(body.status, 'ok', `Server status: ${body.status}`);
    assert.equal(body.db, 'ok', `DB status: ${body.db}`);
    console.log(`[live] Server healthy at ${BASE_URL}`);
  });

  afterAll(async () => {
    await Promise.all(createdVaultIds.map(deleteVault));
    console.log(`[live] Cleaned up ${createdVaultIds.length} vaults`);
  });

  // ── Happy path ────────────────────────────────────────────

  it('PASS: stores a durable user preference fact', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-pass-pref', [
      { role: 'user', content: 'I always want TypeScript, never plain JavaScript.' },
      { role: 'assistant', content: 'Got it, TypeScript only from now on.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed', 'Extraction job should complete');
    assert.ok(memories.length > 0, `Expected at least 1 memory, got 0`);

    const hasTypescript = memories.some((m) =>
      m.data.toLowerCase().includes('typescript') ||
      m.subject.toLowerCase().includes('typescript') ||
      m.subject.toLowerCase().includes('user')
    );
    assert.ok(hasTypescript, `Expected a TypeScript-related memory, got: ${JSON.stringify(memories.map((m) => m.data))}`);
    console.log(`[live] PASS: durable preference stored — ${memories.length} memory(s)`);
    memories.forEach((m) => console.log(`  • [${m.type}] "${m.data}"`));
  });

  it('PASS: stores a durable project/system fact', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-pass-project', [
      { role: 'user', content: 'Our project is called fantastic-system and it deploys to Azure Container Apps.' },
      { role: 'assistant', content: 'Noted — fantastic-system on Azure Container Apps.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    assert.ok(memories.length > 0, `Expected at least 1 memory, got 0`);
    console.log(`[live] PASS: project fact stored — ${memories.length} memory(s)`);
    memories.forEach((m) => console.log(`  • [${m.type}] "${m.data}"`));
  });

  // ── Low-salience / filler ─────────────────────────────────

  it('BLOCK: pure conversational filler produces no memories', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-filler', [
      { role: 'user', content: 'Hi there!' },
      { role: 'assistant', content: 'Hello! How can I help you today?' },
      { role: 'user', content: 'Thanks.' },
      { role: 'assistant', content: 'Sure, no problem.' },
      { role: 'user', content: 'Great.' },
      { role: 'assistant', content: 'Okay, sounds good.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const nonFiller = memories.filter((m) => !['hi', 'hello', 'thanks', 'okay', 'great', 'sure'].some((w) =>
      m.data.toLowerCase().includes(w) && m.data.split(' ').length < 5
    ));
    assert.equal(nonFiller.length, 0,
      `Expected no durable memories from pure filler, got: ${JSON.stringify(memories.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: filler suppressed — ${memories.length} memory(s) (expected 0)`);
  });

  it('BLOCK: very short low-salience candidate is filtered out', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-short', [
      { role: 'user', content: 'OK.' },
      { role: 'assistant', content: 'Done.' },
      { role: 'user', content: 'Yes.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const shortMemories = memories.filter((m) => m.data.split(' ').length < 4);
    assert.equal(shortMemories.length, 0,
      `Expected no very-short memories, got: ${JSON.stringify(shortMemories.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: short/empty filler suppressed — ${memories.length} memory(s)`);
  });

  // ── Implementation details ────────────────────────────────

  it('BLOCK: ephemeral implementation detail does not persist', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-impl-detail', [
      { role: 'user', content: 'The error is on line 42 of the stack trace.' },
      { role: 'assistant', content: 'I see the stack trace. The traceback shows a null reference.' },
      { role: 'user', content: 'Run npm install to fix the node_modules issue.' },
      { role: 'assistant', content: 'Running npm run build now.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const implDetails = memories.filter((m) =>
      /line \d+|stack trace|traceback|npm install|npm run|node_modules/.test(m.data.toLowerCase())
    );
    assert.equal(implDetails.length, 0,
      `Expected no implementation-detail memories, got: ${JSON.stringify(implDetails.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: impl details suppressed — ${memories.length} impl-detail memory(s) (expected 0)`);
  });

  it('BLOCK: file-edit ephemera does not persist', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-file-edit', [
      { role: 'assistant', content: 'I opened the file and edited it.' },
      { role: 'assistant', content: 'I renamed the file and moved it to the new folder.' },
      { role: 'assistant', content: 'Reading the file now.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const fileEdits = memories.filter((m) =>
      /opened|edited|renamed|moved|read.*file/i.test(m.data)
    );
    assert.equal(fileEdits.length, 0,
      `Expected no file-edit memories, got: ${JSON.stringify(fileEdits.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: file-edit ephemera suppressed — ${fileEdits.length} memory(s) (expected 0)`);
  });

  // ── Secrets ───────────────────────────────────────────────

  it('BLOCK: API key values are not stored as memories', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-secret-api-key', [
      { role: 'user', content: 'My OpenAI key is sk-proj-abcdefghijklmnopqrstuvwxyz012345.' },
      { role: 'assistant', content: 'I will not store that API key.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const secretMemories = memories.filter((m) =>
      /sk-proj-|sk-[a-z0-9]{20,}/i.test(m.data)
    );
    assert.equal(secretMemories.length, 0,
      `Expected no memories containing the API key value, got: ${JSON.stringify(secretMemories.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: API key not stored — total memories: ${memories.length}`);
    if (memories.length > 0) {
      console.log('  (non-secret memories extracted:)');
      memories.forEach((m) => console.log(`  • [${m.type}] "${m.data}"`));
    }
  });

  it('BLOCK: DB connection strings are not stored as memories', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-secret-db-url', [
      { role: 'user', content: 'The connection string is postgresql://admin:s3cr3tpass@prod-db.example.com/appdb.' },
      { role: 'assistant', content: 'I will not store that database URL.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const secretMemories = memories.filter((m) =>
      /postgresql:\/\/|s3cr3tpass|prod-db\.example\.com/i.test(m.data)
    );
    assert.equal(secretMemories.length, 0,
      `Expected no memories containing the DB URL, got: ${JSON.stringify(secretMemories.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: DB URL not stored — total memories: ${memories.length}`);
  });

  it('BLOCK: bearer tokens are not stored as memories', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-secret-bearer', [
      { role: 'user', content: 'Use Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0In0.abc to call the API.' },
      { role: 'assistant', content: 'I will use that token.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const tokenMemories = memories.filter((m) =>
      /eyJ|Bearer [A-Za-z0-9+/=_.-]{20,}/i.test(m.data)
    );
    assert.equal(tokenMemories.length, 0,
      `Expected no memories with bearer token value, got: ${JSON.stringify(tokenMemories.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: bearer token not stored — total memories: ${memories.length}`);
  });

  // ── Duplicates ────────────────────────────────────────────

  it('BLOCK: exact duplicate extractions in the same session are deduplicated', { timeout: TEST_TIMEOUT_MS }, async () => {
    const fact = 'The user always writes tests before shipping code.';
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-duplicate', [
      { role: 'user', content: fact },
      { role: 'assistant', content: 'Understood, tests before shipping.' },
      { role: 'user', content: fact },
      { role: 'assistant', content: 'Yes, noted again — tests before shipping.' },
      { role: 'user', content: 'I always write tests before shipping code!' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    const testMemories = memories.filter((m) =>
      m.data.toLowerCase().includes('test') && m.data.toLowerCase().includes('ship')
    );
    assert.ok(testMemories.length <= 1,
      `Expected at most 1 deduped memory, got ${testMemories.length}: ${JSON.stringify(testMemories.map((m) => m.data))}`
    );
    console.log(`[live] BLOCK: duplicate collapsed — ${testMemories.length} test-shipping memory(s) (expected ≤1)`);
  });

  // ── Mixed conversation (noise + signal) ───────────────────

  it('FILTER: mixed conversation stores signal facts, blocks noise', { timeout: TEST_TIMEOUT_MS }, async () => {
    const { memories, vault, jobStatus } = await runExtractionTest('filter-live-mixed', [
      // Signal: durable preferences
      { role: 'user', content: 'I always use dark mode and Cursor as my editor.' },
      { role: 'assistant', content: 'Got it — dark mode, Cursor editor.' },
      // Noise: greeting filler
      { role: 'user', content: 'Hey!' },
      { role: 'assistant', content: 'Hi there!' },
      // Noise: implementation detail
      { role: 'user', content: 'The stack trace shows a crash at line 99.' },
      { role: 'assistant', content: 'I see the traceback.' },
      // Signal: project fact
      { role: 'user', content: 'Deploy fantastic-system to Azure every Friday.' },
      { role: 'assistant', content: 'Noted, Friday deploys for fantastic-system to Azure.' },
      // Noise: pure filler
      { role: 'user', content: 'Thanks!' },
      { role: 'assistant', content: 'Sure.' }
    ]);
    createdVaultIds.push(vault.id);

    assert.equal(jobStatus, 'completed');
    assert.ok(memories.length > 0, 'Expected at least 1 memory from mixed conversation');

    // Signal facts should be present
    const hasCursorOrEditor = memories.some((m) =>
      /cursor|editor|dark mode/i.test(m.data + m.subject)
    );
    const hasDeployOrProject = memories.some((m) =>
      /fantastic-system|azure|deploy/i.test(m.data + m.subject)
    );
    assert.ok(hasCursorOrEditor || hasDeployOrProject,
      `Expected at least one signal memory (editor or deploy), got: ${JSON.stringify(memories.map((m) => m.data))}`
    );

    // Noise facts should be absent
    const hasStackTrace = memories.some((m) => /stack trace|line \d+|traceback/i.test(m.data));
    assert.ok(!hasStackTrace, `Stack trace detail should not be stored, got: ${JSON.stringify(memories.map((m) => m.data))}`);

    console.log(`[live] FILTER: mixed — ${memories.length} signal memory(s) stored:`);
    memories.forEach((m) => console.log(`  • [${m.type ?? 'null'}] "${m.data}"`));
  });
});
