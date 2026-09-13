import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    // Real workers claim database-wide queues. Shared-DB suites must not consume
    // another suite's fixtures; unit-only runs retain normal parallel execution.
    fileParallelism: !process.env.PERSISTIO_TEST_DATABASE_URL,
    setupFiles: ['./vitest.setup.ts']
  }
});
