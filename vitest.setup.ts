process.env.DATABASE_URL ??= 'postgres://persistio:test@localhost:5432/persistio_test';
process.env.ADMIN_API_KEY ??= 'test-admin-key';
process.env.OPENAI_API_KEY ??= 'test-openai-key';
process.env.EXTRACTOR_API_KEY ??= 'test-extractor-key';
process.env.CIRCUIT_BREAKER_THRESHOLD ??= '3';
process.env.CIRCUIT_BREAKER_PROBE_INTERVAL_MS ??= '300000';
process.env.CIRCUIT_BREAKER_MAX_PROBE_INTERVAL_MS ??= '600000';
