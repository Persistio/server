import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { recallSchema } from '../../routes/recall';

const root = new URL('../../../../../', import.meta.url);
const read = (path: string) => readFileSync(new URL(path,root),'utf8');

describe('global-rule feature removal',()=>{
  it('removes the service rather than leaving a disabled provider or worker path',()=>{
    for(const name of ['global-arbitration','curator-global-arbitration']) {
      expect(existsSync(fileURLToPath(new URL(`packages/server/src/services/${name}.ts`,root)))).toBe(false);
    }
    for(const path of ['services/extractor.ts','daemon/extraction-worker.ts','daemon/curation-worker.ts','config.ts']) {
      expect(read('packages/server/src/'+path)).not.toMatch(/arbitrateGlobal|arbitrateCuratorGlobals|GLOBAL_ARBITRATION_|global_arbitration/);
    }
  });
  it('has no public opt-in, unconditional result source, or type-specific applicability bypass',()=>{
    expect(recallSchema.shape).not.toHaveProperty('include_global_rules');
    for(const path of ['routes/recall.ts','services/recall-bundle.ts','services/memory-applicability.ts']) {
      expect(read('packages/server/src/'+path)).not.toMatch(/include_global_rules|canIncludeGlobalRules|global_behavioral/);
    }
    expect(read('packages/server/src/services/memory-applicability.ts')).not.toContain('user_rule');
  });
  it('removes published configuration and OpenAPI options',()=>{
    for(const path of ['.env.example','infra/gcp/locals.tf','deployments/gcp/deploy-cloud-run.sh']) {
      expect(read(path)).not.toContain('GLOBAL_ARBITRATION_');
    }
    expect(read('openapi.yaml')).not.toMatch(/include_global_rules|global_behavioral/);
  });
});
