import path from 'node:path';

import { describe, expect, it } from 'vitest';

import { RAW_CHUNK_LOCAL_DIR_DEFAULT, resolveRawChunkLocalRoot } from './raw-chunk-storage-config';

describe('raw chunk local storage config', () => {
  it('resolves relative local storage paths from the server package root', () => {
    expect(resolveRawChunkLocalRoot(RAW_CHUNK_LOCAL_DIR_DEFAULT)).toBe(
      path.resolve(__dirname, '..', '..', RAW_CHUNK_LOCAL_DIR_DEFAULT)
    );
  });

  it('keeps absolute local storage paths unchanged', () => {
    expect(resolveRawChunkLocalRoot('/data/raw-chunks')).toBe('/data/raw-chunks');
  });
});
