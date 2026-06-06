import path from 'node:path';

export const RAW_CHUNK_LOCAL_DIR_DEFAULT = './data/raw-chunks';

export function resolveRawChunkLocalRoot(rootDir: string): string {
  return path.isAbsolute(rootDir)
    ? rootDir
    : path.resolve(__dirname, '..', '..', rootDir);
}
