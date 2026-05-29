import { cp, rm } from 'node:fs/promises';
import { build } from 'esbuild';

await rm(new URL('../dist-lambda', import.meta.url), {
  recursive: true,
  force: true,
});

await build({
  banner: {
    js: [
      "import { createRequire } from 'node:module';",
      "import { dirname as __pathDirname, join as __pathJoin } from 'node:path';",
      "import { fileURLToPath as __fileURLToPath } from 'node:url';",
      'const require = createRequire(import.meta.url);',
      'const __filename = __fileURLToPath(import.meta.url);',
      'const __bundleDirname = __pathDirname(__filename);',
      "const __dirname = __pathJoin(__bundleDirname, 'bullmq/dist/cjs/commands');",
      'const __nativeRequireResolve = require.resolve.bind(require);',
      'require.resolve = (id, options) =>',
      "  id === '@bull-board/ui/package.json'",
      "    ? __pathJoin(__bundleDirname, 'bull-board-ui/package.json')",
      '    : __nativeRequireResolve(id, options);',
    ].join(' '),
  },
  bundle: true,
  entryPoints: ['src/lambda.ts'],
  format: 'esm',
  legalComments: 'none',
  logLevel: 'info',
  outfile: 'dist-lambda/index.mjs',
  platform: 'node',
  sourcemap: false,
  target: 'node22',
});

await cp(
  new URL('../node_modules/@bull-board/ui/dist', import.meta.url),
  new URL('../dist-lambda/bull-board-ui/dist', import.meta.url),
  {
    recursive: true,
  }
);

await cp(
  new URL('../node_modules/@bull-board/ui/package.json', import.meta.url),
  new URL('../dist-lambda/bull-board-ui/package.json', import.meta.url)
);

await cp(
  new URL('../node_modules/bullmq/dist/cjs/commands', import.meta.url),
  new URL('../dist-lambda/bullmq/dist/cjs/commands', import.meta.url),
  {
    recursive: true,
  }
);
