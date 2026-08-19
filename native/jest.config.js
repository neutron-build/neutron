/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/packages'],
  // Built output shadows its own source: `dist/**/*.test.d.ts` declares the
  // same top-level names as `src/**/*.test.ts`, so ts-jest saw each twice and
  // failed the suite with "Cannot redeclare block-scoped variable".
  testPathIgnorePatterns: ['/node_modules/', '/dist/'],
  // The suite was NONDETERMINISTIC without this: `src/__mocks__/react.ts` and
  // its compiled `dist/__mocks__/react.js` share a mock name, so jest's haste
  // map picked one arbitrarily per run (same for react-native and
  // signals-core). Identical invocations alternated between 36/36 passing and
  // 10 suites / 68 tests failing. Only the duplicated mock directory is
  // excluded -- the rest of dist/ must stay resolvable, because packages
  // import each other through it and hiding all of dist takes 10 suites down.
  modulePathIgnorePatterns: ['/dist/__mocks__/'],
  testMatch: ['**/__tests__/**/*.test.ts', '**/__tests__/**/*.test.tsx', '**/*.test.ts', '**/*.test.tsx'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json'],
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      tsconfig: {
        // Without these, every suite touching `global`, `process`,
        // `require.resolve`, `Object.fromEntries`, `Object.entries` or
        // `Array.flatMap` failed to COMPILE -- 82 errors across 6 suites, none
        // of them a test failure. The suites were always fine: 320 tests pass.
        // `lib` predated ES2019 and there were no node types at all.
        lib: ['ES2021', 'DOM'],
        types: ['node', 'jest'],
        jsx: 'react-jsx',
        module: 'commonjs',
        moduleResolution: 'node',
        esModuleInterop: true,
        allowJs: true,
        strict: false,
        skipLibCheck: true,
        resolveJsonModule: true,
        declaration: false,
        outDir: './dist',
        rootDir: '.',
        baseUrl: '.',
      },
    }],
  },
  moduleNameMapper: {
    '^react-native$': '<rootDir>/packages/neutron-native/src/__mocks__/react-native.ts',
    '^react$': '<rootDir>/packages/neutron-native/src/__mocks__/react.ts',
    '^@preact/signals-core$': '<rootDir>/packages/neutron-native/src/__mocks__/signals-core.ts',
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
  transformIgnorePatterns: [
    '/node_modules/(?!(@preact)/)',
  ],
};
