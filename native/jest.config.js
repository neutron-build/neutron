/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/packages'],
  testMatch: ['**/__tests__/**/*.test.ts', '**/__tests__/**/*.test.tsx', '**/*.test.ts', '**/*.test.tsx'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json'],
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      tsconfig: {
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
