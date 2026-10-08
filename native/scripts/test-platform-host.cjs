// Public source-security/real-React tests. This is not an RN runtime/device test.
const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const ts = require('typescript');
const root = path.resolve(__dirname, '..');
for (const extension of ['.ts', '.tsx']) Module._extensions[extension] = (module, filename) => {
  const result = ts.transpileModule(fs.readFileSync(filename, 'utf8'), { fileName: filename, compilerOptions: {
    module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true,
  } });
  module._compile(result.outputText, filename);
};
const resolve = Module._resolveFilename;
const pkgNodeModules = path.join(root, 'packages', 'neutron-native', 'node_modules');
Module._resolveFilename = function (name, parent, ...rest) {
  if (name === 'react-native') return path.join(root, 'tests/platform-host-primitives.cjs');
  try { return resolve.call(this, name, parent, ...rest); }
  catch (error) {
    // pnpm's isolated layout: react/react-dom/happy-dom live in the package's
    // own node_modules, not the workspace root — resolve bare specifiers there.
    if (!name.startsWith('.') && !path.isAbsolute(name)) {
      try { return resolve.call(this, name, { ...parent, filename: path.join(pkgNodeModules, 'noop.js'), paths: Module._nodeModulePaths(pkgNodeModules) }, ...rest); }
      catch { /* fall through to the .js→.ts source probe below */ }
    }
    if (name.startsWith('.') && name.endsWith('.js')) {
      for (const suffix of ['.ts', '.tsx']) {
        const candidate = path.resolve(path.dirname(parent.filename), name.slice(0, -3) + suffix);
        if (fs.existsSync(candidate)) return candidate;
      }
    }
    throw error;
  }
};
for (const source of ['ota-security.node.ts', 'navigation.node.tsx', 'platform-navigation-dom.node.tsx']) require(path.join(root, 'tests', source));
