/**
 * Ambient declarations for the parser-only Babel syntax plugins (v7.29
 * stopped shipping bundled type declarations). They are passed to
 * transformSync as plugin values — PluginItem is the exact shape used.
 * NOTE: no top-level import here — it would turn this file into a module
 * and scope these declarations instead of making them ambient.
 */

declare module '@babel/plugin-syntax-typescript' {
  const plugin: import('@babel/core').PluginItem
  export default plugin
}

declare module '@babel/plugin-syntax-jsx' {
  const plugin: import('@babel/core').PluginItem
  export default plugin
}
