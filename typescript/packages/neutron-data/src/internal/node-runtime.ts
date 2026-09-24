// Runtime-support guard: the database wrapper requires a Node.js runtime.
// Detection is POSITIVE (is a Node process present?), never `typeof window`
// inference — a runtime that provides Node compatibility (process.versions.node)
// is not blocked, and a runtime without it fails here with one precise error
// instead of crashing later inside a driver import or a missing global.

export function assertNodeRuntime(operation: string): void {
  const versions =
    typeof process === "undefined"
      ? undefined
      : (process as { versions?: { node?: string } }).versions;
  if (!versions?.node) {
    throw new Error(
      `${operation} requires a Node.js runtime (no process.versions.node was found). ` +
        `@neutron-build/data's Drizzle integration drives postgres.js and @libsql/client ` +
        `through Node transports; no edge/browser adapter exists. Run it in Node.js ` +
        `(see "Runtime support" in the package README).`
    );
  }
}
