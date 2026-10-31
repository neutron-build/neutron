/**
 * Server Actions: the compile-time half.
 *
 * A module whose directive prologue holds `"use server"` is a server-action
 * module. Every export must be an async function the build can see is one;
 * anything else is a build error (a client stub cannot stand in for a value,
 * and an export the build cannot classify must not become callable). In the
 * CLIENT graph the module is replaced by one stub per export, each bound to a
 * stable id — a hash of the module path (relative to the app root) and the
 * export name — and the module is recorded in a {@link ServerActionRegistry}:
 * the server registers exactly the modules client code reached. The SERVER
 * graph keeps the real module, so a server-side import calls the functions
 * directly.
 *
 * Inline `"use server"` (a directive inside a function body: an action
 * declared in a component or closure) is not supported and is a build error
 * wherever it appears, as is a `"use server"` string that is not part of the
 * module's directive prologue (it would silently do nothing).
 */
import { createHash } from "node:crypto";
import * as path from "node:path";
import { parse } from "@babel/parser";
import {
  SERVER_ACTIONS_MANIFEST_FORMAT,
  type ServerActionsManifest,
} from "../core/server-action-protocol.js";

const DIRECTIVE_TEXT_RE = /["']use server["']/;

/** Cheap pre-check: can `code` contain a `"use server"` directive at all? */
export function mayContainUseServer(code: string): boolean {
  return DIRECTIVE_TEXT_RE.test(code);
}

/** The id an action is invoked by: stable for the same module path and export name. */
export function serverActionId(relativeFile: string, exportName: string): string {
  return createHash("sha256").update(`${relativeFile}#${exportName}`).digest("hex").slice(0, 40);
}

/** Module path relative to the app root, posix: the stable half of an action id. */
export function serverActionModuleKey(rootDir: string, file: string): string {
  return path.relative(rootDir, file).split(path.sep).join("/");
}

export interface ServerActionModuleAnalysis {
  /** The module declares `"use server"` in its directive prologue. */
  isServerActionModule: boolean;
  /** Its action exports (`default` for a default export), when it is one. */
  exports: string[];
  /** Build errors, each a complete sentence naming the problem and the fix. */
  errors: string[];
}

interface Node {
  type: string;
  start?: number | null;
  loc?: { start: { line: number; column: number } } | null;
  [key: string]: unknown;
}

const EXPORT_RULE =
  'Every export of a "use server" module must be an async function: ' +
  "`export async function name() {}` or `export const name = async () => {}`.";

function line(node: Node | undefined): string {
  return node?.loc ? `:${node.loc.start.line}` : "";
}

function isAsyncFunctionNode(node: Node | null | undefined): boolean {
  if (!node) return false;
  if (
    node.type === "FunctionDeclaration" ||
    node.type === "FunctionExpression" ||
    node.type === "ArrowFunctionExpression"
  ) {
    return node.async === true && node.generator !== true;
  }
  // `export const f = (async () => {}) as Action` / `satisfies`
  if (
    node.type === "TSAsExpression" ||
    node.type === "TSSatisfiesExpression" ||
    node.type === "ParenthesizedExpression"
  ) {
    return isAsyncFunctionNode(node.expression as Node);
  }
  return false;
}

function describeNonAction(node: Node | null | undefined): string {
  if (!node) return "has no value";
  if (
    node.type === "FunctionDeclaration" ||
    node.type === "FunctionExpression" ||
    node.type === "ArrowFunctionExpression"
  ) {
    if (node.generator) return "is a generator function";
    return "is not async";
  }
  if (node.type === "ClassDeclaration" || node.type === "ClassExpression") return "is a class";
  if (node.type === "CallExpression") return "is the result of a call the build cannot verify is an async function";
  return "is not a function";
}

/** Walk every node below `root` (Babel AST; skips location metadata). */
function walk(root: unknown, visit: (node: Node) => void): void {
  const stack: unknown[] = [root];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || typeof current !== "object") continue;
    if (Array.isArray(current)) {
      for (const item of current) stack.push(item);
      continue;
    }
    const node = current as Node;
    if (typeof node.type === "string") visit(node);
    for (const [key, value] of Object.entries(node)) {
      if (key === "loc" || key === "start" || key === "end" || key === "extra" || key === "leadingComments" || key === "trailingComments") {
        continue;
      }
      if (value && typeof value === "object") stack.push(value);
    }
  }
}

function hasUseServer(directives: unknown): boolean {
  return (
    Array.isArray(directives) &&
    directives.some((directive) => (directive as { value?: { value?: string } }).value?.value === "use server")
  );
}

/**
 * Classify a module's `"use server"` usage and validate its exports. `label`
 * names the file in error messages.
 */
export function analyzeServerActionModule(code: string, label: string): ServerActionModuleAnalysis {
  const result: ServerActionModuleAnalysis = { isServerActionModule: false, exports: [], errors: [] };
  if (!mayContainUseServer(code)) return result;

  let ast: { program: Node & { body: Node[]; directives?: unknown[] } };
  try {
    ast = parse(code, { sourceType: "module", plugins: ["typescript", "jsx"] }) as unknown as typeof ast;
  } catch {
    // Not ours to report: the real transform reports the syntax error.
    return result;
  }
  const program = ast.program;
  result.isServerActionModule = hasUseServer(program.directives);
  if (result.isServerActionModule && Array.isArray(program.directives)) {
    const useClient = program.directives.some(
      (directive) => (directive as { value?: { value?: string } }).value?.value === "use client"
    );
    if (useClient) {
      result.errors.push(`${label}: a module cannot declare both "use client" and "use server".`);
    }
  }

  // A "use server" string outside the prologue does nothing; inline (function
  // level) directives declare closures the build cannot register.
  for (const statement of program.body) {
    const expression = statement.expression as Node | undefined;
    if (
      statement.type === "ExpressionStatement" &&
      expression?.type === "StringLiteral" &&
      expression.value === "use server"
    ) {
      result.errors.push(
        `${label}${line(statement)}: "use server" must be the first statement of the module ` +
          "(before any import) to declare a server-action module."
      );
    }
  }
  walk(program.body, (node) => {
    if (node.type === "BlockStatement" && hasUseServer(node.directives)) {
      result.errors.push(
        `${label}${line(node)}: inline "use server" functions (declared inside a component, ` +
          "function or closure) are not supported. Move the function to a module whose first " +
          'statement is "use server" and import it.'
      );
    }
  });
  if (!result.isServerActionModule) return result;

  // Top-level bindings an `export { name }` can refer to.
  const bindings = new Map<string, Node | null>();
  for (const statement of program.body) {
    const declaration =
      statement.type === "ExportNamedDeclaration" || statement.type === "ExportDefaultDeclaration"
        ? (statement.declaration as Node | null)
        : statement;
    if (!declaration) continue;
    if (declaration.type === "FunctionDeclaration" && (declaration.id as Node | null)?.name) {
      bindings.set((declaration.id as Node).name as string, declaration);
    } else if (declaration.type === "VariableDeclaration") {
      for (const declarator of declaration.declarations as Node[]) {
        const id = declarator.id as Node;
        if (id.type === "Identifier") {
          bindings.set(id.name as string, declaration.kind === "const" ? (declarator.init as Node | null) : null);
        }
      }
    } else if (declaration.type === "ClassDeclaration" && (declaration.id as Node | null)?.name) {
      bindings.set((declaration.id as Node).name as string, declaration);
    } else if (declaration.type === "ImportDeclaration") {
      for (const specifier of declaration.specifiers as Node[]) {
        bindings.set((specifier.local as Node).name as string, specifier);
      }
    }
  }

  const accept = (name: string) => {
    if (!result.exports.includes(name)) result.exports.push(name);
  };
  const reject = (node: Node, name: string, why: string) => {
    result.errors.push(`${label}${line(node)}: export "${name}" ${why}. ${EXPORT_RULE}`);
  };
  const checkBinding = (statement: Node, exported: string, local: string) => {
    const bound = bindings.get(local);
    if (bound === undefined) {
      reject(statement, exported, "does not name a top-level declaration of this module");
    } else if (bound === null) {
      reject(statement, exported, "is not declared with `const`");
    } else if (bound.type === "ImportSpecifier" || bound.type === "ImportDefaultSpecifier" || bound.type === "ImportNamespaceSpecifier") {
      reject(statement, exported, "re-exports an import; define the action in this module");
    } else if (isAsyncFunctionNode(bound)) {
      accept(exported);
    } else {
      reject(statement, exported, describeNonAction(bound));
    }
  };

  for (const statement of program.body) {
    if (statement.type === "ExportAllDeclaration") {
      if (statement.exportKind === "type") continue;
      result.errors.push(
        `${label}${line(statement)}: \`export *\` is not allowed in a "use server" module; ` +
          "define each action in the module itself."
      );
      continue;
    }
    if (statement.type === "ExportDefaultDeclaration") {
      const declaration = statement.declaration as Node;
      if (declaration.type === "Identifier") {
        checkBinding(statement, "default", declaration.name as string);
      } else if (declaration.type === "TSInterfaceDeclaration") {
        continue;
      } else if (isAsyncFunctionNode(declaration)) {
        accept("default");
      } else {
        reject(statement, "default", describeNonAction(declaration));
      }
      continue;
    }
    if (statement.type !== "ExportNamedDeclaration") continue;
    if (statement.exportKind === "type") continue;
    const declaration = statement.declaration as Node | null;
    if (declaration) {
      if (
        declaration.type === "TSTypeAliasDeclaration" ||
        declaration.type === "TSInterfaceDeclaration" ||
        declaration.declare === true
      ) {
        continue;
      }
      if (declaration.type === "FunctionDeclaration") {
        const name = (declaration.id as Node).name as string;
        if (isAsyncFunctionNode(declaration)) accept(name);
        else reject(statement, name, describeNonAction(declaration));
      } else if (declaration.type === "VariableDeclaration") {
        for (const declarator of declaration.declarations as Node[]) {
          const id = declarator.id as Node;
          const name = id.type === "Identifier" ? (id.name as string) : "(destructured)";
          if (id.type !== "Identifier") {
            reject(statement, name, "is a destructuring pattern");
          } else if (declaration.kind !== "const") {
            reject(statement, name, "is not declared with `const`");
          } else if (isAsyncFunctionNode(declarator.init as Node)) {
            accept(name);
          } else {
            reject(statement, name, describeNonAction(declarator.init as Node));
          }
        }
      } else {
        const id = declaration.id as Node | undefined;
        reject(statement, (id?.name as string) ?? declaration.type, describeNonAction(declaration));
      }
      continue;
    }
    if (statement.source) {
      result.errors.push(
        `${label}${line(statement)}: re-exporting from another module is not allowed in a ` +
          '"use server" module; define each action in the module itself.'
      );
      continue;
    }
    for (const specifier of statement.specifiers as Node[]) {
      if (specifier.exportKind === "type") continue;
      const local = (specifier.local as Node).name as string;
      const exportedNode = specifier.exported as Node;
      const exported = (exportedNode.name ?? exportedNode.value) as string;
      checkBinding(statement, exported, local);
    }
  }
  return result;
}

/**
 * The client graph's replacement for a server-action module: one reference
 * per export, bound to its id.
 */
export function generateServerActionStub(moduleKey: string, exports: string[]): string {
  const lines = ['import { createServerActionReference } from "@neutron-build/core/client";'];
  for (const name of exports) {
    const call = `/*#__PURE__*/ createServerActionReference(${JSON.stringify(serverActionId(moduleKey, name))}, ${JSON.stringify(name)})`;
    lines.push(name === "default" ? `export default ${call};` : `export const ${name} = ${call};`);
  }
  return lines.join("\n") + "\n";
}

/**
 * Server-action modules the client graph reached: what the server registers.
 * `neutron-ts build` shares one across its client passes and writes it to
 * dist; the dev server keeps its own, updated as Vite transforms modules.
 */
export class ServerActionRegistry {
  private readonly modules = new Map<string, string[]>();
  private readonly ids = new Map<string, { file: string; export: string }>();

  /** Record (or replace) a module's action exports. */
  set(moduleKey: string, exports: string[]): void {
    this.delete(moduleKey);
    this.modules.set(moduleKey, [...exports]);
    for (const name of exports) {
      this.ids.set(serverActionId(moduleKey, name), { file: moduleKey, export: name });
    }
  }

  /** Forget a module (it lost its directive, or was deleted). */
  delete(moduleKey: string): void {
    for (const name of this.modules.get(moduleKey) ?? []) {
      this.ids.delete(serverActionId(moduleKey, name));
    }
    this.modules.delete(moduleKey);
  }

  get size(): number {
    return this.ids.size;
  }

  /** Module key and export name for an action id. */
  lookup(id: string): { file: string; export: string } | null {
    return this.ids.get(id) ?? null;
  }

  files(): string[] {
    return [...this.modules.keys()].sort();
  }

  toManifest(): ServerActionsManifest {
    const actions: ServerActionsManifest["actions"] = {};
    for (const file of this.files()) {
      for (const name of [...this.modules.get(file)!].sort()) {
        actions[serverActionId(file, name)] = { file, export: name };
      }
    }
    return { format: SERVER_ACTIONS_MANIFEST_FORMAT, actions };
  }
}
