import { parse } from "@babel/parser";
import MagicString from "magic-string";

const SERVER_EXPORT_NAMES = new Set([
  "loader",
  "action",
  "middleware",
  "headers",
  "getStaticPaths",
]);

const SERVER_FILE_RE = /\.server(?:\.[^/\\?#]+)?(?:[?#].*)?$/i;

interface ImportRecord {
  source: string;
}

interface ParseNode {
  type: string;
  start: number;
  end: number;
  source?: { value?: string };
  specifiers?: Array<{
    type: string;
    start: number;
    end: number;
    local?: { name?: string };
    exported?: { name?: string };
  }>;
  declaration?: {
    type: string;
    id?: { name?: string };
    kind?: string;
    declarations?: Array<{
      id?: { name?: string };
      start: number;
      end: number;
    }>;
  };
}

interface ParseProgram {
  body: ParseNode[];
  program?: {
    body: ParseNode[];
  };
}

function getProgramBody(ast: ParseProgram): ParseNode[] {
  if (Array.isArray(ast.body)) {
    return ast.body;
  }
  if (ast.program && Array.isArray(ast.program.body)) {
    return ast.program.body;
  }
  return [];
}

export function isServerOnlySpecifier(specifier: string): boolean {
  const clean = specifier.split("?")[0].split("#")[0];
  return SERVER_FILE_RE.test(clean);
}

export function parseImports(code: string): ImportRecord[] {
  const ast = parse(code, {
    sourceType: "module",
    plugins: ["typescript", "jsx"],
  }) as unknown as ParseProgram;

  const imports: ImportRecord[] = [];
  for (const node of getProgramBody(ast)) {
    if (node.type !== "ImportDeclaration") {
      continue;
    }
    const source = typeof node.source?.value === "string" ? node.source.value : "";
    imports.push({ source });
  }
  return imports;
}

export function hasServerOnlyImport(code: string): boolean {
  return parseImports(code).some((record) => isServerOnlySpecifier(record.source));
}

export function stripServerOnlyRouteModule(code: string): string {
  const ast = parse(code, {
    sourceType: "module",
    plugins: ["typescript", "jsx"],
  }) as unknown as ParseProgram;
  const magic = new MagicString(code);

  for (const node of getProgramBody(ast)) {
    if (node.type === "ImportDeclaration") {
      const source = typeof node.source?.value === "string" ? node.source.value : "";
      if (isServerOnlySpecifier(source)) {
        magic.remove(node.start, node.end);
      }
      continue;
    }

    if (node.type !== "ExportNamedDeclaration") {
      continue;
    }

    if (node.declaration) {
      const declaration = node.declaration;

      if (declaration.type === "FunctionDeclaration") {
        const exportName = declaration.id?.name;
        if (exportName && SERVER_EXPORT_NAMES.has(exportName)) {
          magic.remove(node.start, node.end);
        }
        continue;
      }

      if (declaration.type === "VariableDeclaration") {
        const declarators = declaration.declarations || [];
        const kept = declarators.filter((decl) => {
          const name = decl.id?.name;
          return !name || !SERVER_EXPORT_NAMES.has(name);
        });

        if (kept.length === declarators.length) {
          continue;
        }

        if (kept.length === 0) {
          magic.remove(node.start, node.end);
          continue;
        }

        const kind = declaration.kind || "const";
        const rebuilt = kept
          .map((decl) => code.slice(decl.start, decl.end))
          .join(", ");
        magic.overwrite(node.start, node.end, `export ${kind} ${rebuilt};`);
      }

      continue;
    }

    const specifiers = node.specifiers || [];
    if (specifiers.length === 0) {
      continue;
    }

    const kept = specifiers.filter((specifier) => {
      const exportedName = specifier.exported?.name || specifier.local?.name || "";
      const localName = specifier.local?.name || "";
      return (
        !SERVER_EXPORT_NAMES.has(exportedName) &&
        !SERVER_EXPORT_NAMES.has(localName)
      );
    });

    if (kept.length === specifiers.length) {
      continue;
    }

    if (kept.length === 0) {
      magic.remove(node.start, node.end);
      continue;
    }

    const rebuilt = kept.map((specifier) => code.slice(specifier.start, specifier.end)).join(", ");
    magic.overwrite(node.start, node.end, `export { ${rebuilt} };`);
  }

  return magic.toString();
}

export function stripQueryFromId(id: string): string {
  return id.split("?")[0];
}
