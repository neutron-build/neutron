import type { Node } from "prosemirror-model";
export function fromMarkdown(markdown: string, lexer: (source: string) => unknown[], decode?: (text: string) => string): Node;
export function toMarkdown(doc: Node): string;
