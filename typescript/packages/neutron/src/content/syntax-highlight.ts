import { escapeHtml } from "../core/escape.js";

interface ShikiHighlighter {
  codeToHtml: (code: string, options: { lang: string; theme: string }) => string;
  getLoadedLanguages: () => string[];
  loadLanguage: (...langs: string[]) => Promise<void>;
}

const highlighterPromises = new Map<string, Promise<ShikiHighlighter | null>>();

async function getHighlighter(theme: string): Promise<ShikiHighlighter | null> {
  // One highlighter per theme: a single cached highlighter would only ever
  // load the first theme requested, and codeToHtml with an unloaded theme
  // throws.
  let highlighterPromise = highlighterPromises.get(theme);
  if (!highlighterPromise) {
    // @ts-ignore -- shiki is an optional peer dependency
    highlighterPromise = import("shiki")
      .then(async (shiki: any) => {
        return shiki.createHighlighter({ themes: [theme], langs: [] }) as Promise<ShikiHighlighter>;
      })
      .catch(() => null);
    highlighterPromises.set(theme, highlighterPromise);
  }
  return highlighterPromise;
}

export async function highlightCode(
  code: string,
  lang: string,
  theme = "github-dark"
): Promise<string> {
  const highlighter = await getHighlighter(theme);
  if (!highlighter) {
    return `<pre><code class="language-${escapeHtml(lang)}">${escapeHtml(code)}</code></pre>`;
  }

  const loaded = highlighter.getLoadedLanguages();
  if (!loaded.includes(lang) && lang !== "text") {
    try {
      await highlighter.loadLanguage(lang as any);
    } catch {
      return `<pre><code class="language-${escapeHtml(lang)}">${escapeHtml(code)}</code></pre>`;
    }
  }

  return highlighter.codeToHtml(code, { lang, theme });
}

interface CodeToken {
  type: string;
  text?: string;
  lang?: string;
}

/**
 * Marked extension that syntax-highlights fenced code blocks with Shiki.
 *
 * Marked v15's renderer methods are synchronous — an `async` renderer is not
 * awaited and stringifies to "[object Promise]". So highlighting happens in the
 * async `walkTokens` hook (which IS awaited under `async: true`), keyed per
 * token in a WeakMap, and the sync `renderer.code` simply returns the
 * pre-computed HTML (falling back to escaped <pre><code> if Shiki is absent).
 */
export function markedShikiExtension(theme = "github-dark"): object {
  const highlighted = new WeakMap<object, string>();
  return {
    async: true,
    async walkTokens(token: CodeToken) {
      if (token.type === "code") {
        highlighted.set(
          token,
          await highlightCode(token.text || "", token.lang || "text", theme),
        );
      }
    },
    renderer: {
      code(token: CodeToken) {
        const cached = highlighted.get(token);
        if (cached !== undefined) {
          return cached;
        }
        const lang = token.lang || "text";
        return `<pre><code class="language-${escapeHtml(lang)}">${escapeHtml(token.text || "")}</code></pre>`;
      },
    },
  };
}
