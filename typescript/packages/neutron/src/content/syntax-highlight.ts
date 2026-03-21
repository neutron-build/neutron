interface ShikiHighlighter {
  codeToHtml: (code: string, options: { lang: string; theme: string }) => string;
  getLoadedLanguages: () => string[];
  loadLanguage: (...langs: string[]) => Promise<void>;
}

let highlighterPromise: Promise<ShikiHighlighter | null> | null = null;

async function getHighlighter(theme: string): Promise<ShikiHighlighter | null> {
  if (!highlighterPromise) {
    // @ts-ignore -- shiki is an optional peer dependency
    highlighterPromise = import("shiki")
      .then(async (shiki: any) => {
        return shiki.createHighlighter({ themes: [theme], langs: [] }) as Promise<ShikiHighlighter>;
      })
      .catch(() => null);
  }
  return highlighterPromise;
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export async function highlightCode(
  code: string,
  lang: string,
  theme = "github-dark"
): Promise<string> {
  const highlighter = await getHighlighter(theme);
  if (!highlighter) {
    return `<pre><code class="language-${lang}">${escapeHtml(code)}</code></pre>`;
  }

  const loaded = highlighter.getLoadedLanguages();
  if (!loaded.includes(lang) && lang !== "text") {
    try {
      await highlighter.loadLanguage(lang as any);
    } catch {
      return `<pre><code class="language-${lang}">${escapeHtml(code)}</code></pre>`;
    }
  }

  return highlighter.codeToHtml(code, { lang, theme });
}

export function markedShikiExtension(theme = "github-dark"): object {
  return {
    async: true,
    renderer: {
      async code({ text, lang }: { text: string; lang?: string }) {
        return highlightCode(text, lang || "text", theme);
      },
    },
  };
}
