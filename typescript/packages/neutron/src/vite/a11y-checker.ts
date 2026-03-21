/**
 * Dev-mode accessibility checker.
 * Scans SSR HTML output for common a11y issues and logs warnings.
 * Regex-based — intentionally lightweight, no DOM parser.
 */

interface A11yWarning {
  message: string;
  line?: number;
}

function estimateLine(html: string, index: number): number {
  let line = 1;
  for (let i = 0; i < index && i < html.length; i++) {
    if (html[i] === "\n") line++;
  }
  return line;
}

export function checkAccessibility(html: string, routeId: string): void {
  const warnings: A11yWarning[] = [];

  // 1. <img> without alt attribute
  const imgPattern = /<img\b([^>]*)>/gi;
  let match: RegExpExecArray | null;
  while ((match = imgPattern.exec(html)) !== null) {
    const attrs = match[1];
    if (!/\balt\s*=/.test(attrs)) {
      warnings.push({
        message: "<img> without alt attribute",
        line: estimateLine(html, match.index),
      });
    }
  }

  // 2. <a> without text content or aria-label
  const anchorPattern = /<a\b([^>]*)>([\s\S]*?)<\/a>/gi;
  while ((match = anchorPattern.exec(html)) !== null) {
    const attrs = match[1];
    const content = match[2].replace(/<[^>]*>/g, "").trim();
    if (!content && !/\baria-label\s*=/.test(attrs)) {
      warnings.push({
        message: "<a> without text content or aria-label",
        line: estimateLine(html, match.index),
      });
    }
  }

  // 3. <button> without text content or aria-label
  const buttonPattern = /<button\b([^>]*)>([\s\S]*?)<\/button>/gi;
  while ((match = buttonPattern.exec(html)) !== null) {
    const attrs = match[1];
    const content = match[2].replace(/<[^>]*>/g, "").trim();
    if (!content && !/\baria-label\s*=/.test(attrs)) {
      warnings.push({
        message: "<button> without text content or aria-label",
        line: estimateLine(html, match.index),
      });
    }
  }

  // 4. <input> without associated <label> or aria-label
  const inputPattern = /<input\b([^>]*)\/?>/gi;
  while ((match = inputPattern.exec(html)) !== null) {
    const attrs = match[1];
    // Skip hidden inputs
    if (/\btype\s*=\s*["']hidden["']/i.test(attrs)) continue;
    if (/\baria-label\s*=/.test(attrs)) continue;

    // Check for id and matching <label for="...">
    const idMatch = /\bid\s*=\s*["']([^"']+)["']/.exec(attrs);
    if (idMatch) {
      const labelForPattern = new RegExp(
        `<label\\b[^>]*\\bfor\\s*=\\s*["']${escapeRegex(idMatch[1])}["']`,
        "i"
      );
      if (labelForPattern.test(html)) continue;
    }

    warnings.push({
      message: "<input> without <label> or aria-label",
      line: estimateLine(html, match.index),
    });
  }

  // 5. Missing <h1> on the page
  if (!/<h1[\s>]/i.test(html)) {
    warnings.push({ message: "page is missing an <h1> element" });
  }

  // 6. Heading levels that skip
  const headingPattern = /<h([1-6])[\s>]/gi;
  const headingLevels: number[] = [];
  while ((match = headingPattern.exec(html)) !== null) {
    headingLevels.push(parseInt(match[1], 10));
  }
  for (let i = 1; i < headingLevels.length; i++) {
    const prev = headingLevels[i - 1];
    const curr = headingLevels[i];
    if (curr > prev + 1) {
      warnings.push({
        message: `heading level skipped: h${prev} -> h${curr} (expected h${prev + 1})`,
      });
    }
  }

  // Log all warnings
  for (const w of warnings) {
    const loc = w.line != null ? ` (line ~${w.line})` : "";
    console.warn(`[neutron] a11y: ${w.message}${loc} [route: ${routeId}]`);
  }
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
