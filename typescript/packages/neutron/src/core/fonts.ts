/**
 * Fonts API - Unified Font Loading with Optimization
 * Inspired by Astro 6's experimental fonts API
 *
 * Automatically generates preload links and fallback fonts
 */

export type FontSource = 'google' | 'bunny' | 'fontsource' | 'local';

export interface FontConfig {
  /**
   * Google Fonts to load
   * Format: "Family:weight1,weight2" or just "Family"
   *
   * @example ["Inter:400,600,700", "Fira Code"]
   */
  google?: string[];

  /**
   * Bunny Fonts to load (privacy-friendly Google Fonts alternative)
   * Same format as google
   */
  bunny?: string[];

  /**
   * Local font files
   * @example [{ family: "Custom", src: "/fonts/custom.woff2", weight: 400 }]
   */
  local?: Array<{
    family: string;
    src: string;
    weight?: number | string;
    style?: string;
    display?: string;
  }>;

  /**
   * Fonts to preload (improves LCP)
   * Should list critical fonts only
   *
   * @example ["Inter-400", "Inter-700"]
   */
  preload?: string[];

  /**
   * Fallback font families
   * Used to reduce CLS while custom fonts load
   *
   * @example { "Inter": "system-ui", "Fira Code": "monospace" }
   */
  fallback?: Record<string, string>;

  /**
   * Font display strategy
   * @default "swap"
   */
  display?: 'auto' | 'block' | 'swap' | 'fallback' | 'optional';
}

interface FontPreload {
  family: string;
  weight: string;
  url: string;
}

interface FontStylesheet {
  source: FontSource;
  url: string;
}

/**
 * Parses Google Fonts format into family and weights
 */
function parseGoogleFont(fontString: string): { family: string; weights: string[] } {
  const [family, weightsStr] = fontString.split(':');
  const weights = weightsStr ? weightsStr.split(',') : ['400'];
  return { family: family.trim(), weights };
}

/**
 * Builds Google Fonts stylesheet URL
 */
function buildGoogleFontsUrl(fonts: string[], display: string = 'swap'): string {
  const families = fonts.map(font => {
    const { family, weights } = parseGoogleFont(font);
    return `family=${encodeURIComponent(family)}:wght@${weights.join(';')}`;
  }).join('&');

  return `https://fonts.googleapis.com/css2?${families}&display=${display}`;
}

/**
 * Builds Bunny Fonts stylesheet URL
 */
function buildBunnyFontsUrl(fonts: string[], display: string = 'swap'): string {
  const families = fonts.map(font => {
    const { family, weights } = parseGoogleFont(font);
    return `family=${encodeURIComponent(family)}:wght@${weights.join(';')}`;
  }).join('&');

  return `https://fonts.bunny.net/css?${families}&display=${display}`;
}

/**
 * Splits a preload spec ("Family-Weight", e.g. "Inter-400") into family and
 * weight. Splits on the LAST dash so hyphenated family names survive.
 */
function splitPreloadSpec(preloadSpec: string): { family: string; weight: string } {
  const dash = preloadSpec.lastIndexOf('-');
  if (dash === -1) {
    return { family: preloadSpec, weight: '400' };
  }
  return { family: preloadSpec.slice(0, dash), weight: preloadSpec.slice(dash + 1) || '400' };
}

/**
 * Generates font preload URLs
 *
 * Only local fonts are preloaded: their file URLs are known from the config.
 * Google/Bunny file URLs carry content hashes that are only knowable by
 * fetching the provider CSS, so fabricating them would 404 every preload.
 */
function generatePreloads(config: FontConfig): FontPreload[] {
  const preloads: FontPreload[] = [];

  if (!config.preload) return preloads;

  for (const preloadSpec of config.preload) {
    const { family, weight } = splitPreloadSpec(preloadSpec);

    const local = config.local?.find((f) => f.family === family);
    if (local) {
      preloads.push({
        family,
        weight,
        url: local.src,
      });
    }
  }

  return preloads;
}

/**
 * Generates fallback font CSS
 * Reduces CLS by matching metrics of system fonts to custom fonts
 */
function generateFallbackCSS(fallbacks: Record<string, string>): string {
  const rules: string[] = [];

  for (const [customFont, systemFont] of Object.entries(fallbacks)) {
    rules.push(`
@font-face {
  font-family: '${customFont} Fallback';
  src: local('${systemFont}');
  size-adjust: 100%;
  ascent-override: 90%;
  descent-override: 22%;
  line-gap-override: 0%;
}
    `.trim());
  }

  return rules.join('\n\n');
}

/**
 * Generates font stylesheet links
 */
function generateStylesheets(config: FontConfig): FontStylesheet[] {
  const stylesheets: FontStylesheet[] = [];
  const display = config.display || 'swap';

  if (config.google && config.google.length > 0) {
    stylesheets.push({
      source: 'google',
      url: buildGoogleFontsUrl(config.google, display),
    });
  }

  if (config.bunny && config.bunny.length > 0) {
    stylesheets.push({
      source: 'bunny',
      url: buildBunnyFontsUrl(config.bunny, display),
    });
  }

  return stylesheets;
}

/**
 * Generates HTML for font loading
 * Returns preload links, stylesheet links, and inline fallback CSS
 */
export function generateFontHTML(config: FontConfig): string {
  const parts: string[] = [];

  // 1. Preload links (for critical fonts)
  const preloads = generatePreloads(config);
  for (const preload of preloads) {
    parts.push(
      `<link rel="preload" href="${preload.url}" as="font" type="font/woff2" crossorigin>`
    );
  }

  // 2. Stylesheet links
  const stylesheets = generateStylesheets(config);
  for (const stylesheet of stylesheets) {
    parts.push(
      `<link rel="stylesheet" href="${stylesheet.url}">`
    );
  }

  // 3. Local fonts
  if (config.local && config.local.length > 0) {
    const localFontFaces = config.local.map(font => `
@font-face {
  font-family: '${font.family}';
  src: url('${font.src}') format('woff2');
  font-weight: ${font.weight || 400};
  font-style: ${font.style || 'normal'};
  font-display: ${font.display || config.display || 'swap'};
}
    `.trim()).join('\n\n');

    parts.push(`<style>\n${localFontFaces}\n</style>`);
  }

  // 4. Fallback fonts (reduce CLS)
  if (config.fallback && Object.keys(config.fallback).length > 0) {
    const fallbackCSS = generateFallbackCSS(config.fallback);
    parts.push(`<style>\n${fallbackCSS}\n</style>`);
  }

  return parts.join('\n');
}

/**
 * Validates font configuration
 */
export function validateFontConfig(config: FontConfig): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  // Check for empty config
  if (!config.google && !config.bunny && !config.local) {
    errors.push('Font config must specify at least one font source (google, bunny, or local)');
  }

  // Validate preload references
  if (config.preload) {
    const allFamilies = [
      ...(config.google || []).map(f => parseGoogleFont(f).family),
      ...(config.bunny || []).map(f => parseGoogleFont(f).family),
      ...(config.local || []).map(f => f.family),
    ];

    for (const preload of config.preload) {
      const { family } = splitPreloadSpec(preload);
      if (!allFamilies.includes(family)) {
        errors.push(`Preload references unknown font family: ${family}`);
      }
    }
  }

  return { valid: errors.length === 0, errors };
}
