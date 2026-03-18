/**
 * Content Security Policy (CSP) Plugin
 * Inspired by Astro 6's hash-based CSP approach
 *
 * Automatically generates CSP headers with SHA-256 hashes of inline scripts/styles
 * Works on static hosts without server-side nonce generation
 */

import crypto from 'node:crypto';
import type { Plugin } from 'vite';

export interface CspDirectives {
  'default-src'?: string[];
  'script-src'?: string[];
  'style-src'?: string[];
  'img-src'?: string[];
  'font-src'?: string[];
  'connect-src'?: string[];
  'frame-src'?: string[];
  'object-src'?: string[];
  'media-src'?: string[];
  'worker-src'?: string[];
  'manifest-src'?: string[];
  'base-uri'?: string[];
  'form-action'?: string[];
  'frame-ancestors'?: string[];
  'upgrade-insecure-requests'?: boolean;
  'block-all-mixed-content'?: boolean;
}

export interface CspConfig {
  /**
   * Enable CSP generation
   */
  enabled: boolean;

  /**
   * CSP directives
   */
  directives: CspDirectives;

  /**
   * Report violations to this URL
   */
  reportUri?: string;

  /**
   * Use report-only mode (doesn't block, only reports)
   */
  reportOnly?: boolean;

  /**
   * Use nonce-based CSP instead of hash-based CSP
   *
   * SECURITY: Nonces are cryptographically random tokens generated per-request
   * that are more secure than hashes for dynamic content. However, they require
   * server-side generation on each request, making them incompatible with static
   * hosting.
   *
   * When enabled:
   * - A random nonce is generated for each request
   * - The nonce is added to script-src and style-src directives
   * - Inline scripts/styles are injected with nonce attributes
   *
   * @default false (uses hash-based CSP)
   */
  useNonce?: boolean;
}

/**
 * Extracts inline scripts from HTML and calculates SHA-256 hashes
 * SECURITY: Uses string splitting instead of complex regex to avoid ReDoS
 */
function extractScriptHashes(html: string): Set<string> {
  const hashes = new Set<string>();

  // Find all script tags using safe string operations
  let pos = 0;
  while (pos < html.length) {
    // Find opening tag
    const openStart = html.indexOf('<script', pos);
    if (openStart === -1) break;

    const openEnd = html.indexOf('>', openStart);
    if (openEnd === -1) break;

    // Check if it has src attribute (skip external scripts)
    const tagContent = html.substring(openStart, openEnd + 1);
    if (tagContent.includes(' src=') || tagContent.includes(' src ')) {
      pos = openEnd + 1;
      continue;
    }

    // Find closing tag
    const closeStart = html.indexOf('</script>', openEnd);
    if (closeStart === -1) break;

    // Extract content between tags
    const content = html.substring(openEnd + 1, closeStart).trim();
    if (content) {
      const hash = crypto.createHash('sha256').update(content, 'utf8').digest('base64');
      hashes.add(`'sha256-${hash}'`);
    }

    pos = closeStart + 9; // '</script>'.length
  }

  return hashes;
}

/**
 * Extracts inline styles from HTML and calculates SHA-256 hashes
 * SECURITY: Uses string splitting instead of complex regex to avoid ReDoS
 */
function extractStyleHashes(html: string): Set<string> {
  const hashes = new Set<string>();

  // Find all style tags using safe string operations
  let pos = 0;
  while (pos < html.length) {
    // Find opening tag
    const openStart = html.indexOf('<style', pos);
    if (openStart === -1) break;

    const openEnd = html.indexOf('>', openStart);
    if (openEnd === -1) break;

    // Find closing tag
    const closeStart = html.indexOf('</style>', openEnd);
    if (closeStart === -1) break;

    // Extract content between tags
    const content = html.substring(openEnd + 1, closeStart).trim();
    if (content) {
      const hash = crypto.createHash('sha256').update(content, 'utf8').digest('base64');
      hashes.add(`'sha256-${hash}'`);
    }

    pos = closeStart + 8; // '</style>'.length
  }

  // Also handle style attributes using a simple, safe regex
  const styleAttrRegex = /style="([^"]*)"/gi;
  let match;
  while ((match = styleAttrRegex.exec(html)) !== null) {
    const content = match[1].trim();
    if (content) {
      const hash = crypto.createHash('sha256').update(content, 'utf8').digest('base64');
      hashes.add(`'sha256-${hash}'`);
    }
  }

  return hashes;
}

/**
 * Builds CSP header string from directives and hashes
 */
function buildCspHeader(
  directives: CspDirectives,
  scriptHashes: Set<string>,
  styleHashes: Set<string>,
  reportUri?: string,
  nonce?: string
): string {
  const finalDirectives = { ...directives };

  if (nonce) {
    // Add nonce to script-src
    finalDirectives['script-src'] = [
      ...(finalDirectives['script-src'] || ["'self'"]),
      `'nonce-${nonce}'`,
    ];

    // Add nonce to style-src
    finalDirectives['style-src'] = [
      ...(finalDirectives['style-src'] || ["'self'"]),
      `'nonce-${nonce}'`,
    ];
  } else {
    // Add script hashes (hash-based CSP)
    if (scriptHashes.size > 0) {
      finalDirectives['script-src'] = [
        ...(finalDirectives['script-src'] || ["'self'"]),
        ...Array.from(scriptHashes),
      ];
    }

    // Add style hashes
    if (styleHashes.size > 0) {
      finalDirectives['style-src'] = [
        ...(finalDirectives['style-src'] || ["'self'"]),
        ...Array.from(styleHashes),
      ];
    }
  }

  // Build header string
  const parts: string[] = [];

  for (const [key, value] of Object.entries(finalDirectives)) {
    if (value === true) {
      parts.push(key);
    } else if (Array.isArray(value)) {
      parts.push(`${key} ${value.join(' ')}`);
    }
  }

  if (reportUri) {
    parts.push(`report-uri ${reportUri}`);
  }

  return parts.join('; ');
}

/**
 * Generates a cryptographically random nonce
 *
 * SECURITY: Uses crypto.randomBytes for cryptographic randomness.
 * The nonce is base64-encoded and suitable for CSP nonce values.
 */
function generateNonce(): string {
  const bytes = crypto.randomBytes(16);
  return bytes.toString('base64');
}

/**
 * Injects nonce attribute into inline scripts and styles
 *
 * SECURITY: Adds nonce="..." to all inline <script> and <style> tags
 * so they will be allowed by the nonce-based CSP policy.
 */
function injectNonce(html: string, nonce: string): string {
  let result = html;

  // Inject nonce into inline scripts
  result = result.replace(
    /<script(?![^>]*\ssrc=)([^>]*)>/gi,
    (match, attrs) => {
      // Skip if already has nonce
      if (attrs.includes('nonce=')) {
        return match;
      }
      return `<script nonce="${nonce}"${attrs}>`;
    }
  );

  // Inject nonce into inline styles
  result = result.replace(
    /<style([^>]*)>/gi,
    (match, attrs) => {
      // Skip if already has nonce
      if (attrs.includes('nonce=')) {
        return match;
      }
      return `<style nonce="${nonce}"${attrs}>`;
    }
  );

  return result;
}

/**
 * Vite plugin for CSP header generation
 */
export function cspPlugin(config: CspConfig): Plugin {
  if (!config.enabled) {
    return {
      name: 'neutron:csp-disabled',
    };
  }

  return {
    name: 'neutron:csp',
    transformIndexHtml: {
      order: 'post',
      handler(html: string) {
        let finalHtml = html;
        let nonce: string | undefined;

        // Generate nonce if enabled
        if (config.useNonce) {
          nonce = generateNonce();
          // Inject nonce into inline scripts and styles
          finalHtml = injectNonce(html, nonce);
        }

        // Extract hashes from inline scripts and styles (for hash-based CSP)
        const scriptHashes = config.useNonce ? new Set<string>() : extractScriptHashes(html);
        const styleHashes = config.useNonce ? new Set<string>() : extractStyleHashes(html);

        // Build CSP header
        const cspHeader = buildCspHeader(
          config.directives,
          scriptHashes,
          styleHashes,
          config.reportUri,
          nonce
        );

        // Inject as meta tag
        const headerName = config.reportOnly
          ? 'Content-Security-Policy-Report-Only'
          : 'Content-Security-Policy';

        const metaTag = `<meta http-equiv="${headerName}" content="${cspHeader}">`;

        // Insert after <head> tag
        return finalHtml.replace(/<head>/, `<head>\n  ${metaTag}`);
      },
    },
  };
}

/**
 * Default CSP configuration (strict)
 */
export const defaultCspConfig: CspDirectives = {
  'default-src': ["'self'"],
  'script-src': ["'self'"],
  'style-src': ["'self'"],
  'img-src': ["'self'", 'data:', 'https:'],
  'font-src': ["'self'", 'data:'],
  'connect-src': ["'self'"],
  'frame-src': ["'none'"],
  'object-src': ["'none'"],
  'base-uri': ["'self'"],
  'form-action': ["'self'"],
  'upgrade-insecure-requests': true,
};
