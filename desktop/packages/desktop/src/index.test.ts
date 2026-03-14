import { describe, it, expect } from 'vitest';
import {
  neutronFetch,
  isDesktop,
  isDevMode,
  getBaseUrl,
  NeutronWindow,
  getCurrentWindow,
  createWindow,
  PlatformContext,
  usePlatform,
} from './index';

// ---------------------------------------------------------------------------
// Verify that index.ts re-exports everything correctly
// ---------------------------------------------------------------------------

describe('index.ts re-exports', () => {
  // -- bridge module --

  it('exports neutronFetch', () => {
    expect(typeof neutronFetch).toBe('function');
  });

  it('exports isDesktop', () => {
    expect(typeof isDesktop).toBe('function');
  });

  it('exports isDevMode', () => {
    expect(typeof isDevMode).toBe('function');
  });

  it('exports getBaseUrl', () => {
    expect(typeof getBaseUrl).toBe('function');
  });

  // -- window module --

  it('exports NeutronWindow class', () => {
    expect(typeof NeutronWindow).toBe('function');
    // It's a class, so it has a prototype
    expect(NeutronWindow.prototype).toBeDefined();
  });

  it('exports getCurrentWindow', () => {
    expect(typeof getCurrentWindow).toBe('function');
  });

  it('exports createWindow', () => {
    expect(typeof createWindow).toBe('function');
  });

  // -- platform module --

  it('exports PlatformContext', () => {
    expect(PlatformContext).toBeDefined();
  });

  it('exports usePlatform', () => {
    expect(typeof usePlatform).toBe('function');
  });
});

// ---------------------------------------------------------------------------
// Cross-module integration: bridge + window
// ---------------------------------------------------------------------------

describe('cross-module integration', () => {
  it('NeutronWindow instances created from getCurrentWindow work with bridge detection', () => {
    const win = getCurrentWindow();
    expect(win).toBeInstanceOf(NeutronWindow);
    // isDesktop should be callable alongside window APIs
    const desktop = isDesktop();
    expect(typeof desktop).toBe('boolean');
  });
});
