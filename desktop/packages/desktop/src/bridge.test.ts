import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { isDesktop, isDevMode, getBaseUrl, neutronFetch, installFetchInterceptor } from './bridge';

// ---------------------------------------------------------------------------
// Helpers to simulate Tauri / dev-mode globals
// ---------------------------------------------------------------------------

function setTauri() {
  (window as any).__TAURI__ = {};
}

function clearTauri() {
  delete (window as any).__TAURI__;
}

function setDevMode(port?: number) {
  (window as any).__NEUTRON_DEV_MODE__ = true;
  if (port !== undefined) {
    (window as any).__NEUTRON_DEV_PORT__ = port;
  }
}

function clearDevMode() {
  delete (window as any).__NEUTRON_DEV_MODE__;
  delete (window as any).__NEUTRON_DEV_PORT__;
}

// ---------------------------------------------------------------------------
// isDesktop()
// ---------------------------------------------------------------------------

describe('isDesktop()', () => {
  afterEach(() => {
    clearTauri();
  });

  it('returns false when __TAURI__ is not on window', () => {
    clearTauri();
    expect(isDesktop()).toBe(false);
  });

  it('returns true when __TAURI__ is present', () => {
    setTauri();
    expect(isDesktop()).toBe(true);
  });

  it('returns true even when __TAURI__ is an empty object', () => {
    (window as any).__TAURI__ = {};
    expect(isDesktop()).toBe(true);
  });

  it('returns true when __TAURI__ is set to a truthy non-object value', () => {
    (window as any).__TAURI__ = 1;
    expect(isDesktop()).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// isDevMode()
// ---------------------------------------------------------------------------

describe('isDevMode()', () => {
  afterEach(() => {
    clearDevMode();
  });

  it('returns false when __NEUTRON_DEV_MODE__ is not set', () => {
    expect(isDevMode()).toBe(false);
  });

  it('returns true when __NEUTRON_DEV_MODE__ is true', () => {
    setDevMode();
    expect(isDevMode()).toBe(true);
  });

  it('returns false when __NEUTRON_DEV_MODE__ is a truthy non-boolean', () => {
    (window as any).__NEUTRON_DEV_MODE__ = 'yes';
    expect(isDevMode()).toBe(false);
  });

  it('returns false when __NEUTRON_DEV_MODE__ is false', () => {
    (window as any).__NEUTRON_DEV_MODE__ = false;
    expect(isDevMode()).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// getBaseUrl()
// ---------------------------------------------------------------------------

describe('getBaseUrl()', () => {
  afterEach(() => {
    clearTauri();
    clearDevMode();
  });

  it('returns empty string on the web (no Tauri, no dev mode)', () => {
    expect(getBaseUrl()).toBe('');
  });

  it('returns neutron://localhost in production desktop', () => {
    setTauri();
    expect(getBaseUrl()).toBe('neutron://localhost');
  });

  it('returns http://127.0.0.1:3001 in dev mode (default port)', () => {
    setTauri();
    setDevMode();
    expect(getBaseUrl()).toBe('http://127.0.0.1:3001');
  });

  it('returns http://127.0.0.1:<custom> when dev port is overridden', () => {
    setTauri();
    setDevMode(4444);
    expect(getBaseUrl()).toBe('http://127.0.0.1:4444');
  });

  it('dev mode takes priority over desktop detection', () => {
    setTauri();
    setDevMode();
    const url = getBaseUrl();
    expect(url).toContain('http://127.0.0.1');
    expect(url).not.toContain('neutron://');
  });

  it('dev mode works even without __TAURI__ (edge case)', () => {
    // isDevMode() only checks __NEUTRON_DEV_MODE__
    setDevMode();
    expect(getBaseUrl()).toBe('http://127.0.0.1:3001');
  });

  it('uses 3001 when __NEUTRON_DEV_PORT__ is 0 (falsy)', () => {
    setDevMode(0);
    // 0 is falsy, so `|| 3001` kicks in
    expect(getBaseUrl()).toBe('http://127.0.0.1:3001');
  });
});

// ---------------------------------------------------------------------------
// neutronFetch()
// ---------------------------------------------------------------------------

describe('neutronFetch()', () => {
  let fetchSpy: ReturnType<typeof vi.fn>;
  const fakeResponse = new Response('ok', { status: 200 });

  beforeEach(() => {
    fetchSpy = vi.fn().mockResolvedValue(fakeResponse);
    vi.stubGlobal('fetch', fetchSpy);
  });

  afterEach(() => {
    clearTauri();
    clearDevMode();
    vi.restoreAllMocks();
  });

  // -- Web (non-desktop) --

  it('passes through to native fetch on the web', async () => {
    clearTauri();
    const resp = await neutronFetch('/api/users');
    expect(fetchSpy).toHaveBeenCalledWith('/api/users', undefined);
    expect(resp).toBe(fakeResponse);
  });

  it('passes through with RequestInit on the web', async () => {
    clearTauri();
    const init: RequestInit = { method: 'POST', body: '{}' };
    await neutronFetch('/api/users', init);
    expect(fetchSpy).toHaveBeenCalledWith('/api/users', init);
  });

  // -- Desktop (production) --

  it('prefixes relative URLs with neutron://localhost on desktop', async () => {
    setTauri();
    await neutronFetch('/api/users');
    expect(fetchSpy).toHaveBeenCalledWith('neutron://localhost/api/users', undefined);
  });

  it('does not prefix absolute URLs on desktop', async () => {
    setTauri();
    await neutronFetch('https://example.com/api/data');
    expect(fetchSpy).toHaveBeenCalledWith('https://example.com/api/data', undefined);
  });

  it('handles URL object input on desktop', async () => {
    setTauri();
    const url = new URL('https://example.com/api/data');
    await neutronFetch(url);
    expect(fetchSpy).toHaveBeenCalledWith('https://example.com/api/data', undefined);
  });

  it('handles Request object input on desktop', async () => {
    setTauri();
    const req = new Request('https://example.com/api/data');
    await neutronFetch(req);
    expect(fetchSpy).toHaveBeenCalledWith('https://example.com/api/data', undefined);
  });

  it('forwards RequestInit on desktop', async () => {
    setTauri();
    const init: RequestInit = { method: 'DELETE', headers: { 'X-Token': 'abc' } };
    await neutronFetch('/api/users/1', init);
    expect(fetchSpy).toHaveBeenCalledWith('neutron://localhost/api/users/1', init);
  });

  // -- Desktop (dev mode) --

  it('routes through dev TCP server in dev mode', async () => {
    setTauri();
    setDevMode();
    await neutronFetch('/api/users');
    expect(fetchSpy).toHaveBeenCalledWith('http://127.0.0.1:3001/api/users', undefined);
  });

  it('uses custom dev port in dev mode', async () => {
    setTauri();
    setDevMode(5555);
    await neutronFetch('/api/users');
    expect(fetchSpy).toHaveBeenCalledWith('http://127.0.0.1:5555/api/users', undefined);
  });

  // -- Edge cases --

  it('does not double-prefix neutron:// URLs', async () => {
    setTauri();
    await neutronFetch('neutron://localhost/api/foo');
    expect(fetchSpy).toHaveBeenCalledWith('neutron://localhost/api/foo', undefined);
  });

  it('handles empty path string on desktop', async () => {
    setTauri();
    await neutronFetch('');
    // empty string does not start with '/', so no prefix
    expect(fetchSpy).toHaveBeenCalledWith('', undefined);
  });

  it('handles root path "/" on desktop', async () => {
    setTauri();
    await neutronFetch('/');
    expect(fetchSpy).toHaveBeenCalledWith('neutron://localhost/', undefined);
  });

  it('propagates fetch errors', async () => {
    setTauri();
    fetchSpy.mockRejectedValueOnce(new TypeError('Network error'));
    await expect(neutronFetch('/fail')).rejects.toThrow('Network error');
  });
});

// ---------------------------------------------------------------------------
// installFetchInterceptor()
// ---------------------------------------------------------------------------

describe('installFetchInterceptor()', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = vi.fn().mockResolvedValue(new Response('ok'));
    vi.stubGlobal('fetch', originalFetch);
  });

  afterEach(() => {
    clearTauri();
    clearDevMode();
    vi.restoreAllMocks();
  });

  it('is a no-op on the web', () => {
    clearTauri();
    const before = window.fetch;
    installFetchInterceptor();
    expect(window.fetch).toBe(before);
  });

  it('replaces window.fetch on desktop', () => {
    setTauri();
    const before = window.fetch;
    installFetchInterceptor();
    expect(window.fetch).not.toBe(before);
  });

  it('intercepted fetch rewrites relative URLs with neutron://', async () => {
    setTauri();
    installFetchInterceptor();
    await window.fetch('/api/tasks');
    expect(originalFetch).toHaveBeenCalledWith('neutron://localhost/api/tasks', undefined);
  });

  it('intercepted fetch passes absolute URLs untouched', async () => {
    setTauri();
    installFetchInterceptor();
    await window.fetch('https://cdn.example.com/asset.js');
    expect(originalFetch).toHaveBeenCalledWith('https://cdn.example.com/asset.js', undefined);
  });

  it('intercepted fetch forwards init options', async () => {
    setTauri();
    installFetchInterceptor();
    const init: RequestInit = { method: 'PUT', body: '{}' };
    await window.fetch('/api/tasks/1', init);
    expect(originalFetch).toHaveBeenCalledWith('neutron://localhost/api/tasks/1', init);
  });

  it('intercepted fetch uses dev TCP server in dev mode', async () => {
    setTauri();
    setDevMode(9090);
    installFetchInterceptor();
    await window.fetch('/api/data');
    expect(originalFetch).toHaveBeenCalledWith('http://127.0.0.1:9090/api/data', undefined);
  });

  it('intercepted fetch passes non-string inputs through unchanged', async () => {
    setTauri();
    installFetchInterceptor();
    const url = new URL('https://example.com/data');
    await window.fetch(url);
    expect(originalFetch).toHaveBeenCalledWith(url, undefined);
  });
});
