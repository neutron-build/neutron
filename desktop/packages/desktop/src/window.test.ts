import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { NeutronWindow, getCurrentWindow, createWindow, type WindowOptions } from './window';

// ---------------------------------------------------------------------------
// Mock __TAURI__.core.invoke
// ---------------------------------------------------------------------------

let invokeMock: ReturnType<typeof vi.fn>;

function installTauriMock() {
  invokeMock = vi.fn().mockResolvedValue(undefined);
  (globalThis as any).__TAURI__ = {
    core: { invoke: invokeMock },
  };
}

function clearTauriMock() {
  delete (globalThis as any).__TAURI__;
}

// ---------------------------------------------------------------------------
// NeutronWindow
// ---------------------------------------------------------------------------

describe('NeutronWindow', () => {
  beforeEach(installTauriMock);
  afterEach(clearTauriMock);

  it('stores the label in a readonly property', () => {
    const win = new NeutronWindow('editor');
    expect(win.label).toBe('editor');
  });

  // -- setTitle --

  it('setTitle invokes plugin:window|set_title', async () => {
    const win = new NeutronWindow('main');
    await win.setTitle('My App');
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_title', {
      label: 'main',
      title: 'My App',
    });
  });

  it('setTitle with empty string', async () => {
    const win = new NeutronWindow('main');
    await win.setTitle('');
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_title', {
      label: 'main',
      title: '',
    });
  });

  it('setTitle with unicode characters', async () => {
    const win = new NeutronWindow('main');
    await win.setTitle('Neutron Desktop \u2014 v0.1');
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_title', {
      label: 'main',
      title: 'Neutron Desktop \u2014 v0.1',
    });
  });

  // -- setSize --

  it('setSize invokes plugin:window|set_size', async () => {
    const win = new NeutronWindow('main');
    await win.setSize(1280, 720);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_size', {
      label: 'main',
      width: 1280,
      height: 720,
    });
  });

  it('setSize with zero dimensions', async () => {
    const win = new NeutronWindow('main');
    await win.setSize(0, 0);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_size', {
      label: 'main',
      width: 0,
      height: 0,
    });
  });

  // -- setPosition --

  it('setPosition invokes plugin:window|set_position', async () => {
    const win = new NeutronWindow('main');
    await win.setPosition(100, 200);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_position', {
      label: 'main',
      x: 100,
      y: 200,
    });
  });

  it('setPosition with negative coordinates', async () => {
    const win = new NeutronWindow('secondary');
    await win.setPosition(-50, -100);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_position', {
      label: 'secondary',
      x: -50,
      y: -100,
    });
  });

  // -- center --

  it('center invokes plugin:window|center', async () => {
    const win = new NeutronWindow('main');
    await win.center();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|center', { label: 'main' });
  });

  // -- minimize --

  it('minimize invokes plugin:window|minimize', async () => {
    const win = new NeutronWindow('main');
    await win.minimize();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|minimize', { label: 'main' });
  });

  // -- maximize --

  it('maximize invokes plugin:window|maximize', async () => {
    const win = new NeutronWindow('main');
    await win.maximize();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|maximize', { label: 'main' });
  });

  // -- unmaximize --

  it('unmaximize invokes plugin:window|unmaximize', async () => {
    const win = new NeutronWindow('main');
    await win.unmaximize();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|unmaximize', { label: 'main' });
  });

  // -- toggleMaximize --

  it('toggleMaximize invokes plugin:window|toggle_maximize', async () => {
    const win = new NeutronWindow('main');
    await win.toggleMaximize();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|toggle_maximize', { label: 'main' });
  });

  // -- setFullscreen --

  it('setFullscreen(true) invokes with fullscreen: true', async () => {
    const win = new NeutronWindow('main');
    await win.setFullscreen(true);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_fullscreen', {
      label: 'main',
      fullscreen: true,
    });
  });

  it('setFullscreen(false) invokes with fullscreen: false', async () => {
    const win = new NeutronWindow('main');
    await win.setFullscreen(false);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|set_fullscreen', {
      label: 'main',
      fullscreen: false,
    });
  });

  // -- close --

  it('close invokes plugin:window|close', async () => {
    const win = new NeutronWindow('main');
    await win.close();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|close', { label: 'main' });
  });

  // -- hide --

  it('hide invokes plugin:window|hide', async () => {
    const win = new NeutronWindow('main');
    await win.hide();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|hide', { label: 'main' });
  });

  // -- show --

  it('show invokes plugin:window|show', async () => {
    const win = new NeutronWindow('main');
    await win.show();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|show', { label: 'main' });
  });

  // -- focus --

  it('focus invokes plugin:window|focus', async () => {
    const win = new NeutronWindow('main');
    await win.focus();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|focus', { label: 'main' });
  });

  // -- Error propagation --

  it('propagates Tauri invoke errors', async () => {
    invokeMock.mockRejectedValueOnce(new Error('permission denied'));
    const win = new NeutronWindow('main');
    await expect(win.setTitle('Nope')).rejects.toThrow('permission denied');
  });

  // -- Multiple windows --

  it('different NeutronWindow instances send their own label', async () => {
    const w1 = new NeutronWindow('editor');
    const w2 = new NeutronWindow('settings');
    await w1.minimize();
    await w2.maximize();
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|minimize', { label: 'editor' });
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|maximize', { label: 'settings' });
  });

  // -- Operation chaining --

  it('supports sequential operations on the same window', async () => {
    const win = new NeutronWindow('main');
    await win.setTitle('Step 1');
    await win.setSize(800, 600);
    await win.center();
    await win.show();
    await win.focus();
    expect(invokeMock).toHaveBeenCalledTimes(5);
  });
});

// ---------------------------------------------------------------------------
// getCurrentWindow()
// ---------------------------------------------------------------------------

describe('getCurrentWindow()', () => {
  beforeEach(installTauriMock);
  afterEach(clearTauriMock);

  it('returns a NeutronWindow with label "main"', () => {
    const win = getCurrentWindow();
    expect(win).toBeInstanceOf(NeutronWindow);
    expect(win.label).toBe('main');
  });

  it('returns a new instance on each call', () => {
    const a = getCurrentWindow();
    const b = getCurrentWindow();
    expect(a).not.toBe(b);
    expect(a.label).toBe(b.label);
  });
});

// ---------------------------------------------------------------------------
// createWindow()
// ---------------------------------------------------------------------------

describe('createWindow()', () => {
  beforeEach(installTauriMock);
  afterEach(clearTauriMock);

  it('invokes plugin:window|create with label only', async () => {
    const win = await createWindow('popup');
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|create', { label: 'popup' });
    expect(win).toBeInstanceOf(NeutronWindow);
    expect(win.label).toBe('popup');
  });

  it('invokes plugin:window|create with full options', async () => {
    const opts: WindowOptions = {
      title: 'Settings',
      width: 600,
      height: 400,
      x: 100,
      y: 200,
      resizable: true,
      decorations: true,
      transparent: false,
      fullscreen: false,
    };
    const win = await createWindow('settings', opts);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|create', {
      label: 'settings',
      ...opts,
    });
    expect(win.label).toBe('settings');
  });

  it('invokes plugin:window|create with partial options', async () => {
    const opts: WindowOptions = { title: 'Quick', width: 300, height: 200 };
    await createWindow('quick', opts);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|create', {
      label: 'quick',
      title: 'Quick',
      width: 300,
      height: 200,
    });
  });

  it('returns a usable NeutronWindow after creation', async () => {
    const win = await createWindow('child');
    await win.setTitle('Child Window');
    expect(invokeMock).toHaveBeenCalledTimes(2); // create + setTitle
    expect(invokeMock).toHaveBeenLastCalledWith('plugin:window|set_title', {
      label: 'child',
      title: 'Child Window',
    });
  });

  it('propagates creation errors', async () => {
    invokeMock.mockRejectedValueOnce(new Error('label already exists'));
    await expect(createWindow('duplicate')).rejects.toThrow('label already exists');
  });

  it('creates window with zero-size options', async () => {
    const opts: WindowOptions = { width: 0, height: 0 };
    const win = await createWindow('tiny', opts);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|create', {
      label: 'tiny',
      width: 0,
      height: 0,
    });
    expect(win.label).toBe('tiny');
  });

  it('creates window with transparent and no decorations', async () => {
    const opts: WindowOptions = { transparent: true, decorations: false };
    const win = await createWindow('overlay', opts);
    expect(invokeMock).toHaveBeenCalledWith('plugin:window|create', {
      label: 'overlay',
      transparent: true,
      decorations: false,
    });
    expect(win.label).toBe('overlay');
  });
});

// ---------------------------------------------------------------------------
// WindowOptions type (compile-time checks)
// ---------------------------------------------------------------------------

describe('WindowOptions type', () => {
  it('accepts all optional fields', () => {
    const opts: WindowOptions = {};
    expect(opts).toBeDefined();
  });

  it('accepts a fully specified options object', () => {
    const opts: WindowOptions = {
      title: 'Test',
      width: 800,
      height: 600,
      x: 0,
      y: 0,
      resizable: true,
      decorations: true,
      transparent: false,
      fullscreen: false,
    };
    expect(opts.title).toBe('Test');
    expect(opts.width).toBe(800);
    expect(opts.height).toBe(600);
    expect(opts.x).toBe(0);
    expect(opts.y).toBe(0);
    expect(opts.resizable).toBe(true);
    expect(opts.decorations).toBe(true);
    expect(opts.transparent).toBe(false);
    expect(opts.fullscreen).toBe(false);
  });
});
