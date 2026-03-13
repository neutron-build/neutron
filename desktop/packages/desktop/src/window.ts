/**
 * Window management API for Neutron Desktop.
 */

declare const __TAURI__: {
  core: {
    invoke: (cmd: string, args?: Record<string, unknown>) => Promise<unknown>;
  };
};

export interface WindowOptions {
  title?: string;
  width?: number;
  height?: number;
  x?: number;
  y?: number;
  resizable?: boolean;
  decorations?: boolean;
  transparent?: boolean;
  fullscreen?: boolean;
}

export class NeutronWindow {
  constructor(public readonly label: string) {}

  async setTitle(title: string): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|set_title', { label: this.label, title });
  }

  async setSize(width: number, height: number): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|set_size', { label: this.label, width, height });
  }

  async setPosition(x: number, y: number): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|set_position', { label: this.label, x, y });
  }

  async center(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|center', { label: this.label });
  }

  async minimize(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|minimize', { label: this.label });
  }

  async maximize(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|maximize', { label: this.label });
  }

  async unmaximize(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|unmaximize', { label: this.label });
  }

  async toggleMaximize(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|toggle_maximize', { label: this.label });
  }

  async setFullscreen(fullscreen: boolean): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|set_fullscreen', { label: this.label, fullscreen });
  }

  async close(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|close', { label: this.label });
  }

  async hide(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|hide', { label: this.label });
  }

  async show(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|show', { label: this.label });
  }

  async focus(): Promise<void> {
    await __TAURI__.core.invoke('plugin:window|focus', { label: this.label });
  }
}

/** Get the current (main) window. */
export function getCurrentWindow(): NeutronWindow {
  return new NeutronWindow('main');
}

/** Create a new window. */
export async function createWindow(label: string, options?: WindowOptions): Promise<NeutronWindow> {
  await __TAURI__.core.invoke('plugin:window|create', { label, ...options });
  return new NeutronWindow(label);
}
