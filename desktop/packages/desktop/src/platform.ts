import { createContext } from 'preact';
import { useContext } from 'preact/hooks';

export interface PlatformAPI {
  platform: 'desktop' | 'web' | 'native';
  navigate: (path: string) => void;
  openFile: () => Promise<string[]>;
  notify: (title: string, body: string) => Promise<void>;
  openUrl: (url: string) => Promise<void>;
}

/** Desktop implementation of PlatformContext. */
const desktopPlatform: PlatformAPI = {
  platform: 'desktop',

  navigate: (path: string) => {
    window.location.hash = path;
  },

  openFile: async () => {
    const { invoke } = await import('@tauri-apps/api/core' as string);
    const result = await invoke('plugin:neutron-fs|show_open_dialog', {});
    return (result as string[]) ?? [];
  },

  notify: async (title: string, body: string) => {
    const { invoke } = await import('@tauri-apps/api/core' as string);
    await invoke('plugin:neutron-notifications|send_notification', {
      notification: { title, body },
    });
  },

  openUrl: async (url: string) => {
    const { invoke } = await import('@tauri-apps/api/core' as string);
    await invoke('plugin:neutron-shell|open_url', { url });
  },
};

/** Web fallback implementation. */
const webPlatform: PlatformAPI = {
  platform: 'web',
  navigate: (path: string) => {
    window.location.href = path;
  },
  openFile: async () => [],
  notify: async (title: string, body: string) => {
    if ('Notification' in window) {
      new Notification(title, { body });
    }
  },
  openUrl: async (url: string) => {
    window.open(url, '_blank');
  },
};

const isDesktop = typeof window !== 'undefined' && '__TAURI__' in window;

export const PlatformContext = createContext<PlatformAPI>(
  isDesktop ? desktopPlatform : webPlatform,
);

export function usePlatform(): PlatformAPI {
  return useContext(PlatformContext);
}
