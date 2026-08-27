import * as os from "node:os";

export const DEFAULT_PREVIEW_HOST = "127.0.0.1";

export interface PreviewArgs {
  host?: string;
}

export function parsePreviewArgs(argv: string[]): PreviewArgs {
  let host: string | undefined;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--host" && argv[i + 1]) {
      host = argv[i + 1];
      i++;
    } else if (arg.startsWith("--host=")) {
      host = arg.split("=")[1];
    }
  }

  return { host };
}

export function resolvePreviewHost(
  cliHost: string | undefined,
  configHost: string | undefined
): string {
  return cliHost || configHost || DEFAULT_PREVIEW_HOST;
}

export function isLoopbackHost(host: string): boolean {
  return host === "localhost" || host === "::1" || host.startsWith("127.");
}

export function resolveNetworkHost(host: string): string | null {
  if (isLoopbackHost(host)) {
    return null;
  }
  if (host === "0.0.0.0" || host === "::") {
    return resolveLanAddress();
  }
  return host;
}

function resolveLanAddress(): string | null {
  for (const addresses of Object.values(os.networkInterfaces())) {
    for (const address of addresses ?? []) {
      if (String(address.family) === "IPv4" && !address.internal) {
        return address.address;
      }
    }
  }
  return null;
}
