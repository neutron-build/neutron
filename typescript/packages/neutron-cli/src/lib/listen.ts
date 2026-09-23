export interface ListenAddress {
  port: number;
  /** Undefined = the caller's own default (e.g. Vite's localhost in dev). */
  host: string | undefined;
  /** True when the port came from a flag, env var, or config — not the default. */
  portExplicit: boolean;
}

export interface ResolveListenAddressInput {
  /** Command arguments after the command name (`--port 4000 --host ::`). */
  argv: readonly string[];
  env: Readonly<Record<string, string | undefined>>;
  config?: { port?: number; host?: string };
  defaultPort?: number;
  defaultHost?: string;
}

/**
 * Port/host precedence for `dev` and `start` (FRAMEWORK_CONTRACT.md §6):
 * CLI flag > NEUTRON_PORT / NEUTRON_HOST > neutron.config `server` > default.
 * An invalid port from a flag or the environment throws instead of silently
 * falling back.
 */
export function resolveListenAddress(input: ResolveListenAddressInput): ListenAddress {
  let flagPort: string | undefined;
  let flagHost: string | undefined;
  const { argv } = input;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--port" && argv[i + 1] !== undefined) {
      flagPort = argv[++i];
    } else if (arg.startsWith("--port=")) {
      flagPort = arg.slice("--port=".length);
    } else if (arg === "--host" && argv[i + 1] !== undefined) {
      flagHost = argv[++i];
    } else if (arg.startsWith("--host=")) {
      flagHost = arg.slice("--host=".length);
    }
  }

  const envPort = nonEmpty(input.env.NEUTRON_PORT);
  const envHost = nonEmpty(input.env.NEUTRON_HOST);
  const configPort = input.config?.port;
  const configHost = nonEmpty(input.config?.host);

  let port: number;
  let portExplicit = true;
  if (flagPort !== undefined) {
    port = parsePort(flagPort, "--port");
  } else if (envPort !== undefined) {
    port = parsePort(envPort, "NEUTRON_PORT");
  } else if (configPort !== undefined) {
    port = configPort;
  } else {
    port = input.defaultPort ?? 3000;
    portExplicit = false;
  }

  const host = nonEmpty(flagHost) ?? envHost ?? configHost ?? input.defaultHost;
  return { port, host, portExplicit };
}

export function parsePort(value: string, source: string): number {
  const trimmed = value.trim();
  const port = /^\d+$/.test(trimmed) ? Number(trimmed) : NaN;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(
      `Invalid ${source} "${value}": expected an integer port between 1 and 65535.`
    );
  }
  return port;
}

function nonEmpty(value: string | undefined): string | undefined {
  return value === undefined || value.trim() === "" ? undefined : value;
}
