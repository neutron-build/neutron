/**
 * Type stubs for CLI dependencies.
 * These replace the real package types when running tsc standalone
 * (without workspace node_modules installed).
 * At runtime the real packages are used.
 */

declare module 'cac' {
  interface Command {
    option(name: string, description: string, config?: { default?: unknown }): this
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    action(callback: (...args: any[]) => unknown): this
  }
  interface CAC {
    command(name: string, description: string): Command
    help(): void
    version(ver: string): void
    parse(): void
  }
  export function cac(name?: string): CAC
}

declare module 'execa' {
  interface ExecaOptions {
    stdio?: 'inherit' | 'pipe' | 'ignore'
    env?: Record<string, string | undefined>
    cwd?: string
  }
  export function execa(
    cmd: string,
    args?: string[],
    options?: ExecaOptions,
  ): Promise<{ stdout: string; stderr: string }> & { catch(fn: (e: unknown) => void): Promise<void> }
}

declare module 'picocolors' {
  interface Colors {
    cyan(text: string): string
    dim(text: string): string
    green(text: string): string
    red(text: string): string
    yellow(text: string): string
    bold(text: string): string
    blue(text: string): string
    white(text: string): string
    gray(text: string): string
  }
  const pc: Colors
  export = pc
}
