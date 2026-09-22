// ---------------------------------------------------------------------------
// @neutron-build/sql — dev SQL logging
// ---------------------------------------------------------------------------

export type Logger = (event: LogEvent) => void;

export interface LogEvent {
  sql: string;
  params: readonly unknown[];
  durationMs: number;
  error?: Error;
}

export type LoggerOption = boolean | Logger;

export function resolveLogger(option: LoggerOption | undefined): Logger | null {
  if (option === undefined || option === false) return null;
  if (option === true) {
    return (event) => {
      const suffix = event.error ? ` -- error: ${event.error.message}` : "";
      console.log(`[neutron-sql] (${event.durationMs.toFixed(1)}ms) ${event.sql}`);
      if (event.params.length > 0) console.log(`[neutron-sql]   params: ${JSON.stringify(event.params)}`);
      if (suffix) console.log(`[neutron-sql]${suffix.slice(1)}`);
    };
  }
  return option;
}
