/**
 * Mock for @tauri-apps/api/core used in tests.
 * The actual invoke function behavior should be overridden per-test via vi.fn().
 */
export async function invoke(_cmd: string, _args?: Record<string, unknown>): Promise<unknown> {
  throw new Error(
    `Tauri invoke("${_cmd}") called without a mock. ` +
    'Use vi.doMock("@tauri-apps/api/core", ...) in your test.',
  );
}
