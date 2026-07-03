export interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  // A caller may never await this promise (e.g. only consuming textStream);
  // register a no-op handler so its rejection is never reported unhandled.
  promise.catch(() => {});
  return { promise, resolve, reject };
}
