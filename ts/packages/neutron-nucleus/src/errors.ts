// ---------------------------------------------------------------------------
// Nucleus client — error hierarchy
// ---------------------------------------------------------------------------

export interface NucleusErrorOptions {
  cause?: Error;
  meta?: Record<string, unknown>;
}

/** Base error for all Nucleus operations. */
export class NucleusError extends Error {
  readonly code: string;
  readonly meta?: Record<string, unknown>;

  constructor(code: string, message: string, options?: NucleusErrorOptions) {
    super(message, { cause: options?.cause });
    this.name = 'NucleusError';
    this.code = code;
    this.meta = options?.meta;
  }
}

/** Thrown when a connection cannot be established or is lost. */
export class NucleusConnectionError extends NucleusError {
  constructor(message: string, options?: NucleusErrorOptions) {
    super('CONNECTION_ERROR', message, options);
    this.name = 'NucleusConnectionError';
  }
}

/** Thrown when a SQL query or command fails on the server. */
export class NucleusQueryError extends NucleusError {
  constructor(message: string, options?: NucleusErrorOptions) {
    super('QUERY_ERROR', message, options);
    this.name = 'NucleusQueryError';
  }
}

/** Thrown when an expected row or resource is not found. */
export class NucleusNotFoundError extends NucleusError {
  constructor(message: string, options?: NucleusErrorOptions) {
    super('NOT_FOUND', message, options);
    this.name = 'NucleusNotFoundError';
  }
}

/** Thrown on unique-constraint violations or optimistic-lock conflicts. */
export class NucleusConflictError extends NucleusError {
  constructor(message: string, options?: NucleusErrorOptions) {
    super('CONFLICT', message, options);
    this.name = 'NucleusConflictError';
  }
}

/** Thrown when a transaction fails to commit or is aborted. */
export class NucleusTransactionError extends NucleusError {
  constructor(message: string, options?: NucleusErrorOptions) {
    super('TRANSACTION_ERROR', message, options);
    this.name = 'NucleusTransactionError';
  }
}

/** Thrown when a Nucleus-only feature is used against plain PostgreSQL. */
export class NucleusFeatureError extends NucleusError {
  constructor(feature: string, options?: NucleusErrorOptions) {
    super(
      'FEATURE_UNAVAILABLE',
      `${feature} requires Nucleus database, but connected to plain PostgreSQL`,
      options,
    );
    this.name = 'NucleusFeatureError';
  }
}

/** Thrown when the server rejects the request for authentication reasons. */
export class NucleusAuthError extends NucleusError {
  constructor(message: string, options?: NucleusErrorOptions) {
    super('AUTH_ERROR', message, options);
    this.name = 'NucleusAuthError';
  }
}
