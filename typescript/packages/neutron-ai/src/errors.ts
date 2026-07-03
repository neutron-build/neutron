/** RFC 7807 Problem Details, per FRAMEWORK_CONTRACT.md section 2. */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
}

const ERROR_TYPE_BASE = "https://neutron.dev/errors/";

const STATUS_CODES: Record<number, { suffix: string; title: string }> = {
  400: { suffix: "bad-request", title: "Bad Request" },
  401: { suffix: "unauthorized", title: "Unauthorized" },
  403: { suffix: "forbidden", title: "Forbidden" },
  404: { suffix: "not-found", title: "Not Found" },
  409: { suffix: "conflict", title: "Conflict" },
  422: { suffix: "validation", title: "Validation Failed" },
  429: { suffix: "rate-limited", title: "Rate Limited" },
  500: { suffix: "internal", title: "Internal Server Error" },
};

/**
 * Build a problem-details object from an HTTP status. Unlisted statuses
 * normalize to 500 (server side) or 400 (client side) so consumers only
 * ever see the standard error codes from the framework contract.
 */
export function problemFromStatus(status: number, detail: string, instance?: string): ProblemDetails {
  const normalized = STATUS_CODES[status] ? status : status >= 500 ? 500 : 400;
  const { suffix, title } = STATUS_CODES[normalized]!;
  const problem: ProblemDetails = { type: ERROR_TYPE_BASE + suffix, title, status: normalized, detail };
  if (instance !== undefined) {
    problem.instance = instance;
  }
  return problem;
}

export class AIError extends Error {
  readonly problem: ProblemDetails;
  readonly provider?: string;

  constructor(problem: ProblemDetails, options?: { provider?: string; cause?: unknown }) {
    super(problem.detail, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "AIError";
    this.problem = problem;
    if (options?.provider !== undefined) {
      this.provider = options.provider;
    }
  }
}
