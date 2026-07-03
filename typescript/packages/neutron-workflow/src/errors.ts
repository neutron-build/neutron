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
  404: { suffix: "not-found", title: "Not Found" },
  409: { suffix: "conflict", title: "Conflict" },
  422: { suffix: "validation", title: "Validation Failed" },
  500: { suffix: "internal", title: "Internal Server Error" },
};

export function problemFromStatus(status: number, detail: string): ProblemDetails {
  const normalized = STATUS_CODES[status] ? status : status >= 500 ? 500 : 400;
  const { suffix, title } = STATUS_CODES[normalized]!;
  return { type: ERROR_TYPE_BASE + suffix, title, status: normalized, detail };
}

export class WorkflowError extends Error {
  readonly problem: ProblemDetails;

  constructor(problem: ProblemDetails, options?: { cause?: unknown }) {
    super(problem.detail, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "WorkflowError";
    this.problem = problem;
  }
}

/**
 * The workflow function diverged from its event log — renamed, reordered,
 * added, or removed a deterministic operation between deploys. The run
 * cannot safely continue; the log names exactly what mismatched.
 */
export class NondeterminismError extends WorkflowError {
  constructor(detail: string) {
    super({
      type: ERROR_TYPE_BASE + "workflow-nondeterminism",
      title: "Workflow Nondeterminism",
      status: 409,
      detail,
    });
    this.name = "NondeterminismError";
  }
}
