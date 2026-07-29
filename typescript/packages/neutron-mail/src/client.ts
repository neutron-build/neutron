/**
 * HTTP client for the neutron-mail engine.
 *
 * The engine mirrors a user's existing mailbox — Gmail, Microsoft 365,
 * Fastmail, any IMAP host — and serves it over this API. Nothing here speaks
 * a mail protocol; that is the engine's job, and the reason both an inbox UI
 * and a chat tool can sit on the same surface.
 */

export interface Address {
  name?: string;
  email: string;
}

export interface Keywords {
  seen: boolean;
  flagged: boolean;
  draft: boolean;
  answered: boolean;
  custom?: string[];
}

export interface Envelope {
  ID: string;
  ThreadID: string;
  MailboxIDs: string[];
  From: Address[];
  To: Address[];
  Cc: Address[];
  Bcc: Address[];
  ReplyTo: Address[];
  Subject: string;
  SentAt: string;
  ReceivedAt: string;
  Keywords: Keywords;
  HasAttachment: boolean;
  Size: number;
  Preview: string;
  MessageIDHeader: string;
  Fingerprint: string;
}

export interface BodyPart {
  part_id: string;
  type: string;
  charset?: string;
  disposition?: string;
  filename?: string;
  size: number;
  content_id?: string;
}

export interface Body {
  MessageID: string;
  Text: string;
  HTML: string;
  Parts: BodyPart[];
}

export interface Mailbox {
  ID: string;
  Name: string;
  Role: string;
  ParentID: string;
  Native: string;
}

export interface Account {
  ID: string;
  Provider: string;
  Email: string;
  Name: string;
  NeedsReauth: boolean;
}

export interface SyncReport {
  Account: string;
  Mailbox: string;
  Created: number;
  Updated: number;
  Deleted: number;
  Upgraded: number;
  Pages: number;
  Reset: boolean;
}

export type OperationKind = "add_keyword" | "remove_keyword" | "move" | "delete";

export interface MailClientOptions {
  /** Base URL of the neutron-mail service, e.g. http://localhost:8090 */
  baseUrl: string;
  /** Bearer token, if the service sits behind auth. */
  token?: string;
  /** Request timeout in milliseconds. Defaults to 30s. */
  timeoutMs?: number;
  fetch?: typeof globalThis.fetch;
}

/**
 * Error carrying the RFC 7807 problem document the service returned.
 *
 * `needsReauth` is surfaced separately because it is the one failure a caller
 * must handle differently: no retry will fix it, and the user has to
 * reconnect the account.
 */
export class MailError extends Error {
  readonly status: number;
  readonly title: string;
  readonly detail: string;

  constructor(status: number, title: string, detail: string) {
    super(detail ? `${title}: ${detail}` : title);
    this.name = "MailError";
    this.status = status;
    this.title = title;
    this.detail = detail;
  }

  /** The account's credential was rejected permanently. */
  get needsReauth(): boolean {
    return this.status === 401;
  }

  /** The provider is throttling; the same call may succeed later. */
  get rateLimited(): boolean {
    return this.status === 429;
  }

  get notFound(): boolean {
    return this.status === 404;
  }
}

export interface MailClient {
  accounts(): Promise<Account[]>;
  mailboxes(account: string): Promise<Mailbox[]>;
  search(account: string, query: string, limit?: number): Promise<Envelope[]>;
  thread(account: string, threadId: string): Promise<Envelope[]>;
  message(account: string, messageId: string): Promise<Envelope>;
  body(account: string, messageId: string): Promise<Body>;
  sync(account: string): Promise<SyncReport[]>;
  apply(
    account: string,
    op: { kind: OperationKind; ids: string[]; keyword?: string; target?: string },
  ): Promise<number>;
  health(): Promise<{ status: string; nucleus: boolean; version: string }>;
}

export function createMailClient(options: MailClientOptions): MailClient {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const doFetch = options.fetch ?? globalThis.fetch;
  const timeoutMs = options.timeoutMs ?? 30_000;

  if (typeof doFetch !== "function") {
    throw new Error("@neutron-build/mail: no fetch implementation available");
  }

  async function request<T>(path: string, init?: RequestInit): Promise<T> {
    // Every request is bounded. A chat tool calling this sits inside its own
    // request budget, so an unbounded fetch would hang the whole turn.
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const headers: Record<string, string> = {
        accept: "application/json",
        ...(init?.headers as Record<string, string> | undefined),
      };
      if (options.token) headers.authorization = `Bearer ${options.token}`;
      if (init?.body) headers["content-type"] = "application/json";

      const response = await doFetch(`${baseUrl}${path}`, {
        ...init,
        headers,
        signal: controller.signal,
      });

      if (!response.ok) throw await toMailError(response);
      return (await response.json()) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    async accounts() {
      const r = await request<{ accounts: Account[] | null }>("/v1/accounts");
      return r.accounts ?? [];
    },

    async mailboxes(account) {
      const r = await request<{ mailboxes: Mailbox[] | null }>(
        `/v1/accounts/${encodeURIComponent(account)}/mailboxes`,
      );
      return r.mailboxes ?? [];
    },

    async search(account, query, limit) {
      const params = new URLSearchParams({ q: query });
      if (limit !== undefined) params.set("limit", String(limit));
      const r = await request<{ messages: Envelope[] | null }>(
        `/v1/accounts/${encodeURIComponent(account)}/search?${params}`,
      );
      return r.messages ?? [];
    },

    async thread(account, threadId) {
      const r = await request<{ messages: Envelope[] | null }>(
        `/v1/accounts/${encodeURIComponent(account)}/threads/${encodeURIComponent(threadId)}`,
      );
      return r.messages ?? [];
    },

    message(account, messageId) {
      return request<Envelope>(
        `/v1/accounts/${encodeURIComponent(account)}/messages/${encodeURIComponent(messageId)}`,
      );
    },

    body(account, messageId) {
      return request<Body>(
        `/v1/accounts/${encodeURIComponent(account)}/messages/${encodeURIComponent(messageId)}/body`,
      );
    },

    async sync(account) {
      const r = await request<{ reports: SyncReport[] | null }>(
        `/v1/accounts/${encodeURIComponent(account)}/sync`,
        { method: "POST" },
      );
      return r.reports ?? [];
    },

    async apply(account, op) {
      const r = await request<{ applied: number }>(
        `/v1/accounts/${encodeURIComponent(account)}/operations`,
        { method: "POST", body: JSON.stringify(op) },
      );
      return r.applied;
    },

    health() {
      return request<{ status: string; nucleus: boolean; version: string }>("/health");
    },
  };
}

/**
 * Builds a MailError from a response, tolerating a body that is not a problem
 * document — a proxy returning HTML on a 502 must not turn into a JSON parse
 * error that hides the real status.
 */
async function toMailError(response: Response): Promise<MailError> {
  let title = response.statusText || "Request Failed";
  let detail = "";
  try {
    const problem = (await response.json()) as { title?: string; detail?: string };
    if (problem.title) title = problem.title;
    if (problem.detail) detail = problem.detail;
  } catch {
    // Body was not JSON; the status is still meaningful on its own.
  }
  return new MailError(response.status, title, detail);
}
