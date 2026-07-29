/**
 * Agent tools over a mailbox.
 *
 * These are the second face on the same engine: Akiroo renders an inbox from
 * the client directly, while a chat agent calls these. Both read the same
 * mirror, so neither has a private view of the mailbox.
 *
 * The shape matches `tool()` from @neutron-build/ai without importing it, so
 * this package stays dependency-free and the tools can also be handed to
 * @neutron-build/mcp or any other loop that accepts a JSON Schema.
 */

import type { Envelope, MailClient } from "./client.js";

export interface ToolDefinition<TInput = Record<string, unknown>> {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  execute: (input: TInput) => Promise<unknown>;
  needsApproval?: boolean;
}

export interface MailToolOptions {
  client: MailClient;
  /** Account the tools operate on. */
  account: string;
  /**
   * Cap on messages returned by a search. Defaults to 20 — a chat turn
   * cannot usefully reason over more, and every extra row is context spent.
   */
  maxResults?: number;
  /**
   * Allow tools that change the mailbox. Off by default: a read-only agent
   * cannot lose someone's mail, and marking that boundary explicit is worth
   * more than the convenience.
   */
  allowMutations?: boolean;
}

/**
 * Trims an envelope to the fields worth spending context on.
 *
 * The full envelope carries threading headers, fingerprints, and mailbox
 * memberships that matter to the engine and mean nothing to a model.
 */
function summarize(env: Envelope) {
  return {
    id: env.ID,
    thread_id: env.ThreadID,
    from: env.From?.map((a) => a.name || a.email).join(", ") ?? "",
    to: env.To?.map((a) => a.name || a.email).join(", ") ?? "",
    subject: env.Subject,
    received_at: env.ReceivedAt,
    preview: env.Preview,
    unread: env.Keywords ? !env.Keywords.seen : false,
    flagged: env.Keywords?.flagged ?? false,
    has_attachment: env.HasAttachment,
  };
}

/** Builds the mail toolset for an agent. */
export function createMailTools(options: MailToolOptions): ToolDefinition[] {
  const { client, account } = options;
  const maxResults = options.maxResults ?? 20;

  const tools: ToolDefinition[] = [
    {
      name: "mail_search",
      description:
        "Search the user's mailbox by keyword. Matches subject, sender, and preview text. " +
        "Returns message summaries; use mail_read to get the full body of one.",
      inputSchema: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Words to search for, such as a sender name or subject phrase.",
          },
          limit: {
            type: "integer",
            description: `Maximum messages to return (default ${maxResults}).`,
          },
        },
        required: ["query"],
      },
      async execute(input) {
        const { query, limit } = input as { query: string; limit?: number };
        const results = await client.search(
          account,
          query,
          Math.min(limit ?? maxResults, maxResults),
        );
        return { count: results.length, messages: results.map(summarize) };
      },
    },

    {
      name: "mail_read",
      description:
        "Read one message in full, including its body text. Takes a message id from mail_search.",
      inputSchema: {
        type: "object",
        properties: {
          message_id: { type: "string", description: "Message id from a search result." },
        },
        required: ["message_id"],
      },
      async execute(input) {
        const { message_id } = input as { message_id: string };
        const [envelope, body] = await Promise.all([
          client.message(account, message_id),
          client.body(account, message_id),
        ]);
        return {
          ...summarize(envelope),
          body: body.Text || stripHtml(body.HTML),
          attachments: body.Parts?.filter((p) => p.disposition === "attachment").map((p) => ({
            filename: p.filename,
            type: p.type,
            size: p.size,
          })),
        };
      },
    },

    {
      name: "mail_thread",
      description:
        "Read every message in a conversation, oldest first. Takes a thread id from mail_search.",
      inputSchema: {
        type: "object",
        properties: {
          thread_id: { type: "string", description: "Thread id from a search result." },
        },
        required: ["thread_id"],
      },
      async execute(input) {
        const { thread_id } = input as { thread_id: string };
        const messages = await client.thread(account, thread_id);
        return { count: messages.length, messages: messages.map(summarize) };
      },
    },

    {
      name: "mail_mailboxes",
      description:
        "List the mailboxes or labels in the account, with their roles (inbox, sent, archive).",
      inputSchema: { type: "object", properties: {} },
      async execute() {
        const boxes = await client.mailboxes(account);
        return boxes.map((b) => ({ id: b.ID, name: b.Name, role: b.Role }));
      },
    },
  ];

  if (options.allowMutations) {
    tools.push(
      {
        name: "mail_mark",
        description:
          "Mark messages as read, unread, flagged, or unflagged. Changes the user's real mailbox.",
        inputSchema: {
          type: "object",
          properties: {
            message_ids: {
              type: "array",
              items: { type: "string" },
              description: "Message ids to change.",
            },
            keyword: {
              type: "string",
              enum: ["seen", "flagged"],
              description: "Which flag to change.",
            },
            value: { type: "boolean", description: "true to set the flag, false to clear it." },
          },
          required: ["message_ids", "keyword", "value"],
        },
        // Every mutation reaches the user's real mailbox, so each one is a
        // human decision rather than something the loop does on its own.
        needsApproval: true,
        async execute(input) {
          const { message_ids, keyword, value } = input as {
            message_ids: string[];
            keyword: string;
            value: boolean;
          };
          const applied = await client.apply(account, {
            kind: value ? "add_keyword" : "remove_keyword",
            ids: message_ids,
            keyword,
          });
          return { applied };
        },
      },
      {
        name: "mail_move",
        description:
          "Move messages to another mailbox. Changes the user's real mailbox.",
        inputSchema: {
          type: "object",
          properties: {
            message_ids: {
              type: "array",
              items: { type: "string" },
              description: "Message ids to move.",
            },
            target: { type: "string", description: "Destination mailbox id." },
          },
          required: ["message_ids", "target"],
        },
        needsApproval: true,
        async execute(input) {
          const { message_ids, target } = input as { message_ids: string[]; target: string };
          const applied = await client.apply(account, {
            kind: "move",
            ids: message_ids,
            target,
          });
          return { applied };
        },
      },
    );
  }

  return tools;
}

/**
 * Reduces HTML to readable text for messages that carry no plain-text part.
 *
 * Deliberately crude: this feeds a model, not a renderer, and a real HTML
 * parser would be a dependency bought for nothing.
 */
function stripHtml(html: string): string {
  if (!html) return "";
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}
