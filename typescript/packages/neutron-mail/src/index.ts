export { createMailClient, MailError } from "./client.js";
export type {
  Account,
  Address,
  Body,
  BodyPart,
  Envelope,
  Keywords,
  Mailbox,
  MailClient,
  MailClientOptions,
  MailCredential,
  OperationKind,
  SendRequest,
  SyncReport,
} from "./client.js";

export { createMailTools } from "./tools.js";
export type { MailToolOptions, ToolDefinition } from "./tools.js";
