import { AIError, problemFromStatus } from "./errors.js";
import { streamPartsFromResponse } from "./event-stream.js";
import type { Message } from "./types.js";

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
}

export type ChatStatus = "idle" | "streaming" | "error";

export interface ChatState {
  messages: ChatMessage[];
  status: ChatStatus;
  error?: AIError;
}

export interface ChatStoreOptions {
  /** Endpoint that accepts POST { messages } and answers in the event-stream wire format. */
  api: string;
  headers?: Record<string, string>;
  fetch?: typeof globalThis.fetch;
  initialMessages?: ChatMessage[];
}

/**
 * Framework-free chat state machine over the event-stream wire format.
 * The /preact useChat hook is a thin subscription wrapper around this;
 * other UI layers (native, plain DOM) consume it directly.
 */
export class ChatStore {
  #options: ChatStoreOptions;
  #state: ChatState;
  #listeners = new Set<() => void>();
  #abort: AbortController | null = null;
  #counter = 0;

  constructor(options: ChatStoreOptions) {
    this.#options = options;
    this.#state = { messages: options.initialMessages ?? [], status: "idle" };
  }

  getState(): ChatState {
    return this.#state;
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Send a user message; resolves when the response stream ends. Errors land in state, not throws. */
  async send(text: string): Promise<void> {
    if (this.#state.status === "streaming") {
      throw new AIError(problemFromStatus(409, "A response is already streaming; call stop() first."));
    }
    const userMessage: ChatMessage = { id: this.#nextId(), role: "user", content: text };
    const assistantId = this.#nextId();
    const history = [...this.#state.messages, userMessage];
    this.#set({
      messages: [...history, { id: assistantId, role: "assistant", content: "" }],
      status: "streaming",
    });

    this.#abort = new AbortController();
    try {
      const fetchImpl = this.#options.fetch ?? globalThis.fetch;
      const response = await fetchImpl(this.#options.api, {
        method: "POST",
        headers: { "content-type": "application/json", ...this.#options.headers },
        body: JSON.stringify({ messages: history.map(toWireMessage) }),
        signal: this.#abort.signal,
      });
      for await (const part of streamPartsFromResponse(response)) {
        if (part.type === "text-delta") {
          this.#appendText(assistantId, part.text);
        }
      }
      this.#set({ status: "idle" });
    } catch (error) {
      if (this.#abort?.signal.aborted) {
        this.#set({ status: "idle" });
        return;
      }
      const aiError =
        error instanceof AIError
          ? error
          : new AIError(problemFromStatus(500, error instanceof Error ? error.message : String(error)));
      this.#set({ status: "error", error: aiError });
    } finally {
      this.#abort = null;
    }
  }

  stop(): void {
    this.#abort?.abort();
  }

  #nextId(): string {
    this.#counter += 1;
    return `msg-${this.#counter}`;
  }

  #set(next: Partial<ChatState>): void {
    this.#state = { ...this.#state, ...next };
    for (const listener of this.#listeners) listener();
  }

  #appendText(messageId: string, delta: string): void {
    const messages = this.#state.messages.map((message) =>
      message.id === messageId ? { ...message, content: message.content + delta } : message,
    );
    this.#set({ messages });
  }
}

function toWireMessage(message: ChatMessage): Message {
  return { role: message.role, content: message.content };
}
