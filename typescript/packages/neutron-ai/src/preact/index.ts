import { useEffect, useMemo, useState } from "preact/hooks";

import { ChatStore } from "../chat-store.js";
import type { ChatState, ChatStoreOptions } from "../chat-store.js";

export type { ChatMessage, ChatState, ChatStatus, ChatStoreOptions } from "../chat-store.js";
export { ChatStore } from "../chat-store.js";

export interface UseChatResult extends ChatState {
  send: (text: string) => Promise<void>;
  stop: () => void;
}

/**
 * Chat state over the event-stream wire format. Options are captured on
 * first render; create a new component (or key it) to change the endpoint.
 */
export function useChat(options: ChatStoreOptions): UseChatResult {
  // eslint-disable-next-line react-hooks/exhaustive-deps -- store lives for the component's lifetime
  const store = useMemo(() => new ChatStore(options), []);
  const [state, setState] = useState<ChatState>(() => store.getState());
  useEffect(() => store.subscribe(() => setState(store.getState())), [store]);
  return {
    ...state,
    send: (text: string) => store.send(text),
    stop: () => store.stop(),
  };
}
