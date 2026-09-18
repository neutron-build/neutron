/**
 * Transport-installed peer identity for the current request (TS-32).
 *
 * SECURITY: forwarding headers (`X-Real-IP`, `X-Forwarded-For`,
 * `X-Forwarded-Proto`) are attacker-controlled on ANY request that did not
 * demonstrably arrive through a configured trusted proxy. Deriving "which
 * proxy sent me this" from those headers is circular — a directly connected
 * client can forge them wholesale. The only trustworthy nearest-hop
 * identity is the transport's own socket address, so the node adapter
 * installs it here from `incoming.socket.remoteAddress` and consumers
 * (session Secure-cookie decisions, rate-limit keying) authorize the proxy
 * chain from THAT, never from headers.
 */

export interface TransportPeer {
  readonly remoteAddress: string;
}

const peerByRequest = new WeakMap<Request, TransportPeer>();

/**
 * Install immutable peer metadata for a request. Call from the transport
 * (the actual socket), never from header values.
 */
export function installTransportPeer(request: Request, remoteAddress: string): void {
  if (!Number.isFinite(remoteAddress.length) || remoteAddress.length === 0) {
    return;
  }
  peerByRequest.set(request, Object.freeze({ remoteAddress }));
}

/**
 * The transport-installed peer for this request, when the adapter supplied
 * one. Absent for hand-constructed Requests (tests, non-HTTP callers) —
 * consumers must treat absence as "no trustworthy peer", not as "trust
 * headers".
 */
export function transportPeer(request: Request): TransportPeer | undefined {
  return peerByRequest.get(request);
}
