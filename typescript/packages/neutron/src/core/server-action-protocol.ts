/**
 * Wire constants shared by the server-action client stub (browser), the
 * request handler (server, edge runtime) and the build. Kept free of imports
 * so the browser bundle carries nothing else.
 */

/** The one endpoint every server action is invoked through (POST). */
export const SERVER_ACTION_PATH = "/_neutron/action";

/** Request header naming the action id. Also makes the request non-simple (CORS preflight cross-origin). */
export const SERVER_ACTION_HEADER = "X-Neutron-Action";

/** Response header marking a body as the action's serialized return value. */
export const SERVER_ACTION_RESULT_HEADER = "X-Neutron-Action-Result";

/** Multipart field holding the serialized argument list when arguments carry FormData or Blobs. */
export const SERVER_ACTION_ARGS_FIELD = "$neutron_args";

/** Multipart field prefix of the entries of the `i`th FormData argument: `$f<i>:<name>`. */
export const SERVER_ACTION_FORM_PREFIX = "$f";

/** Multipart field of the `i`th Blob/File argument: `$b<i>`. */
export const SERVER_ACTION_BLOB_PREFIX = "$b";

/** Default cap on an action request body: 1 MiB, as Next's `bodySizeLimit`. */
export const DEFAULT_SERVER_ACTION_BODY_LIMIT = 1024 * 1024;

/** dist file recording the actions a build registered (`neutron-ts build`). */
export const SERVER_ACTIONS_MANIFEST = ".neutron-server-actions.json";
export const SERVER_ACTIONS_MANIFEST_FORMAT = 1;

export interface ServerActionsManifest {
  format: typeof SERVER_ACTIONS_MANIFEST_FORMAT;
  /** Action id → module (path relative to the app root, posix) and export name. */
  actions: Record<string, { file: string; export: string }>;
}

export function parseServerActionsManifest(source: string): ServerActionsManifest | null {
  try {
    const parsed = JSON.parse(source) as Partial<ServerActionsManifest>;
    if (
      parsed?.format !== SERVER_ACTIONS_MANIFEST_FORMAT ||
      !parsed.actions ||
      typeof parsed.actions !== "object"
    ) {
      return null;
    }
    return parsed as ServerActionsManifest;
  } catch {
    return null;
  }
}
