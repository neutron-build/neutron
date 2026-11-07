import { createHash } from "node:crypto";
import { ADMIN_CSS, ADMIN_JS, EDIT_JS, EDITOR_JS } from "./assets.generated.js";

const CSS = "text/css; charset=utf-8";
const JS = "text/javascript; charset=utf-8";
const files = {
  "admin.css": { body: ADMIN_CSS, type: CSS },
  "admin.js": { body: ADMIN_JS, type: JS },
  "edit.js": { body: EDIT_JS, type: JS },
  "editor.js": { body: EDITOR_JS, type: JS },
};

export type AssetName = keyof typeof files;

const versions = Object.fromEntries(
  Object.entries(files).map(([name, file]) => [name, createHash("sha256").update(file.body).digest("hex").slice(0, 12)]),
) as Record<AssetName, string>;

/** The version is in the address, so an update can never meet an old copy in a browser's cache. */
export const assetUrl = (admin: string, name: AssetName) => `${admin}/${name}?v=${versions[name]}`;

export const isAsset = (name: string | undefined): name is AssetName => name !== undefined && Object.hasOwn(files, name);

export function serveAsset(name: AssetName, request: Request): Response {
  const current = new URL(request.url).searchParams.get("v") === versions[name];
  const scope = name === "admin.css" ? "public" : "private";
  return new Response(files[name].body, {
    headers: { "Content-Type": files[name].type, "Cache-Control": current ? `${scope}, max-age=31536000, immutable` : "no-store" },
  });
}
