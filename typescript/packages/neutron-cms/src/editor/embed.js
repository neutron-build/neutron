// Embeds: a YouTube, Vimeo or Spotify link made into that provider's player, or an <iframe> pasted
// from anywhere on https. Pure, so the tests can run it; the editor shows and inserts the result.

const ATTRIBUTE = /\s+([^\s"'<>/=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/y;

/** The attributes of a source that is exactly one <iframe ...></iframe> with an https src, or null. */
export function iframeAttributes(source) {
  const text = String(source).trim();
  const open = /^<iframe\b/i.exec(text);
  if (!open) return null;
  const attributes = {};
  let at = open[0].length;
  for (;;) {
    ATTRIBUTE.lastIndex = at;
    const match = ATTRIBUTE.exec(text);
    if (!match) break;
    const name = match[1].toLowerCase();
    // Event handlers and inline documents can run code wherever the post is shown.
    if (name.startsWith("on") || name === "srcdoc" || Object.hasOwn(attributes, name)) return null;
    attributes[name] = match[2] ?? match[3] ?? match[4] ?? "";
    at = ATTRIBUTE.lastIndex;
  }
  if (!/^\s*>\s*<\/iframe\s*>$/i.test(text.slice(at))) return null;
  try {
    const src = new URL(attributes.src?.replace(/&amp;/g, "&") ?? "");
    if (src.protocol !== "https:") return null;
  } catch {
    return null;
  }
  return attributes;
}

export const isEmbed = (source) => iframeAttributes(source) !== null;

const VIDEO = 'width="560" height="315" style="width:100%;height:auto;aspect-ratio:16/9;border:0"';
// YouTube's player refuses to start without knowing the page's origin, so ask for it to be sent.
const YOUTUBE = `${VIDEO} loading="lazy" referrerpolicy="strict-origin-when-cross-origin" allow="encrypted-media; picture-in-picture; fullscreen" allowfullscreen`;

/** Seconds from YouTube's t= or start=: "90", "90s" or "1m30s". */
function seconds(value) {
  const parts = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s?)?$/.exec(value ?? "");
  if (!parts || !value) return 0;
  return Number(parts[1] ?? 0) * 3600 + Number(parts[2] ?? 0) * 60 + Number(parts[3] ?? 0);
}

function youtube(url) {
  const host = url.hostname.replace(/^(www|m)\./, "");
  let id = null;
  if (host === "youtu.be") id = url.pathname.slice(1);
  else if (host === "youtube.com" || host === "youtube-nocookie.com") {
    if (url.pathname === "/watch") id = url.searchParams.get("v");
    else if (url.pathname === "/playlist" && /^[\w-]+$/.test(url.searchParams.get("list") ?? "")) {
      return `<iframe src="https://www.youtube-nocookie.com/embed/videoseries?list=${url.searchParams.get("list")}" title="YouTube playlist" ${YOUTUBE}></iframe>`;
    } else id = /^\/(?:embed|shorts|live)\/([^/]+)\/?$/.exec(url.pathname)?.[1] ?? null;
  } else return null;
  if (!/^[\w-]{11}$/.test(id ?? "")) return null;
  const start = seconds(url.searchParams.get("t") ?? url.searchParams.get("start"));
  return `<iframe src="https://www.youtube-nocookie.com/embed/${id}${start ? `?start=${start}` : ""}" title="YouTube video" ${YOUTUBE}></iframe>`;
}

function vimeo(url) {
  const host = url.hostname.replace(/^www\./, "");
  const path = host === "vimeo.com" ? /^\/(?:channels\/[\w-]+\/|groups\/[\w-]+\/videos\/)?(\d+)(?:\/([0-9a-f]+))?\/?$/.exec(url.pathname)
    : host === "player.vimeo.com" ? /^\/video\/(\d+)\/?$/.exec(url.pathname) : null;
  if (!path) return null;
  // An unlisted video needs its hash.
  const hash = path[2] ?? (/^[0-9a-f]+$/.test(url.searchParams.get("h") ?? "") ? url.searchParams.get("h") : null);
  return `<iframe src="https://player.vimeo.com/video/${path[1]}${hash ? `?h=${hash}` : ""}" title="Vimeo video" ${VIDEO} loading="lazy" allow="picture-in-picture; fullscreen" allowfullscreen></iframe>`;
}

function spotify(url) {
  if (url.hostname !== "open.spotify.com") return null;
  const path = /^\/(?:intl-[\w-]+\/)?(?:embed\/)?(track|album|playlist|episode|show|artist)\/([A-Za-z0-9]+)\/?$/.exec(url.pathname);
  if (!path) return null;
  const height = path[1] === "track" || path[1] === "episode" ? 152 : 352;
  return `<iframe src="https://open.spotify.com/embed/${path[1]}/${path[2]}" title="Spotify ${path[1]}" width="100%" height="${height}" style="border:0;border-radius:12px" loading="lazy" allow="encrypted-media; fullscreen; picture-in-picture"></iframe>`;
}

/** The player for a YouTube, Vimeo or Spotify link, or null for anything else. */
export function providerEmbed(link) {
  let text = String(link).trim();
  if (/^(www\.|m\.)?(youtube\.com|youtu\.be|vimeo\.com|open\.spotify\.com)\//i.test(text)) text = `https://${text}`;
  if (!/^https?:\/\/\S+$/i.test(text)) return null;
  let url;
  try {
    url = new URL(text);
  } catch {
    return null;
  }
  return youtube(url) ?? vimeo(url) ?? spotify(url);
}

/** What the Embed box makes of what was pasted into it: a player, the pasted iframe as it is, or null. */
export function embedFrom(input) {
  const text = String(input).trim();
  if (text.startsWith("<")) return isEmbed(text) ? text : null;
  return providerEmbed(text);
}
