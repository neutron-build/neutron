/**
 * The one width ladder shared by `<Image>`/`<Picture>` (which widths they
 * emit) and `/_neutron/image` (which widths it will resize to). The endpoint
 * serves any other width at the next ladder width up, so arbitrary `w` values
 * cannot fill the image cache.
 */

/** Default `images.widths`: small fixed-size widths, then device widths. */
export const DEFAULT_IMAGE_WIDTHS: readonly number[] = [
  16, 32, 48, 64, 96, 128, 256, 384,
  640, 750, 828, 1080, 1200, 1920, 2048, 3840,
];

/**
 * globalThis key holding the app's configured ladder. A global rather than
 * module state because the SSR graph loads its own copy of this module
 * (Vite module runner / server bundle) alongside the one the server imported.
 */
export const IMAGE_WIDTHS_GLOBAL = "__neutronImageWidths";

/** Positive integers, deduped, ascending. */
export function normalizeImageWidths(widths: readonly number[] | undefined): number[] {
  const valid = (widths ?? []).filter((value) => Number.isSafeInteger(value) && value > 0);
  return [...new Set(valid)].sort((left, right) => left - right);
}

/** The configured ladder when it has any valid width, else the default. */
export function resolveImageWidths(widths: readonly number[] | undefined): readonly number[] {
  const normalized = normalizeImageWidths(widths);
  return normalized.length > 0 ? normalized : DEFAULT_IMAGE_WIDTHS;
}

/** Record the app's `images.widths` for components rendered in this realm. */
export function configureImageWidths(widths: readonly number[] | undefined): void {
  const normalized = normalizeImageWidths(widths);
  (globalThis as Record<string, unknown>)[IMAGE_WIDTHS_GLOBAL] =
    normalized.length > 0 ? normalized : undefined;
}

/** The ladder in effect: configured via {@link configureImageWidths}, else the default. */
export function getImageWidths(): readonly number[] {
  const configured = (globalThis as Record<string, unknown>)[IMAGE_WIDTHS_GLOBAL];
  return Array.isArray(configured) ? resolveImageWidths(configured) : DEFAULT_IMAGE_WIDTHS;
}

export interface ImageCandidateOptions {
  /** Ladder to pick from. Defaults to {@link getImageWidths}. */
  widths?: readonly number[];
  /** Layout width in CSS pixels, when known. */
  width?: number;
  /** The `sizes` attribute, as authored. */
  sizes?: string;
  /** Natural pixel width of the source, when known. */
  intrinsicWidth?: number;
}

export interface ImageCandidates {
  /** Widths for the `w`-descriptor srcset, ascending. */
  widths: number[];
  /** Width for the single `src` fallback. */
  src: number;
  /** The `sizes` attribute to emit. */
  sizes: string;
}

const FIXED_PX_SIZES = /^\s*(\d+(?:\.\d+)?)px\s*$/;

/**
 * Pick srcset candidates.
 *
 * Fixed layout width W (a single `Npx` `sizes`, or a `width` prop with no
 * `sizes`): only the smallest ladder widths >= W and >= 2W, covering 1x and
 * 2x displays. Otherwise (viewport-relative or media-conditioned `sizes`):
 * the whole ladder, trimmed to the 2x candidate when `width` bounds it.
 * Widths above `intrinsicWidth` are dropped; at least one candidate remains.
 */
export function selectImageCandidates(options: ImageCandidateOptions = {}): ImageCandidates {
  let ladder = normalizeImageWidths(options.widths ?? getImageWidths());
  if (ladder.length === 0) {
    ladder = [...getImageWidths()];
  }
  const intrinsic = positive(options.intrinsicWidth);
  if (intrinsic !== undefined) {
    const within = ladder.filter((value) => value <= intrinsic);
    ladder = within.length > 0 ? within : [ladder[0]];
  }

  const pick = (target: number): number =>
    ladder.find((value) => value >= target) ?? ladder[ladder.length - 1];

  const sizesMatch = options.sizes === undefined ? null : FIXED_PX_SIZES.exec(options.sizes);
  const layoutWidth = positive(options.width);
  const fixedWidth = sizesMatch
    ? positive(Number(sizesMatch[1]))
    : options.sizes === undefined
      ? layoutWidth
      : undefined;

  if (fixedWidth !== undefined) {
    const oneX = pick(fixedWidth);
    const twoX = pick(fixedWidth * 2);
    return {
      widths: oneX === twoX ? [oneX] : [oneX, twoX],
      src: oneX,
      sizes: options.sizes ?? `${fixedWidth}px`,
    };
  }

  if (layoutWidth !== undefined) {
    const cap = pick(layoutWidth * 2);
    return {
      widths: ladder.filter((value) => value <= cap),
      src: pick(layoutWidth),
      sizes: options.sizes ?? "100vw",
    };
  }

  // No known width: a middle candidate rather than the largest, so a client
  // that ignores srcset does not fetch the biggest variant.
  return {
    widths: ladder,
    src: ladder[Math.floor(ladder.length / 2)],
    sizes: options.sizes ?? "100vw",
  };
}

function positive(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : undefined;
}
