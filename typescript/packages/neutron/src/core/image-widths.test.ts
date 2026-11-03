import { afterEach, describe, expect, it } from "vitest";
import {
  DEFAULT_IMAGE_WIDTHS,
  configureImageWidths,
  getImageWidths,
  selectImageCandidates,
} from "./image-widths.js";

afterEach(() => configureImageWidths(undefined));

describe("selectImageCandidates", () => {
  it("fixed px sizes emits only the 1x and 2x candidates", () => {
    expect(selectImageCandidates({ sizes: "370px" })).toEqual({
      widths: [384, 750],
      src: 384,
      sizes: "370px",
    });
  });

  it("a width prop without sizes is a fixed layout width", () => {
    expect(selectImageCandidates({ width: 64 })).toEqual({ widths: [64, 128], src: 64, sizes: "64px" });
  });

  it("fixed sizes wins over the width prop", () => {
    expect(selectImageCandidates({ width: 800, sizes: "370px" }).widths).toEqual([384, 750]);
  });

  it("dedupes when 1x and 2x land on the same width", () => {
    expect(selectImageCandidates({ sizes: "3000px" })).toEqual({
      widths: [3840],
      src: 3840,
      sizes: "3000px",
    });
  });

  it("viewport-relative sizes emits the full ladder with a middle src", () => {
    const result = selectImageCandidates({ sizes: "50vw" });
    expect(result.widths).toEqual([...DEFAULT_IMAGE_WIDTHS]);
    expect(result.src).toBe(640);
    expect(result.sizes).toBe("50vw");
  });

  it("media-condition sizes is treated as viewport-relative", () => {
    const sizes = "(max-width: 640px) 100vw, 370px";
    expect(selectImageCandidates({ sizes }).widths).toEqual([...DEFAULT_IMAGE_WIDTHS]);
  });

  it("no sizes and no width defaults to 100vw over the full ladder", () => {
    expect(selectImageCandidates().sizes).toBe("100vw");
    expect(selectImageCandidates().widths).toEqual([...DEFAULT_IMAGE_WIDTHS]);
  });

  it("viewport sizes with a width prop trims the ladder at the 2x candidate", () => {
    const result = selectImageCandidates({ width: 370, sizes: "100vw" });
    expect(result.widths).toEqual([16, 32, 48, 64, 96, 128, 256, 384, 640, 750]);
    expect(result.src).toBe(384);
  });

  it("respects a per-component ladder exactly", () => {
    expect(selectImageCandidates({ widths: [320, 640, 960], sizes: "370px" }).widths).toEqual([640, 960]);
  });

  it("respects configured images.widths", () => {
    configureImageWidths([1600, 320, 640, 960, 1200, 640]);
    expect(getImageWidths()).toEqual([320, 640, 960, 1200, 1600]);
    expect(selectImageCandidates({ sizes: "370px" }).widths).toEqual([640, 960]);
    expect(selectImageCandidates({ sizes: "100vw" }).widths).toEqual([320, 640, 960, 1200, 1600]);
  });

  it("ignores an empty configured ladder", () => {
    configureImageWidths([]);
    expect(getImageWidths()).toEqual(DEFAULT_IMAGE_WIDTHS);
  });

  it("never emits widths above the intrinsic width", () => {
    expect(selectImageCandidates({ sizes: "370px", intrinsicWidth: 500 })).toEqual({
      widths: [384],
      src: 384,
      sizes: "370px",
    });
    expect(Math.max(...selectImageCandidates({ sizes: "100vw", intrinsicWidth: 1000 }).widths)).toBe(828);
  });

  it("keeps one candidate when the source is smaller than the whole ladder", () => {
    expect(selectImageCandidates({ sizes: "370px", intrinsicWidth: 10 }).widths).toEqual([16]);
  });
});
