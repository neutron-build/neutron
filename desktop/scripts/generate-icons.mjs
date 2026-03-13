#!/usr/bin/env node
/**
 * generate-icons.mjs — Generate platform-specific icons for Neutron Desktop (Tauri 2.0)
 *
 * Usage:
 *   node scripts/generate-icons.mjs <source-png>
 *   node scripts/generate-icons.mjs assets/app-icon.png
 *
 * The source PNG should be 1024x1024 or larger. Generates:
 *   - macOS:   icon.icns (iconset with @1x/@2x variants)
 *   - Windows: icon.ico  (16..256 multi-res)
 *   - Linux:   icon.png  (32, 128, 256, 512)
 *   - Tauri:   Square*Logo.png for Windows Store, StoreLogo.png
 *
 * Output directory: examples/starter/src-tauri/icons/
 *
 * Requires: sharp (npm install --save-dev sharp)
 * Falls back to copying the source PNG if sharp is unavailable.
 */

import { existsSync, mkdirSync, copyFileSync, writeFileSync, unlinkSync } from "node:fs";
import { resolve, dirname, basename } from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT = resolve(__dirname, "..");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const OUTPUT_DIR = resolve(ROOT, "examples/starter/src-tauri/icons");

// macOS iconset sizes: each pair is [size, suffix]
// Apple requires both @1x and @2x for each logical size
const MACOS_SIZES = [
  [16, "16x16"],
  [32, "16x16@2x"],
  [32, "32x32"],
  [64, "32x32@2x"],
  [128, "128x128"],
  [256, "128x128@2x"],
  [256, "256x256"],
  [512, "256x256@2x"],
  [512, "512x512"],
  [1024, "512x512@2x"],
];

// Windows ICO layers
const WINDOWS_ICO_SIZES = [16, 24, 32, 48, 64, 128, 256];

// Linux PNG sizes
const LINUX_PNG_SIZES = [32, 128, 256, 512];

// Tauri-specific sizes (Windows Store logos)
const TAURI_SQUARE_SIZES = [30, 44, 71, 89, 107, 142, 150, 284, 310];
const TAURI_STORE_LOGO_SIZE = 50;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ensureDir(dir) {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
}

function parseArgs() {
  const args = process.argv.slice(2);
  if (args.length === 0 || args.includes("--help") || args.includes("-h")) {
    console.log(`
Usage: node scripts/generate-icons.mjs <source-png>

Arguments:
  <source-png>   Path to source PNG image (1024x1024 recommended)

Options:
  --output, -o   Output directory (default: examples/starter/src-tauri/icons/)
  --help, -h     Show this help message

Example:
  node scripts/generate-icons.mjs assets/app-icon.png
  node scripts/generate-icons.mjs logo.png -o my-app/src-tauri/icons/
`);
    process.exit(args.length === 0 ? 1 : 0);
  }

  let sourcePath = null;
  let outputDir = OUTPUT_DIR;

  for (let i = 0; i < args.length; i++) {
    if ((args[i] === "--output" || args[i] === "-o") && args[i + 1]) {
      outputDir = resolve(args[i + 1]);
      i++;
    } else if (!args[i].startsWith("-")) {
      sourcePath = resolve(args[i]);
    }
  }

  if (!sourcePath) {
    console.error("Error: No source PNG provided.");
    process.exit(1);
  }
  if (!existsSync(sourcePath)) {
    console.error(`Error: Source file not found: ${sourcePath}`);
    process.exit(1);
  }

  return { sourcePath, outputDir };
}

// ---------------------------------------------------------------------------
// ICO format writer (no native dependencies)
// ---------------------------------------------------------------------------

/**
 * Build an ICO file from an array of PNG buffers.
 * ICO format: header + directory entries + image data
 */
function buildIco(pngBuffers) {
  const count = pngBuffers.length;
  const headerSize = 6;
  const dirEntrySize = 16;
  const dirSize = dirEntrySize * count;
  const dataOffset = headerSize + dirSize;

  // Calculate offsets
  let offset = dataOffset;
  const offsets = [];
  for (const buf of pngBuffers) {
    offsets.push(offset);
    offset += buf.length;
  }

  const totalSize = offset;
  const ico = Buffer.alloc(totalSize);

  // ICO header: reserved(2) + type(2, 1=ICO) + count(2)
  ico.writeUInt16LE(0, 0); // reserved
  ico.writeUInt16LE(1, 2); // type = ICO
  ico.writeUInt16LE(count, 4); // image count

  // Directory entries
  for (let i = 0; i < count; i++) {
    const entryOffset = headerSize + i * dirEntrySize;
    const size = WINDOWS_ICO_SIZES[i];
    // Width/Height: 0 means 256
    ico.writeUInt8(size >= 256 ? 0 : size, entryOffset);
    ico.writeUInt8(size >= 256 ? 0 : size, entryOffset + 1);
    ico.writeUInt8(0, entryOffset + 2); // color palette
    ico.writeUInt8(0, entryOffset + 3); // reserved
    ico.writeUInt16LE(1, entryOffset + 4); // color planes
    ico.writeUInt16LE(32, entryOffset + 6); // bits per pixel
    ico.writeUInt32LE(pngBuffers[i].length, entryOffset + 8); // image size
    ico.writeUInt32LE(offsets[i], entryOffset + 12); // data offset
  }

  // Image data
  for (let i = 0; i < count; i++) {
    pngBuffers[i].copy(ico, offsets[i]);
  }

  return ico;
}

// ---------------------------------------------------------------------------
// Sharp-based generation (full quality)
// ---------------------------------------------------------------------------

async function generateWithSharp(sourcePath, outputDir) {
  const { default: sharp } = await import("sharp");

  const source = sharp(sourcePath);
  const meta = await source.metadata();

  if (meta.width < 1024 || meta.height < 1024) {
    console.warn(
      `Warning: Source image is ${meta.width}x${meta.height}. ` +
        `1024x1024 or larger is recommended for best quality.`
    );
  }

  console.log(`Source: ${sourcePath} (${meta.width}x${meta.height})`);
  console.log(`Output: ${outputDir}/`);
  console.log();

  ensureDir(outputDir);

  // --- macOS: Generate iconset then convert to .icns ---
  const iconsetDir = resolve(outputDir, "icon.iconset");
  ensureDir(iconsetDir);

  console.log("macOS icons:");
  for (const [size, name] of MACOS_SIZES) {
    const outPath = resolve(iconsetDir, `icon_${name}.png`);
    await sharp(sourcePath)
      .resize(size, size, { fit: "cover", kernel: "lanczos3" })
      .png({ quality: 100 })
      .toFile(outPath);
    console.log(`  icon_${name}.png (${size}x${size})`);
  }

  // Convert iconset to icns using macOS iconutil (only on macOS)
  if (process.platform === "darwin") {
    try {
      const icnsPath = resolve(outputDir, "icon.icns");
      execSync(`iconutil -c icns -o "${icnsPath}" "${iconsetDir}"`);
      console.log("  icon.icns (compiled)");

      // Clean up iconset directory after successful conversion
      execSync(`rm -rf "${iconsetDir}"`);
    } catch (err) {
      console.warn("  Warning: iconutil failed, keeping .iconset directory");
      console.warn(`  ${err.message}`);
    }
  } else {
    console.log(
      "  Skipping .icns compilation (not on macOS). " +
        "Run `iconutil -c icns` manually or on a macOS CI runner."
    );
  }

  // --- Windows: Generate ICO ---
  console.log("\nWindows icons:");
  const icoPngs = [];
  for (const size of WINDOWS_ICO_SIZES) {
    const buf = await sharp(sourcePath)
      .resize(size, size, { fit: "cover", kernel: "lanczos3" })
      .png()
      .toBuffer();
    icoPngs.push(buf);
    console.log(`  ${size}x${size} layer`);
  }

  const icoBuffer = buildIco(icoPngs);
  writeFileSync(resolve(outputDir, "icon.ico"), icoBuffer);
  console.log("  icon.ico (compiled)");

  // --- Linux: Standard PNG sizes ---
  console.log("\nLinux icons:");
  for (const size of LINUX_PNG_SIZES) {
    const outPath = resolve(outputDir, `${size}x${size}.png`);
    await sharp(sourcePath)
      .resize(size, size, { fit: "cover", kernel: "lanczos3" })
      .png()
      .toFile(outPath);
    console.log(`  ${size}x${size}.png`);
  }

  // --- Tauri: Main icon.png (512x512) and @2x variant ---
  const mainPng = resolve(outputDir, "icon.png");
  await sharp(sourcePath)
    .resize(512, 512, { fit: "cover", kernel: "lanczos3" })
    .png()
    .toFile(mainPng);
  console.log("\nTauri:");
  console.log("  icon.png (512x512)");

  // Tauri expects 128x128@2x.png as a standalone file (256x256 pixels)
  const retina128 = resolve(outputDir, "128x128@2x.png");
  await sharp(sourcePath)
    .resize(256, 256, { fit: "cover", kernel: "lanczos3" })
    .png()
    .toFile(retina128);
  console.log("  128x128@2x.png (256x256)");

  // --- Tauri: Windows Store logos ---
  console.log("\nWindows Store logos:");
  for (const size of TAURI_SQUARE_SIZES) {
    const outPath = resolve(outputDir, `Square${size}x${size}Logo.png`);
    await sharp(sourcePath)
      .resize(size, size, { fit: "cover", kernel: "lanczos3" })
      .png()
      .toFile(outPath);
    console.log(`  Square${size}x${size}Logo.png`);
  }

  const storeLogo = resolve(outputDir, "StoreLogo.png");
  await sharp(sourcePath)
    .resize(TAURI_STORE_LOGO_SIZE, TAURI_STORE_LOGO_SIZE, {
      fit: "cover",
      kernel: "lanczos3",
    })
    .png()
    .toFile(storeLogo);
  console.log(`  StoreLogo.png (${TAURI_STORE_LOGO_SIZE}x${TAURI_STORE_LOGO_SIZE})`);
}

// ---------------------------------------------------------------------------
// Fallback: copy source PNG to output directory
// ---------------------------------------------------------------------------

function generateFallback(sourcePath, outputDir) {
  console.warn("Warning: sharp is not available. Falling back to simple copy.");
  console.warn("Install sharp for proper icon generation: npm install --save-dev sharp\n");

  ensureDir(outputDir);

  // Copy source as icon.png (main Tauri icon)
  const dest = resolve(outputDir, "icon.png");
  copyFileSync(sourcePath, dest);
  console.log(`Copied source to ${dest}`);

  // On macOS, try sips for basic resizing
  if (process.platform === "darwin") {
    console.log("\nUsing macOS sips for basic resizing...\n");

    // Create iconset
    const iconsetDir = resolve(outputDir, "icon.iconset");
    ensureDir(iconsetDir);

    for (const [size, name] of MACOS_SIZES) {
      const outPath = resolve(iconsetDir, `icon_${name}.png`);
      try {
        copyFileSync(sourcePath, outPath);
        execSync(
          `sips -z ${size} ${size} "${outPath}" --out "${outPath}" 2>/dev/null`
        );
        console.log(`  icon_${name}.png (${size}x${size})`);
      } catch {
        console.warn(`  Failed to resize: icon_${name}.png`);
      }
    }

    try {
      const icnsPath = resolve(outputDir, "icon.icns");
      execSync(`iconutil -c icns -o "${icnsPath}" "${iconsetDir}"`);
      console.log("  icon.icns (compiled via sips + iconutil)");
      execSync(`rm -rf "${iconsetDir}"`);
    } catch {
      console.warn("  Could not compile .icns");
    }

    // Generate standard sizes with sips
    for (const size of LINUX_PNG_SIZES) {
      const outPath = resolve(outputDir, `${size}x${size}.png`);
      try {
        copyFileSync(sourcePath, outPath);
        execSync(
          `sips -z ${size} ${size} "${outPath}" --out "${outPath}" 2>/dev/null`
        );
        console.log(`  ${size}x${size}.png`);
      } catch {
        console.warn(`  Failed to resize: ${size}x${size}.png`);
      }
    }
  } else {
    console.log(
      "\nNote: Only icon.png was generated. Install sharp for full icon set generation."
    );
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const { sourcePath, outputDir } = parseArgs();

  try {
    await generateWithSharp(sourcePath, outputDir);
  } catch (err) {
    if (
      err.code === "ERR_MODULE_NOT_FOUND" ||
      err.message?.includes("Cannot find module") ||
      err.message?.includes("Cannot find package")
    ) {
      generateFallback(sourcePath, outputDir);
    } else {
      throw err;
    }
  }

  console.log("\nDone. Update tauri.conf.json bundle.icon to reference these files.");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
