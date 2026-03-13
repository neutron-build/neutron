#!/usr/bin/env bash
set -euo pipefail

# sign-macos.sh — Codesign and notarize a Neutron Desktop .app bundle
#
# Usage:
#   ./scripts/sign-macos.sh <path-to-app-bundle>
#   ./scripts/sign-macos.sh target/release/bundle/macos/NeutronApp.app
#
# Environment variables:
#   APPLE_SIGNING_IDENTITY  — Developer ID Application certificate name
#   APPLE_ID                — Apple ID email for notarization
#   APPLE_PASSWORD          — App-specific password for notarization
#   APPLE_TEAM_ID           — Apple Developer Team ID
#   ENTITLEMENTS_PATH       — (optional) Path to entitlements.plist

APP_PATH="${1:?Usage: sign-macos.sh <path-to.app>}"
ENTITLEMENTS_PATH="${ENTITLEMENTS_PATH:-}"

# ---------------------------------------------------------------------------
# Validate environment
# ---------------------------------------------------------------------------

if [[ -z "${APPLE_SIGNING_IDENTITY:-}" ]]; then
  echo "Error: APPLE_SIGNING_IDENTITY is not set."
  echo "Example: export APPLE_SIGNING_IDENTITY='Developer ID Application: Your Name (TEAMID)'"
  exit 1
fi

if [[ ! -d "$APP_PATH" ]]; then
  echo "Error: App bundle not found at $APP_PATH"
  exit 1
fi

echo "==> Signing: $APP_PATH"
echo "    Identity: $APPLE_SIGNING_IDENTITY"

# ---------------------------------------------------------------------------
# Step 1: Code sign the .app bundle
# ---------------------------------------------------------------------------

CODESIGN_ARGS=(
  --deep
  --force
  --verify
  --verbose=2
  --sign "$APPLE_SIGNING_IDENTITY"
  --options runtime
  --timestamp
)

if [[ -n "$ENTITLEMENTS_PATH" && -f "$ENTITLEMENTS_PATH" ]]; then
  echo "    Entitlements: $ENTITLEMENTS_PATH"
  CODESIGN_ARGS+=(--entitlements "$ENTITLEMENTS_PATH")
fi

echo ""
echo "==> Step 1/4: Codesigning..."
codesign "${CODESIGN_ARGS[@]}" "$APP_PATH"

# ---------------------------------------------------------------------------
# Step 2: Verify the signature
# ---------------------------------------------------------------------------

echo ""
echo "==> Step 2/4: Verifying signature..."
codesign --verify --deep --strict --verbose=2 "$APP_PATH"

echo ""
echo "==> Gatekeeper assessment:"
spctl --assess --type execute --verbose=2 "$APP_PATH" || {
  echo "Warning: Gatekeeper assessment failed (expected before notarization)"
}

# ---------------------------------------------------------------------------
# Step 3: Notarize
# ---------------------------------------------------------------------------

if [[ -z "${APPLE_ID:-}" || -z "${APPLE_PASSWORD:-}" || -z "${APPLE_TEAM_ID:-}" ]]; then
  echo ""
  echo "==> Skipping notarization (APPLE_ID, APPLE_PASSWORD, or APPLE_TEAM_ID not set)"
  echo "    Set these environment variables to enable notarization."
  exit 0
fi

echo ""
echo "==> Step 3/4: Creating zip for notarization..."

ZIP_PATH="${APP_PATH%.app}.zip"
ditto -c -k --sequesterRsrc --keepParent "$APP_PATH" "$ZIP_PATH"
echo "    Archive: $ZIP_PATH"

echo ""
echo "==> Submitting to Apple notary service..."
xcrun notarytool submit "$ZIP_PATH" \
  --apple-id "$APPLE_ID" \
  --password "$APPLE_PASSWORD" \
  --team-id "$APPLE_TEAM_ID" \
  --wait \
  --timeout 600

# Clean up the zip
rm -f "$ZIP_PATH"

# ---------------------------------------------------------------------------
# Step 4: Staple the notarization ticket
# ---------------------------------------------------------------------------

echo ""
echo "==> Step 4/4: Stapling notarization ticket..."
xcrun stapler staple "$APP_PATH"

echo ""
echo "==> Verifying stapled ticket..."
xcrun stapler validate "$APP_PATH"

echo ""
echo "Done. App is signed, notarized, and stapled:"
echo "  $APP_PATH"
