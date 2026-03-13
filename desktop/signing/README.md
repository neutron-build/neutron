# Neutron Desktop — Code Signing

This document covers code signing and notarization for Neutron Desktop
(Tauri 2.0) on macOS, Windows, and Linux.

## Required Secrets (GitHub Actions)

Set these in your repository or environment secrets:

| Secret | Platform | Description |
|--------|----------|-------------|
| `APPLE_SIGNING_IDENTITY` | macOS | Developer ID Application certificate name (e.g. `Developer ID Application: Your Name (TEAMID)`) |
| `APPLE_CERTIFICATE` | macOS | Base64-encoded .p12 certificate |
| `APPLE_CERTIFICATE_PASSWORD` | macOS | Password for the .p12 certificate |
| `APPLE_ID` | macOS | Apple ID email for notarization |
| `APPLE_PASSWORD` | macOS | App-specific password (generate at appleid.apple.com) |
| `APPLE_TEAM_ID` | macOS | Apple Developer Team ID (10-character string) |
| `WINDOWS_CERTIFICATE` | Windows | Base64-encoded .pfx certificate |
| `WINDOWS_CERTIFICATE_PASSWORD` | Windows | Password for the .pfx certificate |
| `TAURI_SIGNING_PRIVATE_KEY` | All | Tauri updater signing key (generate with `tauri signer generate`) |
| `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` | All | Password for the Tauri updater key |

## macOS Code Signing & Notarization

### Prerequisites

1. **Apple Developer Program membership** ($99/year)
2. **Developer ID Application** certificate from Apple Developer portal
3. **App-specific password** for notarization (appleid.apple.com > Security)

### Local Signing

```bash
# Export your certificate to .p12 from Keychain Access
# Import it on your build machine:
security import certificate.p12 -k ~/Library/Keychains/login.keychain-db -P "$PASSWORD" -T /usr/bin/codesign

# Sign the app
./scripts/sign-macos.sh target/release/bundle/macos/NeutronApp.app

# Or manually:
codesign --deep --force --verify --verbose \
  --sign "Developer ID Application: Your Name (TEAMID)" \
  --options runtime \
  --entitlements entitlements.plist \
  target/release/bundle/macos/NeutronApp.app
```

### Notarization

```bash
# Submit for notarization
xcrun notarytool submit target/release/bundle/macos/NeutronApp.app.zip \
  --apple-id "$APPLE_ID" \
  --password "$APPLE_PASSWORD" \
  --team-id "$APPLE_TEAM_ID" \
  --wait

# Staple the ticket
xcrun stapler staple target/release/bundle/macos/NeutronApp.app
```

### Entitlements

Create `entitlements.plist` if your app needs special capabilities:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>com.apple.security.cs.allow-jit</key>
  <true/>
  <key>com.apple.security.cs.allow-unsigned-executable-memory</key>
  <true/>
  <key>com.apple.security.cs.allow-dyld-environment-variables</key>
  <true/>
</dict>
</plist>
```

## Windows Code Signing

### Prerequisites

1. **EV or OV code signing certificate** from a trusted CA (DigiCert, Sectigo, etc.)
2. For EV certs: USB hardware token (SafeNet, YubiKey) or cloud HSM

### Local Signing

```powershell
# Sign with certificate from Windows certificate store
.\scripts\sign-windows.ps1 -AppPath "target\release\bundle\nsis\NeutronApp_0.1.0_x64-setup.exe"

# Or manually with signtool:
signtool sign /sha1 $THUMBPRINT /fd sha256 /tr http://timestamp.digicert.com /td sha256 "NeutronApp.exe"
```

### Cloud Signing (CI)

For CI, import the certificate from a base64 secret:

```powershell
$cert = [Convert]::FromBase64String($env:WINDOWS_CERTIFICATE)
[IO.File]::WriteAllBytes("cert.pfx", $cert)
certutil -importpfx -p "$env:WINDOWS_CERTIFICATE_PASSWORD" cert.pfx
```

## Linux

Linux packages (.deb, .AppImage, .rpm) do not require code signing for
distribution, but you can GPG-sign release artifacts:

```bash
gpg --armor --detach-sign neutron-desktop_0.1.0_amd64.deb
```

For Flatpak/Snap distribution, signing is handled by the respective store
submission process.

## Tauri Updater Signing

Tauri's built-in updater uses its own signing key (separate from platform
code signing). Generate one:

```bash
npx tauri signer generate -w ~/.tauri/neutron.key
```

Set `TAURI_SIGNING_PRIVATE_KEY` and `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`
in your CI secrets. The public key goes in `tauri.conf.json`:

```json
{
  "plugins": {
    "updater": {
      "pubkey": "dW50cnVzdGVkIGNvbW1lbnQ6...",
      "endpoints": ["https://releases.neutron.build/desktop/{{target}}/{{arch}}/{{current_version}}"]
    }
  }
}
```

## Troubleshooting

### macOS: "App is damaged and can't be opened"
- The app was not signed or notarized. Run `sign-macos.sh`.
- Check with: `spctl --assess --verbose target/release/bundle/macos/NeutronApp.app`

### macOS: Notarization fails with "invalid signature"
- Ensure `--options runtime` is passed to `codesign` (hardened runtime).
- Ensure all nested frameworks/binaries are also signed.

### Windows: SmartScreen warning
- Use an EV certificate (immediate reputation) or wait for OV cert to build reputation.
- Ensure timestamp is included (`/tr` flag) so signature survives cert expiry.

### CI: Certificate not found
- Verify the base64 encoding: `base64 -i cert.p12 | pbcopy`
- Ensure the keychain is unlocked in CI (macOS) or the cert is properly imported (Windows).
