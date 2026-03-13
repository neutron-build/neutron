# sign-windows.ps1 — Sign Neutron Desktop executables and installers on Windows
#
# Usage:
#   .\scripts\sign-windows.ps1 -AppPath "target\release\bundle\nsis\NeutronApp_0.1.0_x64-setup.exe"
#
# Environment variables (for CI):
#   WINDOWS_CERTIFICATE            — Base64-encoded .pfx certificate
#   WINDOWS_CERTIFICATE_PASSWORD   — Password for the .pfx
#   WINDOWS_CERTIFICATE_THUMBPRINT — SHA-1 thumbprint (alternative to pfx import)
#
# For local signing, the certificate should already be in the Windows certificate store.

param(
    [Parameter(Mandatory = $true)]
    [string]$AppPath,

    [string]$TimestampUrl = "http://timestamp.digicert.com",

    [string]$CertificateThumbprint = $env:WINDOWS_CERTIFICATE_THUMBPRINT,

    [string]$Description = "Neutron Desktop Application"
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Locate signtool.exe
# ---------------------------------------------------------------------------

function Find-SignTool {
    # Check PATH first
    $signtool = Get-Command signtool.exe -ErrorAction SilentlyContinue
    if ($signtool) {
        return $signtool.Source
    }

    # Search Windows SDK installations
    $sdkPaths = @(
        "${env:ProgramFiles(x86)}\Windows Kits\10\bin",
        "${env:ProgramFiles}\Windows Kits\10\bin"
    )

    foreach ($sdkPath in $sdkPaths) {
        if (Test-Path $sdkPath) {
            # Find the latest SDK version
            $versions = Get-ChildItem -Path $sdkPath -Directory | Sort-Object Name -Descending
            foreach ($version in $versions) {
                $tool = Join-Path $version.FullName "x64\signtool.exe"
                if (Test-Path $tool) {
                    return $tool
                }
            }
        }
    }

    throw "signtool.exe not found. Install the Windows SDK or add signtool to your PATH."
}

# ---------------------------------------------------------------------------
# Import certificate from environment (CI)
# ---------------------------------------------------------------------------

function Import-CertificateFromEnv {
    $certBase64 = $env:WINDOWS_CERTIFICATE
    $certPassword = $env:WINDOWS_CERTIFICATE_PASSWORD

    if (-not $certBase64) {
        return $null
    }

    Write-Host "==> Importing certificate from WINDOWS_CERTIFICATE env var..."

    $certBytes = [Convert]::FromBase64String($certBase64)
    $pfxPath = Join-Path $env:TEMP "neutron-signing-cert.pfx"
    [IO.File]::WriteAllBytes($pfxPath, $certBytes)

    try {
        $securePassword = ConvertTo-SecureString -String $certPassword -AsPlainText -Force
        $cert = Import-PfxCertificate `
            -FilePath $pfxPath `
            -CertStoreLocation Cert:\CurrentUser\My `
            -Password $securePassword

        Write-Host "    Imported certificate: $($cert.Subject)"
        Write-Host "    Thumbprint: $($cert.Thumbprint)"

        return $cert.Thumbprint
    }
    finally {
        # Always clean up the pfx file
        Remove-Item -Path $pfxPath -Force -ErrorAction SilentlyContinue
    }
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

Write-Host "Neutron Desktop — Windows Code Signing"
Write-Host "======================================="
Write-Host ""

# Validate input
if (-not (Test-Path $AppPath)) {
    throw "File not found: $AppPath"
}

# Find signtool
$signtool = Find-SignTool
Write-Host "==> Using signtool: $signtool"

# Get certificate thumbprint
if (-not $CertificateThumbprint) {
    $CertificateThumbprint = Import-CertificateFromEnv
}

if (-not $CertificateThumbprint) {
    throw @"
No certificate configured. Set one of:
  - WINDOWS_CERTIFICATE_THUMBPRINT environment variable
  - WINDOWS_CERTIFICATE + WINDOWS_CERTIFICATE_PASSWORD environment variables
  - Pass -CertificateThumbprint parameter
"@
}

Write-Host "==> Certificate thumbprint: $CertificateThumbprint"
Write-Host "==> Signing: $AppPath"
Write-Host ""

# ---------------------------------------------------------------------------
# Step 1: Sign the executable
# ---------------------------------------------------------------------------

Write-Host "==> Step 1/2: Signing executable..."

$signArgs = @(
    "sign",
    "/sha1", $CertificateThumbprint,
    "/fd", "sha256",
    "/tr", $TimestampUrl,
    "/td", "sha256",
    "/d", $Description,
    $AppPath
)

& $signtool @signArgs
if ($LASTEXITCODE -ne 0) {
    throw "signtool sign failed with exit code $LASTEXITCODE"
}

# ---------------------------------------------------------------------------
# Step 2: Verify the signature
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "==> Step 2/2: Verifying signature..."

$verifyArgs = @(
    "verify",
    "/pa",
    "/v",
    $AppPath
)

& $signtool @verifyArgs
if ($LASTEXITCODE -ne 0) {
    throw "signtool verify failed with exit code $LASTEXITCODE"
}

Write-Host ""
Write-Host "Done. Signed and verified: $AppPath"
