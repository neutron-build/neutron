# CLI installer endpoint

`scripts/install.sh` at the repository root is the canonical installer.
`cli/scripts/install.sh` delegates to it for repository callers. Both macOS
and Linux releases support amd64 and arm64; Windows uses the Linux installer
inside WSL. Downloads require the release's exact SHA-256 checksum before
extraction or installation. No checksum tool or entry means failure.

From a clean checkout of the verified release source:

```sh
python3 cli/scripts/test-install.py
python3 cli/scripts/test-installer-host.py # Docker; --engine podman is supported
sh cli/installer-host/prepare.sh
teploy plan --project-dir cli/installer-host --version <release-source-sha>
teploy deploy --project-dir cli/installer-host --version <release-source-sha>
```

The Docker context includes the prepared, ignored `dist/install.sh` through
`.teployignore`. The container serves the same bytes at `/` and `/install.sh`
with `text/plain` and no-cache headers. The public Caddy route must point
`get.neutron.build` at this app. If that host already has a manually configured
static response, inspect the resulting route and replace that old response
with the app route; a successful container health check alone does not prove
the public host uses it. Verify after deployment:

```sh
curl -fsSL https://get.neutron.build -o /tmp/neutron-install.sh
cmp scripts/install.sh /tmp/neutron-install.sh
NEUTRON_VERSION=<released-cli-version> sh /tmp/neutron-install.sh
neutron version
```

Use a clean consumer install directory for release verification. Deploying
this endpoint is separate from deploying the docs site and publishing CLI
archives; publish and verify the archives first. CLI releases also attach
the canonical `install.sh` and include its hash in `checksums.txt`, so the
hosted endpoint can be compared with the installer from the tagged release.
