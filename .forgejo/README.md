# Forgejo Actions — CI on the source of truth

`origin` is the self-hosted Forgejo instance; GitHub is a one-way mirror. Until
these workflows existed, **all 23 CI workflows fired only on the mirror**, so a
push to the repository that is actually authoritative was verified by zero
gates. That is what this directory fixes.

Nothing here runs until a runner is registered. **No agent can do that step** —
it needs a registration token from the Forgejo admin UI and root on a host.
Section 2 is the exact procedure.

## 1. What runs where, and why

| Gate | Home | Reason |
|---|---|---|
| `cargo fmt --check`, `clippy --all-targets`, `cargo test --lib`, core-only build/test, `metrics.sh --check`, license + unsafe policy | **Forgejo** (`nucleus-gates.yml`) | These are the gates that decide whether a commit is good. They belong on the branch you actually push to. |
| Integration tests, probe suite at `ci` scale | **Forgejo** (`nucleus-gates.yml`) | Same, but slower; kept in separate jobs so a fast failure reports fast. |
| PostgreSQL differential, binary-COPY, ORM, JDBC compat | **Forgejo** (`nucleus-compat.yml`) | Needs a real PostgreSQL 17, Node, Python and a JDK on the runner. Skips (exit 0) when a toolchain is missing — read the "toolchains present" step before believing a green. |
| Multi-hour soak, full-scale probe sweep, scale loads | **Forgejo only** (`nucleus-long.yml`) | GitHub-hosted jobs are capped at **6 hours**. This cap, not cost, is why the soak has never run in CI. A self-hosted runner has no cap. |
| Release builds, **cosign keyless signing, SLSA provenance**, GHCR push | **GitHub only** (`.github/workflows/nucleus-release.yml`) | Keyless signing derives its identity from GitHub's OIDC issuer, and provenance is only worth something when a third party can verify the builder. Do **not** move signing onto a runner sitting on the same LAN as the databases under test. |
| Cross-language workflows (typescript, go, python, zig, …) | **GitHub** | Out of scope for this change. They are unaffected. |

### Which workflow files Forgejo picks up

Forgejo resolves workflows from the first of these that exists in the commit:

```
.forgejo/workflows/  →  .gitea/workflows/  →  .github/workflows/
```

Because `.forgejo/workflows/` now exists, Forgejo should run **only** the files
in this directory and ignore the 23 GitHub ones.

**Verify this on the first push** rather than trusting it: open
`<repo>/actions` on Forgejo and confirm the only jobs listed are
`Nucleus gates`, `Nucleus compatibility` and `Nucleus long-running`.

There is a second, independent safety net. Register the runner with **only**
the `nucleus-lab` label (step 2.4). Every GitHub workflow requests
`runs-on: ubuntu-latest`, `macos-latest` or `windows-latest`; a job whose label
no runner offers is never dispatched. So even if a Forgejo version does scan
`.github/workflows/`, those jobs sit unassigned instead of running and failing.

## 2. Registering the runner (owner action — cannot be automated from here)

Target host: a Linux VM. Suggested shape: 8 vCPU, 16–32 GB RAM,
200+ GB disk. Do **not** use the macOS dev box: `probe_soak`'s RSS leak gate
reads `/proc/self/status` and is a silent no-op on macOS, and
`nucleus-long.yml` fails deliberately if it finds a non-Linux runner.

### 2.1 Enable Actions

In Forgejo `app.ini`:

```ini
[actions]
ENABLED = true
```

Then repository → Settings → Units → tick **Actions**.

### 2.2 Get a registration token

Site Administration → Actions → Runners → **Create new runner** gives an
instance-wide token. A repository-scoped token is under repo Settings →
Actions → Runners. Either works; instance-wide is easier to reuse.

### 2.3 Install the runner binary

```bash
# On the Debian VM, as root
RUNNER_VERSION=6.3.1   # check https://code.forgejo.org/forgejo/runner/releases
curl -fsSL -o /usr/local/bin/forgejo-runner \
  "https://code.forgejo.org/forgejo/runner/releases/download/v${RUNNER_VERSION}/forgejo-runner-${RUNNER_VERSION}-linux-amd64"
chmod +x /usr/local/bin/forgejo-runner
forgejo-runner --version
```

### 2.4 Register with the `nucleus-lab` label

The label after the colon selects the executor. `:host` runs jobs directly on
the VM, which is what these workflows assume — they rely on a persistent
`/var/cache/forgejo-runner` so that `cargo` does not rebuild the whole crate
from scratch on every run.

```bash
useradd --system --create-home --home-dir /var/lib/forgejo-runner forgejo-runner
install -d -o forgejo-runner -g forgejo-runner /var/cache/forgejo-runner/cargo
install -d -o forgejo-runner -g forgejo-runner /var/cache/forgejo-runner/target

sudo -u forgejo-runner -H bash -lc '
cd /var/lib/forgejo-runner
forgejo-runner register --no-interactive \
  --instance <FORGEJO_URL> \
  --token   <REGISTRATION_TOKEN> \
  --name    nucleus-lab \
  --labels  nucleus-lab:host
forgejo-runner generate-config > config.yml
'
```

In `config.yml`, set:

```yaml
runner:
  capacity: 1          # these jobs are I/O and CPU heavy; do not overlap them
  timeout: 24h         # must exceed nucleus-long.yml's timeout-minutes: 1440
  fetch_timeout: 30s
```

`capacity: 1` also keeps a benchmark from ever sharing the box with a soak.

### 2.5 Host toolchains

The `:host` executor runs jobs with the host's `PATH`. Install:

```bash
apt-get update && apt-get install -y \
    build-essential cmake pkg-config git curl \
    nodejs npm python3 python3-venv default-jdk \
    postgresql-17 postgresql-client-17

# Rust, as the runner user
sudo -u forgejo-runner -H bash -lc \
  'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
     --component clippy,rustfmt'
```

`node` is required even for the pure-Rust jobs: `actions/checkout` is a
JavaScript action, and `compat/*/run.sh` uses `node -e` to pick a free port.
PostgreSQL 17 is what makes `compat/pgregress` and `compat/copybinary` a real
differential rather than a SKIP.

Optional, and it closes the last hole in the M6 client matrix: the .NET SDK,
for Npgsql. Npgsql is currently untested purely for lack of a toolchain.

### 2.6 Run it as a service

```ini
# /etc/systemd/system/forgejo-runner.service
[Unit]
Description=Forgejo Actions runner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=forgejo-runner
WorkingDirectory=/var/lib/forgejo-runner
ExecStart=/usr/local/bin/forgejo-runner daemon --config /var/lib/forgejo-runner/config.yml
Restart=always
RestartSec=5
# The long workflow can run for a day.
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now forgejo-runner
systemctl status forgejo-runner
```

The runner appears under Site Administration → Actions → Runners with the label
`nucleus-lab`.

### 2.7 Prove it works

```bash
# From a clone with origin = Forgejo
git commit --allow-empty -m "ci: verify forgejo runner"
git push origin HEAD
```

Then confirm, in this order:

1. `<repo>/actions` lists `Nucleus gates` and nothing from `.github/workflows`.
2. The `Gates` job reaches "Toolchain versions" — that proves checkout,
   the action resolver, and the host `PATH` all work.
3. The `Doc metrics match source` step passes — that is the cheapest real gate
   in the repo and it fails loudly when docs drift.
4. `cargo clippy --all-targets -- -D warnings` passes. First run is a cold
   build, 20+ minutes. Second run should be minutes; if it is not, the
   `CARGO_TARGET_DIR` cache is not persisting and step 2.4's directories are
   wrong or unwritable.

### 2.8 If `actions/checkout` fails to resolve

Forgejo fetches JavaScript actions from `DEFAULT_ACTIONS_URL`, which defaults
to `https://data.forgejo.org` and mirrors `actions/checkout`. If the VM has no
route to it, either set an explicit mirror in `app.ini`:

```ini
[actions]
DEFAULT_ACTIONS_URL = github
```

or pin the full URL in each workflow
(`uses: https://github.com/actions/checkout@v4`).

## 3. Known limitations of these workflows

State these rather than discovering them later:

- **Not yet executed.** These files were written against the Forgejo Actions
  schema and validated only as YAML and against the workflow-schema rules
  (`scripts/check-workflows.py`). No job has run, because no runner exists.
  Section 2.7 is the acceptance test.
- **A single runner is a single failure domain.** It is also the machine the
  soak runs on. It proves nothing about multi-host behaviour: one host means
  one page cache, one fsync path, one clock and shared fate, so a green run
  here is weaker than it looks for anything durability- or timing-related.
- **The compat workflow is honest but weak until the toolchains are installed.**
  Every harness SKIPs rather than fails when its toolchain is missing, so a
  green run on a bare VM means "nothing ran".
- **No benchmark or regression budget lives here yet.** A budget on a runner
  that also runs soaks is noise. That needs a separate host that runs nothing
  else, with pinned cores, a fixed CPU governor and a dedicated disk.
