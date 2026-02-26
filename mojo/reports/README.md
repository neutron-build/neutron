# Mojo Validation Reports

This directory stores generated validation outputs from:

- `mojo/scripts/validate-core.sh`
- `mojo/scripts/validate-core.ps1`

Generated artifacts:

- `core-validation-latest.md` (latest summary)
- `core-validation-<timestamp>.md` (run snapshot)
- `core-validation-<timestamp>.csv` (per-test statuses)
- `core-validation-<timestamp>.log` (full command output)

List-only inventory (no execution):

- Bash: `bash mojo/scripts/validate-core.sh --list-only`
- PowerShell: `pwsh mojo/scripts/validate-core.ps1 -ListOnly`

Full execution:

- Bash: `bash mojo/scripts/validate-core.sh`
- PowerShell: `pwsh mojo/scripts/validate-core.ps1`

Windows note:

- If your Mojo toolchain lives in a Linux pixi environment (`.pixi/.../bin/mojo`),
  run the bash validator from WSL instead of native PowerShell.
