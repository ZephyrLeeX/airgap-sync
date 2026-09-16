# Offline release tooling

Reusable offline Release / Install / Upgrade / Rollback tooling for
Windows Server 2019 x64 (Source) and CentOS 7.9 x86_64 (Destination).
Full operator documentation: `docs/offline-deployment.md`.

## Files

| File | Purpose |
| --- | --- |
| `build_release.py` | Runs on a networked dev machine; builds both bundles into `dist/offline/` |
| `runtime-versions.json` | Python version source of truth, Windows external policy, and Linux runtime pin |
| `release_manifest.py` | Shared manifest/SHA256SUMS/schema helpers; copied into every bundle |
| `airgap-sync-deploy.sh` | Linux deploy script (bundled; CentOS 7 base tools only) |
| `airgap-sync-deploy.ps1` | Windows deploy script (bundled; PowerShell 5.1 compatible) |
| `service-examples/` | systemd unit examples and the Windows worker wrapper |

## Build (networked machine)

```bash
uv run python scripts/offline/build_release.py            # production bundle
uv run python scripts/offline/build_release.py --include-tests   # + pytest wheels
```

Requirements enforced by the builder: clean git tree, `uv.lock` present and
locked, all runtime dependencies resolve to binary wheels for both
`win_amd64` and `manylinux2014_x86_64` (any sdist-only dependency fails the
build), every non-universal Linux wheel carries at least one glibc <= 2.17
compatible platform tag (generic `linux_x86_64` / `musllinux` /
`manylinux_2_18+`-only wheels fail the build), and the bundled Linux runtime
checksum is verified against python-build-standalone upstream SHA256SUMS.
Windows bundles contain no Python installer or runtime archive; Windows
administrators provide the exactly pinned x64 Python through PATH.

## Deploy (offline machine)

Extract the bundle anywhere (USB stick, D:\, /tmp, ...) and run its deploy
script with `verify` first; see `docs/offline-deployment.md` for the full
install/upgrade/rollback walkthrough.

Install is for FIRST INSTALLS only: when `current` already points at a
different release the deploy scripts refuse with
`INSTALL_BLOCKED_EXISTING_DEPLOYMENT` (re-running the same release is an
idempotent no-op). Switching to a different release must go through
`upgrade`, which preserves worker stop, config/SQLite backups, the schema
guard and switch failure recovery.

Bundle-consuming actions verify `SHA256SUMS` before sourcing `release.env`;
SHA256SUMS entries must stay inside the bundle (no absolute paths, no `..`).
Windows install and upgrade validate the exact PATH Python version,
architecture, standard-library modules, and temporary venv creation before
creating the per-release venv. The Windows `current` switch stages a
GUID-named junction first and restores the old pointer if the switch fails.
Retired junctions are removed with the non-recursive .NET directory primitive
only after verifying that the path is a directory reparse point, so cleanup is
non-interactive and never traverses the release directory it targets.

For a production Windows Source, copy `service-examples/run-source-worker.ps1`
to the install root and run it from Task Scheduler as SYSTEM. The wrapper accepts
`-Config` and `-LogFile`, combines native stdout/stderr into the log, and returns
the worker's real native exit code. Recommended Task Scheduler settings are:
At startup, Run with highest privileges, Ignore new instances, restart after 2
minutes, and no execution time limit. The task starts one resident worker; the
7-day/retry cadence belongs in YAML under `schedule`, not in task triggers.
Upgrade with `-ScheduledTaskName "Airgap Sync Source Worker"` so the deploy
script stops and restarts the task around backup/install/switch.

## Tests

Unit tests live in `tests/test_offline_*.py`. Network downloads are never
part of the test suite; `tests/test_deploy_scripts.py` additionally runs the
real Linux deploy script end-to-end against a checksum-valid synthetic
bundle (install guard, upgrade ordering, SQLite backup, integrity gate), and
`bash -n` syntax-checks `airgap-sync-deploy.sh`. `airgap-sync-deploy.ps1` is
syntax-checked when `pwsh` is available and covered by static constraint
tests otherwise (real validation happens on the first Windows Server 2019
deployment).

## Field upgrade compatibility notes

- Release `6117c6d` had invalid `python -c` quote escaping under Windows
  PowerShell 5.1 in `Test-ExternalPython`; payloads now use PowerShell double
  quotes with Python single quotes.
- Its retired-junction cleanup could prompt because `Remove-Item` treated the
  link as a directory with children. Cleanup now unlinks only a verified
  directory reparse point, without recursion.
- Destination metadata inspection no longer assumes MySQL exposes
  `GENERATION_EXPRESSION`: the field is capability-probed and MySQL 5.6 uses a
  three-column metadata query. Source DDL is unchanged and still fails normally
  if the target server cannot execute it.
- Windows PowerShell 5.1 can wrap normal native stderr as `NativeCommandError`.
  The Source worker wrapper logs that stream without treating it as process
  failure; its result is determined by `$LASTEXITCODE`.
