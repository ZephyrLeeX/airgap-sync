# Offline release tooling

Reusable offline Release / Install / Upgrade / Rollback tooling for
Windows Server 2019 x64 (Source) and CentOS 7.9 x86_64 (Destination).
Full operator documentation: `docs/offline-deployment.md`.

## Files

| File | Purpose |
| --- | --- |
| `build_release.py` | Runs on a networked dev machine; builds both bundles into `dist/offline/` |
| `runtime-versions.json` | The single place where Python runtime versions/URLs are pinned |
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
`manylinux_2_18+`-only wheels fail the build), runtime checksums verified
against python-build-standalone upstream SHA256SUMS (python.org installers
carry no machine-readable checksum: a warning is printed and the artifact
hash is recorded in the bundle's SHA256SUMS instead).

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
Upgrades install the target Python runtime side-by-side BEFORE the SQLite
backup so Python patch upgrades (3.13.x -> 3.13.y) can always complete the
backup. The Windows `current` switch stages a GUID-named junction first and
restores the old pointer if the switch fails.

## Tests

Unit tests live in `tests/test_offline_*.py`. Network downloads are never
part of the test suite; `tests/test_deploy_scripts.py` additionally runs the
real Linux deploy script end-to-end against a checksum-valid synthetic
bundle (install guard, upgrade ordering, SQLite backup, integrity gate), and
`bash -n` syntax-checks `airgap-sync-deploy.sh`. `airgap-sync-deploy.ps1` is
syntax-checked when `pwsh` is available and covered by static constraint
tests otherwise (real validation happens on the first Windows Server 2019
deployment).
