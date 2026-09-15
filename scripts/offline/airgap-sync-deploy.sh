#!/usr/bin/env bash
# Airgap Sync offline deploy script - CentOS 7 x86_64 / any Linux with glibc >= 2.17.
#
# Actions:
#   verify            verify bundle integrity, platform and architecture
#   install           FIRST INSTALL ONLY; refuses when a different release
#                     is already current (use upgrade for that)
#   upgrade           install a new release side-by-side and switch current
#   rollback          re-point current to an earlier installed release
#   status            show install root, current release and installed releases
#   verify-installed  smoke-check the current release (no database access)
#   init-config       copy an example config (never overwrites)
#
# Usage examples:
#   ./airgap-sync-deploy.sh verify
#   ./airgap-sync-deploy.sh install
#   ./airgap-sync-deploy.sh upgrade --assume-worker-stopped
#   ./airgap-sync-deploy.sh upgrade --service-name airgap-sync-destination.service
#   ./airgap-sync-deploy.sh rollback --to-release 0.1.0-cc8e17a --assume-worker-stopped
#   ./airgap-sync-deploy.sh init-config --role destination
#
# Requires only CentOS 7 base tools (no jq/yq/python preinstalled).

set -euo pipefail

BUNDLE_ROOT="$(cd "$(dirname "$0")" && pwd)"

ACTION=""
INSTALL_ROOT="/opt/airgap-sync"
CONFIG_ROOT="/etc/airgap-sync"
DATA_ROOT="/var/lib/airgap-sync"
SERVICE_NAME=""
TO_RELEASE=""
ROLE=""
ASSUME_WORKER_STOPPED=0
SERVICE_WAS_STOPPED=0

usage() {
  cat <<'EOF'
Airgap Sync offline deploy script (CentOS 7 x86_64, glibc >= 2.17)

Usage: ./airgap-sync-deploy.sh <action> [options]

Actions:
  verify            verify bundle integrity, platform and architecture
  install           FIRST INSTALL ONLY; refuses when a different release
                    is already current (use upgrade for that)
  upgrade           install a new release side-by-side and switch current
  rollback          re-point current to an earlier installed release
  status            show install root, current release and installed releases
  verify-installed  smoke-check the current release (no database access)
  init-config       copy an example config (never overwrites)

Options:
  --install-root DIR    default /opt/airgap-sync
  --config-root DIR     default /etc/airgap-sync
  --data-root DIR       default /var/lib/airgap-sync (SQLite backup source)
  --service-name UNIT   systemctl unit to stop/start around upgrade/rollback
  --to-release ID       rollback target release id (required for rollback)
  --role source|destination  config template for init-config
  --assume-worker-stopped    confirm the worker is stopped when no service
                        manager coordinates the upgrade/rollback
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

stage() {
  echo "== $*"
}

# If we stopped a service for upgrade/rollback, restart it when the run fails.
on_exit() {
  rc=$?
  if [ "$rc" -ne 0 ] && [ "$SERVICE_WAS_STOPPED" -eq 1 ]; then
    echo "NOTE: attempting to restart $SERVICE_NAME after failed run" >&2
    systemctl start "$SERVICE_NAME" || true
  fi
  exit "$rc"
}
trap on_exit EXIT

while [ $# -gt 0 ]; do
  case "$1" in
    verify|install|upgrade|rollback|status|verify-installed|init-config)
      ACTION="$1" ;;
    --install-root) INSTALL_ROOT="$2"; shift ;;
    --config-root) CONFIG_ROOT="$2"; shift ;;
    --data-root) DATA_ROOT="$2"; shift ;;
    --service-name) SERVICE_NAME="$2"; shift ;;
    --to-release) TO_RELEASE="$2"; shift ;;
    --role) ROLE="$2"; shift ;;
    --assume-worker-stopped) ASSUME_WORKER_STOPPED=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
  shift
done

[ -n "$ACTION" ] || { usage; die "an action is required"; }

# Bundle-consuming actions must pass the checksum gate BEFORE any bundle
# metadata is read: SHA256SUMS is verified first and only then is release.env
# (executable metadata from the bundle's point of view) sourced. status and
# verify-installed operate on the installed tree only and merely use
# release.env best-effort when it happens to be present.
gate_bundle() {
  [ -f "$BUNDLE_ROOT/SHA256SUMS" ] \
    || die "SHA256SUMS not found next to this script; run from an extracted bundle"
  [ -f "$BUNDLE_ROOT/release.env" ] \
    || die "release.env not found next to this script; run from an extracted bundle"
  stage "Verify bundle checksums (gate before reading release.env)"
  (cd "$BUNDLE_ROOT" && sha256sum --check --quiet --strict SHA256SUMS) \
    || die "bundle integrity check failed; do not use this bundle"
  # shellcheck disable=SC1091
  . "$BUNDLE_ROOT/release.env"
}

case "$ACTION" in
  verify|install|upgrade|rollback|init-config)
    gate_bundle ;;
  status|verify-installed)
    if [ -f "$BUNDLE_ROOT/release.env" ]; then
      # shellcheck disable=SC1091
      . "$BUNDLE_ROOT/release.env"
    fi
    ;;
esac

RUNTIME_DIR="$INSTALL_ROOT/runtimes/python-${AIRGAP_PYTHON_VERSION:-unknown}"
RUNTIME_PY="$RUNTIME_DIR/python/bin/python3"
RELEASES_DIR="$INSTALL_ROOT/releases"
CURRENT="$INSTALL_ROOT/current"
RELEASE_DIR="$RELEASES_DIR/${AIRGAP_RELEASE_ID:-unknown}"
VENV="$RELEASE_DIR/venv"
VENV_PY="$VENV/bin/python"
APP_CLI="$VENV/bin/airgap-sync"

# version_ge A B: exit 0 when A >= B (numeric, up to 3 components).
version_ge() {
  awk -v a="$1" -v b="$2" 'BEGIN {
    split(a, av, "."); split(b, bv, ".");
    for (i = 1; i <= 3; i++) {
      an = (i in av) ? av[i] + 0 : 0;
      bn = (i in bv) ? bv[i] + 0 : 0;
      if (an > bn) exit 0;
      if (an < bn) exit 1;
    }
    exit 0;
  }'
}

check_platform() {
  stage "Check platform (require x86_64, glibc >= ${AIRGAP_MINIMUM_GLIBC:-2.17})"
  local arch glibc
  arch="$(uname -m)"
  [ "$arch" = "x86_64" ] || die "this machine is $arch; this bundle requires x86_64"
  glibc="$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}')" \
    || die "cannot determine glibc version (getconf GNU_LIBC_VERSION failed)"
  version_ge "$glibc" "${AIRGAP_MINIMUM_GLIBC:-2.17}" \
    || die "glibc $glibc is older than required ${AIRGAP_MINIMUM_GLIBC:-2.17}"
  echo "  arch=$arch glibc=$glibc"
}

check_bundle_layout() {
  [ -n "${AIRGAP_PLATFORM_OS:-}" ] || die "release.env is missing AIRGAP_PLATFORM_OS"
  [ "$AIRGAP_PLATFORM_OS" = "linux" ] \
    || die "this bundle targets os=$AIRGAP_PLATFORM_OS; it cannot be installed on linux"
  [ "${AIRGAP_PLATFORM_ARCH:-}" = "x86_64" ] \
    || die "this bundle targets arch=${AIRGAP_PLATFORM_ARCH:-?}, not x86_64"
  [ -f "$BUNDLE_ROOT/release.json" ] || die "release.json missing"
  [ -f "$BUNDLE_ROOT/SHA256SUMS" ] || die "SHA256SUMS missing"
  [ -f "$BUNDLE_ROOT/runtime/$AIRGAP_RUNTIME_ARTIFACT" ] \
    || die "runtime/$AIRGAP_RUNTIME_ARTIFACT missing"
  [ -f "$BUNDLE_ROOT/app/$AIRGAP_APP_WHEEL" ] || die "app/$AIRGAP_APP_WHEEL missing"
  ls "$BUNDLE_ROOT"/wheelhouse/*.whl >/dev/null 2>&1 \
    || die "wheelhouse contains no wheels"
}

# Metadata + integrity only; platform check stays a separate install stage.
verify_bundle_action() {
  stage "Verify release metadata"
  check_bundle_layout
  echo "  release=$AIRGAP_RELEASE_ID app=$AIRGAP_APP_VERSION python=$AIRGAP_PYTHON_VERSION"
  stage "Verify SHA256SUMS"
  (cd "$BUNDLE_ROOT" && sha256sum --check --quiet --strict SHA256SUMS) \
    || die "bundle integrity check failed; do not install this bundle"
}

do_verify() {
  verify_bundle_action
  check_platform
  echo "Bundle verification OK"
}

install_runtime() {
  stage "Install Python runtime $AIRGAP_PYTHON_VERSION (shared across releases)"
  if [ -f "$RUNTIME_DIR/.runtime-installed" ]; then
    echo "  runtime already installed at $RUNTIME_DIR, reusing"
  else
    if [ -d "$RUNTIME_DIR" ]; then
      echo "  incomplete runtime found, re-extracting"
      rm -rf "$RUNTIME_DIR"
    fi
    mkdir -p "$RUNTIME_DIR"
    tar -xzf "$BUNDLE_ROOT/runtime/$AIRGAP_RUNTIME_ARTIFACT" -C "$RUNTIME_DIR"
    [ -x "$RUNTIME_PY" ] || die "runtime python not found at $RUNTIME_PY after extraction"
    "$RUNTIME_PY" "$BUNDLE_ROOT/release_manifest.py" smoke-runtime
    touch "$RUNTIME_DIR/.runtime-installed"
  fi
  # Always confirm this bundle's release.env and release.json agree, even when
  # the shared runtime is reused: the bundle is new even though the runtime
  # is not.
  "$RUNTIME_PY" "$BUNDLE_ROOT/release_manifest.py" check-env "$BUNDLE_ROOT" \
    || die "release.env and release.json disagree; this bundle is inconsistent"
}

# release_state DIR: missing | incomplete | complete (bash-only, no python).
release_state() {
  if [ -f "$1/installed.json" ]; then
    echo complete
  elif [ -d "$1" ]; then
    echo incomplete
  else
    echo missing
  fi
}

current_release_id() {
  [ -L "$CURRENT" ] || return 1
  basename "$(readlink "$CURRENT")"
}

switch_current() {
  local target_dir="$RELEASES_DIR/$1" tmp="$INSTALL_ROOT/.current.new"
  [ -d "$target_dir" ] || die "cannot switch current: $target_dir does not exist"
  rm -f "$tmp"
  ln -s "$target_dir" "$tmp"
  mv -T "$tmp" "$CURRENT"
}

ensure_release_installed() {
  local state
  state="$(release_state "$RELEASE_DIR")"
  if [ "$state" = "complete" ]; then
    echo "  release $AIRGAP_RELEASE_ID already installed, reusing venv"
    return 0
  fi
  if [ "$state" = "incomplete" ]; then
    echo "  incomplete install detected, rebuilding $RELEASE_DIR"
    rm -rf "$RELEASE_DIR"
  fi
  mkdir -p "$RELEASE_DIR"

  stage "Create venv for $AIRGAP_RELEASE_ID"
  "$RUNTIME_PY" -m venv "$VENV"

  stage "Offline pip install (app wheel + locked dependencies)"
  PIP_NO_INDEX=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    "$VENV_PY" -m pip install --no-index --find-links "$BUNDLE_ROOT/wheelhouse" \
    "$BUNDLE_ROOT/app/$AIRGAP_APP_WHEEL" \
    || die "offline pip install failed"

  stage "Smoke test application imports"
  "$VENV_PY" "$BUNDLE_ROOT/release_manifest.py" smoke-app
  local version_output
  version_output="$("$APP_CLI" --version)" || die "airgap-sync --version failed"
  case "$version_output" in
    *"$AIRGAP_APP_VERSION"*) ;;
    *) die "unexpected --version output: $version_output" ;;
  esac
  echo "  $version_output"

  stage "Write release metadata"
  "$RUNTIME_PY" "$BUNDLE_ROOT/release_manifest.py" mark-installed \
    "$RELEASE_DIR" "$BUNDLE_ROOT/release.json"
  cp -p "$BUNDLE_ROOT/release_manifest.py" "$RELEASE_DIR/release_manifest.py"
}

stop_worker() {
  if [ -n "$SERVICE_NAME" ]; then
    stage "Stop worker service $SERVICE_NAME"
    systemctl stop "$SERVICE_NAME" || die "systemctl stop $SERVICE_NAME failed"
    SERVICE_WAS_STOPPED=1
  else
    [ "$ASSUME_WORKER_STOPPED" -eq 1 ] \
      || die "refusing to continue: the worker may still be running. Pass --service-name <unit> or --assume-worker-stopped."
    echo "  assuming worker already stopped (--assume-worker-stopped)"
  fi
}

start_worker() {
  if [ "$SERVICE_WAS_STOPPED" -eq 1 ]; then
    stage "Start worker service $SERVICE_NAME"
    systemctl start "$SERVICE_NAME" || die "systemctl start $SERVICE_NAME failed"
  fi
}

backup_configs() {
  [ -d "$CONFIG_ROOT" ] || return 0
  ls "$CONFIG_ROOT"/*.yaml >/dev/null 2>&1 || return 0
  local dest="$CONFIG_ROOT/backups/$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$dest"
  cp -p "$CONFIG_ROOT"/*.yaml "$dest/"
  echo "  config backup: $dest"
}

backup_sqlite() {
  local db="$DATA_ROOT/state/meta.db"
  if [ ! -f "$db" ]; then
    echo "  no SQLite state at $db, skipping backup"
    return 0
  fi
  local dest="$DATA_ROOT/backups/$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$dest"
  [ -x "$RUNTIME_PY" ] || die "runtime python required for SQLite backup is missing"
  "$RUNTIME_PY" - "$db" "$dest/meta.db" <<'PYEOF' || die "SQLite backup failed"
import sqlite3
import sys

source = sqlite3.connect(sys.argv[1])
target = sqlite3.connect(sys.argv[2])
with target:
    source.backup(target)
source.close()
target.close()
PYEOF
  echo "  SQLite backup: $dest/meta.db"
}

print_next_steps() {
  echo
  echo "Next steps (worker NOT started automatically):"
  echo "  1. ./airgap-sync-deploy.sh init-config --role source   # or destination"
  echo "  2. Edit $CONFIG_ROOT/<role>.yaml (secrets stay in env vars)"
  echo "  3. Set the password/token environment variables"
  echo "  4. $CURRENT/venv/bin/airgap-sync config validate --config ..."
  echo "  5. airgap-sync source check | source relay-check | source sync --table SMALL_TABLE"
  echo "     airgap-sync destination check | destination process --run RUN_ID"
}

do_install() {
  stage "[1/6] Verify release bundle"
  verify_bundle_action
  stage "[2/6] Check platform"
  check_platform
  stage "[3/6] Check for existing deployment"
  if [ -e "$CURRENT" ] || [ -L "$CURRENT" ]; then
    local current
    current="$(current_release_id || true)"
    if [ "$current" = "$AIRGAP_RELEASE_ID" ] \
      && [ "$(release_state "$RELEASE_DIR")" = "complete" ]; then
      echo "  release $AIRGAP_RELEASE_ID is already installed and current; nothing to do"
      echo "Install complete (no-op): current -> $AIRGAP_RELEASE_ID"
      return 0
    fi
    # Install is for first installs only: switching to a different release
    # here would silently bypass worker stop, config/SQLite backups, the
    # schema guard and upgrade failure recovery.
    echo "Existing deployment detected." >&2
    echo "Use upgrade instead of install." >&2
    die "INSTALL_BLOCKED_EXISTING_DEPLOYMENT: current -> ${current:-<broken pointer>}, this bundle installs $AIRGAP_RELEASE_ID"
  fi
  echo "  no existing deployment; proceeding with first install"
  stage "[4/6] Install Python runtime"
  install_runtime
  stage "[5/6] Create release directory $RELEASE_DIR"
  ensure_release_installed
  stage "[6/6] Switch current"
  switch_current "$AIRGAP_RELEASE_ID"
  echo "Install complete: current -> $AIRGAP_RELEASE_ID"
  print_next_steps
}

do_upgrade() {
  stage "[1/9] Verify release bundle"
  verify_bundle_action
  stage "[2/9] Check platform"
  check_platform
  stage "[3/9] Stop worker"
  stop_worker
  local current
  if current="$(current_release_id)"; then
    echo "  current release: $current"
    if [ "$current" = "$AIRGAP_RELEASE_ID" ]; then
      echo "  $AIRGAP_RELEASE_ID is already current; nothing to do"
      start_worker
      return 0
    fi
  else
    echo "  no current release (first install via upgrade)"
  fi
  stage "[4/9] Backup config"
  backup_configs
  # Install the TARGET runtime before the SQLite backup: the backup runs under
  # the target bundle's python, which does not exist yet on a Python patch
  # upgrade (e.g. 3.13.15 -> 3.13.16). Installing this side-by-side runtime
  # is safe before the backup: it runs no application code and migrates no
  # metadata. Everything that touches application state (new venv, schema
  # guard, current switch) still happens only after the backup.
  stage "[5/9] Install target Python runtime (side-by-side, shared)"
  install_runtime
  stage "[6/9] Backup Source SQLite state (when present)"
  backup_sqlite
  stage "[7/9] Install new release"
  ensure_release_installed
  stage "[8/9] Schema compatibility check"
  if [ -f "$RELEASES_DIR/$current/installed.json" ]; then
    "$RUNTIME_PY" "$BUNDLE_ROOT/release_manifest.py" schema-check \
      "$RELEASES_DIR/$current/installed.json" "$BUNDLE_ROOT/release.json" \
      || die "schema compatibility check failed; current release left unchanged"
  else
    echo "  no previous release metadata, skipping"
  fi
  stage "[9/9] Switch current"
  switch_current "$AIRGAP_RELEASE_ID"
  start_worker
  echo "Upgrade complete: current -> $AIRGAP_RELEASE_ID (previous: ${current:-none})"
}

do_rollback() {
  [ -n "$TO_RELEASE" ] || die "rollback requires --to-release <release-id>"
  local target_dir="$RELEASES_DIR/$TO_RELEASE"
  stage "[1/5] Stop worker"
  stop_worker
  stage "[2/5] Check target release"
  [ -d "$target_dir" ] || die "release $TO_RELEASE is not installed under $RELEASES_DIR"
  [ "$(release_state "$target_dir")" = "complete" ] \
    || die "release $TO_RELEASE has no completion marker (incomplete install)"
  local current
  current="$(current_release_id || true)"
  echo "  rollback: ${current:-none} -> $TO_RELEASE"
  stage "[3/5] Schema compatibility check"
  if [ -n "$current" ] && [ -f "$RELEASES_DIR/$current/installed.json" ]; then
    [ -x "$RUNTIME_PY" ] || die "runtime python required for schema check is missing"
    "$RUNTIME_PY" "$BUNDLE_ROOT/release_manifest.py" schema-check \
      "$RELEASES_DIR/$current/installed.json" "$target_dir/installed.json" \
      || die "ROLLBACK BLOCKED: target release uses older metadata schemas"
  else
    echo "  no current release metadata, skipping schema check"
  fi
  stage "[4/5] Switch current (new release directory is kept)"
  switch_current "$TO_RELEASE"
  stage "[5/5] Start worker"
  start_worker
  echo "Rollback complete: current -> $TO_RELEASE"
}

do_status() {
  echo "Install root      $INSTALL_ROOT"
  local current python
  current="$(current_release_id || true)"
  echo "Current release   ${current:-(none)}"
  python=""
  [ -x "$RUNTIME_PY" ] && python="$RUNTIME_PY"
  if [ -n "$current" ] && [ -z "$python" ] && [ -x "$RELEASES_DIR/$current/venv/bin/python" ]; then
    python="$RELEASES_DIR/$current/venv/bin/python"
  fi
  if [ -n "$current" ] && [ -n "$python" ]; then
    "$python" - "$RELEASES_DIR/$current/installed.json" <<'PYEOF'
import json
import sys

with open(sys.argv[1]) as handle:
    data = json.load(handle)
print("App version       %s" % data["app_version"])
print("Git commit        %s" % data["git_commit"])
print("Python version    %s" % data["python_version"])
print("Source schema     %s" % data["source_state_schema"])
print("Destination schema %s" % data["destination_metadata_schema"])
PYEOF
  fi
  echo "Installed releases:"
  if [ -d "$RELEASES_DIR" ]; then
    local dir name marker
    for dir in "$RELEASES_DIR"/*; do
      [ -d "$dir" ] || continue
      name="$(basename "$dir")"
      if [ -f "$dir/installed.json" ]; then marker="complete"; else marker="INCOMPLETE"; fi
      if [ "$name" = "${current:-}" ]; then
        echo "  $name  $marker  <- current"
      else
        echo "  $name  $marker"
      fi
    done
  else
    echo "  (none)"
  fi
}

do_verify_installed() {
  stage "Verify installed release"
  local current
  current="$(current_release_id || true)" || die "current pointer is missing"
  [ -n "$current" ] || die "current pointer is missing; run install first"
  local release_dir="$RELEASES_DIR/$current"
  [ -f "$release_dir/installed.json" ] || die "current release has no completion marker"
  local venv_py="$release_dir/venv/bin/python"
  local app_cli="$release_dir/venv/bin/airgap-sync"
  [ -x "$venv_py" ] || die "venv python missing: $venv_py"
  [ -x "$app_cli" ] || die "airgap-sync CLI missing: $app_cli"
  "$venv_py" "$release_dir/release_manifest.py" smoke-app || die "application imports failed"
  local version_output
  version_output="$("$app_cli" --version)" || die "airgap-sync --version failed"
  echo "  $version_output"
  echo "  current -> $current"
  echo "Installed release verification OK (no database connection was made)"
}

do_init_config() {
  [ -n "$ROLE" ] || die "init-config requires --role source|destination"
  [ "$ROLE" = "source" ] || [ "$ROLE" = "destination" ] \
    || die "role must be source or destination, got: $ROLE"
  local example="$BUNDLE_ROOT/config/$ROLE.example.yaml"
  local target="$CONFIG_ROOT/$ROLE.yaml"
  [ -f "$example" ] || die "example config missing in bundle: $example"
  if [ -e "$target" ]; then
    die "refusing to overwrite existing config: $target (edits are never touched by upgrades)"
  fi
  mkdir -p "$CONFIG_ROOT"
  cp -p "$example" "$target"
  chmod 600 "$target"
  echo "Config initialized: $target"
  echo "Edit it, set the password/token environment variables, then run:"
  echo "  $CURRENT/venv/bin/airgap-sync config validate --config $target"
}

case "$ACTION" in
  verify) do_verify ;;
  install) do_install ;;
  upgrade) do_upgrade ;;
  rollback) do_rollback ;;
  status) do_status ;;
  verify-installed) do_verify_installed ;;
  init-config) do_init_config ;;
  *) die "unhandled action: $ACTION" ;;
esac
