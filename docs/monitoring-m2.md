# Monitoring M2 — Source Telemetry Reporter

M2 adds a separate one-shot Source command. It reads Source SQLite metadata and local host facts, uploads one JSON file to Relay, then exits. It never takes the Source worker lock, starts the worker, opens business MySQL, or changes Run state. M3 ingestion, M4 alerts, M5 timeline, and M6 instrumentation are not implemented. Relay acceptance is **not** Destination ingestion.

## Configuration and manual check

Add to `source.yaml` (existing `relay` and `paths.data_dir` remain required):

```yaml
monitoring:
  node_id: source-01
  worker_task_name: Airgap Sync Source Worker
  collect_cpu: true
  collect_memory: true
  managed_storage_interval: 1h
  log_dirs:
    - 'C:\Program Files\AirgapSync\logs'
  upload_connect_timeout_seconds: 3
  upload_read_timeout_seconds: 10
  upload_max_attempts: 2
```

`node_id` is 1–64 ASCII letters, digits, `_` or `-`, beginning with a letter or digit. Relay token is read only from `relay.token_env` under the execution account. No Monitoring configuration is required for the existing worker. Run manually:

```powershell
& 'C:\Program Files\AirgapSync\current\venv\Scripts\airgap-sync.exe' source monitor-report --config 'C:\ProgramData\AirgapSync\config\source.yaml'
```

Exit 0 means the command was invoked with valid configuration, including when collection/upload failed. Partial collection logs one bounded warning containing unique allowlisted `collection_errors` codes and still uploads available measurements. If no valid payload can be built, collection failure is logged once and upload is reported as skipped. Collection and upload outcomes are independent; successful upload does not assert complete collection. No payload, raw exception, token or password is logged. A failed upload logs a generic warning without URL, token, response body or raw exception. Exit 1 is for invalid command parameters, wrong role, missing monitoring/relay section, invalid YAML, or missing token environment variable. The success message says only that Relay accepted the file. 429, timeout and connection errors have at most two attempts by default, with short per-request timeouts. No heartbeat enters the business retry queue.

## Windows scheduled task

Copy `service/run-source-monitor.ps1` from the offline bundle to the install root, alongside the existing worker wrapper. Run the bundled installer from an elevated PowerShell session, adjusting paths. The task runs as SYSTEM. This account needs read access to the YAML, SQLite and storage directories, write access to `<data_dir>\monitor` and the wrapper log, and its own `relay.token_env` environment variable. A `setx` value in another user's profile is not inherited by SYSTEM. Provision the token through the existing host secret mechanism for this account; do not add it to task arguments.

```powershell
& '.\service\install-source-monitor-task.ps1' -InstallRoot 'C:\Program Files\AirgapSync' -Config 'C:\ProgramData\AirgapSync\config\source.yaml'
Start-ScheduledTask -TaskName 'Airgap Sync Source Monitor'
Get-ScheduledTask -TaskName 'Airgap Sync Source Monitor' | Select-Object TaskName,State
Get-ScheduledTaskInfo -TaskName 'Airgap Sync Source Monitor' | Select-Object LastRunTime,LastTaskResult
```

The monitor task is independent of `Airgap Sync Source Worker` and uses `IgnoreNew` for overlapping invocations. The installer does not change the running worker task. Check the wrapper log and Relay receipt during installation. PowerShell task creation and process discovery are covered by fixtures/static tests here; they require a Windows Server field check.

### Explicit project log directories

`monitoring.log_dirs` is optional and defaults to `[]` (unknown `logs_bytes`). Set it to the absolute, non-overlapping directories dedicated to this project's logs, at most eight. Reporter does not infer the installation root or use `<data_dir>/logs` as a fallback. Both PowerShell wrappers default to `<InstallRoot>\logs`, independently of `data_dir`. When using `-LogFile`, list its parent directory; if Worker and Monitor log to different directories, list both. Never list a shared system log directory, drive root or installation root. An explicitly configured directory that genuinely does not exist contributes zero; inaccessible directories or linked roots make the category unknown.

Example with a custom installation and separate data/log locations:

```yaml
paths:
  data_dir: 'E:\AirgapData'
monitoring:
  node_id: source-01
  log_dirs:
    - 'F:\AirgapLogs'
```

Configure the existing Worker task's wrapper argument as `-LogFile 'F:\AirgapLogs\source-worker.log'`. Install the Monitor task with the same project log directory:

```powershell
& '.\service\install-source-monitor-task.ps1' -InstallRoot 'D:\AirgapApp' -Config 'C:\ProgramData\AirgapSync\config\source.yaml' -LogFile 'F:\AirgapLogs\source-monitor.log'
```

These examples do not change or restart the business Worker. An existing deployment must explicitly supply its actual log directories in the YAML.

## Wire format and sources

The PUT body is UTF-8 JSON, `schema_version: 1`, at most 64 KiB. Filename: `airgap-monitor-v1--<node_id>--<UTC YYYYMMDDTHHMMSSZ>--<random 8 hex>.json`. The existing Bearer PUT, SHA256 header and 201 confirmation checks are reused. Destination's current manifest discovery ignores this name. The Relay server is external to this repository; verify its filename/extension and size allowlist accepts it before field use.

| Field | Source and meaning |
| --- | --- |
| `node_id`, `role`, `captured_at`, `agent_version` | Configured ID, `SOURCE`, UTC capture time, packaged application version. No invented build hash. |
| `host`, `system` | Local OS, hostname, uptime/boot time, Windows CIM CPU and memory; Linux `/proc` fallback. Unavailable measurements are null. |
| `worker` | Windows Task Scheduler state independently from matching Source worker process command, `--config` path and PID. Absolute configuration paths are compared after Windows lexical normalization (case, separators, `.`/`..`), with quoted arguments and both `--config PATH` and `--config=PATH`. Relative Worker paths and unreadable candidate command lines give `UNKNOWN`; Reporter never supplies its own cwd for a Worker path. A matched console launcher/Python chain (including the Windows venv redirector) with the same known entrypoint and configuration counts as one Worker, reporting the final child PID; multiple independent matches give `UNKNOWN`. Reporter and unrelated commands are excluded. Query failure gives `UNKNOWN`; Linux gives `UNKNOWN`. |
| `cycle`, `next_action_at` | Latest persisted Cycle and bounded cycle-table status counts; retry or completion schedule. A running Cycle's action time is capture time. |
| `current_run`, `last_run` | Latest active and latest persisted Run; final row/chunk/byte counters and artifact upload aggregates. Null means unrecorded, including live rows during generation. |
| `filesystems` | Actual system, executable, config, data and temp locations, grouped by volume/device. A failed volume is null. |
| `managed_storage` | Bounded scans of project-owned `outbox`, `backups`, `spool` beneath `data_dir` plus explicit `monitoring.log_dirs`. Unconfigured logs are null; explicitly selected missing directories are zero. Failed Runs live within `outbox`, so `failed_runs_bytes` remains null instead of double counting or scanning unlimited Run history. |
| `connectivity` | `UNKNOWN` because Reporter makes no separate Source MySQL probe and upload outcome is known only after serialization. |
| `collection_errors` | Bounded generic codes only; no raw SQLite, subprocess or HTTP error text. |

SQLite opens with a one-second busy timeout and read-only URI. Missing, busy or incompatible metadata produces a generic collection code while host information remains available. It never initializes or migrates SQLite, queries a business table, or scans rows for progress. `UNKNOWN` is an observation state; null is an absent measurement; numeric zero is a real recorded zero. Do not infer Source liveness from recent sync time.

Managed storage cache is `<data_dir>/monitor/storage-cache.json`, atomically replaced after a scan and stamped in UTC. The default scan interval is one hour. Each directory-tree scan checks the 15-second and 100,000-entry budgets between entry enumeration and metadata calls, including inside a single large directory. No partial category total is returned. Child symlinks and Windows reparse points (including junctions) are skipped; linked roots or ancestors are rejected. These are cooperative checks: a single blocked filesystem call cannot be forcibly interrupted, and filesystem changes during a scan are not an atomic snapshot. Unknown/failed categories are cached for the same interval, so a five-minute task does not repeatedly rescan a failed tree. The cache includes a local-only fingerprint of all scan paths; changing log directories invalidates it immediately, including older caches without a fingerprint. The cache is limited to 4 KiB on read and a corrupt/stale cache triggers a rescan. If the cache directory is not writable, storage measurements stay null and the scan is skipped. The payload's temporary file is in `<data_dir>/monitor/tmp` and is removed after the attempt. Each invocation removes abandoned monitor temp directories older than one day. Neither cache nor temporary telemetry goes into business SQLite or the sync spool.

## Regression coverage and field checks

Fixture tests exercise Windows argument parsing, path identity and launcher/child grouping; simulated directory entries and a controllable clock exercise budgets, nesting, reparse points, failure caching and log-path changes. CLI tests cover partial collection with accepted/rejected uploads, unique bounded warnings and secret suppression. These are not Windows field acceptance. Verify actual CIM visibility and launcher ancestry under SYSTEM, PowerShell task arguments (including spaces and custom logs), NTFS junction behavior, and the external Relay filename/size allowlist and 201 confirmation at deployment. Destination ingestion remains outside M2.

Process-chain handling is based on [distlib's Windows launcher](https://github.com/pypa/distlib/blob/master/PC/launcher.c) and [CPython 3.12's venv redirector](https://github.com/python/cpython/blob/3.12/PC/launcher.c). Argument parsing follows [Microsoft CRT quoting rules](https://learn.microsoft.com/en-us/cpp/c-language/parsing-c-command-line-arguments).
