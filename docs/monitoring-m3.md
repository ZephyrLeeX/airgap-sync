# Monitoring M3 — Telemetry Ingest + monitor.db

M3 adds Source observations to the independent Destination Monitor service. It consumes the
**current M2 Reporter** protocol; no Source change is needed. Business Worker state machines,
Source `meta.db`, Destination `airgap_sync_meta`, and remote Relay files are unchanged.
M1 Destination facts remain available independently. M4 alerts, M5 full timelines/performance,
M6 instrumentation and web control actions are outside this change.

## Configuration and launch

Destination Linux only; `monitor_ingest` is opt-in. Omit it for existing M1 deployments.
Source `monitoring` retains its existing meaning and validation. The business Destination
Worker accepts but never uses `monitor_ingest`.

```yaml
monitor_ingest:
  incoming: /var/lib/airgap-sync/incoming
  db_path: /var/lib/airgap-sync-monitor/monitor.db
  poll_seconds: 10
  batch_size: 100
  scan_limit: 2000
  settle_seconds: 30
  invalid_grace_seconds: 300
  history_days: 35
  dedup_days: 90
  quarantine_days: 7
  quarantine_max_files: 1000
  future_seconds: 300
```

Paths must be absolute; the database filename must be `monitor.db`, in a dedicated Monitor
directory. The DB parent may be the same directory as a dedicated telemetry incoming directory.
It must never be a renamed or linked business database. Use a local Linux filesystem supporting
SQLite WAL, atomic rename and `flock`, not an NFS/FTP mount. The incoming path can equal the
business incoming path. FTP should download locally there. The service account needs directory
read/write/execute access, and access to its dedicated database directory. Ancestor symlinks
are rejected. Private processing/quarantine directories must not be group/world writable.
Protect their parents and lock files from other local users; the FTP producer should only
write its delivery files, not Monitor directories or lock files. Do not delete lock files while
Monitor is running. Lock ownership is kernel-managed and released on exit/crash.

```bash
install -d -m 0700 /var/lib/airgap-sync-monitor
/opt/airgap-sync/current/venv/bin/airgap-sync destination monitor-web \
  --config /etc/airgap-sync/destination.yaml --host 127.0.0.1 --port 8080
```

Use the bundled `service/airgap-sync-monitor.service.example` as
`/etc/systemd/system/airgap-sync-monitor.service`, adjust its EnvironmentFile/account/paths,
then `systemctl daemon-reload` and `systemctl enable --now airgap-sync-monitor`.
The example uses `ExecStartPre=install -d` for CentOS 7 compatibility rather than newer
systemd `StateDirectory`. A custom db path requires adjusting this command.
The separate Destination Worker service does not depend on Monitor.

For an isolated diagnostic round, stop **only Monitor**, then run:

```bash
airgap-sync destination monitor-ingest --config /etc/airgap-sync/destination.yaml
# Invoke again after settle_seconds; a first observation is deliberately not ingested.
airgap-sync destination monitor-ingest --config /etc/airgap-sync/destination.yaml
```

The CLI reports bounded counters; exit 1 means unavailable/locked or file I/O/database errors.
`waiting` is not an error. Invalid/conflicting input is counted as `rejected` after its grace
period. Restart Monitor afterward. The service runs without any browser requests. One thread
owns ingestion for its lifespan; shutdown signals its event and joins it off the async loop.
Filesystem/SQLite work is in that thread; synchronous FastAPI read handlers run in the threadpool.
Single-round failure does not stop subsequent rounds. Initialization/lock failure is retried.
Use one Web process by default. Multiple processes contend on **Monitor-only** locks in the
DB directory and inbox. Directory identity is checked by device and inode, so one physical
directory is locked once. A shared inbox excludes a second ingestor even with a different DB;
a shared DB excludes a second ingestor even with a different inbox. Independent directory pairs
can ingest concurrently. Locks are non-blocking and released with their file descriptors on
normal exit or initialization failure. Contending processes can serve read-only queries and retry
ownership. No business Worker lock is acquired. `/api/dashboard` reports ingest ownership/health
separately from Source observations; these are not M4 heartbeat alarms.

## Discovery, completeness and failure recovery

Only complete names matching this grammar are considered:

```
airgap-monitor-v<1–3 decimal digits>--<node_id>--<YYYYMMDDTHHMMSSZ>--<8 lowercase hex>.json
```

`node_id` is 1–64 ASCII letters/digits/underscore/hyphen, starting with a letter/digit.
Version 1 is the only accepted protocol. Broader version discovery allows unknown versions
to be rejected once instead of retried forever. Malformed names, temporary extensions,
business manifests/chunks, symlinks, directories and other special files are left untouched.
Malformed names require external housekeeping; Monitor cannot safely infer their ownership.

Each round enumerates at most `scan_limit` entries and attempts at most `batch_size` candidates
**in each** of incoming and `.airgap-monitor-processing` (defaults: 4,000 entries / 200 attempts
total). Persistent directory cursors provide eventual traversal despite unrelated business
files or permanently unreadable candidates. A restart/one-shot restarts enumeration; repeatedly
restarting in a huge shared directory can delay later entries. Prefer the service or a separate
telemetry incoming directory for large installations. Memory and individual reads are bounded.

The DB persists stability observations: device, inode, actual file size, mtime and ctime must
match on at least two observations separated by `settle_seconds`. Incoming and processing
have separate observation identities. Stable candidates are atomically moved within the
incoming filesystem to private processing; an existing processing file is never overwritten.
Claimed inode/size/mtime are rechecked. A replacement is retained for independent observation.
The file is opened with `O_NOFOLLOW`, regular-file identity is checked, and at most **65,537
actual bytes** are read, irrespective of stat size. Identity is rechecked after reading and
before deletion. Once moved, a new delivery at the original incoming name cannot be deleted
by completion of the claimed file. Pending processing files survive restart.

FTP should write a temporary filename, close it, then atomically rename to the final name.
Final-name direct writes are supported conservatively by stability observation and an extra
`invalid_grace_seconds` (default five minutes) before rejection. Changes restart stability.
Moving a file does not stop an FTP writer holding its inode open. Cooperative fingerprint
checks detect ongoing writes but cannot prove that a paused writer will never resume; a pause
longer than the configured grace can lead to rejection of incomplete input. Atomic rename is
the reliable completion contract. Increase delays for legacy FTP clients and verify their
actual behavior onsite. Single filesystem calls cannot be forcibly interrupted, so bounds on
entries/bytes do not promise a hard wall-clock deadline on a stalled filesystem.

Successful transaction commit precedes deletion. Delete failure leaves a recoverable file;
replay after a crash is deduplicated. DB locking/write failure retains input (possibly in
processing). A bad file does not block other candidates. A failed file mutation is never logged
with its raw exception, payload, host strings or arbitrary input.

Permanent invalid/version/conflict files use **metadata-only quarantine**, an intentional
space-bounded alternative to retaining untrusted payloads: after grace, write a small record
containing safe transport identity, allowlisted `INVALID_TELEMETRY` or `TRANSFER_CONFLICT`, and rejection time, then
remove the same processed inode. The original invalid bytes are not retained. Records under
`.airgap-monitor-quarantine` expire after seven days; count is capped at 1,000, with at most two
per-directory batches of temporary slack until the next round. An interrupted rejection can
leave an extra record, still subject to these bounds. Processing retains failed DB inputs;
this backlog is intentionally not discarded for space control. Monitor the underlying disk.

## Validation policy

UTF-8 JSON, maximum 64 KiB. Duplicate keys (at any nesting), NaN/Infinity, deeply invalid JSON,
non-object roots, wrong roles, bool-as-number, invalid timestamps and out-of-range values are
rejected. All M2 keys are required, including nested keys; **missing differs from null**.
Extra fields and unknown enums are rejected rather than silently discarded. A compatible
protocol extension needs a deliberate validator update. Date strings use M2's UTC seconds
form `YYYY-MM-DDTHH:MM:SSZ`; filename time and node must match payload exactly.

Nullable measurements remain null; no old-value fallback is used. Integers must be in
`0..2^63-1`, percentages finite `0..100`; memory/disk free cannot exceed a known total.
Strings are 1–512 characters and cannot contain control characters or unpaired Unicode surrogates; node identity has the
stricter filename bound. At most five filesystem items, five location labels each and sixteen
allowlisted collection-error codes are accepted. Worker states are RUNNING/STOPPED/UNKNOWN;
Task Scheduler accepts its current Unknown/Disabled/Queued/Ready/Running strings plus UNKNOWN.
Cycle and Run states match Source state enums, including ABANDONED, SNAPSHOT_READY and
DISK_PRESSURE. Current connectivity only accepts UNKNOWN. `agent_version` is preserved as
reported (often `0.1.0`), never converted to an invented Git hash. Unconfigured logs are null
and are not treated as a collection error.

## Database, identity and time semantics

Dedicated SQLite schema version 1 uses `PRAGMA user_version` and a Monitor application ID.
Only a nonexistent database is initialized. Existing empty files, foreign DBs, corruption or
unknown schemas are not deleted/rebuilt/migrated. Preserve the DB and WAL for investigation;
restore a known-good Monitor backup through an operator procedure. An interrupted first-time
initialization can require this explicit intervention. Read-only Web never creates a DB.

Tables:

- `nodes`: node ID, first Destination receipt, latest valid sample reference.
- `node_samples`: canonical full M2 observation, content hash, capture and receipt times.
- `filesystem_samples`: ordered disk history linked to the sample with cascading deletion.
- `managed_storage_samples`: storage history and its independent measurement time.
- `content_receipts`, `transfer_receipts`: deduplication and conflict detection.
- `observations`: bounded-lifecycle file stability state, not business Run events.

Canonical JSON sorts keys and removes whitespace; its SHA-256 hashes the **entire validated
payload**. Identical content under a different random filename, or different JSON whitespace/
object ordering, produces the same sample. JSON numeric spellings such as integer `0` versus
float `0.0` remain distinct representations; both retain their measurement meaning. M2 emits
consistent numeric types. Transport identity is the complete validated filename. Reuse with
different content is a conflict, never an update. A sample, children, node pointer and receipts
commit together under `BEGIN IMMEDIATE`. No `alerts` or `sync_events` business behavior exists.

Latest order is `(Source captured_at, content SHA-256)` descending. Same-time samples therefore
have deterministic order independent of arrival. Old out-of-order samples enter history without
replacing latest. Deduplicated delivery changes neither first receipt nor capture time and
cannot appear as a restored Source. `nodes.first_received` is the first accepted observation;
`node_samples.received` is the first committed receipt of that content (not FTP mtime).
`managed_storage.captured_at` is the cached storage scan time and can precede the heartbeat.

Samples more than `future_seconds` ahead of Destination time (default five minutes), or older
than `history_days` (default 35 days), are rejected after grace. Small future skew is accepted
as reported and displayed as a signed age; it can lead latest by at most five minutes at receipt.
No online/offline threshold or alert recovery is inferred. Correct Destination time/NTP remains
an operating prerequisite. Clock rollback resets file stability observations. Historical query
windows use Source capture time, and future skew is not silently normalized to receipt time.

## Retention and query bounds

Each round deletes at most `batch_size` expired samples by Destination receipt time, excluding
one pinned latest sample per node. Children cascade in the same transaction. A stale node's
latest observation remains visible with its original timestamps and increasing age; individual
fields are never filled from earlier samples. Unknown disks remain visible. First node identity
and latest references persist; many independently configured nodes require capacity planning.

Content receipts outlive history (`dedup_days > history_days`, default 90 days) and remain pinned
while their sample exists. Transport receipts expire after 90 days, including renamed deliveries
of a pinned sample. Expired content cannot normally reenter history: its original Source time
is already outside the 35-day acceptance window. Replay after cleanup is rejected, not presented
as a fresh observation. Changing acceptance/retention policy or resetting the DB can change this
horizon; never advertise perpetual deduplication after deliberate state loss. Expired receipts
and stability rows are each cleaned in bounded batches. Stability rows expire after one day idle.

WAL permits readers alongside the single writer; connections have one-second lock timeout,
foreign keys enabled, and are closed per round/request. SQL has an instruction budget (two
million VM instructions per connection) as a guard against unexpected expensive plans. Indexed
node/time/hash history and receipt-time cleanup avoid unrestricted history scans. Maintenance
runs PASSIVE checkpoint; SQLite's normal auto-checkpoint also applies. Freed pages are reused;
the main file does not automatically shrink. No per-round VACUUM. For exceptional compaction,
stop only Monitor and use an operator-controlled SQLite backup/maintenance procedure. Do not
copy a live WAL database's main file alone as a backup.

Read-only endpoints:

- `GET /api/sources?limit=100&before=<node cursor>`: latest observations, maximum 200 nodes.
- `GET /api/sources/{node_id}/history?window=24h|7d|30d&limit=100&before=<cursor>`:
  time/hash keyset pagination, maximum 200 observations per page.
- `GET /source-history?node=<node_id>&window=24h|7d|30d`: offline HTML history,
  100 records per page, expandable complete observations and an older-page link.

Responses expose `status`, `samples`, `next_cursor`; each sample includes `sample_id`,
`received_at`, signed `age_seconds` and the unmodified validated `payload`. Empty history is
not an error. DB/schema/read failure yields `UNAVAILABLE`, disabled M3 yields `DISABLED`.
Overview/System show latest Source identity, worker/PID/task state, CPU/memory, disks/storage,
Cycle/Current Run/Last Run, next action, collection errors and timestamps. These Source facts
remain distinct from Destination VERIFIED facts. RDS failure does not prevent reading Source
endpoints; Monitor DB failure does not prevent M1 Destination queries. Templates escape external
strings; static assets ship offline. All routes are read-only and localhost remains default.
HTML pages display Source capture, Destination receipt, storage measurement, host boot, Cycle,
Run and next-action times in `destination.report_timezone`, like Destination facts. The same
configured zone labels Overview, System and Source history. The expandable history JSON is
explicitly labeled as the raw UTC payload; its human-readable summary and details use the
configured zone. Null times remain unknown. Stored observations, canonical hashes, API payloads,
UTC history windows, sort order and pagination cursors are unchanged.

The incoming successful-file and local history policies do **not** clean business files,
business databases or Relay. Relay/FTP remote retention remains external operations' responsibility.

## Validation and field acceptance

Automated tests use temporary directories, controlled timestamps and injected failures. Coverage
includes actual M2 `build_payload`, schema rejection, duplicate keys/oversize, null/0/UNKNOWN,
unconfigured logs, stable/direct writes, claim/delete replacement, symlinks, duplicate/renamed/
conflicting and out-of-order samples, deterministic tie ordering, transaction rollback, SQLite
busy/recovery, restart after delete failure, retention/replay, quarantine caps, cursor bounds,
background startup/shutdown/retry, read-only APIs, independent degradation, HTML escaping and
bundle inclusion. Existing M1/M2 tests remain in the regression suite.

Field procedure (not claimed as completed by automated tests):

1. Validate YAML, directory permissions, local filesystem/WAL/lock support, service account and
   `monitor-ingest` counters. Confirm only Monitor owns its dedicated locks.
2. Run the unchanged Source Reporter under its real Windows SYSTEM task. Verify Relay filename/
   size acceptance and external FTP transfer of JSON, preferably temporary-file atomic rename.
3. Observe Source capture, Destination receipt and older storage-cache timestamps on `/system`.
   Check unknown measurements, zero values and DELIVERED labeling against the actual Source.
4. Redeliver an identical file under a new nonce and confirm one sample/unchanged receipt. Confirm
   the FTP client does not treat Monitor removal as an instruction to delete remote business files.
5. Stop/restart only Monitor, test planned RDS unavailability and Monitor state-volume access
   failure, then restore permissions/connectivity. Verify business Worker continues independently.
6. Validate realistic backlog throughput, retention disk usage and shutdown time on CentOS 7.9.
   Source Windows/CIM, FTP completion semantics and external Relay retention need onsite evidence.

M3 establishes the observation/receipt/history foundation for M4 code work. M4 still needs its own
reviewed rules, freshness/clock policy, persisted alert schema/state machine and recovery tests;
no current observation status should be mistaken for that implementation.

Validation recorded 2026-09-28 on Linux / Python 3.14.7, using the existing temporary M2 audit
environment (the repository `.venv` interpreter link was unavailable):

- `pytest tests/test_monitor_ingest.py tests/test_monitor.py tests/test_monitor_report.py -q`:
  **151 passed** (55 M3 tests, 96 existing M1/M2 tests).
- Complete default non-integration suite: **688 passed, 8 skipped, 12 deselected**.
- Repository-wide Ruff, format checks on all changed Python files and `git diff --check`: passed.
- Offline bundle assembly tests verify M3 documentation, destination configuration, service
  example and checksum inclusion. These use fixture wheels/runtime; no production offline bundle
  or Windows/FTP/Relay field acceptance is claimed.

The pre-existing M2 review fixes in Reporter, its tests and `monitoring-m2.md` were preserved.
No dependency or lockfile changes were needed. The M3 code review's same-directory lock and
Source page timezone findings have regression coverage. M3 has no remaining known code review
blocker to starting M4 development; the onsite checks above remain deployment acceptance items.
