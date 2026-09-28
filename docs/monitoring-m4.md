# Monitoring M4: Alert Engine

M4 is optional. Configure `monitor_ingest` and `monitor_alerts` in the Destination Monitor
configuration; omitting `monitor_alerts` leaves M1–M3 behavior in place. Destination Worker
does not evaluate alerts. Monitor Web serves read-only `/alerts` and `/api/alerts`; its background
thread evaluates without browser requests. No Source connection or business database write is made.

## Evidence and event semantics

`TRUE` is a supported abnormal observation, `FALSE` is a supported recovery observation, and
`UNKNOWN` is missing, stale, failed or insufficient evidence. `UNKNOWN` retains an OPEN event
and updates `evaluation_state`; it never recovers one. `last_seen_at` means the last successful
TRUE evaluation, `evaluated_at` the last evaluation/availability update, and `observed_at` the
original evidence time. API timestamps are UTC; HTML uses `destination.report_timezone`.

Fingerprints are JSON pairs of rule and stable object identity, without severity. A TRUE creates
one OPEN row; later TRUE updates it, including severity changes. FALSE closes it as RECOVERED.
A later TRUE with newer evidence creates a new row; old replay cannot reopen. For
`SOURCE_HEARTBEAT` and `FRESHNESS`, the same evidence that proved recovery can later exceed
the time threshold and open a new row without changing its `observed_at`. Worker, disk and Run
observations still require newer evidence after recovery. A partial batch
does not evaluate unvisited objects. `DISABLED` marks an administrative configuration transition,
not observed recovery. A changed rule configuration closes current OPEN events as DISABLED before
reevaluation, so threshold changes are not reported as recoveries. Disabling `monitor_alerts`
marks OPEN events DISABLED while retaining history. Recovered and disabled rows expire after
`recovered_retention_days`; OPEN rows never expire through this retention. Incoming stability
state without an OPEN alert, and removed expected-node activation state, expire in bounded
100-row batches after the same interval. Configured never-seen nodes and OPEN incoming alerts
retain their first-observed time across restarts.

| Rule / fingerprint object | TRUE | FALSE | UNKNOWN |
| --- | --- | --- | --- |
| `SOURCE_HEARTBEAT` / node | Latest valid Source `captured_at` age >10m WARNING, >20m CRITICAL; expected never-seen node after persisted 10m grace | Newer fresh valid heartbeat | No evidence before grace; late/duplicate receipt cannot refresh capture |
| `SOURCE_WORKER` / node | Fresh heartbeat with process STOPPED | Fresh heartbeat with process RUNNING | Worker UNKNOWN or heartbeat older than warning threshold; Task Scheduler `Ready` is separate |
| `SOURCE_DISK` / node+mount | Fresh heartbeat and either metric below configured threshold | Both free bytes and percent known and both meet warning threshold | Stale heartbeat, missing disk or missing metric when the known metric is healthy |
| `DESTINATION_WORKER` / destination | systemd reports inactive/failed | systemd reports active | service query unavailable; process state is not inferred from task schedule |
| `DESTINATION_DISK` / `st_dev` | Local disk usage crosses either threshold | Both metrics known and healthy | stat/usage unavailable; RDS disk is not inferred from managed storage |
| `FRESHNESS` / source database+table | Latest VERIFIED version's **Source snapshot creation** age >8d/10d | New latest VERIFIED version with age in range | No VERIFIED version, RDS failure or uncovered table |
| `DESTINATION_RUN` / source database+table | Latest Run is FAILED/MISMATCH/DISK_PRESSURE/RETRY_WAIT | Latest Run is VERIFIED, including later successful replacement | Other state, failed metadata query or uncovered table |
| `SOURCE_RUN` / run and `SOURCE_CYCLE` / cycle | Fresh telemetry explicitly reports failure/retry state | Same identity reports delivered/completed | Source observations cannot prove all historical failures after sampling changes |
| `INCOMING_STALE` / run | Complete manifest, schema and all chunk files have stable inode/size/mtime/ctime across observations for >2h, and an available RDS query confirms no Run metadata | Destination metadata contains that Run | Partial/writing file set, RDS failure, read failure or disappearance alone |
| `VERSION` / node | Fresh Source and Destination known package versions differ | Comparable package versions match | Missing/stale/uncomparable versions; equal `0.1.0` does **not** prove equal Git commits |

Threshold comparisons are strict `<` for disk and strict `>` for time, so equality is healthy.
Observed Source nodes are monitored even when absent from `expected_sources`; that list adds
never-seen detection and does not act as a filter.
Disk severity chooses the highest triggered level. Source disks use the stable mount name and
Destination disks use the local device identity; purpose labels do not create extra events.
Incoming checks request at most 1 MiB plus one byte from an opened manifest and stat at most
1000 chunks per candidate. They reject an oversized, replaced, non-regular or changing manifest
as insufficient evidence. The reader checks the opened file and its path before and after the
bounded read; a file-system call is not subject to a hard time limit. Incoming checks
they do not hash business chunks, rename files, or use the Worker lock. The stat check is not a
cryptographic proof of completed FTP writing: temporary-file atomic rename remains the transfer
contract. RDS evidence, not directory disappearance, closes incoming alerts. Historical failed
Run counts on Problems remain historical facts, not permanently OPEN alerts.

## Storage, scheduling and upgrade

M3 `monitor.db` v1 migrates transactionally to v2 under the existing Monitor directory/inbox
locks. The migration adds `alerts` and `alert_state`, retaining all M3 samples, receipts, latest
pointers and stability observations. A failed migration rolls back and is retried at the next
startup. Foreign, corrupt and unknown-version databases are refused rather than replaced.
Stop the old Monitor Web and any `monitor-ingest` one-shot before starting M4; back up SQLite
with its WAL or SQLite backup API, then start one M4 Monitor process. Do not copy only the main
file from a live WAL database. Web GET requests never migrate or write.

The existing ingest owner performs evaluation in its dedicated thread at `interval_seconds`
cadence, independently of ingest polling and browser traffic; each family is isolated on
exception. The Source read covers at most 1000 nodes with a persistent cursor. Destination
metadata covers at most 1000 latest versions/runs per round with separate persistent cursors;
recovery checks cover at most 100 OPEN incoming alerts per round using a persisted ascending ID
cursor that wraps at the end. The cursor advances only after a successful RDS metadata query,
including when the incoming files have already been removed;
incoming enumeration
at most 2000 directory entries and 20 manifest candidates per round with a persistent cursor,
and alert retention deletes
100 closed rows per round. Uncovered objects retain their OPEN state. Ingest/file system/RDS
latency can delay the next evaluation; investigate `ingest_status` and UNKNOWN evidence when the
service is degraded. No SQL row scan of business tables or automatic cleanup is performed.
Alerts HTML and API treat empty `status`, `severity`, `node` and `kind` query values as unspecified;
nonempty enum, length and pagination limits still apply. Filtered page links keep the filters.

## Field acceptance

Automated Linux tests do not establish Windows Source Task Scheduler, Relay/FTP completion,
CentOS systemd/disk identity or RDS MySQL 5.6 timings. On site, verify the Source heartbeat
capture/receipt skew, actual volume names, systemd service state, large incoming manifests,
metadata query latency, monitor.db migration backup/rollback procedure and alert behavior during
planned RDS/FTP outages. Keep Destination Worker running while restarting only Monitor.

Local validation on Linux / Python 3.14.7 used a temporary environment installed from
`uv.lock` because the repository `.venv` interpreter was unavailable: the complete default
non-integration suite passed (711 passed, 8 skipped, 12 deselected); M4 monitor tests passed
(46 passed); repository-wide Ruff,
format checks on changed Python files and `git diff --check` passed. These checks do not replace
the field acceptance above.
