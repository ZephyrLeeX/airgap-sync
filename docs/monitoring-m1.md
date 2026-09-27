# Monitoring M1 — Destination dashboard

**M1 is destination-only.** It reads `airgap_sync_meta`, the Destination incoming directory, and local Linux system facts. The five pages show Overview, Tables, Runs, System, and current Problems. Tables reuse the existing `table_versions` statistics for row count, 本次净增 and 月度净增. Runs show only timestamps already recorded by Destination. Source host/worker/disk status is unavailable until M2 Source Telemetry Reporter and M3 ingest. Persisted Alerts arrive in M4; full end-to-end timing in M5; enhanced live progress in M6.

Start independently of the worker:

```bash
airgap-sync destination monitor-web --config /etc/airgap-sync/destination.yaml
# optional: --host 127.0.0.1 --port 8080
```

Default bind is `127.0.0.1:8080`. The Monitor reads the MySQL password environment variable named in the Destination YAML. It never initializes or migrates metadata. Missing RDS or incompatible schema leaves local system/incoming information visible and marks database data unavailable. No web action writes to metadata, incoming, or the worker; there are no control endpoints. The web process is separate from `airgap-sync-destination.service` and never acquires worker locks.

Monitor MySQL connections use separate 3-second connection, read, and write timeouts. A failed metadata query stops further queries on that request so an unresponsive connection does not incur repeated waits. The page and API keep local system and incoming facts and report `DEGRADED` database status with a generic error that does not expose connection details. A stopped worker or known failed Run can still make the overall status `CRITICAL`. The Destination worker retains its existing database timeout behavior.

Tables combines verified `table_versions` statistics with table identities from all metadata Runs. Each table's latest Destination status comes from a separate read-only query ordered by source snapshot time (`source_created_at`), then manifest receipt time, then Run ID as a deterministic tie breaker. Cleanup changes to `updated_at` do not change that status, and the query has no global recent-run limit. A table without a VERIFIED version shows unknown verification time, row count, net changes, and data age. Runs continues to show a bounded list ordered by last update.

Overview and System show the root, incoming, and install filesystems. Shared devices appear once with every use and path listed; failed disk measurements appear as unknown. System also shows the Destination worker PID when systemd provides it.

The offline release includes `airgap-sync-monitor.service.example` alongside the worker service. Its `ExecStart` uses `/opt/airgap-sync/current/venv/bin/airgap-sync`; set the same `EnvironmentFile` used for the MySQL password. The services can restart independently. An optional nginx reverse proxy can expose the loopback listener:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080/;
}
```

Restrict access to the reverse proxy using your site's existing controls. Templates, CSS and JavaScript are packaged with the Python wheel; the monitor needs no Node runtime, CDN, or network package installation after deployment.
