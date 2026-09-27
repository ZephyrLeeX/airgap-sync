# Monitoring M1 — Destination dashboard

**M1 is destination-only.** It reads `airgap_sync_meta`, the Destination incoming directory, and local Linux system facts. The five pages show Overview, Tables, Runs, System, and current Problems. Tables reuse the existing `table_versions` statistics for row count, 本次净增 and 月度净增. Runs show only timestamps already recorded by Destination. Source host/worker/disk status is unavailable until M2 Source Telemetry Reporter and M3 ingest. Persisted Alerts arrive in M4; full end-to-end timing in M5; enhanced live progress in M6.

Start independently of the worker:

```bash
airgap-sync destination monitor-web --config /etc/airgap-sync/destination.yaml
# optional: --host 127.0.0.1 --port 8080
```

Default bind is `127.0.0.1:8080`. The Monitor reads the MySQL password environment variable named in the Destination YAML. It never initializes or migrates metadata. Missing RDS or incompatible schema leaves local system/incoming information visible and marks database data unavailable. No web action writes to metadata, incoming, or the worker; there are no control endpoints. The web process is separate from `airgap-sync-destination.service` and never acquires worker locks.

The offline release includes `airgap-sync-monitor.service.example` alongside the worker service. Its `ExecStart` uses `/opt/airgap-sync/current/venv/bin/airgap-sync`; set the same `EnvironmentFile` used for the MySQL password. The services can restart independently. An optional nginx reverse proxy can expose the loopback listener:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080/;
}
```

Restrict access to the reverse proxy using your site's existing controls. Templates, CSS and JavaScript are packaged with the Python wheel; the monitor needs no Node runtime, CDN, or network package installation after deployment.
