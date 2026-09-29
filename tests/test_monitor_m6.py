"""Independent progress observations and strict v3 transfer behavior."""

import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from airgap_sync.common.models import AppConfig, MonitorIngestConfig
from airgap_sync.monitor import store
from airgap_sync.monitor.progress import Recorder, display, merge_received, path_for, read
from airgap_sync.monitor.protocol import PROGRESS_STAGE_V4, decode
from airgap_sync.source import monitor_report
from airgap_sync.source.monitor_report import build_payload
from airgap_sync.source.state import SourceState, state_db_path


def sample(tmp_path):
    recorder = Recorder(tmp_path, "n", "d", "t", "r")
    recorder.update("source_encode_write", rows=10, bytes_=100, force=True)
    return recorder, {
        "run_id": "r",
        "table_name": "t",
        "stages": [
            {"stage": "source_encode_write", **read(recorder.path)["stages"]["source_encode_write"]}
        ],
    }


def test_progress_complete_retry_restart_reset_and_stale(tmp_path):
    recorder, fact = sample(tmp_path)
    assert display(fact["stages"][0], remote=True)["state"] == "LAST_OBSERVED"
    recorder.update("source_encode_write", rows=5, bytes_=50, state="COMPLETE", force=True)
    complete = read(recorder.path)["stages"]["source_encode_write"]
    assert complete["rows"] == 15 and complete["bytes"] == 150
    assert display(complete)["state"] == "COMPLETE"
    recorder.update("source_upload_call", bytes_=10, force=True)
    assert read(recorder.path)["stages"]["source_encode_write"] == complete
    restarted = Recorder(tmp_path, "n", "d", "t", "r")
    restarted.update("source_encode_write", rows=2, bytes_=20, force=True)
    new = read(recorder.path)["stages"]["source_encode_write"]
    assert new["attempt"] != complete["attempt"] and new["rows"] == 2
    assert new["window_seconds"] is None
    restarted.update("source_encode_write", state="INTERRUPTED", force=True)
    assert display(read(recorder.path)["stages"]["source_encode_write"])["state"] == "INTERRUPTED"
    stale = dict(new, state="RUNNING", observed_at="2020-01-01T00:00:00Z")
    assert display(stale)["state"] == "STALE"
    assert display({**new, "window_seconds": 0, "window_rows": 10})["rows_per_second"] is None
    assert display({**new, "total_rows": None})["percent"] is None


def test_progress_duplicate_out_of_order_and_counter_reset(tmp_path):
    recorder, fact = sample(tmp_path)
    root = tmp_path / "received"
    first = fact["stages"][0]
    merge_received(root, "n", "d", fact)
    newer = {**first, "sequence": first["sequence"] + 1, "rows": 20}
    merge_received(root, "n", "d", {**fact, "stages": [newer]})
    merge_received(root, "n", "d", fact)
    reset = {**newer, "sequence": newer["sequence"] + 1, "rows": 1}
    merge_received(root, "n", "d", {**fact, "stages": [reset]})
    saved = read(path_for(root, "n", "d", "t", "r"))["stages"]["source_encode_write"]
    assert saved["rows"] == 20
    later_attempt = {
        **reset,
        "attempt": "different",
        "generation": reset["generation"] + 1,
        "sequence": 1,
    }
    merge_received(root, "n", "d", {**fact, "stages": [later_attempt]})
    saved = read(path_for(root, "n", "d", "t", "r"))["stages"]["source_encode_write"]
    assert saved["rows"] == 1 and saved["attempt"] == "different"


@pytest.mark.parametrize("final_state", ["COMPLETE", "INTERRUPTED"])
def test_same_second_final_and_late_attempt_cannot_regress(tmp_path, final_state):
    recorder, fact = sample(tmp_path)
    first = fact["stages"][0]
    recorder.update("source_encode_write", rows=5, state=final_state, force=True)
    final = {"stage": "source_encode_write", **read(recorder.path)["stages"]["source_encode_write"]}
    assert first["observed_at"] == final["observed_at"]
    PROGRESS_STAGE_V4(final)
    root = tmp_path / "received"
    merge_received(root, "n", "d", fact)
    merge_received(root, "n", "d", {**fact, "stages": [final]})
    merge_received(root, "n", "d", fact)
    merge_received(root, "n", "d", {**fact, "stages": [final]})
    second = Recorder(tmp_path, "n", "d", "t", "r")
    second.update("source_encode_write", rows=2, force=True)
    restarted = {
        "stage": "source_encode_write",
        **read(second.path)["stages"]["source_encode_write"],
    }
    merge_received(root, "n", "d", {**fact, "stages": [restarted]})
    merge_received(root, "n", "d", {**fact, "stages": [final]})
    saved = read(path_for(root, "n", "d", "t", "r"))["stages"]["source_encode_write"]
    assert saved["attempt"] == second.attempt and saved["rows"] == 2
    assert saved["state"] == "RUNNING"


def test_legacy_progress_compatibility(tmp_path):
    recorder, fact = sample(tmp_path)
    stage = {
        key: value
        for key, value in fact["stages"][0].items()
        if key not in ("generation", "sequence", "processed_rows", "processed_bytes")
    }
    root = tmp_path / "received"
    merge_received(root, "n", "d", {**fact, "stages": [stage]})
    merge_received(root, "n", "d", fact)
    merge_received(
        root, "n", "d", {**fact, "stages": [{**stage, "observed_at": "2099-01-01T00:00:00Z"}]}
    )
    saved = read(path_for(root, "n", "d", "t", "r"))["stages"]["source_encode_write"]
    assert saved["generation"] == 1
    recorder.path.write_text(
        json.dumps(
            {
                "identity": recorder.identity,
                "stages": {
                    "source_encode_write": {
                        key: value for key, value in stage.items() if key != "stage"
                    },
                    "source_read_wait": {"generation": "damaged"},
                },
            }
        )
    )
    restarted = Recorder(tmp_path, "n", "d", "t", "r")
    restarted.update("source_encode_write", rows=1, force=True)
    assert read(recorder.path)["stages"]["source_encode_write"]["generation"] == 1


def test_monitor_write_failure_never_raises(tmp_path, monkeypatch):
    recorder = Recorder(tmp_path, "n", "d", "t", "r")

    def fail_replace(*args):
        raise OSError("injected")

    monkeypatch.setattr("airgap_sync.monitor.progress.os.replace", fail_replace)
    recorder.update("source_read_wait", rows=5, state="COMPLETE", force=True)
    assert read(recorder.path) is None


def test_v3_wire_ingest_and_v1_v2_strict(config_data, tmp_path):
    config_data["monitoring"] = {"node_id": "source-01"}
    config = AppConfig.model_validate(config_data)
    moment = datetime(2026, 9, 29, 12, tzinfo=UTC)
    payload = build_payload(config, tmp_path / "source.yaml", moment)
    payload.update(
        schema_version=3,
        source_database=config.mysql.database,
        run_facts_status="OK",
        run_facts=[],
        run_progress=[],
    )
    name = "airgap-monitor-v3--source-01--20260929T120000Z--11111111.json"
    body = json.dumps(payload).encode()
    assert decode(body, name)[0] == payload
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    parsed, canonical, digest = decode(body, name)
    with store.connect(cfg.db_path, write=True) as db:
        assert store.ingest(db, cfg, name, parsed, canonical, digest, moment.timestamp())
    for version in (1, 2):
        old = {**payload, "schema_version": version}
        if version == 1:
            for key in ("source_database", "run_facts_status", "run_facts"):
                old.pop(key)
        with pytest.raises(ValueError):
            decode(json.dumps(old).encode(), name.replace("-v3--", f"-v{version}--"))
    with pytest.raises(ValueError):
        decode(body + b" " * (65537 - len(body)), name)
    _, progress = sample(tmp_path / "wire")
    legacy_progress = {
        **progress,
        "stages": [
            {
                key: value
                for key, value in progress["stages"][0].items()
                if key not in ("generation", "sequence", "processed_rows", "processed_bytes")
            }
        ],
    }
    legacy_wire = {**payload, "run_progress": [legacy_progress]}
    assert decode(json.dumps(legacy_wire).encode(), name)[0] == legacy_wire
    v4 = {**payload, "schema_version": 4, "run_progress": [progress]}
    v4_name = name.replace("-v3--", "-v4--")
    assert decode(json.dumps(v4).encode(), v4_name)[0] == v4
    with pytest.raises(ValueError):
        decode(json.dumps(v4).encode(), name)
    with pytest.raises(ValueError):
        decode(json.dumps({**payload, "run_progress": [progress]}).encode(), name)
    broken = json.loads(json.dumps(v4))
    broken["run_progress"][0]["stages"][0]["sequence"] = -1
    with pytest.raises(ValueError):
        decode(json.dumps(broken).encode(), v4_name)


def test_progress_window_and_unknown_total(tmp_path):
    recorder, _ = sample(tmp_path)
    stage = read(recorder.path)["stages"]["source_encode_write"]
    stage["window_seconds"] = 2
    stage["window_rows"] = 4
    stage["window_bytes"] = 40
    shown = display(stage, now=datetime.now(UTC) + timedelta(seconds=1), remote=True)
    assert shown["rows_per_second"] == 2
    assert shown["bytes_per_second"] == 20
    assert shown["percent"] is None
    assert display({**stage, "window_rows": 0})["rows_per_second"] is None
    assert display({**stage, "window_bytes": 0})["bytes_per_second"] is None
    assert display({**stage, "chunks": 1, "total_chunks": 2})["chunk_percent"] == 50


def test_reporter_transfers_observed_stage_without_business_scan(
    config_data, tmp_path, monkeypatch
):
    from airgap_sync.common.models import RelayConfig

    config_data["monitoring"] = {"node_id": "source-01"}
    config = AppConfig.model_validate(config_data).model_copy(
        update={"relay": RelayConfig(base_url="https://relay.invalid", token_env="M6_TOKEN")}
    )
    db_path = state_db_path(config.paths.data_dir)
    with SourceState(db_path) as state:
        state.initialize()
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO sync_runs(run_id,table_name,status,created_at) VALUES (?,?,?,?)",
            ("r1", "t_snapshot", "GENERATING", "2026-09-29T00:00:00+00:00"),
        )
    recorder = Recorder(
        config.paths.data_dir / "monitor" / "progress" / "source",
        "source-01",
        config.mysql.database,
        "t_snapshot",
        "r1",
    )
    recorder.update("source_encode_write", rows=10, bytes_=100, force=True)
    sent = []

    def upload(_self, path, name, digest):
        sent.append((name, path.read_bytes()))

    monkeypatch.setenv("M6_TOKEN", "secret")
    monkeypatch.setattr(monitor_report.RelayUploader, "upload", upload)
    assert monitor_report.report(config, tmp_path / "source.yaml")[0]
    name, body = sent[0]
    assert name.startswith("airgap-monitor-v4--")
    parsed, canonical, digest = decode(body, name)
    assert parsed["run_progress"][0]["stages"][0]["rows"] == 10
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    with store.connect(cfg.db_path, write=True) as db:
        assert store.ingest(db, cfg, name, parsed, canonical, digest, datetime.now(UTC).timestamp())
    saved = read(
        path_for(
            cfg.db_path.parent / "progress" / "source",
            "source-01",
            config.mysql.database,
            "t_snapshot",
            "r1",
        )
    )
    assert saved["stages"]["source_encode_write"]["rows"] == 10


def test_reporter_progress_rotates_40_runs_after_failure_and_restart(
    config_data, tmp_path, monkeypatch
):
    from airgap_sync.common.models import RelayConfig
    from airgap_sync.source.uploader import UploadError

    config_data["monitoring"] = {"node_id": "source-01"}
    config = AppConfig.model_validate(config_data).model_copy(
        update={"relay": RelayConfig(base_url="https://relay.invalid", token_env="M6_TOKEN")}
    )
    with SourceState(state_db_path(config.paths.data_dir)) as state:
        state.initialize()
    root = config.paths.data_dir / "monitor" / "progress" / "source"
    with sqlite3.connect(state_db_path(config.paths.data_dir)) as db:
        for index in range(40):
            run_id = f"r{index:02d}"
            db.execute(
                "INSERT INTO sync_runs(run_id,table_name,status,created_at) VALUES (?,?,?,?)",
                (run_id, "t_snapshot", "GENERATING", datetime.now(UTC).isoformat()),
            )
            Recorder(root, "source-01", config.mysql.database, "t_snapshot", run_id).update(
                "source_encode_write", rows=index + 1, force=True
            )
    sent = []

    def upload(_self, path, name, _digest):
        parsed = decode(path.read_bytes(), name)[0]
        assert len(path.read_bytes()) <= monitor_report.MAX_PAYLOAD
        assert len(parsed["run_facts"]) == 20
        assert len(parsed["run_progress"]) <= 5
        sent.append([item["run_id"] for item in parsed["run_progress"]])

    monkeypatch.setenv("M6_TOKEN", "secret")
    monkeypatch.setattr(monitor_report.RelayUploader, "upload", upload)
    for _ in range(3):
        assert monitor_report.report(config, tmp_path / "source.yaml")[0]
    cursor = (config.paths.data_dir / "monitor" / "progress-cursor.json").read_bytes()

    def fail(*_args):
        raise UploadError("UPLOAD_RETRY_EXHAUSTED", "injected", attempts=1)

    monkeypatch.setattr(monitor_report.RelayUploader, "upload", fail)
    assert not monitor_report.report(config, tmp_path / "source.yaml")[0]
    assert (config.paths.data_dir / "monitor" / "progress-cursor.json").read_bytes() == cursor
    monkeypatch.setattr(monitor_report.RelayUploader, "upload", upload)
    for _ in range(5):
        assert monitor_report.report(config, tmp_path / "source.yaml")[0]
    assert len({run for batch in sent for run in batch}) == 40
    assert sent[0] == [f"r{i:02d}" for i in range(5)]
    assert sent[3] == [f"r{i:02d}" for i in range(15, 20)]
    path_for(root, "source-01", config.mysql.database, "t_snapshot", "r00").unlink()
    path_for(root, "source-01", config.mysql.database, "t_snapshot", "r01").write_text("{")
    path_for(root, "source-01", config.mysql.database, "t_snapshot", "r02").write_bytes(
        b"x" * 20000
    )
    assert monitor_report.report(config, tmp_path / "source.yaml")[0]
    assert sent[-1] == [f"r{i:02d}" for i in range(3, 8)]
    assert monitor_report.report(config, tmp_path / "source.yaml")[0]
    assert sent[-1] == [f"r{i:02d}" for i in range(8, 13)]


def test_worker_exception_path_keeps_previous_attempt_snapshot(tmp_path):
    first = Recorder(tmp_path, "n", "d", "t", "r")
    first.update("destination_import", rows=100, chunks=2, state="COMPLETE", force=True)
    old_import = read(first.path)["stages"]["destination_import"]
    # A retry starts only verification; import is not restarted this attempt.
    second = Recorder(tmp_path, "n", "d", "t", "r")
    second.update("destination_verify", total_rows=100, force=True)
    # Worker exception path: every destination stage is asked to interrupt.
    second.update("destination_import", state="INTERRUPTED", force=True)
    second.update("destination_verify", state="INTERRUPTED", force=True)
    saved = read(second.path)["stages"]
    assert saved["destination_import"] == old_import
    assert saved["destination_import"]["state"] == "COMPLETE"
    assert saved["destination_import"]["rows"] == 100
    assert saved["destination_verify"]["state"] == "INTERRUPTED"
    assert saved["destination_verify"]["attempt"] == second.attempt
    # Later writes of other stages still never dump a fabricated import stage.
    second.update("destination_verify", rows=10, force=True)
    assert read(second.path)["stages"]["destination_import"] == old_import


def test_interrupt_for_never_started_stage_writes_nothing(tmp_path):
    first = Recorder(tmp_path, "n", "d", "t", "r")
    first.update("destination_import", rows=5, state="COMPLETE", force=True)
    before = read(first.path)
    second = Recorder(tmp_path, "n", "d", "t", "r")
    # Interrupts for stages that do not exist for this attempt are fully ignored:
    # no stage object is created or replaced, so nothing can be dumped later.
    second.update("source_upload_call", state="INTERRUPTED", force=True)
    assert read(second.path) == before
    second.update("destination_import", state="INTERRUPTED", force=True)
    assert read(second.path) == before
    second.update("destination_verify", total_rows=3, force=True)
    saved = read(second.path)["stages"]
    assert set(saved) == {"destination_import", "destination_verify"}
    assert saved["destination_import"] == before["stages"]["destination_import"]
    assert saved["destination_verify"]["attempt"] == second.attempt


def test_restarted_stage_runs_completes_and_interrupts_normally(tmp_path):
    first = Recorder(tmp_path, "n", "d", "t", "r")
    first.update("destination_import", rows=100, state="COMPLETE", force=True)
    second = Recorder(tmp_path, "n", "d", "t", "r")
    assert second.generation == first.generation + 1
    second.update("destination_import", state="INTERRUPTED", force=True)  # not applicable
    second.update("destination_import", rows=7, force=True)  # genuinely starts again
    saved = read(second.path)["stages"]["destination_import"]
    assert saved["rows"] == 7 and saved["attempt"] == second.attempt
    assert saved["generation"] == second.generation
    second.update("destination_import", rows=3, state="COMPLETE", force=True)
    saved = read(second.path)["stages"]["destination_import"]
    assert saved["rows"] == 10 and saved["state"] == "COMPLETE"
    third = Recorder(tmp_path, "n", "d", "t", "r")
    third.update("destination_import", rows=1, force=True)
    third.update("destination_import", state="INTERRUPTED", force=True)
    saved = read(third.path)["stages"]["destination_import"]
    assert saved["state"] == "INTERRUPTED" and saved["attempt"] == third.attempt
