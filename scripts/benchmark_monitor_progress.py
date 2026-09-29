"""Repeatable local microbenchmark for bounded progress recording."""

from __future__ import annotations

import statistics
import tempfile
import time
from pathlib import Path

from airgap_sync.common.models import ChunkConfig, SnapshotConfig
from airgap_sync.destination.processor import DestinationVerifier
from airgap_sync.monitor.progress import Recorder
from airgap_sync.source.scanner import scan_table


def measure(count: int = 10000, rounds: int = 5):
    baseline = []
    observed = []
    forced = []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for index in range(rounds):
            start = time.perf_counter()
            total = 0
            for _ in range(count):
                total += 1000
            baseline.append(time.perf_counter() - start)
            recorder = Recorder(root, "node", "database", "table", f"run-{index}")
            start = time.perf_counter()
            for _ in range(count):
                recorder.update("source_encode_write", rows=1000, bytes_=100000)
            observed.append(time.perf_counter() - start)
            start = time.perf_counter()
            for _ in range(100):
                recorder.update("source_encode_write", force=True)
            forced.append(time.perf_counter() - start)
            assert total == count * 1000
    print(f"count={count} rounds={rounds}")
    print(f"baseline_ms={statistics.median(baseline) * 1000:.3f}")
    print(f"recorded_ms={statistics.median(observed) * 1000:.3f}")
    incremental = (statistics.median(observed) - statistics.median(baseline)) * 1e6 / count
    print(f"incremental_us_per_update={incremental:.3f}")
    print(f"forced_write_ms_per_update={statistics.median(forced) * 1000 / 100:.3f}")

    class Stream:
        columns = ["value"]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __iter__(self):
            for _ in range(10):
                yield [(index,) for index in range(500)]

    class Source:
        def stream_table(self, table, fetch_size):
            return Stream()

    scan_times = {False: [], True: []}
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for index in range(rounds):
            for enabled in (False, True):
                progress = (
                    Recorder(root / "progress", "node", "database", "table", f"scan-{index}")
                    if enabled
                    else None
                )
                started = time.perf_counter()
                scan_table(
                    Source(),
                    "table",
                    root / f"scan-{index}-{enabled}",
                    SnapshotConfig(),
                    ChunkConfig(),
                    progress=progress,
                )
                scan_times[enabled].append(time.perf_counter() - started)
    baseline_scan = statistics.median(scan_times[False])
    observed_scan = statistics.median(scan_times[True])
    print(f"scan_without_progress_ms={baseline_scan * 1000:.3f}")
    print(f"scan_with_progress_ms={observed_scan * 1000:.3f}")
    print(f"scan_overhead_percent={(observed_scan / baseline_scan - 1) * 100:.1f}")

    class Rows:
        def __enter__(self):
            return iter([(number,) for number in range(5000)])

        def __exit__(self, *args):
            pass

    class Destination:
        def stream_table_columns(self, table, columns, fetch_size):
            return Rows()

    verify_times = {False: [], True: []}
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for index in range(rounds):
            for enabled in (False, True):
                recorder = Recorder(root, "destination", "database", "table", f"verify-{index}")

                def callback(rows, seconds, current=recorder):
                    current.update("destination_verify", rows=rows, seconds=seconds)

                started = time.perf_counter()
                DestinationVerifier(Destination(), 500).verify(
                    "table", ["value"], callback if enabled else None
                )
                verify_times[enabled].append(time.perf_counter() - started)
    base_verify = statistics.median(verify_times[False])
    observed_verify = statistics.median(verify_times[True])
    print(f"verify_without_progress_ms={base_verify * 1000:.3f}")
    print(f"verify_with_progress_ms={observed_verify * 1000:.3f}")
    print(f"verify_overhead_percent={(observed_verify / base_verify - 1) * 100:.1f}")


if __name__ == "__main__":
    measure()
