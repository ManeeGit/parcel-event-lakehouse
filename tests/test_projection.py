import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from parcel_lakehouse.common import InjectedCrash, IntegrityError, canonical, sha256
from parcel_lakehouse.lake import consume, evolve, table
from parcel_lakehouse.projection import current, project, validate, verify_generation
from parcel_lakehouse.source import connect, enqueue, record_event, seed


def add(root, events):
    with connect(root) as db, db:
        for event in events:
            enqueue(
                db,
                canonical(event) if isinstance(event, dict) else event,
                "2026-01-02T00:00:00+00:00",
            )
    consume(root)


def read(root, name):
    return pq.read_table(current(root) / f"{name}.parquet").to_pylist()


def test_demo_quality_accounting_and_idempotence(root):
    seed(root, parcels=20)
    consume(root)
    result = project(root)
    metrics = result["metrics"]
    assert metrics["raw_rows"] == 64
    assert metrics["accepted_events"] == 60
    assert metrics["duplicate_rows"] == 1
    assert metrics["quarantine_rows"] == 3
    assert metrics["conflicting_rows"] == 2
    assert metrics["late_events"] > 0
    assert metrics["raw_rows"] == sum(
        metrics[key] for key in ("accepted_events", "duplicate_rows", "quarantine_rows")
    )
    manifest = verify_generation(Path(result["path"]))
    rerun = project(root)
    assert rerun["reused"]
    assert rerun["files"] == manifest["files"]


def test_dedup_out_of_order_and_deterministic_tie(root, event):
    delivered = dict(event, event_id="e2", status="delivered", event_time="2026-01-01T02:00:00Z")
    tie = dict(delivered, event_id="e3", status="exception")
    add(root, [delivered, event, event, tie])
    project(root)
    assert len(read(root, "events")) == 3
    assert read(root, "parcel_status")[0]["status"] == "delivered"
    metrics = json.loads((current(root) / "metrics.json").read_text())
    assert metrics["duplicate_rows"] == 1
    assert metrics["late_events"] == 1


def test_conflicts_quarantine_all_versions_not_arbitrary_winner(root, event):
    add(root, [event, dict(event, hub="south"), event])
    project(root)
    assert read(root, "events") == []
    assert len(read(root, "quarantine")) == 3
    assert all(r["reason"] == "conflicting_event_id" for r in read(root, "quarantine"))


def test_late_data_corrects_published_old_window(root, event):
    future = dict(event, event_id="future", event_time="2026-01-01T04:00:00Z")
    add(root, [event, future])
    first = project(root)
    add(root, [dict(event, event_id="late", status="delivered", event_time="2026-01-01T00:10:00Z")])
    second = project(root)
    assert second["metrics"]["late_events"] == 1
    changes = read(root, "corrections")
    assert len(changes) == 1
    assert changes[0]["event_delta"] == 1
    assert changes[0]["delivered_delta"] == 1
    assert Path(first["path"]).exists()


def test_conflict_retracts_previously_published_totals(root, event):
    add(root, [event])
    project(root)
    add(root, [dict(event, hub="south")])
    project(root)
    changes = read(root, "corrections")
    assert len(changes) == 1
    assert changes[0]["event_delta"] == -1
    assert read(root, "events") == []


def test_historical_replay_does_not_move_live_pointer(root, event):
    add(root, [event])
    first = project(root)
    add(root, [dict(event, event_id="e2", status="delivered")])
    project(root)
    before = (root / "CURRENT.json").read_text()
    replay = project(root, snapshot_id=first["snapshot_id"])
    assert (root / "CURRENT.json").read_text() == before
    assert replay["replay"]
    assert pq.read_table(Path(replay["path"]) / "events.parquet").num_rows == 1
    assert replay["files"]["events.parquet"] == first["files"]["events.parquet"]


def test_actual_iceberg_schema_evolution_preserves_old_rows(root, event):
    record_event(root, event)
    consume(root)
    sid = table(root).current_snapshot().snapshot_id
    before = table(root).schema().schema_id
    evolved = evolve(root)
    assert evolved["schema_id"] > before
    assert "ingestion_source" in evolved["columns"]
    assert evolve(root)["schema_id"] == evolved["schema_id"]
    v2 = dict(event, event_id="e2", schema_version=2, service_level="express")
    record_event(root, v2)
    consume(root)
    raw = sorted(table(root).scan().to_arrow().to_pylist(), key=lambda r: r["source_seq"])
    assert raw[0]["ingestion_source"] is None
    assert raw[1]["ingestion_source"] == "sqlite-outbox"
    assert len(table(root).scan(snapshot_id=sid).to_arrow()) == 1
    project(root)
    assert {r["service_level"] for r in read(root, "events")} == {None, "express"}


@pytest.mark.parametrize("fail_at", ["projection_before_publish", "projection_after_publish"])
def test_projection_publish_recovery_keeps_old_generation(root, event, fail_at):
    add(root, [event])
    project(root)
    old_pointer = (root / "CURRENT.json").read_text()
    add(root, [dict(event, event_id="e2")])
    with pytest.raises(InjectedCrash):
        project(root, fail_at=fail_at)
    assert (root / "CURRENT.json").read_text() == old_pointer
    assert len(read(root, "events")) == 1
    project(root)
    assert len(read(root, "events")) == 2


def test_projection_tamper_detected(root, event):
    add(root, [event])
    project(root)
    path = current(root)
    (path / "events.parquet").write_bytes(b"bad")
    with pytest.raises(IntegrityError, match="checksum"):
        project(root)


def test_malformed_and_unknown_schema_quarantine(root, event):
    add(
        root,
        [
            '{"broken":',
            "[]",
            dict(event, schema_version=99),
            dict(event, event_time="2026-01-01T00:00:00"),
            dict(event, extra="oops"),
            dict(event, schema_version=True),
        ],
    )
    result = project(root)
    assert result["metrics"]["quarantine_rows"] == 6
    assert result["metrics"]["accepted_events"] == 0


def test_projection_row_budget_checked_before_materializing(root, event):
    add(root, [event, dict(event, event_id="e2")])
    with pytest.raises(ValueError, match="max-rows"):
        project(root, max_rows=1)
    assert not (root / "CURRENT.json").exists()


@pytest.mark.parametrize(
    "change",
    [
        {"status": "teleported"},
        {"event_id": ""},
        {"schema_version": "1"},
        {"event_time": "2200-01-01T00:00:00Z"},
        {"schema_version": 2, "service_level": "overnight"},
    ],
)
def test_validation_contract(event, change):
    payload = canonical({**event, **change})
    with pytest.raises(ValueError):
        validate({"source_seq": 1, "payload": payload, "payload_hash": sha256(payload)})


def test_normalized_timezones_deduplicate(root, event):
    add(root, [event, dict(event, event_time="2025-12-31T19:00:00-05:00")])
    result = project(root)
    assert result["metrics"]["accepted_events"] == 1
    assert result["metrics"]["duplicate_rows"] == 1


def test_deeply_nested_malformed_payload_quarantines(root):
    add(root, ["[" * 2000 + "]" * 2000])
    result = project(root)
    assert result["metrics"]["quarantine_rows"] == 1


def test_hash_corruption_quarantines_in_projection(root, event):
    raw = {"source_seq": 1, "payload": canonical(event), "payload_hash": "invalid"}
    with pytest.raises(ValueError, match="payload_hash mismatch"):
        validate(raw)


def test_exact_lateness_boundary_is_not_late(root, event):
    add(root, [dict(event, event_id="future", event_time="2026-01-01T01:00:00Z"), event])
    result = project(root)
    assert result["metrics"]["late_events"] == 0


def test_new_historical_window_emits_correction(root, event):
    add(root, [dict(event, event_id="future", event_time="2026-01-01T04:00:00Z")])
    project(root)
    add(root, [event])
    project(root)
    assert read(root, "corrections")[0]["old_event_count"] == 0
    assert read(root, "corrections")[0]["new_event_count"] == 1


def test_invalid_unicode_field_quarantines_instead_of_poisoning_batch(root, event):
    add(root, [dict(event, event_id="bad\ud800"), event])
    result = project(root)
    assert result["metrics"]["quarantine_rows"] == 1
    assert result["metrics"]["accepted_events"] == 1


@pytest.mark.parametrize("fail_at", ["projection_before_publish", "projection_after_publish"])
def test_abrupt_exit_during_publication_preserves_old_pointer(root, event, fail_at):
    import os
    import subprocess
    import sys

    add(root, [event])
    project(root)
    old_pointer = (root / "CURRENT.json").read_text()
    add(root, [dict(event, event_id="e2")])
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "parcel_lakehouse",
            "--root",
            str(root),
            "project",
            "--fail-at",
            fail_at,
        ],
        env={**os.environ, "PARCEL_HARD_CRASH": "1"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 73, proc.stderr
    assert (root / "CURRENT.json").read_text() == old_pointer
    project(root)
    assert len(read(root, "events")) == 2
