import os
import subprocess
import sys

import pytest

from parcel_lakehouse.common import InjectedCrash, IntegrityError, writer_lock
from parcel_lakehouse.lake import consume, table
from parcel_lakehouse.source import connect, seed, source_stats


@pytest.mark.parametrize("fail_at", ["after_prepare", "after_iceberg_commit", "after_checkpoint"])
def test_recovery_at_each_boundary(root, fail_at):
    seed(root, parcels=3, anomalies=False)
    with pytest.raises(InjectedCrash):
        consume(root, batch_size=4, fail_at=fail_at)
    # A different requested batch size must not change a prepared batch's identity.
    result = consume(root, batch_size=2, max_batches=20)
    rows = table(root).scan().to_arrow().to_pylist()
    assert len(rows) == 9
    assert sorted(row["source_seq"] for row in rows) == list(range(1, 10))
    assert result["checkpoint"] == 9
    if fail_at == "after_iceberg_commit":
        assert result["recovered_batches"] == 1
    assert consume(root)["rows_consumed"] == 0
    assert len(table(root).scan().to_arrow()) == 9


@pytest.mark.parametrize("fail_at", ["after_prepare", "after_iceberg_commit", "after_checkpoint"])
def test_actual_abrupt_process_exit_recovery(root, fail_at):
    seed(root, parcels=2, anomalies=False)
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "parcel_lakehouse",
            "--root",
            str(root),
            "consume",
            "--batch-size",
            "2",
            "--fail-at",
            fail_at,
        ],
        env={**os.environ, "PARCEL_HARD_CRASH": "1"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 73, proc.stderr
    consume(root, batch_size=3)
    rows = table(root).scan().to_arrow().to_pylist()
    assert len(rows) == 6
    assert len({r["source_seq"] for r in rows}) == 6
    assert source_stats(root)["checkpoint"] == 6


def test_consumer_is_bounded(root):
    seed(root, parcels=10, anomalies=False)
    result = consume(root, batch_size=3, max_batches=2)
    assert result["rows_consumed"] == 6
    assert result["unconsumed_rows"] == 24


def test_changed_prepared_source_fails_closed(root):
    seed(root, parcels=2, anomalies=False)
    with pytest.raises(InjectedCrash):
        consume(root, batch_size=2, fail_at="after_prepare")
    with connect(root) as db, db:
        db.execute("UPDATE outbox SET payload='corrupt' WHERE seq=1")
    with pytest.raises(IntegrityError, match="source changed"):
        consume(root)
    assert table(root).current_snapshot() is None


def test_partial_restore_detected(root):
    seed(root, parcels=2, anomalies=False)
    consume(root)
    with connect(root) as db, db:
        db.execute("UPDATE checkpoint SET last_seq=0")
    with pytest.raises(IntegrityError, match="frontier"):
        consume(root)


def test_source_identity_prevents_cross_wiring(root):
    seed(root, parcels=2)
    consume(root)
    with connect(root) as db, db:
        db.execute("UPDATE metadata SET value='another-source' WHERE key='source_id'")
    with pytest.raises(IntegrityError, match="Source identity"):
        consume(root)


def test_duplicate_physical_batch_evidence_fails_closed(root):
    seed(root, parcels=2, anomalies=False)
    with pytest.raises(InjectedCrash):
        consume(root, batch_size=2, fail_at="after_iceberg_commit")
    target = table(root)
    target.append(target.scan().to_arrow())
    with pytest.raises(IntegrityError, match="incomplete or conflicting"):
        consume(root)


def test_single_writer_lock(root):
    with writer_lock(root):
        proc = subprocess.run(
            [sys.executable, "-m", "parcel_lakehouse", "--root", str(root), "consume"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert proc.returncode == 1
        assert "Another pipeline writer" in proc.stderr


@pytest.mark.parametrize("batch_size,max_batches", [(0, 1), (1, 0), (10001, 1)])
def test_invalid_bounds(root, batch_size, max_batches):
    with pytest.raises(ValueError):
        consume(root, batch_size, max_batches)
