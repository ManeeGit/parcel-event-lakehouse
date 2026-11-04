import pytest

from parcel_lakehouse.common import InjectedCrash
from parcel_lakehouse.source import connect, ingest_file, record_event, seed, source_stats


def test_transactional_outbox_rolls_back_business_write(root, event):
    with pytest.raises(InjectedCrash):
        record_event(root, event, fail_at="after_business_write")
    with connect(root) as db:
        assert db.execute("SELECT count(*) FROM parcels").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    record_event(root, event)
    with connect(root) as db:
        assert db.execute("SELECT count(*) FROM parcels").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1


def test_seed_bounded_repeatable_and_no_accidental_double_seed(root, tmp_path):
    other = tmp_path / "other"
    assert seed(root, 3)["outbox_rows"] == 13
    seed(other, 3)
    with connect(root) as a, connect(other) as b:
        assert [tuple(r) for r in a.execute("SELECT * FROM outbox")] == [
            tuple(r) for r in b.execute("SELECT * FROM outbox")
        ]
    with pytest.raises(ValueError, match="empty"):
        seed(root, 3)
    assert source_stats(root)["source_rows"] == 13


@pytest.mark.parametrize("count", [0, 1, 2])
def test_ingest_line_counts(root, tmp_path, count):
    path = tmp_path / "input.jsonl"
    path.write_text("{}\n" * count)
    assert ingest_file(root, path) == count
    assert source_stats(root)["source_rows"] == count


def test_ingest_limit_rolls_back_whole_import(root, tmp_path):
    path = tmp_path / "input.jsonl"
    path.write_text("{}\n{}\n")
    with pytest.raises(ValueError, match="rolled back"):
        ingest_file(root, path, max_rows=1)
    assert source_stats(root)["source_rows"] == 0


def test_ingest_size_limit(root, tmp_path):
    path = tmp_path / "input.jsonl"
    path.write_text("x" * 65537)
    with pytest.raises(ValueError, match="64 KiB"):
        ingest_file(root, path)
    assert source_stats(root)["source_rows"] == 0


def test_failed_outbox_insert_rolls_back_business_update(root, event, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("simulated outbox failure")

    monkeypatch.setattr("parcel_lakehouse.source.enqueue", fail)
    with pytest.raises(RuntimeError, match="outbox failure"):
        record_event(root, event)
    with connect(root) as db:
        assert db.execute("SELECT count(*) FROM parcels").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


def test_operational_timestamp_comparison_normalizes_offsets(root, event):
    record_event(root, dict(event, event_time="2026-01-01T09:00:00+05:00"))
    record_event(
        root, dict(event, event_id="e2", status="delivered", event_time="2026-01-01T08:00:00Z")
    )
    with connect(root) as db:
        row = db.execute("SELECT status,event_time FROM parcels").fetchone()
        assert row["status"] == "delivered"
        assert row["event_time"] == "2026-01-01T08:00:00.000000+00:00"
