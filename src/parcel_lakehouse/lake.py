from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from .common import IntegrityError, canonical, crash, now, sha256, writer_lock
from .source import connect, initialize, source_stats

IDENTIFIER = "parcel.bronze"
BRONZE_SCHEMA = Schema(
    NestedField(1, "source_seq", LongType(), required=True),
    NestedField(2, "batch_id", StringType(), required=True),
    NestedField(3, "payload", StringType(), required=True),
    NestedField(4, "payload_hash", StringType(), required=True),
    NestedField(5, "received_at", StringType(), required=True),
)


def catalog(root: Path) -> SqlCatalog:
    root = root.resolve()
    return SqlCatalog(
        "local", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=(root / "warehouse").as_uri()
    )


def table(root: Path):
    return catalog(root).load_table(IDENTIFIER)


def bootstrap(root: Path):
    source_id = initialize(root)
    cat = catalog(root)
    cat.create_namespace_if_not_exists("parcel")
    try:
        target = cat.load_table(IDENTIFIER)
    except NoSuchTableError:
        target = cat.create_table(
            IDENTIFIER,
            schema=BRONZE_SCHEMA,
            properties={"source_id": source_id, "format-version": "2"},
        )
    if target.properties.get("source_id") != source_id:
        raise IntegrityError(
            "Source identity differs from Iceberg table; restore a consistent backup"
        )
    return target


def _fingerprint(rows: list[dict]) -> str:
    return sha256(canonical(rows))


def _source_rows(db, start: int, end: int) -> list[dict]:
    return [
        dict(row)
        for row in db.execute(
            "SELECT seq,payload,payload_hash,received_at FROM outbox "
            "WHERE seq BETWEEN ? AND ? ORDER BY seq",
            (start, end),
        )
    ]


def _verify_payloads(rows: list[dict]) -> None:
    for row in rows:
        if sha256(row["payload"]) != row["payload_hash"]:
            raise IntegrityError(f"Source hash mismatch at sequence {row['seq']}")


def _prepare(root: Path, batch_size: int):
    with connect(root) as db, db:
        db.execute("BEGIN IMMEDIATE")
        pending = db.execute("SELECT * FROM batches WHERE state='prepared'").fetchone()
        checkpoint = db.execute(
            "SELECT last_seq FROM checkpoint WHERE consumer='bronze'"
        ).fetchone()[0]
        if pending:
            batch = dict(pending)
            rows = _source_rows(db, batch["start_seq"], batch["end_seq"])
            if batch["start_seq"] <= checkpoint:
                raise IntegrityError("Pending batch overlaps checkpoint")
            if len(rows) != batch["row_count"] or _fingerprint(rows) != batch["payload_hash"]:
                raise IntegrityError("Pending batch source changed; refusing an unsafe retry")
        else:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT seq,payload,payload_hash,received_at FROM outbox "
                    "WHERE seq>? ORDER BY seq LIMIT ?",
                    (checkpoint, batch_size),
                )
            ]
            if not rows:
                return None, []
            source_id = db.execute("SELECT value FROM metadata WHERE key='source_id'").fetchone()[0]
            digest = _fingerprint(rows)
            batch = {
                "batch_id": sha256(f"{source_id}:{rows[0]['seq']}:{rows[-1]['seq']}:{digest}"),
                "start_seq": rows[0]["seq"],
                "end_seq": rows[-1]["seq"],
                "row_count": len(rows),
                "payload_hash": digest,
                "state": "prepared",
                "snapshot_id": None,
                "created_at": now(),
            }
            db.execute(
                "INSERT INTO batches VALUES(:batch_id,:start_seq,:end_seq,:row_count,"
                ":payload_hash,:state,:snapshot_id,:created_at)",
                batch,
            )
        _verify_payloads(rows)
        return batch, rows


def _snapshot_evidence(target, batch: dict) -> int | None:
    found = []
    for snap in target.snapshots():
        props = snap.summary.additional_properties
        if props.get("batch_id") == batch["batch_id"]:
            if props.get("payload_hash") != batch["payload_hash"]:
                raise IntegrityError("Snapshot batch hash does not match prepared batch")
            found.append(snap.snapshot_id)
    if len(found) > 1:
        raise IntegrityError("Multiple snapshots claim the same batch")
    return found[0] if found else None


def _verify_commit(target, batch: dict, rows: list[dict]) -> int | None:
    """Use a committed table scan and snapshot summary, never a loose Parquet file."""
    committed = (
        target.scan(
            row_filter=And(
                GreaterThanOrEqual("source_seq", batch["start_seq"]),
                LessThanOrEqual("source_seq", batch["end_seq"]),
            )
        )
        .to_arrow()
        .to_pylist()
    )
    snapshot_id = _snapshot_evidence(target, batch)
    if not committed:
        if snapshot_id is not None:
            raise IntegrityError("Snapshot claims batch but its rows are missing")
        return None
    expected = [
        {
            "source_seq": r["seq"],
            "batch_id": batch["batch_id"],
            "payload": r["payload"],
            "payload_hash": r["payload_hash"],
            "received_at": r["received_at"],
        }
        for r in rows
    ]
    keys = list(expected[0])
    actual = [{key: row[key] for key in keys} for row in committed]
    actual.sort(key=lambda row: row["source_seq"])
    if actual != expected or snapshot_id is None:
        raise IntegrityError(
            "Iceberg batch evidence is incomplete or conflicting; refusing to append"
        )
    return snapshot_id


def _check_frontier(root: Path, target) -> None:
    """Catch unsupported partial database restores before they can duplicate old source ranges."""
    with connect(root) as db:
        checkpoint = db.execute(
            "SELECT last_seq FROM checkpoint WHERE consumer='bronze'"
        ).fetchone()[0]
        pending = db.execute("SELECT end_seq FROM batches WHERE state='prepared'").fetchone()
    if target.current_snapshot() is None:
        if checkpoint:
            raise IntegrityError("Checkpoint exists but lake has no snapshot")
        return
    max_seq = max(
        (
            pc.max(chunk.column(0)).as_py() or 0
            for chunk in target.scan(selected_fields=("source_seq",)).to_arrow_batch_reader()
        ),
        default=0,
    )
    allowed = {checkpoint, pending[0]} if pending else {checkpoint}
    if max_seq not in allowed:
        raise IntegrityError("Iceberg and source frontier disagree; restore all state together")


def consume(
    root: Path, batch_size: int = 100, max_batches: int = 10, fail_at: str | None = None
) -> dict:
    if not 1 <= batch_size <= 10_000 or not 1 <= max_batches <= 10_000:
        raise ValueError("batch_size and max_batches must each be in 1..10000")
    with writer_lock(root):
        target = bootstrap(root)
        _check_frontier(root, target)
        committed_batches = recovered_batches = rows_consumed = 0
        for _ in range(max_batches):
            batch, rows = _prepare(root, batch_size)
            if batch is None:
                break
            crash(fail_at, "after_prepare")
            target.refresh()
            snapshot_id = _verify_commit(target, batch, rows)
            if snapshot_id is None:
                output = [
                    {
                        "source_seq": r["seq"],
                        "batch_id": batch["batch_id"],
                        "payload": r["payload"],
                        "payload_hash": r["payload_hash"],
                        "received_at": r["received_at"],
                    }
                    for r in rows
                ]
                if "ingestion_source" in target.schema().column_names:
                    for row in output:
                        row["ingestion_source"] = "sqlite-outbox"
                arrow = pa.Table.from_pylist(output, schema=target.schema().as_arrow())
                target.append(
                    arrow,
                    snapshot_properties={
                        "batch_id": batch["batch_id"],
                        "payload_hash": batch["payload_hash"],
                        "source_start": str(batch["start_seq"]),
                        "source_end": str(batch["end_seq"]),
                    },
                )
                crash(fail_at, "after_iceberg_commit")
                target.refresh()
                snapshot_id = _verify_commit(target, batch, rows)
            else:
                recovered_batches += 1
            if snapshot_id is None:
                raise IntegrityError("Append returned without committed batch evidence")
            with connect(root) as db, db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "UPDATE batches SET state='committed', snapshot_id=? WHERE batch_id=?",
                    (snapshot_id, batch["batch_id"]),
                )
                db.execute(
                    "UPDATE checkpoint SET last_seq=? WHERE consumer='bronze'", (batch["end_seq"],)
                )
            crash(fail_at, "after_checkpoint")
            committed_batches += 1
            rows_consumed += len(rows)
        return {
            "committed_batches": committed_batches,
            "recovered_batches": recovered_batches,
            "rows_consumed": rows_consumed,
            **source_stats(root),
        }


def evolve(root: Path) -> dict:
    with writer_lock(root):
        target = bootstrap(root)
        if "ingestion_source" not in target.schema().column_names:
            with target.update_schema() as update:
                update.add_column("ingestion_source", StringType())
        target.refresh()
        return {"schema_id": target.schema().schema_id, "columns": target.schema().column_names}


def snapshots(root: Path) -> list[dict]:
    target = table(root)
    return [
        {
            "snapshot_id": s.snapshot_id,
            "parent_id": s.parent_snapshot_id,
            "timestamp_ms": s.timestamp_ms,
            "summary": s.summary.model_dump(),
        }
        for s in target.snapshots()
    ]
