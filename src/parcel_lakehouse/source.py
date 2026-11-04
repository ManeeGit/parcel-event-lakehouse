from __future__ import annotations

import random
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .common import canonical, crash, now, sha256
from .contracts import validate

DDL = """
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS parcels(
 parcel_id TEXT PRIMARY KEY, status TEXT NOT NULL, event_time TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbox(
 seq INTEGER PRIMARY KEY AUTOINCREMENT,
 payload TEXT NOT NULL, payload_hash TEXT NOT NULL, received_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoint(
 consumer TEXT PRIMARY KEY, last_seq INTEGER NOT NULL);
INSERT OR IGNORE INTO checkpoint VALUES('bronze', 0);
CREATE TABLE IF NOT EXISTS batches(
 batch_id TEXT PRIMARY KEY, start_seq INTEGER NOT NULL, end_seq INTEGER NOT NULL,
 row_count INTEGER NOT NULL, payload_hash TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('prepared','committed')),
 snapshot_id INTEGER, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_pending_batch ON batches(state) WHERE state='prepared';
"""


@contextmanager
def connect(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / "source.db", timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    try:
        yield db
    finally:
        db.close()


def initialize(root: Path) -> str:
    with connect(root) as db, db:
        db.executescript(DDL)
        db.execute("INSERT OR IGNORE INTO metadata VALUES('source_id', ?)", (str(uuid.uuid4()),))
        return db.execute("SELECT value FROM metadata WHERE key='source_id'").fetchone()[0]


def enqueue(db: sqlite3.Connection, payload: str, received_at: str | None = None) -> int:
    if len(payload.encode()) > 64 * 1024:
        raise ValueError("An event exceeds 64 KiB")
    cursor = db.execute(
        "INSERT INTO outbox(payload,payload_hash,received_at) VALUES(?,?,?)",
        (payload, sha256(payload), received_at or now()),
    )
    return cursor.lastrowid


def record_event(root: Path, event: dict, *, fail_at: str | None = None) -> int:
    """Business row and outbox insert are a single local database transaction."""
    payload = canonical(event)
    normalized = validate({"source_seq": 0, "payload": payload, "payload_hash": sha256(payload)})
    event_time = normalized["event_time"].isoformat(timespec="microseconds")
    with connect(root) as db, db:
        db.execute(
            "INSERT INTO parcels VALUES(?,?,?) ON CONFLICT(parcel_id) DO UPDATE SET "
            "status=excluded.status,event_time=excluded.event_time "
            "WHERE excluded.event_time >= parcels.event_time",
            (event["parcel_id"], event["status"], event_time),
        )
        crash(fail_at, "after_business_write")
        return enqueue(db, payload)


def seed(root: Path, parcels: int = 20, seed_value: int = 7, anomalies: bool = True) -> dict:
    """Deterministic fixture contents; generated once per source, never an endless loop."""
    if not 1 <= parcels <= 100_000:
        raise ValueError("parcels must be between 1 and 100000")
    initialize(root)
    rng = random.Random(seed_value)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    with connect(root) as db, db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute("SELECT count(*) FROM outbox").fetchone()[0]:
            raise ValueError("Seed requires an empty source; choose a new --root")
        events = []
        for i in range(parcels):
            for j, status in enumerate(("created", "in_transit", "delivered")):
                event = {
                    "event_id": f"evt-{i:06d}-{j}",
                    "parcel_id": f"parcel-{i:06d}",
                    "status": status,
                    "event_time": (base + timedelta(minutes=i * 2 + j * 40)).isoformat(
                        timespec="microseconds"
                    ),
                    "hub": rng.choice(("north", "south", "east")),
                    "schema_version": 1,
                }
                events.append(event)
        # Out-of-order arrival is intentional, while the source represents current business state.
        rng.shuffle(events)
        for index, event in enumerate(events):
            db.execute(
                "INSERT INTO parcels VALUES(?,?,?) ON CONFLICT(parcel_id) DO UPDATE SET "
                "status=excluded.status,event_time=excluded.event_time "
                "WHERE excluded.event_time >= parcels.event_time",
                (event["parcel_id"], event["status"], event["event_time"]),
            )
            enqueue(db, canonical(event), (base + timedelta(hours=3, seconds=index)).isoformat())
        if anomalies:
            enqueue(db, canonical(events[0]), (base + timedelta(hours=4)).isoformat())
            enqueue(db, '{"broken":', (base + timedelta(hours=4)).isoformat())
            conflict = dict(events[-1], hub="conflicting-hub")
            enqueue(db, canonical(conflict), (base + timedelta(hours=4)).isoformat())
            late = dict(
                events[0],
                event_id="evt-late",
                parcel_id="parcel-late",
                status="delivered",
                event_time=(base + timedelta(minutes=5)).isoformat(),
                schema_version=2,
                service_level="express",
            )
            enqueue(db, canonical(late), (base + timedelta(days=1)).isoformat())
        count = db.execute("SELECT count(*) FROM outbox").fetchone()[0]
    return {"parcels": parcels, "outbox_rows": count, "seed": seed_value}


def ingest_file(root: Path, path: Path, max_rows: int = 1000) -> int:
    """Transport simulator: preserve every raw line, even malformed JSON, for quarantine."""
    if not 1 <= max_rows <= 100_000:
        raise ValueError("max_rows must be between 1 and 100000")
    initialize(root)
    with path.open(encoding="utf-8") as handle, connect(root) as db, db:
        count = 0
        for index, line in enumerate(iter(lambda: handle.readline(65538), "")):
            if index >= max_rows:
                raise ValueError("Input exceeds --max-rows; entire import rolled back")
            if len(line.rstrip("\r\n").encode()) > 64 * 1024:
                raise ValueError("An event exceeds 64 KiB; entire import rolled back")
            enqueue(db, line.rstrip("\r\n"))
            count += 1
        return count


def source_stats(root: Path) -> dict:
    with connect(root) as db:
        total = db.execute("SELECT count(*) FROM outbox").fetchone()[0]
        offset = db.execute("SELECT last_seq FROM checkpoint WHERE consumer='bronze'").fetchone()[0]
        remaining = db.execute("SELECT count(*) FROM outbox WHERE seq>?", (offset,)).fetchone()[0]
        return {"source_rows": total, "checkpoint": offset, "unconsumed_rows": remaining}
