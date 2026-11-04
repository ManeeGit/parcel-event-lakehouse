from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path


class IntegrityError(RuntimeError):
    """Persisted evidence differs from the expected source. Do not guess or skip."""


class InjectedCrash(RuntimeError):
    """An intentional crash point for recovery exercises."""


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def now() -> str:
    return datetime.now(UTC).isoformat()


def crash(point: str | None, expected: str) -> None:
    if point == expected:
        if os.environ.get("PARCEL_HARD_CRASH") == "1":
            os._exit(73)
        raise InjectedCrash(f"Injected crash: {point}")


@contextmanager
def writer_lock(root: Path):
    """Kernel releases the advisory lock even on SIGKILL; never unlink this file."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / "writer.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another pipeline writer holds writer.lock") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    directory = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
