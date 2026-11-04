from pathlib import Path

import pytest

from parcel_lakehouse.source import initialize


@pytest.fixture
def root(tmp_path: Path) -> Path:
    initialize(tmp_path)
    return tmp_path


@pytest.fixture
def event() -> dict:
    return {
        "event_id": "e1",
        "parcel_id": "p1",
        "status": "created",
        "event_time": "2026-01-01T00:00:00+00:00",
        "hub": "north",
        "schema_version": 1,
    }
