"""Read a checksum-verified published generation, never unfinished staging files.

Run after the demo: python examples/query_outputs.py .state
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

from parcel_lakehouse.projection import current

root = Path(sys.argv[1] if len(sys.argv) > 1 else ".state").resolve()
generation = current(root)
if generation is None:
    raise SystemExit("No published generation. Run consume then project first.")
with duckdb.connect() as db:
    db.execute("SET memory_limit='512MB'")
    db.execute("SET threads=2")
    db.read_parquet(str(generation / "parcel_status.parquet")).create_view("parcel_status")
    result = db.execute("""
        SELECT status, count(*) AS parcels
        FROM parcel_status GROUP BY status ORDER BY parcels DESC, status
    """).fetchall()
    print(json.dumps([{"status": status, "parcels": count} for status, count in result], indent=2))
