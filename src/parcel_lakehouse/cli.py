from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .common import InjectedCrash, IntegrityError, writer_lock
from .lake import bootstrap, consume, evolve, snapshots
from .projection import current, project
from .source import ingest_file, seed, source_stats


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(
        description="Inspect a real, local outbox → Iceberg → DuckDB pipeline"
    )
    cli.add_argument(
        "--root", type=Path, default=Path(".state"), help="State directory (default .state)"
    )
    sub = cli.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="Create the source database and Iceberg catalog/table")
    producer = sub.add_parser("seed", help="Create bounded deterministic fictional events")
    producer.add_argument("--parcels", type=int, default=20)
    producer.add_argument("--seed", type=int, default=7)
    producer.add_argument("--no-anomalies", action="store_true")
    ingest = sub.add_parser("ingest", help="Atomically import bounded raw JSONL transport messages")
    ingest.add_argument("file", type=Path)
    ingest.add_argument("--max-rows", type=int, default=1000)
    consumer = sub.add_parser("consume", help="Drain at most max-batches durable source batches")
    consumer.add_argument("--batch-size", type=int, default=100)
    consumer.add_argument("--max-batches", type=int, default=10)
    consumer.add_argument(
        "--fail-at", choices=["after_prepare", "after_iceberg_commit", "after_checkpoint"]
    )
    projection = sub.add_parser(
        "project", help="Publish current snapshot or replay a historical one"
    )
    projection.add_argument("--snapshot-id", type=int)
    projection.add_argument("--max-rows", type=int, default=200_000)
    projection.add_argument(
        "--fail-at", choices=["projection_before_publish", "projection_after_publish"]
    )
    sub.add_parser("evolve", help="Add optional ingestion_source to the real Iceberg schema")
    sub.add_parser("snapshots", help="List retained Iceberg snapshot IDs and batch evidence")
    sub.add_parser("metrics", help="Show source lag and currently published quality metrics")
    demo = sub.add_parser("demo", help="Seed an empty root, consume, and publish a projection")
    demo.add_argument("--parcels", type=int, default=20)
    return cli


def main(argv: list[str] | None = None) -> int:
    os.environ.setdefault("PYICEBERG_MAX_WORKERS", "2")
    import pyarrow as pa

    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    args = parser().parse_args(argv)
    root = args.root.resolve()
    try:
        match args.command:
            case "init":
                with writer_lock(root):
                    target = bootstrap(root)
                    result = {"root": str(root), "table": str(target.name())}
            case "seed":
                result = seed(root, args.parcels, args.seed, not args.no_anomalies)
            case "ingest":
                result = {"imported_rows": ingest_file(root, args.file, args.max_rows)}
            case "consume":
                result = consume(root, args.batch_size, args.max_batches, args.fail_at)
            case "project":
                result = project(root, args.snapshot_id, args.max_rows, fail_at=args.fail_at)
            case "evolve":
                result = evolve(root)
            case "snapshots":
                result = snapshots(root)
            case "metrics":
                path = current(root)
                result = {
                    "source": source_stats(root),
                    "projection": json.loads((path / "metrics.json").read_text()) if path else None,
                }
            case "demo":
                result = {
                    "seed": seed(root, args.parcels),
                    "consume": consume(root, batch_size=1000, max_batches=1000),
                    "projection": project(root),
                }
        print(json.dumps(result, indent=2, default=str, sort_keys=True))
        return 0
    except InjectedCrash as exc:
        print(str(exc), file=sys.stderr)
        return 73
    except (IntegrityError, ValueError, RuntimeError, FileNotFoundError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
