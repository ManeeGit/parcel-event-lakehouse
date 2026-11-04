# Parcel Event Lakehouse

A local, runnable data-engineering project that follows fictional parcel events from a **transactional outbox** into a **real Apache Iceberg table**, then builds **DuckDB SQL projections** with deduplication, quarantine, current parcel status, and late-event corrections.

The interesting part is failure recovery: crash after an Iceberg commit but before the source checkpoint, restart, and verify that the batch is not physically appended twice.

**No Kafka, Spark, Docker, or cloud account required.** Designed for learning on macOS or Linux, including a small demo on an 8 GB Mac. It is a single-machine teaching project, not a production streaming platform.

## Quickstart

Use Python 3.11–3.13. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
parcel-lakehouse --root .state demo --parcels 20
parcel-lakehouse --root .state metrics
parcel-lakehouse --root .state snapshots
python -m pytest -q
python -m ruff check .
```

To reproduce the tested dependency versions, install with `python -m pip install -r requirements.lock -e ".[dev]"`. The lock records one tested environment; platform-specific wheel availability can vary.

The first installation needs internet access for Python packages. The pipeline itself uses local files and seeded fictional data. Run the demo with a fresh root; seeding refuses to overwrite an existing source.

The default 20-parcel fixture creates 60 lifecycle messages plus 4 anomaly messages: an exact duplicate, malformed JSON, a conflicting event identity, and a late version-2 event. Silver keeps the raw evidence in bronze and applies its documented quality rules rather than silently discarding problems. The default run reports 64 raw rows, 60 accepted events, 1 duplicate row, and 3 quarantined rows (including both conflict participants).

## What you build

```text
SQLite parcel + outbox transaction
        |
        v
bounded batches + durable recovery state
        |
        v
Iceberg bronze / real Parquet / SQLite catalog
        |
        v
DuckDB validation and deduplication
        |
        +--> accepted events and current parcel status
        +--> quarantined malformed/conflicting records
        +--> event-time hourly metrics and corrections
        |
        v
versioned output files + atomic CURRENT.json pointer
```

## Run each stage yourself

```bash
parcel-lakehouse --root .state-manual init
parcel-lakehouse --root .state-manual seed --parcels 20 --seed 7
parcel-lakehouse --root .state-manual consume --batch-size 25 --max-batches 10
parcel-lakehouse --root .state-manual project
parcel-lakehouse --root .state-manual metrics
```

`consume` is deliberately bounded by both batch size and maximum batch count. Inspect `unconsumed_rows`; if it is nonzero, run it again. Running an idle consumer does not create new events. Input lines can also be imported with `ingest FILE --max-rows 1000`; this transport simulator preserves raw messages for validation downstream and does not update operational parcel rows.

All `--root` flags precede the subcommand. Run `parcel-lakehouse --help` or `parcel-lakehouse consume --help` for the exact options.

## Try the recovery lab

Use a fresh directory:

```bash
parcel-lakehouse --root .state-recovery seed --parcels 5 --no-anomalies
parcel-lakehouse --root .state-recovery consume --batch-size 5 --fail-at after_iceberg_commit
# The intentional failure above must exit nonzero. Now recover without the flag:
parcel-lakehouse --root .state-recovery consume --batch-size 5
parcel-lakehouse --root .state-recovery project
parcel-lakehouse --root .state-recovery metrics
```

On recovery, the first already-committed batch is recognized from snapshot and row evidence before checkpointing. Expect a recovered batch and 15 total raw events after all three batches complete, not 20. Other ingestion failure points are `after_prepare` and `after_checkpoint`. See the [runbook](docs/runbook.md) for the failure matrix and inspection steps.

## Outputs

Under the selected root:

- `source.db`: operational parcels, outbox, checkpoints, and prepared/committed batch records.
- `catalog.db` and `warehouse/`: the local Iceberg catalog, table metadata, and Parquet data files.
- `projections/snapshot-ID/`: `events.parquet`, `parcel_status.parquet`, `hourly_metrics.parquet`, `quarantine.parquet`, `corrections.parquet`, `metrics.json`, and `manifest.json`.
- `CURRENT.json`: the atomically replaced pointer identifying the published projection.
- `replays/snapshot-ID/`: historical projection results, separate from current output.

Read `CURRENT.json` to find the current published version rather than guessing the latest directory name. Rebuilds and historical scans are explained in [replay and evolution](docs/replay-and-evolution.md).

Try `python examples/query_outputs.py .state` to query the published parcel states. `examples/events.jsonl` is a small import fixture containing out-of-order arrivals, a duplicate, a version-2 payload, and one intentionally malformed record.

## Learn the design

1. [Architecture and correctness](docs/architecture.md): transaction boundaries, recovery invariants, and local limits.
2. [Data contract and metrics](docs/data-contract.md): what is accepted, quarantined, counted, and corrected.
3. [Learning checkpoints](docs/learning-guide.md): small exercises and questions to explain the design.
4. [Operations runbook](docs/runbook.md): injected failures, retries, diagnostics, and backup cautions.
5. [Replay, snapshots, and schema evolution](docs/replay-and-evolution.md): historical reads and safe compatibility changes.

## Verification and scope

`make check` runs lint and tests after activating the virtual environment. The [verification record](docs/verification.md) records 49 passing local tests, a wheel build, and a small Linux memory smoke. GitHub Actions is configured for Python 3.11/3.12 on Linux and macOS; a workflow file is not evidence that those hosted jobs have run.

The fixture is reproducible in business content, not in source UUIDs, Iceberg snapshot IDs, or runtime timestamps. No external dataset or customer information is needed.

The project caps full analytical rebuilds at 200,000 raw rows and 64 MiB of Arrow input and configures DuckDB with a 512 MB memory limit and two threads. These settings are guardrails, not a guarantee that the whole Python process uses at most 512 MB. Start small; full-table validation, Arrow data, and SQL projections have additional memory costs. There is no benchmark-backed scale claim, continuous ingestion daemon, multi-host writer coordination, or automatic compaction/retention service.

## License

[MIT](LICENSE).
