# Operations and recovery runbook

## Before running

Use Python 3.11–3.13 on macOS or Linux, a virtual environment, and a local writable disk. The pipeline uses POSIX `flock`, so native Windows is not supported; use a Linux environment instead. Start with the small fictional fixture. Never use an existing directory from an unrelated experiment.

The CLI root selects a complete local environment. Preserve its source database, catalog database, Iceberg metadata and Parquet files, and state together. Paths inside table metadata can be absolute: moving only the warehouse or renaming the root is not a supported migration.

## Recovery decision table

| Failure point | Durable state | Correct next action |
| --- | --- | --- |
| Before source transaction commit | Neither business update nor outbox is committed | Retry the source operation |
| After business write, before outbox insert | The uncommitted transaction rolls back | Retry the complete business operation |
| After batch preparation, before Iceberg append | Prepared batch survives; checkpoint unchanged | Retry ingestion with that same prepared batch |
| After Iceberg commit, before checkpoint | Snapshot/data evidence exists; checkpoint is old | Recover the committed batch, then checkpoint without appending it again |
| After checkpoint | Batch is complete | Continue from the next source sequence |
| During silver computation | Bronze is intact; prior published output remains authoritative | Retry projection after resolving the underlying error |
| Another writer is running | Local advisory lock is held | Wait for the writer or investigate it; do not bypass the lock |
| Source, batch, or snapshot evidence disagrees | Integrity cannot be established | Stop and inspect; do not advance offsets or guess |
| Disk is full or files are unavailable | Commit outcome may need inspection | Restore access/capacity and use normal recovery before rerunning blindly |

Failure injection raises an intentional exception. This is useful for exercising transaction ordering but does not simulate every OS crash, filesystem failure, or power-loss behavior. Do not claim complete power-failure durability from these tests alone.

## Triage checklist

1. Capture the exact command, root directory, exception, and code revision.
2. Ensure you are using the intended virtual environment and installed package.
3. Check free disk and whether a writer is still active.
4. Inspect the source checkpoint, prepared batches, and current Iceberg snapshots.
5. Preserve the entire environment before manual investigation.
6. Retry the ordinary command if the error is a recoverable interruption. Let the program select recovery behavior.
7. If integrity checks fail, keep the evidence and inspect the corresponding source records. Never “fix” recovery by skipping rows or deleting the pending batch.

## Common symptoms

**“Another pipeline writer holds writer.lock”**: another process owns the kernel lock. The lock file's presence alone does not mean a writer is alive. Do not delete it: replacing the inode can let two processes hold different locks under the same name.

**Seed requires an empty source**: seeding is deliberately not additive. Choose a fresh root for a new experiment; do not wipe a root you need to preserve.

**Malformed records**: these are expected in the default fixture. Inspect quarantine and its reasons. Raw ingestion success does not imply business-event validity.

**Conflicting event ID**: two different payloads used the same identity. Quarantine is intentional. An upstream fix should provide a well-defined correction policy rather than silently mutating historical bronze rows.

**Older metrics changed**: check for late events and event-time buckets. The demo recomputes history; it does not enforce a finalization watermark.

**Memory pressure**: reduce fixture size. Bronze batches bound append work, but the analytical projection rebuild reads the complete bronze dataset. Do not assume changing batch size fixes a full-scan memory limit.

## Backup and reset

Stop all writers before making a filesystem copy. SQLite may have WAL files and Iceberg references a collection of metadata and data files. Copying one `.db` file while a writer runs is not a verified backup strategy. For this teaching project, preserve the full root consistently and test restoration in a separate location with the original path assumptions considered.

For a fresh lesson, choose a new `--root` directory. This is safer than deleting the old one and leaves evidence available for comparison. There is no automatic destructive reset or production backup/restore service here.

## Repeatable failure labs

Use a new root for each failure point. For example:

```bash
parcel-lakehouse --root .state-prepare seed --parcels 5 --no-anomalies
parcel-lakehouse --root .state-prepare consume --batch-size 5 --fail-at after_prepare
parcel-lakehouse --root .state-prepare consume --batch-size 5
```

Repeat with roots `.state-commit` and `.state-checkpoint`, substituting `after_iceberg_commit` and `after_checkpoint`. The injected command is expected to fail. The next ordinary consumer run should finish the backlog. `after_business_write` is exercised by the Python `record_event` API and tests, not the `consume` command.

Inspect SQLite without a separate command-line client:

```bash
python - <<'PY'
import sqlite3
from pathlib import Path
root = Path('.state-prepare')  # use the root for your experiment
with sqlite3.connect(root / 'source.db') as db:
    print('Checkpoint:', db.execute('SELECT * FROM checkpoint').fetchall())
    print('Batches:', db.execute(
        'SELECT start_seq,end_seq,row_count,state,snapshot_id FROM batches'
    ).fetchall())
PY
parcel-lakehouse --root .state-prepare snapshots
```

The source contains 15 raw rows in each clean five-parcel lab. After recovery, all rows should be checkpointed and each source sequence should appear once in physical bronze. A recovered batch count of zero is correct for `after_prepare` (no prior commit) and `after_checkpoint` (already checkpointed); `after_iceberg_commit` should report a recovered batch.

## Historical projection

```bash
parcel-lakehouse --root .state snapshots
# Replace the number below with a real snapshot ID printed above:
parcel-lakehouse --root .state project --snapshot-id 123456789
```

The last command is an illustrative template, not a valid fixed snapshot. Historical outputs go under `replays/` and must not replace `CURRENT.json`. Snapshot IDs vary between runs. Projection limits are explicit: use `project --max-rows N` to lower the cap when needed; the default and hard maximum are 200,000 raw rows. A separate 64 MiB Arrow-input budget also applies.


## Hard exits and publication failures

For a subprocess-level crash rather than an exception, prefix a failure-injection command with `PARCEL_HARD_CRASH=1`. The process exits immediately with status 73; Python cleanup handlers do not run. Use only a disposable lab root.

```bash
PARCEL_HARD_CRASH=1 parcel-lakehouse --root .state-hard consume --fail-at after_iceberg_commit
```

Seed `.state-hard` first. Retry without the environment variable and failure flag. This tests process death, not physical power loss.

Projection supports `--fail-at projection_before_publish` and `--fail-at projection_after_publish`. The latter is after the complete generation directory is renamed but before `CURRENT.json` is updated. Retry the ordinary `project` command: it verifies checksums and publishes that already-complete generation. A pre-publication hard exit may leave a `.building-*` directory; it is not a published output. Readers follow `CURRENT.json`, not temporary directories.
