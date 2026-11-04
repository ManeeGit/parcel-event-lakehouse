# Learning guide

Work through these checkpoints in order. The objective is to explain a failure and its recovery, not merely to get a green terminal.

## 1. Find the transaction

Read the source write path. Locate the single SQLite transaction containing the parcel change and outbox insert.

**Explain:** What would happen if those writes were performed in separate transactions? Why does an event ID not, by itself, guarantee that an event is delivered?

## 2. Inspect the lake instead of only the report

Run the demo, inspect the data directory, and locate the catalog database, Iceberg metadata, and Parquet files. Inspect the table's snapshots through the supplied CLI or PyIceberg.

**Explain:** Which file contains records? Which metadata points at the current table state? Why is a directory full of Parquet files alone not an Iceberg table?

## 3. Follow a batch

Trace a source sequence from SQLite through bronze to the checkpoint. Run ingestion again with no new source records.

**Expect:** No new source data is appended merely because the command is repeated. Distinguish a no-op ingestion from an intentionally repeated analytical rebuild.

**Explain:** Why must checkpointing follow the table commit rather than precede it?

## 4. Interrupt ingestion

Use the failure-injection commands in the runbook. Inspect state before retrying. Compare the final physical bronze row count and source checkpoint with a clean run.

**Explain:** How does recovery distinguish “commit never happened” from “commit happened but the acknowledgment was lost”? What state must you preserve for this to work?

## 5. Test data quality

Inspect duplicate, malformed, and conflicting events in the fixture and tests. Follow each through the valid-event or quarantine SQL.

**Explain:** Why can two identical messages be collapsed while two different bodies with the same ID should not be silently collapsed? Why retain malformed raw input in bronze?

## 6. Make a late event visible

Run the late-event example. Compare event-time hourly metrics before and after it. The new message arrives now but can change an old hour.

**Explain:** Why does a count by ingestion time tell a different story from a count by event time? Is an old hourly result final in this system?

## 7. Rebuild derived state

Recompute silver from bronze without creating new source events. Compare semantic outputs rather than run timestamps or file names.

**Explain:** Which outputs can be reconstructed? Which source/catalog/checkpoint files cannot simply be deleted? Why is resetting the ingestion offset not the same as replaying an analytical projection?

## 8. Describe the production gap

Write a short design note proposing a multi-writer catalog, object storage, operational monitoring, retention, and compaction. Identify what must change in the recovery protocol before moving off local disk.

**Explain:** Which guarantees are demonstrated by tests and which would need new failure testing? Avoid claiming scale based only on the presence of Iceberg.

## Suggested portfolio explanation

“I built a local parcel-event lakehouse using a transactional outbox, real Iceberg/Parquet bronze storage, and DuckDB projections. I tested the commit-before-checkpoint failure window, handled event duplicates and conflicts explicitly, and showed late events correcting event-time aggregates. It is a single-machine teaching system, not a production streaming cluster.”

## Hands-on: observe an old hour change

After running the README demo under `.state`, import one additional fictional event:

```bash
cat > /tmp/parcel-late-example.jsonl <<'JSONL'
{"event_id":"extra-late-001","parcel_id":"extra-parcel-001","status":"delivered","event_time":"2026-01-01T00:05:00Z","hub":"north","schema_version":2,"service_level":"express"}
JSONL
parcel-lakehouse --root .state ingest /tmp/parcel-late-example.jsonl
parcel-lakehouse --root .state consume
parcel-lakehouse --root .state project
parcel-lakehouse --root .state metrics
```

The new accepted event belongs to the 00:00 UTC hour on January 1, 2026, irrespective of today's receipt time. That existing north/hub hour should gain one event and one delivered event. Inspect the published correction file:

```bash
python - <<'PY'
import json
from pathlib import Path
import pyarrow.parquet as pq
root = Path('.state')
generation = json.loads((root / 'CURRENT.json').read_text())['generation']
print(pq.read_table(root / 'projections' / generation / 'corrections.parquet').to_pylist())
PY
```

Run this once for the intended `+1` comparison. Importing the identical line again creates another raw row but the event is deduplicated, so it should not add another accepted event. Keep the same root throughout the exercise.
