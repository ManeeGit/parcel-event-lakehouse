# Architecture and correctness

## Why this project exists

This is a small, executable model of a logistics event platform. Its purpose is to teach the boundary between an operational transaction, durable analytical storage, and a rebuildable SQL projection. Everything runs on one machine. SQLite is the operational database and the Iceberg catalog backend; it is not a pretend replacement for the Parquet files in the lake.

```text
                       one SQLite transaction
                     +-------------------------+
fictional generator -> parcel state + outbox   |
                     +-------------+-----------+
                                   | ordered source sequence
                                   v
                          bounded ingestion batch
                          durable batch identity
                                   |
                          single-writer lock
                                   |
                                   v
                     +-------------------------+
                     | Iceberg bronze table    |
                     | raw envelope + payload  |
                     | real Parquet data files |
                     | snapshots and manifests |
                     +-------------+-----------+
                                   |
                     commit first, checkpoint second
                                   |
                    source offset stored in SQLite
                                   |
                                   v
                         DuckDB SQL projection
                     /             |             \
               valid events    quarantine     current status
                     \             |             /
                      event-time hourly aggregates
                                   |
                          atomic file publication
```

## The transaction boundaries

1. **Operational write:** the parcel change and its outbox record are written in the same SQLite transaction. Either both commit or neither does. Writing an event after committing the parcel would leave an unrepairable gap if the process died between the two operations.
2. **Bronze append:** the reader takes a bounded ordered batch from the outbox. Its durable identity and source-sequence bounds connect the append to the ingestion progress record. Iceberg commits a new table snapshot atomically.
3. **Checkpoint:** the source offset advances only after the Iceberg commit is known to have happened. The catalog and source checkpoint are separate transactions. This is the deliberate crash window the project explores.
4. **Projection publication:** silver results are rebuilt from durable bronze and published as files. They are derived state, not the authoritative event history.

These boundaries are not one distributed transaction. Recovery, stable identities, and careful ordering make the local workflow retryable.

## The hardest failure window

A process can die after Iceberg accepts an append but before SQLite records the new offset. Blindly retrying would append the same physical rows twice. Blindly advancing the checkpoint could lose data if the append never happened.

The recovery path preserves the original batch identity and checks durable Iceberg evidence before choosing whether to append or just finish checkpointing. Keep the catalog, table metadata, data files, and ingestion state together. Deleting snapshots or state behind the writer can remove the evidence it needs.

Distinguish two different duplicates:

- **Transport/retry duplication:** the same source records are retried after a crash. Batch recovery handles this at ingestion.
- **Business-event duplication:** separate source rows carry the same event ID. Silver validates and deduplicates them. Conflicting bodies for one event ID are quarantined rather than choosing an arbitrary winner.

Do not describe this as universal “exactly once.” The guarantee is constrained by a single writer, intact local state, retained commit evidence, and the implemented recovery protocol. External consumers still need their own idempotency strategy.

## Ordering is a business decision

Source sequence answers “when did this system record the message?” Event time answers “when did the business event happen?” They are not interchangeable. A delayed delivered event belongs to its original event-time hour and may change a previously computed result.

The current-state projection orders accepted events by event time, then status rank and stable tie breakers. This is deterministic, but it is not a substitute for a real carrier's transition rules. In particular, event-time ordering and a status precedence rule encode assumptions that a production team must agree on.

## Technology choices

| Component | Local implementation | What it teaches |
| --- | --- | --- |
| Operational store | SQLite | Transactions and the outbox pattern |
| Reader | Python bounded batches | Backpressure and checkpoint ordering |
| Catalog | PyIceberg SQL catalog backed by SQLite | Names, schemas, metadata pointers |
| Bronze storage | Iceberg with Parquet files | Append snapshots, durable raw history |
| SQL projection | DuckDB | Validation, deduplication, window functions |
| Serving outputs | Local files | Rebuildable analytical products |

Kafka, Spark, Kubernetes, and a cloud account are deliberately unnecessary. Adding them would change deployment and failure modes, not eliminate the need for these invariants.

## Local limits

- One machine and one writer, not a distributed service or high-availability system.
- Local file locking coordinates cooperating local processes, not writers on multiple hosts.
- Iceberg metadata is real, but this is not a demonstration of a production REST catalog, object-store concurrency, or a multi-engine governance layer.
- The appended batch is bounded, but ingestion frontier validation scans source-sequence data across bronze. The full silver rebuild also scans bronze. SQL processing and Arrow conversion can become memory limits. The default projection cap is 200,000 raw rows; the DuckDB 512 MB limit does not cover the entire Python process.
- An 8 GB Mac should use the small demo first, close memory-heavy apps, and increase event counts gradually. No measured throughput or capacity claim is implied.
- Repeated small batches produce small files and more snapshots. Production pipelines need measured compaction, retention, catalog backup, and observability policies.
- All parcel data is fictional. Do not load customer addresses or personal identifiers into this teaching repository.
