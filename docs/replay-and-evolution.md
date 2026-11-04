# Replay, snapshots, and schema evolution

## Three different meanings of replay

1. **Retry an interrupted ingestion batch:** use the normal ingestion command and recovery state. The original batch identity must survive the retry.
2. **Rebuild silver:** rerun SQL projections against the existing bronze table. This is the usual way to fix an analytical transformation without rewriting raw history.
3. **Re-ingest into a new environment:** create a separate working directory and catalog, then feed the original source records under an explicit migration/reprocessing plan. This creates a new history and is not the same as recovering a pending batch.

Do not manually reset the checkpoint in the same live lake. That bypasses the normal source-to-batch mapping and may create physical duplicates or invalidate recovery assumptions.

## What an Iceberg snapshot means

An Iceberg append commits table metadata referencing a set of data files. A snapshot ID identifies that committed view, not a wall-clock business watermark. Current-state reports and event-time metrics can differ between snapshots when late events arrive.

PyIceberg exposes snapshot metadata and historical scans. For a loaded table:

```python
for snapshot in table.snapshots():
    print(snapshot.snapshot_id, snapshot.parent_snapshot_id, snapshot.summary)

# Use an actual snapshot ID from your table, not this illustrative placeholder.
# historical_rows = table.scan(snapshot_id=YOUR_SNAPSHOT_ID).to_arrow()
```

A historical bronze scan does not automatically produce a historical silver report: run the same version of your projection over that snapshot if you want reproducible analytical results. Record the code version, input snapshot, contract version, and parameters together.

Snapshot expiration and orphan-file cleanup are not harmless housekeeping while a pending batch may require commit evidence. This project does not provide a production-safe retention policy. Keep snapshots and files intact while learning the recovery flow.

## Envelope schema versus payload schema

The bronze envelope stores raw payload text and ingestion metadata. Adding a field inside the JSON payload is different from adding an Iceberg column:

- An extra JSON field can be retained in bronze without immediately changing the table schema. Silver still needs explicit parsing and validation rules if the field is to be used.
- Adding a typed envelope column changes the Iceberg schema and the Arrow writer contract. Update both, and test old and new rows.
- Renaming a JSON key does not gain Iceberg's field-ID behavior. It is a payload-contract change and needs compatibility logic or a versioned event type.

## A safe optional-column exercise

Do this only in a separate disposable workspace after completing the basic demo:

1. Save the current snapshot ID and code revision.
2. Add an optional field to the payload contract; teach the projection to accept its absence.
3. Add tests containing older messages, newer messages, and invalid values.
4. Rebuild silver from the same bronze and verify the existing metrics do not change unexpectedly.
5. If you instead add a typed Iceberg column, use the catalog/table schema API rather than editing metadata JSON by hand. Ensure the writer supplies a compatible Arrow schema.

The supplied `evolve` command demonstrates one compatible Iceberg change: adding the optional string column `ingestion_source`. Existing rows read as null for this new field; subsequent ingested rows carry `sqlite-outbox`. Repeating the command is a no-op. It does not rewrite historical files.

```bash
parcel-lakehouse --root .state evolve
```

The broader exercise above is guidance, not a claim that general automated schema migration, compatibility enforcement, or rollback tooling is shipped. Treat destructive type changes and required-field additions as separate migrations requiring design and tests.
