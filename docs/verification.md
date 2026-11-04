# Verification record

Verified in a Linux cloud workspace on 2026-11-04 with Python 3.12.14. This is a local verification record, not a claim that remote GitHub Actions or a physical Mac has already run.

- `python -m pytest -q`: **49 passed**. Coverage includes transaction rollback, bounded ingestion, changed batch-size retries, real abrupt process exits at all three ingestion boundaries, physical duplicate prevention, conflict quarantine, malformed Unicode and deeply nested JSON, timezone normalization, event-time ties, lateness boundaries, late corrections and retractions, schema evolution, historical snapshot replay, publication crash recovery, checksums, and single-writer exclusion.
- `python -m ruff check .`: passed.
- `python -m ruff format --check .`: passed.
- `python -m pip check`: no broken requirements.
- `python -m compileall -q src tests examples`: passed.
- `python -m pip wheel --no-deps .`: wheel built successfully.
- README quickstarts and runbook exercises: demo, manual stages, recovery, schema evolution, replay and late corrections executed against real PyIceberg tables.
- `examples/query_outputs.py`: successfully queried a published generation with DuckDB.

The final small Linux smoke seeded 1,000 parcels, consumed 3,004 raw events in four batches, and published 3,000 accepted events, one duplicate and three quarantined rows. It recorded 227,472 KiB peak process RSS (about 222 MiB). Pipeline work after Python imports took about 0.64 seconds on that particular host. This is a reproducibility check, not a throughput or Mac performance guarantee; filesystem caches and host hardware affect results.

PyIceberg's pinned release emits upstream Pyparsing/Pydantic deprecation warnings. They did not fail these checks. Dependencies are pinned in `requirements.lock`; hosted Linux/macOS CI is configured separately in `.github/workflows/ci.yml`.

## Not established by these checks

- Power-loss durability across arbitrary filesystems or broken storage hardware.
- Windows support, multi-host coordination or distributed exactly-once delivery.
- A production-scale memory/throughput guarantee. Batch and input limits are not total process memory limits.
- Safe automatic snapshot expiration, compaction, source-outbox retention or metadata relocation. These features are deliberately absent.
- Remote CI outcome before this repository is pushed and those jobs actually run.
