# Data contract and metric semantics

## Bronze envelope

The Iceberg table is `parcel.bronze`, using format version 2. Every incoming outbox row is preserved before business validation.

| Field | Iceberg type | Meaning |
| --- | --- | --- |
| `source_seq` | required long | Monotonically allocated SQLite outbox sequence |
| `batch_id` | required string | Stable identity derived from source identity, range, and batch fingerprint |
| `payload` | required string | Original message text, including malformed JSON |
| `payload_hash` | required string | SHA-256 of the exact payload text |
| `received_at` | required string | Receipt timestamp supplied by the local transport |
| `ingestion_source` | optional string, after `evolve` | Null for older rows; `sqlite-outbox` for new rows |

Payload hashes are byte/text fidelity checks, not signatures or authentication. Two JSON strings with different whitespace have different raw hashes. The batch fingerprint also covers ordering and envelope content, so recovery can reject changed source records.

Source IDs are generated per environment. Reusing a catalog with an unrelated source database is not supported even when sequence numbers happen to match.

## Event payload

Example version 1:

```json
{
  "event_id": "evt-000001-2",
  "parcel_id": "parcel-000001",
  "status": "delivered",
  "event_time": "2026-01-01T01:22:00+00:00",
  "hub": "north",
  "schema_version": 1
}
```

Version 2 adds the optional `service_level` attribute, such as `express`. A payload version is distinct from the Iceberg schema ID. The seeded fixture contains both versions.

The outbox is a transport history. Importing raw NDJSON with `ingest` deliberately accepts malformed messages so they can be retained and quarantined; it does not mean every imported row is a valid parcel transition. Each line is capped at 64 KiB, and exceeding the configured import row cap rolls back the entire import transaction.

## Time vocabulary

- **Event time:** the business occurrence timestamp inside the payload.
- **Receipt time:** when the transport recorded that message, stored on the bronze envelope.
- **Source sequence:** arrival order in this local outbox, not business chronology.
- **Snapshot time:** when an Iceberg table state was committed, not the time of every event it contains.

Use timezone-qualified event timestamps. The seeded fixture uses UTC. The `record_event` API validates business events and stores operational event times in fixed-width UTC microsecond form before text comparison, so different timezone offsets do not misorder source state. Events arrive out of order on purpose. Do not substitute ingestion order for event time when deriving business state or hourly metrics.

## Analytical scope

The analytical tables are regenerated from a chosen bronze snapshot. A snapshot is a reproducible input boundary; it is not a promise that all events for a historical hour have arrived. Late data can change earlier hours. There is no watermark that permanently closes an hour and no approximation algorithm hiding dropped late records.

A correction compares newly computed aggregates with the previously published projection. It explains changed results; it is not itself an additional parcel event. Historical replay is separate from publication and must not update current results.

The complete transformation logic is in the repository's SQL and projection code. Validation rules and metric column definitions below should be read together with their tests when changing a contract.

## Exact acceptance and quarantine rules

Validation happens in Python before relational SQL deduplication:

- The raw payload must match its SHA-256 hash and parse as a JSON object.
- All six version-1 fields shown above are required. `schema_version` must be the integer 1 or 2, not a boolean or string.
- Unknown fields are rejected. Version 1 does not accept `service_level`; version 2 accepts it as absent, null, `standard`, or `express`.
- Event ID, parcel ID, status, and hub must be nonblank, UTF-8 encodable strings of at most 128 characters. Values are checked but not trimmed or case-normalized.
- Status must be exactly `created`, `in_transit`, `delivered`, or `exception`.
- Event time must be a timezone-qualified ISO-8601 string. It is normalized to UTC, and the normalized year must be in 2000–2100 inclusive.
- Hub is an arbitrary valid string, not restricted to the fixture's north/south/east values.

Invalid rows go to `quarantine.parquet` with `source_seq`, raw `payload`, and an error `reason`; their `event_id` is null because parsing/validation did not establish a trusted identity.

Valid payloads are normalized, including UTC event time and an explicit null service level, then hashed as canonical sorted-key JSON. SQL groups by event ID:

- Same event ID and same normalized content hash: retain the lowest source sequence and count the others as duplicate rows. JSON whitespace/key order alone does not create a business conflict.
- Same event ID and different normalized content hashes: quarantine **all valid rows for that ID**, including any otherwise identical copies, with reason `conflicting_event_id`. No winner is chosen. The contract version is part of this content hash.

A malformed row does not poison an otherwise valid ID because it never reaches the validated relation. If a later snapshot introduces a conflicting version of an earlier accepted event, the full rebuild removes that ID from accepted events and can reduce old aggregate counts.

## Derived tables

| File | Grain and meaning |
| --- | --- |
| `events.parquet` | One accepted normalized event per event ID, including `source_seq`, `content_hash`, and `is_late` |
| `parcel_status.parquet` | One row per parcel with an accepted event; latest by event time, then status rank, then descending event ID |
| `hourly_metrics.parquet` | One row per UTC event-time hour and hub |
| `quarantine.parquet` | One row per rejected raw source row, including all conflict participants |
| `corrections.parquet` | Changed hour/hub counts relative to the previous publication under the rules below |

For equal event times, the status rank is `delivered > exception > in_transit > created`. Event ID descending resolves the remaining tie. A later event time beats status rank: this is not a monotonic “delivered forever” state machine. Operational parcel state and analytical state use different logic and are not assumed identical.

Hourly columns:

- `window_start`: UTC hour containing the event time.
- `hub`: the event's hub, not necessarily the parcel's final hub.
- `event_count`: number of accepted unique events in that hour/hub.
- `delivered_count`: number of those events with status `delivered`, not a count of distinct parcels or proof of on-time delivery.
- `late_event_count`: number of those accepted events flagged late by source-order comparison.

## Lateness and watermark

Among accepted, deduplicated events ordered by source sequence, an event is late if its event time is **strictly earlier** than the maximum event time among preceding accepted events minus 60 minutes. The first event is not late. Receipt time is not used in this calculation.

The reported watermark is the maximum accepted event time minus 60 minutes. It is a teaching indicator, not an enforced drop threshold. Late events remain in accepted events and aggregates. The 60-minute allowance is fixed for projection version 1, so changing a parameter cannot silently change an existing snapshot's interpretation.

## Corrections are comparisons, not a second event stream

On the first publication there is no prior report, so `corrected_windows` is zero even if `late_events` is nonzero. On a subsequent new snapshot, emit a correction when event count or delivered count changes and either:

1. the hour/hub existed in the previous projection, or
2. it is a newly appearing hour/hub whose hour ends at or before the previous watermark.

Each correction records old/new event counts, old/new delivered counts, signed deltas, and reason `recomputed_from_snapshot`. Counts may fall when a new conflict invalidates previously accepted data. A change only in the late-event count does not emit a correction. Existing hours can receive corrections even if they are ahead of the watermark.

Corrections depend on the publication baseline, not only the bronze snapshot. The manifest records `previous_generation`, and an existing snapshot generation is reused without recomputing its correction history. Historical replays have no publication baseline and therefore no correction rows. Changing transformation semantics requires a deliberate projection-version/migration plan, not editing an already published directory.

## `metrics.json` definitions

| Key | Definition |
| --- | --- |
| `snapshot_id` | Bronze snapshot used for this projection |
| `raw_rows` | All physical bronze rows in that snapshot |
| `accepted_events` | Unique valid, nonconflicting event IDs |
| `duplicate_rows` | Valid redundant rows after excluding all conflict participants |
| `quarantine_rows` | Invalid raw rows plus conflict participants |
| `conflicting_rows` | Raw valid rows belonging to conflicted event IDs; a subset of quarantine |
| `late_events` | Accepted events flagged late |
| `parcel_count` | Distinct parcels represented by accepted events |
| `corrected_windows` | Number of correction records, each a distinct hour/hub |
| `watermark` | Maximum accepted event time minus 60 minutes, or null with no accepted events |
| `allowed_lateness_minutes` | 60 for projection version 1 |

The useful accounting identity is `raw_rows = accepted_events + duplicate_rows + quarantine_rows`. Do not add `conflicting_rows` again: those rows are already quarantined. The CLI's separate `source` section reports `source_rows`, `checkpoint`, and `unconsumed_rows`; a published projection can lag a later source or bronze state.
