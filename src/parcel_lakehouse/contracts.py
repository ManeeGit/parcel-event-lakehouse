from __future__ import annotations

import json
from datetime import UTC, datetime

from .common import canonical, sha256


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("event_time must be an ISO-8601 string")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("event_time requires a timezone")
    result = result.astimezone(UTC)
    if not 2000 <= result.year <= 2100:
        raise ValueError("event_time must be in 2000..2100")
    return result


def validate(raw: dict) -> dict:
    if sha256(raw["payload"]) != raw["payload_hash"]:
        raise ValueError("payload_hash mismatch")
    event = json.loads(raw["payload"])
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    required = {"event_id", "parcel_id", "status", "event_time", "hub", "schema_version"}
    if not required.issubset(event):
        raise ValueError("missing required field")
    version = event["schema_version"]
    if type(version) is not int or version not in (1, 2):
        raise ValueError("unsupported schema_version")
    allowed = required | ({"service_level"} if version == 2 else set())
    if set(event) - allowed:
        raise ValueError("unknown field for schema_version")
    for key in ("event_id", "parcel_id", "status", "hub"):
        if not isinstance(event[key], str) or not event[key].strip() or len(event[key]) > 128:
            raise ValueError(f"{key} must be a nonempty string of at most 128 characters")
        try:
            event[key].encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError(f"{key} must contain valid Unicode") from exc
    if event["status"] not in ("created", "in_transit", "delivered", "exception"):
        raise ValueError("unsupported status")
    service = event.get("service_level")
    if service is not None and service not in ("standard", "express"):
        raise ValueError("service_level must be standard or express")
    event_time = _timestamp(event["event_time"])
    normalized = {**event, "event_time": event_time.isoformat(), "service_level": service}
    return {
        **normalized,
        "event_time": event_time,
        "source_seq": raw["source_seq"],
        "content_hash": sha256(canonical(normalized)),
    }
