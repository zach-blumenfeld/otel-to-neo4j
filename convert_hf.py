#!/usr/bin/env python3
"""
Convert HuggingFace agent-trace datasets into OTLP-shape JSON that ingest.py consumes.

Two formats supported today:
  - halo    inference-net/HALO-* datasets. JSONL/parquet, one span per row,
            flat attributes dict with OpenInference keys.
  - trail   PatronusAI/TRAIL. Parquet, one span per row, plus step-level
            error annotations that become Span properties.

Usage:
    # HALO (open access; ships as a single traces.jsonl)
    hf download inference-net/HALO-Gemini-3-Flash-AppWorld \\
      --repo-type dataset --local-dir data/halo
    python convert_hf.py --format halo data/halo/traces.jsonl --out data/halo.json

    # TRAIL (gated; requires HF login + accepting terms first; ships as parquet)
    hf download PatronusAI/TRAIL \\
      --repo-type dataset --local-dir data/trail
    python convert_hf.py --format trail data/trail/*.parquet --out data/trail.json

    # Then ingest like any other OTLP file
    python ingest.py data/halo.json data/trail.json

The output is OTLP JSON — `resourceSpans -> scopeSpans -> spans` — which
ingest.py and normalize.py handle natively. The normalizer happens to accept
flat-dict attributes too (a quirk of how OTLP-via-JSON exporters vary), so we
don't even need to repackage attributes into the OTLP `[{key, value: ...}]`
list shape. We just bundle rows by trace_id and write the envelope.
"""

from __future__ import annotations
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

# Lazy import — only need pandas if a parquet file is given
def _load_rows(filepath: Path) -> Iterable[dict]:
    """Read JSON / JSONL / parquet into row dicts."""
    suffix = filepath.suffix.lower()
    if suffix == ".jsonl":
        with filepath.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
    elif suffix == ".json":
        with filepath.open() as f:
            data = json.load(f)
        if isinstance(data, list):
            yield from data
        else:
            yield data
    elif suffix == ".parquet":
        try:
            import pandas as pd
        except ImportError:
            sys.exit("pandas required to read parquet. Install with: pip install pandas pyarrow")
        df = pd.read_parquet(filepath)
        for record in df.to_dict(orient="records"):
            yield record
    else:
        sys.exit(f"Unsupported file extension: {suffix} (expected .json, .jsonl, or .parquet)")


def _iso_to_unix_nano(iso: str | None) -> str:
    """Parse ISO 8601 timestamp to nanoseconds since epoch as a string.
    OTLP wants a string; treats null/empty input as '0' (which my normalizer
    handles gracefully — duration_ms ends up None)."""
    if not iso:
        return "0"
    try:
        # ISO 8601 with nanosecond precision: "2023-05-18T12:00:00.000000000Z"
        # Python's fromisoformat handles up to microseconds; trim if longer.
        s = iso.rstrip("Z")
        # Truncate fractional part to microseconds (6 digits)
        if "." in s:
            base, frac = s.rsplit(".", 1)
            frac = frac[:6].ljust(6, "0")
            s = f"{base}.{frac}"
        dt = datetime.fromisoformat(s)
        return str(int(dt.timestamp() * 1_000_000_000))
    except (ValueError, TypeError):
        return "0"


def _normalize_attrs(raw: Any) -> dict[str, Any]:
    """Turn whatever 'attributes' shape we got into a flat dict.
    Handles: dict (most HF datasets), list of {key, value} (real OTLP),
    JSON string (some exports), None."""
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        # Already-OTLP shape — let normalize.py handle it natively
        # by passing through unchanged in the envelope.
        return raw  # type: ignore
    return {}


def _coerce_dict(value: Any) -> dict[str, Any]:
    """Pandas + parquet sometimes hands us numpy/pyarrow scalars or
    json-encoded strings where we expect dicts. Coerce defensively."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    # numpy/pyarrow have asdict-like behavior; try the dict ctor as a last resort
    try:
        return dict(value)
    except (TypeError, ValueError):
        return {}


# ============================================================================
# HALO converter
# ============================================================================

def convert_halo(rows: Iterable[dict]) -> dict:
    """Convert HALO format (one span per row) into OTLP JSON.

    HALO row schema (from inference-net/HALO-* datasets):
        trace_id, span_id, parent_span_id, trace_state, name, kind,
        start_time (ISO 8601), end_time (ISO 8601),
        status: {code, message},
        resource: {attributes: {...}},
        scope: {name, version},
        attributes: {openinference.span.kind, llm.*, tool.*, ...}
    """
    # Group spans by (resource fingerprint, scope fingerprint) so the OTLP
    # envelope is correctly structured. In practice HALO uses one resource
    # and one scope across the whole dataset, but we don't assume that.
    groups: dict[tuple, list[dict]] = defaultdict(list)
    resources: dict[tuple, dict] = {}
    scopes: dict[tuple, dict] = {}

    for row in rows:
        resource = _coerce_dict(row.get("resource"))
        scope = _coerce_dict(row.get("scope"))
        # Fingerprints are JSON-serialized so we can use them as dict keys
        rkey = json.dumps(resource, sort_keys=True, default=str)
        skey = json.dumps(scope, sort_keys=True, default=str)
        key = (rkey, skey)
        resources[key] = resource
        scopes[key] = scope

        status = _coerce_dict(row.get("status"))
        # HALO uses string status codes already (STATUS_CODE_OK, etc.) — pass through

        otlp_span = {
            "traceId": row.get("trace_id") or "",
            "spanId": row.get("span_id") or "",
            "parentSpanId": row.get("parent_span_id") or "",
            "name": row.get("name") or "",
            "startTimeUnixNano": _iso_to_unix_nano(row.get("start_time")),
            "endTimeUnixNano": _iso_to_unix_nano(row.get("end_time")),
            "status": status,
            "attributes": _normalize_attrs(row.get("attributes")),
            "events": row.get("events") or [],
            "links": row.get("links") or [],
        }
        groups[key].append(otlp_span)

    resource_spans = []
    for key, spans in groups.items():
        resource_spans.append({
            "resource": resources[key],
            "scopeSpans": [{
                "scope": scopes[key],
                "spans": spans,
            }],
        })

    return {"resourceSpans": resource_spans}


# ============================================================================
# TRAIL converter
# ============================================================================
#
# TRAIL is gated and I haven't downloaded a sample to verify the exact column
# schema. Per the dataset card it's OpenTelemetry/OpenInference, so we expect
# columns very similar to HALO — but with additional error annotation columns.
#
# Defensive strategy: try standard HALO-style columns first, fall back to
# alternative names where TRAIL might differ. Lift any error annotations onto
# spans as `trail.error_*` attributes so Q4 (error blast radius) lights up.
#
# If the schema differs in practice, the fix is small: edit FIELD_ALIASES below.
# ============================================================================

# Map of canonical field name -> ordered list of column names to try
TRAIL_FIELD_ALIASES = {
    "trace_id":       ["trace_id", "traceId"],
    "span_id":        ["span_id", "spanId"],
    "parent_span_id": ["parent_span_id", "parentSpanId", "parent_id"],
    "name":           ["name", "span_name"],
    "start_time":     ["start_time", "startTime", "start_time_iso"],
    "end_time":       ["end_time", "endTime", "end_time_iso"],
    "status":         ["status", "status_code"],
    "attributes":     ["attributes", "span_attributes"],
    "resource":       ["resource", "resource_attributes"],
    "scope":          ["scope", "instrumentation_scope"],
    # Error annotation columns — TRAIL's distinguishing feature
    "error_category": ["error_category", "category", "error_type"],
    "error_severity": ["error_severity", "severity", "impact_level", "impact"],
    "error_evidence": ["error_evidence", "evidence", "error_supporting_evidence"],
    "error_description": ["error_description", "description", "error_desc"],
}


def _pick(row: dict, canonical: str) -> Any:
    """Get a row value by canonical name, trying TRAIL_FIELD_ALIASES."""
    for candidate in TRAIL_FIELD_ALIASES.get(canonical, [canonical]):
        if candidate in row:
            return row[candidate]
    return None


def convert_trail(rows: Iterable[dict]) -> dict:
    """Convert TRAIL parquet rows into OTLP JSON, lifting error annotations
    onto each span as `trail.error_*` attributes."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    resources: dict[tuple, dict] = {}
    scopes: dict[tuple, dict] = {}

    for row in rows:
        resource = _coerce_dict(_pick(row, "resource"))
        scope = _coerce_dict(_pick(row, "scope"))
        rkey = json.dumps(resource, sort_keys=True, default=str)
        skey = json.dumps(scope, sort_keys=True, default=str)
        key = (rkey, skey)
        resources[key] = resource
        scopes[key] = scope

        # Start with the OpenInference attributes already on the span
        attrs = _normalize_attrs(_pick(row, "attributes"))
        if isinstance(attrs, list):
            # Already OTLP shape; convert to flat dict to merge error annotations,
            # then rely on normalize.py to read the flat dict
            from normalize import _attrs_to_dict
            attrs = _attrs_to_dict(attrs)

        # Lift TRAIL error annotations as custom span attributes.
        # Q4 (error blast radius) and Q6 (conversation flow) will now show
        # WHY a span errored, not just that it did.
        for canonical_field, attr_key in [
            ("error_category", "trail.error_category"),
            ("error_severity", "trail.error_severity"),
            ("error_evidence", "trail.error_evidence"),
            ("error_description", "trail.error_description"),
        ]:
            value = _pick(row, canonical_field)
            if value is not None and value != "":
                attrs[attr_key] = str(value)

        # If TRAIL marked this span as having an error, force status to ERROR
        # even if the original OTel status was OK (TRAIL annotates after the fact)
        original_status = _coerce_dict(_pick(row, "status"))
        status = dict(original_status) if original_status else {"code": "STATUS_CODE_UNSET"}
        if attrs.get("trail.error_category"):
            status["code"] = "STATUS_CODE_ERROR"
            if attrs.get("trail.error_description"):
                status["message"] = str(attrs["trail.error_description"])[:500]

        otlp_span = {
            "traceId": _pick(row, "trace_id") or "",
            "spanId": _pick(row, "span_id") or "",
            "parentSpanId": _pick(row, "parent_span_id") or "",
            "name": _pick(row, "name") or "",
            "startTimeUnixNano": _iso_to_unix_nano(_pick(row, "start_time")),
            "endTimeUnixNano": _iso_to_unix_nano(_pick(row, "end_time")),
            "status": status,
            "attributes": attrs,
            "events": row.get("events") or [],
            "links": row.get("links") or [],
        }
        groups[key].append(otlp_span)

    resource_spans = []
    for key, spans in groups.items():
        resource_spans.append({
            "resource": resources[key],
            "scopeSpans": [{
                "scope": scopes[key],
                "spans": spans,
            }],
        })

    return {"resourceSpans": resource_spans}


# ============================================================================
# CLI
# ============================================================================

CONVERTERS = {
    "halo": convert_halo,
    "trail": convert_trail,
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("inputs", nargs="+", type=Path,
                        help="Input file(s) — JSON, JSONL, or Parquet")
    parser.add_argument("--format", required=True, choices=list(CONVERTERS.keys()),
                        help="Source dataset format")
    parser.add_argument("--out", required=True, type=Path,
                        help="Output OTLP JSON file path")
    parser.add_argument("--max-traces", type=int, default=None,
                        help="Optional cap on number of distinct traces to convert")
    args = parser.parse_args()

    converter = CONVERTERS[args.format]

    # Stream rows from all inputs, optionally capped by trace count
    def stream_rows():
        seen_traces: set[str] = set()
        for filepath in args.inputs:
            if not filepath.exists():
                print(f"  ! Skipping {filepath} (not found)", file=sys.stderr)
                continue
            print(f"  Reading {filepath}...", file=sys.stderr)
            for row in _load_rows(filepath):
                if args.max_traces is not None:
                    tid = row.get("trace_id") or row.get("traceId") or ""
                    if tid not in seen_traces:
                        if len(seen_traces) >= args.max_traces:
                            return
                        seen_traces.add(tid)
                yield row

    otlp_doc = converter(stream_rows())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as f:
        json.dump(otlp_doc, f, indent=2, default=str)

    span_count = sum(
        len(ss["spans"])
        for rs in otlp_doc["resourceSpans"]
        for ss in rs["scopeSpans"]
    )
    trace_count = len({
        span["traceId"]
        for rs in otlp_doc["resourceSpans"]
        for ss in rs["scopeSpans"]
        for span in ss["spans"]
    })
    print(f"Wrote {args.out}: {trace_count} traces, {span_count} spans")
    return 0


if __name__ == "__main__":
    sys.exit(main())
