#!/usr/bin/env python3
"""
Ingest OTLP JSON traces into Neo4j as a property graph.

Works with any Neo4j distribution that speaks Bolt:
  - Local Docker / Neo4j Desktop
  - AuraDB (Free, Pro, Enterprise)
  - Self-hosted single instance or cluster

Reads connection settings from .env (see .env.example).

Normalization across competing semantic conventions (OpenInference, OTel GenAI,
Vercel AI SDK, MLflow, Traceloop) is delegated to the `otela` library
(https://github.com/zach-blumenfeld/otela). otela spills normalized records
to a temporary Parquet directory and we stream those parquet files in chunks
into Neo4j via batched UNWIND'd MERGEs (the standard Neo4j-data-loading
pattern). This keeps memory bounded for million-span corpora and ingests
~50-100x faster than per-row transactions.

Usage:
    python ingest.py --init                       # Create constraints/indexes
    python ingest.py sample_traces/*.json         # Ingest one or more files
    python ingest.py --reset                      # Drop all OTel data (careful!)

The ingester is idempotent: rerunning on the same file is safe (MERGE-based).
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import LiteralString, cast

import otela
import pyarrow.parquet as pq
from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver


# Truncate large I/O strings to keep the graph navigable. Tunable via env.
# Set IO_MAX_CHARS=0 to disable truncation entirely (production at your own risk).
IO_MAX_CHARS = int(os.getenv("IO_MAX_CHARS", "2000"))

# Rows per UNWIND batch. Tune via env if you have a beefy DB and want fewer
# round-trips, or shrink it on tight memory budgets. 1k is a safe default for
# local Docker / AuraDB Free.
BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "1000"))


# === Cypher: all writes are UNWIND $rows AS row + MERGE ===
# This is the canonical Neo4j-data-loading batching pattern. One round-trip
# per N rows instead of one per row. Constraints in schema.cypher make the
# MERGE lookups O(1) per row.

UPSERT_TRACE_BATCH: LiteralString = """
UNWIND $rows AS row
MERGE (t:Trace {trace_id: row.trace_id})
  ON CREATE SET t.first_seen_at = timestamp()
SET t.last_seen_at = timestamp(),
    t.service_name = coalesce(row.service_name, t.service_name),
    t.root_span_id = row.root_span_id,
    t.root_span_name = row.root_span_name,
    t.start_time_ns = row.start_time_ns,
    t.end_time_ns = row.end_time_ns,
    t.duration_ms = row.duration_ms,
    t.span_count = row.span_count,
    t.error_count = row.error_count,
    t.status = row.status,
    t.total_input_tokens = row.total_input_tokens,
    t.total_output_tokens = row.total_output_tokens,
    t.total_tokens = row.total_tokens
"""

UPSERT_SPAN_BATCH: LiteralString = """
UNWIND $rows AS row
MERGE (s:Span {trace_id: row.trace_id, span_id: row.span_id})
SET s.name = row.name,
    s.kind = row.kind,
    s.convention = row.convention,
    s.status = row.status,
    s.status_message = row.status_message,
    s.start_time_ns = row.start_time_ns,
    s.end_time_ns = row.end_time_ns,
    s.duration_ms = row.duration_ms,
    s.service_name = row.service_name,
    s.input_tokens = row.input_tokens,
    s.output_tokens = row.output_tokens,
    s.total_tokens = row.total_tokens,
    s.input_text = row.input_text,
    s.output_text = row.output_text,
    s.io_format = row.io_format,
    s += row.raw_attributes
WITH s, row
MATCH (t:Trace {trace_id: row.trace_id})
MERGE (t)-[:CONTAINS]->(s)
"""

UPSERT_DOCUMENT_BATCH: LiteralString = """
UNWIND $rows AS row
MATCH (s:Span {trace_id: row.trace_id, span_id: row.span_id})
MERGE (d:Document {id: coalesce(row.document_id, row.span_id + '#' + toString(row.position))})
SET d.content_preview = row.content,
    d.last_score = row.score
MERGE (s)-[r:RETRIEVED]->(d)
SET r.score = row.score, r.position = row.position
"""

LINK_PARENT_BATCH: LiteralString = """
UNWIND $rows AS row
MATCH (child:Span {trace_id: row.trace_id, span_id: row.span_id})
MATCH (parent:Span {trace_id: row.trace_id, span_id: row.parent_span_id})
MERGE (parent)-[:PARENT_OF]->(child)
"""

LINK_TOOL_BATCH: LiteralString = """
UNWIND $rows AS row
MATCH (s:Span {trace_id: row.trace_id, span_id: row.span_id})
MERGE (t:Tool {name: row.tool_name})
MERGE (s)-[:CALLS_TOOL]->(t)
"""

LINK_AGENT_BATCH: LiteralString = """
UNWIND $rows AS row
MATCH (s:Span {trace_id: row.trace_id, span_id: row.span_id})
MERGE (a:Agent {name: row.agent_name})
MERGE (s)-[:INVOKES_AGENT]->(a)
"""

LINK_MODEL_BATCH: LiteralString = """
UNWIND $rows AS row
MATCH (s:Span {trace_id: row.trace_id, span_id: row.span_id})
MERGE (m:Model {name: row.model_name})
MERGE (s)-[:USES_MODEL]->(m)
"""

LINK_SERVICE_BATCH: LiteralString = """
UNWIND $rows AS row
MATCH (s:Span {trace_id: row.trace_id, span_id: row.span_id})
MERGE (svc:Service {name: row.service_name})
MERGE (s)-[:EMITTED_BY]->(svc)
"""

LINK_SPAN_LINK_BATCH: LiteralString = """
UNWIND $rows AS row
MATCH (src:Span {trace_id: row.trace_id, span_id: row.span_id})
MATCH (dst:Span {trace_id: row.linked_trace_id, span_id: row.linked_span_id})
MERGE (src)-[:LINKS_TO]->(dst)
"""

RESET_GRAPH: LiteralString = """
MATCH (n)
WHERE n:Span OR n:Trace OR n:Tool OR n:Agent OR n:Model OR n:Service OR n:Document
DETACH DELETE n
"""


def load_env() -> dict[str, str]:
    """Load Neo4j connection settings from .env. Fail loudly if missing."""
    load_dotenv()
    required = ["NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        sys.exit(
            f"Missing required env vars: {', '.join(missing)}.\n"
            f"Copy .env.example to .env and fill in your Neo4j credentials."
        )
    return {
        "uri": os.environ["NEO4J_URI"],
        "username": os.environ["NEO4J_USERNAME"],
        "password": os.environ["NEO4J_PASSWORD"],
        "database": os.getenv("NEO4J_DATABASE", "neo4j"),
    }


def make_driver(cfg: dict[str, str]) -> Driver:
    """Construct a driver. The URI scheme handles encryption/routing automatically:
    bolt://, bolt+s://, neo4j://, neo4j+s:// all just work."""
    return GraphDatabase.driver(cfg["uri"], auth=(cfg["username"], cfg["password"]))


def init_schema(driver: Driver, database: str) -> None:
    """Run schema.cypher to create constraints and indexes."""
    schema_file = Path(__file__).parent / "schema.cypher"
    with driver.session(database=database) as session:
        for statement in schema_file.read_text().split(";"):
            stmt = statement.strip()
            if not stmt:
                continue
            # A statement chunk may begin with `//` comment lines (file header
            # or section banners). Cypher accepts inline `//` comments, so we
            # only skip the chunk if every non-empty line is a comment.
            non_comment = [
                line for line in stmt.splitlines()
                if line.strip() and not line.strip().startswith("//")
            ]
            if not non_comment:
                continue
            # schema.cypher is checked into the repo and trusted; cast to
            # satisfy the driver's LiteralString constraint. `.consume()` is
            # required — the driver's Result is lazy and DDL statements
            # otherwise never reach the server.
            session.run(cast(LiteralString, stmt)).consume()
    print(f"Schema initialized in database '{database}'.")


def reset_graph(driver: Driver, database: str) -> None:
    """Drop all OTel-related nodes. Confirms first."""
    confirm = input("This will DELETE all Trace/Span/Tool/Agent/Model/Service nodes. Type 'yes' to confirm: ")
    if confirm.strip().lower() != "yes":
        print("Aborted.")
        return
    with driver.session(database=database) as session:
        session.run(RESET_GRAPH)
    print("Graph reset.")


def _truncate(s):
    """Truncate strings over IO_MAX_CHARS, with a marker. None passes through."""
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    if IO_MAX_CHARS and len(s) > IO_MAX_CHARS:
        return s[:IO_MAX_CHARS] + f"... [truncated, {len(s)} total chars]"
    return s


def _scalar_attrs(d) -> dict:
    """Keep only non-null scalar values — Neo4j property values must be
    primitive, and null in a `s += map` clause removes the property."""
    if not d:
        return {}
    return {
        k: v for k, v in d.items()
        if v is not None and isinstance(v, (str, int, float, bool))
    }


def _stream_parquet(path: Path, batch_size: int):
    """Yield row dicts from a parquet file in chunks of batch_size.
    Bounded memory regardless of file size."""
    if not path.exists():
        return
    pf = pq.ParquetFile(path)
    if pf.metadata.num_rows == 0:
        return
    for batch in pf.iter_batches(batch_size=batch_size):
        yield batch.to_pylist()


def _ns_to_ms(ns) -> float | None:
    return ns / 1_000_000 if isinstance(ns, (int, float)) else None


def _prep_trace_row(r: dict) -> dict:
    """Map otela trace columns to the names UPSERT_TRACE_BATCH expects."""
    return {
        "trace_id": r["trace_id"],
        "service_name": r.get("service_name"),
        "root_span_id": r.get("root_span_id"),
        "root_span_name": r.get("root_span_name"),
        "start_time_ns": r.get("start_time_unix_nano"),
        "end_time_ns": r.get("end_time_unix_nano"),
        "duration_ms": _ns_to_ms(r.get("duration_ns")),
        "span_count": r.get("span_count"),
        "error_count": r.get("error_count"),
        "status": r.get("status"),
        "total_input_tokens": r.get("total_input_tokens"),
        "total_output_tokens": r.get("total_output_tokens"),
        "total_tokens": r.get("total_tokens"),
    }


def _prep_span_row(r: dict) -> dict:
    """Map otela span columns to the names UPSERT_SPAN_BATCH expects.
    Parses raw_attributes_json into a scalar-only map so `s += row.raw_attributes`
    is safe."""
    raw_json = r.get("raw_attributes_json")
    raw_attributes: dict = {}
    if isinstance(raw_json, (str, bytes, bytearray)):
        try:
            raw_attributes = _scalar_attrs(json.loads(raw_json))
        except (TypeError, ValueError):
            raw_attributes = {}
    return {
        "trace_id": r["trace_id"],
        "span_id": r["span_id"],
        "parent_span_id": r.get("parent_span_id") or None,
        "name": r.get("name") or "",
        "kind": r.get("kind"),
        "convention": r.get("convention"),
        "status": r.get("status_code"),
        "status_message": r.get("status_message") or "",
        "start_time_ns": r.get("start_time_unix_nano"),
        "end_time_ns": r.get("end_time_unix_nano"),
        "duration_ms": _ns_to_ms(r.get("duration_ns")),
        "service_name": r.get("service_name") or "unknown",
        "input_tokens": r.get("input_tokens"),
        "output_tokens": r.get("output_tokens"),
        "total_tokens": r.get("total_tokens"),
        "input_text": _truncate(r.get("input_text")),
        "output_text": _truncate(r.get("output_text")),
        "io_format": r.get("io_format"),
        "tool_name": r.get("tool_name"),
        "agent_name": r.get("agent_name"),
        "model_name": r.get("model_name"),
        "raw_attributes": raw_attributes,
    }


def _prep_doc_row(r: dict) -> dict:
    return {
        "trace_id": r["trace_id"],
        "span_id": r["span_id"],
        "document_id": r.get("document_id"),
        "content": _truncate(r.get("content")),
        "score": r.get("score"),
        "position": r.get("position"),
    }


def ingest_file(driver: Driver, database: str, filepath: Path,
                batch_size: int = BATCH_SIZE) -> dict:
    """Ingest one OTLP JSON file via otela.

    Pipeline:
      1. otela.to_parquet() spills normalized records to a temp dir (5 files:
         traces, spans, messages, documents, links).
      2. We stream each parquet file in chunks of `batch_size` rows.
      3. Each chunk is sent to Neo4j as a single UNWIND'd MERGE.
    """
    stats = {
        "spans": 0, "traces": 0, "tools": 0, "agents": 0,
        "models": 0, "documents": 0, "parent_links": 0, "span_links": 0,
    }

    with tempfile.TemporaryDirectory(prefix="otela-") as tmp:
        tmp_dir = Path(tmp)
        otela.to_parquet(str(filepath), str(tmp_dir), batch_size=batch_size)

        traces_path = tmp_dir / "traces.parquet"
        spans_path = tmp_dir / "spans.parquet"
        documents_path = tmp_dir / "documents.parquet"
        links_path = tmp_dir / "links.parquet"

        # Headline counts (cheap — parquet metadata, no row scan).
        if traces_path.exists():
            stats["traces"] = pq.ParquetFile(traces_path).metadata.num_rows
        if spans_path.exists():
            stats["spans"] = pq.ParquetFile(spans_path).metadata.num_rows
        print(f"  Found {stats['spans']} spans across {stats['traces']} traces in {filepath.name}")

        with driver.session(database=database) as session:
            # 1. Traces
            for chunk in _stream_parquet(traces_path, batch_size):
                rows = [_prep_trace_row(r) for r in chunk]
                session.run(UPSERT_TRACE_BATCH, rows=rows).consume()

            # 2. Spans + denormalized link nodes (Service/Tool/Agent/Model)
            #    + retrieved Documents — all written per-chunk so peak memory
            #    is bounded by batch_size.
            for chunk in _stream_parquet(spans_path, batch_size):
                rows = [_prep_span_row(r) for r in chunk]
                session.run(UPSERT_SPAN_BATCH, rows=rows).consume()

                services = [r for r in rows if r["service_name"]]
                if services:
                    session.run(LINK_SERVICE_BATCH, rows=services).consume()

                tools = [r for r in rows if r.get("tool_name")]
                if tools:
                    session.run(LINK_TOOL_BATCH, rows=tools).consume()
                    stats["tools"] += len(tools)

                agents = [r for r in rows if r.get("agent_name")]
                if agents:
                    session.run(LINK_AGENT_BATCH, rows=agents).consume()
                    stats["agents"] += len(agents)

                models = [r for r in rows if r.get("model_name")]
                if models:
                    session.run(LINK_MODEL_BATCH, rows=models).consume()
                    stats["models"] += len(models)

            # 3. Retrieved documents
            for chunk in _stream_parquet(documents_path, batch_size):
                rows = [_prep_doc_row(r) for r in chunk]
                session.run(UPSERT_DOCUMENT_BATCH, rows=rows).consume()
                stats["documents"] += len(rows)

            # 4. Parent/child edges (re-stream spans, only rows with a parent)
            for chunk in _stream_parquet(spans_path, batch_size):
                parents = [
                    {"trace_id": r["trace_id"], "span_id": r["span_id"],
                     "parent_span_id": r["parent_span_id"]}
                    for r in chunk
                    if r.get("parent_span_id")
                ]
                if parents:
                    session.run(LINK_PARENT_BATCH, rows=parents).consume()
                    stats["parent_links"] += len(parents)

            # 5. Span links (fan-in / scatter-gather)
            for chunk in _stream_parquet(links_path, batch_size):
                if chunk:
                    session.run(LINK_SPAN_LINK_BATCH, rows=chunk).consume()
                    stats["span_links"] += len(chunk)

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("files", nargs="*", type=Path,
                        help="OTLP JSON files to ingest")
    parser.add_argument("--init", action="store_true",
                        help="Create constraints and indexes (also run automatically "
                             "before any ingest; this flag is just for init-only runs)")
    parser.add_argument("--reset", action="store_true",
                        help="Drop all OTel-related nodes (DANGER: will prompt to confirm)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Rows per UNWIND batch (default: {BATCH_SIZE}, "
                             "tunable via INGEST_BATCH_SIZE env var)")
    args = parser.parse_args()

    cfg = load_env()
    driver = make_driver(cfg)

    try:
        driver.verify_connectivity()
        print(f"Connected to Neo4j at {cfg['uri']} (database: {cfg['database']})")

        if args.init:
            init_schema(driver, cfg["database"])

        if args.reset:
            reset_graph(driver, cfg["database"])

        if args.files:
            # Schema DDL is idempotent (`IF NOT EXISTS`), and the MERGE-based
            # ingest depends on the uniqueness constraints to be efficient and
            # correct. Always ensure the schema before ingesting.
            if not args.init:
                init_schema(driver, cfg["database"])

            grand_total = {"spans": 0, "traces": 0, "parent_links": 0, "span_links": 0}
            for filepath in args.files:
                if not filepath.exists():
                    print(f"  ! Skipping {filepath} (not found)")
                    continue
                print(f"Ingesting {filepath}...")
                stats = ingest_file(driver, cfg["database"], filepath, args.batch_size)
                print(f"  Ingested {stats['spans']} spans, {stats['traces']} traces, "
                      f"{stats['parent_links']} parent links, {stats['span_links']} span links.")
                for k in grand_total:
                    grand_total[k] += stats[k]
            if len(args.files) > 1:
                print(f"\nTotal: {grand_total}")
        elif not args.init and not args.reset:
            parser.print_help()

    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
