#!/usr/bin/env python3
"""
Ingest OTLP JSON traces into Neo4j as a property graph.

Works with any Neo4j distribution that speaks Bolt:
  - Local Docker / Neo4j Desktop
  - AuraDB (Free, Pro, Enterprise)
  - Self-hosted single instance or cluster

Reads connection settings from .env (see .env.example).

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
from pathlib import Path
from typing import Iterator

from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver

from normalize import normalize_span, _attrs_to_dict


# === Cypher queries — separated for clarity and easier modification ===

# Upsert one Trace node (tracks per-trace metadata).
UPSERT_TRACE = """
MERGE (t:Trace {trace_id: $trace_id})
  ON CREATE SET t.first_seen_at = timestamp()
SET t.last_seen_at = timestamp(),
    t.service_name = coalesce($service_name, t.service_name)
"""

# Upsert one Span node and link it to its Trace.
UPSERT_SPAN = """
MERGE (s:Span {trace_id: $trace_id, span_id: $span_id})
SET s.name = $name,
    s.kind = $kind,
    s.convention = $convention,
    s.status = $status,
    s.status_message = $status_message,
    s.start_time_ns = $start_time_ns,
    s.end_time_ns = $end_time_ns,
    s.duration_ms = $duration_ms,
    s.service_name = $service_name,
    s.input_tokens = $input_tokens,
    s.output_tokens = $output_tokens,
    s.total_tokens = $total_tokens,
    s.input_text = $input_text,
    s.output_text = $output_text,
    s.io_format = $io_format,
    s += $raw_attributes
WITH s
MATCH (t:Trace {trace_id: $trace_id})
MERGE (t)-[:CONTAINS]->(s)
"""

# Upsert one retrieved document and link to the RETRIEVER span that fetched it.
# Documents become first-class nodes so you can ask cross-trace questions like
# "which documents are retrieved most often", "which traces saw doc X", etc.
UPSERT_DOCUMENT = """
MATCH (s:Span {trace_id: $trace_id, span_id: $span_id})
MERGE (d:Document {id: coalesce($doc_id, $span_id + '#' + toString($index))})
SET d.content_preview = $content,
    d.last_score = $score
MERGE (s)-[r:RETRIEVED]->(d)
SET r.score = $score, r.position = $index
"""

# Link span to its parent (CHILD_OF / PARENT_OF semantics).
LINK_PARENT = """
MATCH (child:Span {trace_id: $trace_id, span_id: $span_id})
MATCH (parent:Span {trace_id: $trace_id, span_id: $parent_span_id})
MERGE (parent)-[:PARENT_OF]->(child)
"""

# Link to a Tool node (denormalized for cross-trace queries).
LINK_TOOL = """
MATCH (s:Span {trace_id: $trace_id, span_id: $span_id})
MERGE (t:Tool {name: $tool_name})
MERGE (s)-[:CALLS_TOOL]->(t)
"""

# Link to an Agent node.
LINK_AGENT = """
MATCH (s:Span {trace_id: $trace_id, span_id: $span_id})
MERGE (a:Agent {name: $agent_name})
MERGE (s)-[:INVOKES_AGENT]->(a)
"""

# Link to a Model node.
LINK_MODEL = """
MATCH (s:Span {trace_id: $trace_id, span_id: $span_id})
MERGE (m:Model {name: $model_name})
MERGE (s)-[:USES_MODEL]->(m)
"""

# Link to a Service node.
LINK_SERVICE = """
MATCH (s:Span {trace_id: $trace_id, span_id: $span_id})
MERGE (svc:Service {name: $service_name})
MERGE (s)-[:EMITTED_BY]->(svc)
"""

# Span.Link relationship (fan-in / scatter-gather across traces).
LINK_SPAN_LINK = """
MATCH (src:Span {trace_id: $src_trace_id, span_id: $src_span_id})
MATCH (dst:Span {trace_id: $dst_trace_id, span_id: $dst_span_id})
MERGE (src)-[:LINKS_TO]->(dst)
"""

# Reset query: drop everything OTel-related. Useful for dev, dangerous in prod.
RESET_GRAPH = """
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
        # Split on `;` and run each statement; skip empties.
        for statement in schema_file.read_text().split(";"):
            stmt = statement.strip()
            if stmt and not stmt.startswith("//"):
                session.run(stmt)
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


def iter_spans_from_otlp(otlp_doc: dict) -> Iterator[tuple[dict, dict]]:
    """Walk an OTLP JSON doc and yield (span, resource_attrs) pairs.
    OTLP structure: resourceSpans[].scopeSpans[].spans[]"""
    # Handle both camelCase (proto JSON) and snake_case (some exporters)
    resource_spans = otlp_doc.get("resourceSpans") or otlp_doc.get("resource_spans") or []
    for rs in resource_spans:
        resource = rs.get("resource", {})
        resource_attrs = _attrs_to_dict(resource.get("attributes", []))
        scope_spans = rs.get("scopeSpans") or rs.get("scope_spans") or []
        for ss in scope_spans:
            for span in ss.get("spans", []):
                yield span, resource_attrs


def ingest_file(driver: Driver, database: str, filepath: Path) -> dict:
    """Ingest one OTLP JSON file. Returns counts."""
    with filepath.open() as f:
        otlp_doc = json.load(f)

    spans = list(iter_spans_from_otlp(otlp_doc))
    print(f"  Found {len(spans)} spans in {filepath.name}")

    stats = {
        "spans": 0, "traces": set(), "tools": 0, "agents": 0,
        "models": 0, "parent_links": 0, "span_links": 0,
    }

    with driver.session(database=database) as session:
        # First pass: upsert traces and spans (no parent links yet — parents
        # may be later in the file, so we do a second pass).
        for raw_span, resource_attrs in spans:
            normalized = normalize_span(raw_span, resource_attrs)
            stats["traces"].add(normalized["trace_id"])

            session.run(UPSERT_TRACE,
                        trace_id=normalized["trace_id"],
                        service_name=normalized["service_name"])

            tokens = normalized["token_counts"]
            session.run(
                UPSERT_SPAN,
                trace_id=normalized["trace_id"],
                span_id=normalized["span_id"],
                name=normalized["name"],
                kind=normalized["kind"],
                convention=normalized["convention"],
                status=normalized["status"],
                status_message=normalized["status_message"],
                start_time_ns=normalized["start_time_ns"],
                end_time_ns=normalized["end_time_ns"],
                duration_ms=normalized["duration_ms"],
                service_name=normalized["service_name"],
                input_tokens=tokens["input_tokens"],
                output_tokens=tokens["output_tokens"],
                total_tokens=tokens["total_tokens"],
                input_text=normalized["input_text"],
                output_text=normalized["output_text"],
                io_format=normalized["io_format"],
                raw_attributes=normalized["raw_attributes"],
            )
            stats["spans"] += 1

            # Service link
            session.run(LINK_SERVICE,
                        trace_id=normalized["trace_id"],
                        span_id=normalized["span_id"],
                        service_name=normalized["service_name"])

            # Tool / Agent / Model denormalized links
            if normalized["tool_name"]:
                session.run(LINK_TOOL,
                            trace_id=normalized["trace_id"],
                            span_id=normalized["span_id"],
                            tool_name=normalized["tool_name"])
                stats["tools"] += 1

            if normalized["agent_name"]:
                session.run(LINK_AGENT,
                            trace_id=normalized["trace_id"],
                            span_id=normalized["span_id"],
                            agent_name=normalized["agent_name"])
                stats["agents"] += 1

            if normalized["model_name"]:
                session.run(LINK_MODEL,
                            trace_id=normalized["trace_id"],
                            span_id=normalized["span_id"],
                            model_name=normalized["model_name"])
                stats["models"] += 1

            # Retrieved documents — make each its own node so cross-trace
            # queries like "which docs are retrieved most often" work.
            for idx, doc in enumerate(normalized.get("documents") or []):
                session.run(UPSERT_DOCUMENT,
                            trace_id=normalized["trace_id"],
                            span_id=normalized["span_id"],
                            doc_id=doc.get("id"),
                            content=doc.get("content"),
                            score=doc.get("score"),
                            index=idx)

        # Second pass: link parents and span-links.
        # All spans are now in the graph, so MATCH is safe.
        for raw_span, resource_attrs in spans:
            normalized = normalize_span(raw_span, resource_attrs)

            if normalized["parent_span_id"]:
                result = session.run(LINK_PARENT,
                                     trace_id=normalized["trace_id"],
                                     span_id=normalized["span_id"],
                                     parent_span_id=normalized["parent_span_id"])
                if result.consume().counters.relationships_created:
                    stats["parent_links"] += 1

            for link in normalized["links"]:
                if link["span_id"] and link["trace_id"]:
                    session.run(LINK_SPAN_LINK,
                                src_trace_id=normalized["trace_id"],
                                src_span_id=normalized["span_id"],
                                dst_trace_id=link["trace_id"],
                                dst_span_id=link["span_id"])
                    stats["span_links"] += 1

    stats["traces"] = len(stats["traces"])
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("files", nargs="*", type=Path,
                        help="OTLP JSON files to ingest")
    parser.add_argument("--init", action="store_true",
                        help="Create constraints and indexes (run once per database)")
    parser.add_argument("--reset", action="store_true",
                        help="Drop all OTel-related nodes (DANGER: will prompt to confirm)")
    args = parser.parse_args()

    cfg = load_env()
    driver = make_driver(cfg)

    try:
        # Verify connectivity before doing anything destructive.
        driver.verify_connectivity()
        print(f"Connected to Neo4j at {cfg['uri']} (database: {cfg['database']})")

        if args.init:
            init_schema(driver, cfg["database"])

        if args.reset:
            reset_graph(driver, cfg["database"])

        if args.files:
            grand_total = {"spans": 0, "traces": 0, "parent_links": 0, "span_links": 0}
            for filepath in args.files:
                if not filepath.exists():
                    print(f"  ! Skipping {filepath} (not found)")
                    continue
                print(f"Ingesting {filepath}...")
                stats = ingest_file(driver, cfg["database"], filepath)
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
