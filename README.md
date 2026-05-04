# OTel → Neo4j: agent traces as graphs

A tiny, configurable utility for ingesting OpenTelemetry agent traces into Neo4j and running the kind of analytics that flat span lists can't.

Works with **any** Neo4j distribution that speaks Bolt — local Docker, Neo4j Desktop, AuraDB Free/Pro, self-hosted, cluster. Connection details come from a `.env` file; the code doesn't care which.

Handles both major LLM trace semantic conventions:

- **OpenInference** (Arize-led) — `openinference.span.kind`, `llm.*`, `tool.*`, `retrieval.*`
- **OTel GenAI Semantic Conventions** (CNCF) — `gen_ai.operation.name`, `gen_ai.tool.name`, `gen_ai.agent.name`

Auto-detects which convention each span uses and normalizes them into a single property-graph schema you can query with Cypher.

---

## Why this exists

OpenTelemetry [defines a trace as a DAG of spans connected by parent/child relationships](https://opentelemetry.io/docs/concepts/signals/traces/). Most observability UIs render that DAG as a flat span list with indentation. Useful for debugging single requests; awful for asking structural questions across many traces.

Questions like:

- *Which traces actually parallelized their tool calls vs serialized them?*
- *What's the most common 3-step motif my agents follow?*
- *When errors happen, how much downstream work gets poisoned?*
- *Which tool is most frequently called right after `web_search`?*

These are graph queries. They're awkward in span-list form and natural in Cypher.

This project gives you the smallest possible bridge: read OTLP JSON, push it into Neo4j as a property graph, and run the analytics.

## Quickstart

```bash
# 1. Clone or download this directory, then:
uv sync

# 2. Configure your Neo4j connection
cp .env.example .env
# Edit .env with your credentials — works for any Neo4j distro

# 3. Initialize the schema (one-time per database)
uv run python ingest.py --init

# 4. Ingest the sample traces
uv run python ingest.py openinference_sample.json otel_genai_sample.json

# 5. Open Neo4j Browser and run queries
```

That's the whole thing. Five steps, no other moving parts.

## Configuring against any Neo4j distro

The `.env` file is the only thing you need to change. The URI scheme tells the driver what to do:

| Distro | Example `NEO4J_URI` |
|---|---|
| Local Docker (`neo4j:5`) | `bolt://localhost:7687` |
| Neo4j Desktop | `bolt://localhost:7687` |
| AuraDB Free / Pro | `neo4j+s://xxxxxxxx.databases.neo4j.io` |
| Self-hosted with TLS | `bolt+s://your-host:7687` |
| Self-hosted cluster | `neo4j://your-host:7687` |

`bolt+s://` and `neo4j+s://` are the encrypted variants. `neo4j://` enables routing for clusters; `bolt://` is single-instance only. The driver picks the right transport automatically.

```env
NEO4J_URI=neo4j+s://abc12345.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your-aura-password
NEO4J_DATABASE=neo4j
```

## What the graph looks like

```
(:Trace)-[:CONTAINS]->(:Span)
(:Span)-[:PARENT_OF]->(:Span)         # the OTel parent/child DAG
(:Span)-[:LINKS_TO]->(:Span)          # OTel Span Links (fan-in)
(:Span)-[:CALLS_TOOL]->(:Tool)        # denormalized: tools across traces
(:Span)-[:INVOKES_AGENT]->(:Agent)    # denormalized: agents across traces
(:Span)-[:USES_MODEL]->(:Model)       # denormalized: models across traces
(:Span)-[:EMITTED_BY]->(:Service)     # service.name from OTel resource
```

Each `:Span` carries the canonical fields from `normalize.py` — `kind` (`LLM`/`TOOL`/`AGENT`/`RETRIEVER`/...), `convention` (which semconv it came from), `status`, `start_time_ns`, `end_time_ns`, `duration_ms`, plus token counts and the raw flat attribute set for anything you didn't anticipate.

Why both the structural parent edges *and* the denormalized `:Tool`/`:Agent`/`:Model` nodes? The structural edges preserve the DAG; the denormalized labels make cross-trace queries fast. ("Which tools were called by the most agents across the last 1000 traces?" is a one-line Cypher query against the denormalized labels; it's a much uglier query against pure structural data.)

## The semantic-convention bit

The Decision Model brief identifies a real problem: **observability vendors use OpenTelemetry but disagree on the attribute conventions on top of it.** Arize's [OpenInference](https://arize-ai.github.io/openinference/spec/) and the [OTel GenAI Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/) are the two real contenders. They disagree on basically everything: span-kind taxonomy, attribute names, operation taxonomy.

`normalize.py` reads both. It also tolerates Vercel AI SDK (`ai.*`), MLflow (`mlflow.*`), and Traceloop (`traceloop.*`) attributes as fallbacks. The output is a single canonical record: same span-kind enum, same model-name field, same token-count fields, regardless of which convention the source SDK used.

The two sample files exercise both code paths:

| File | Convention | Domain |
|---|---|---|
| `openinference_sample.json` | OpenInference | Research agent with parallel retrievers |
| `otel_genai_sample.json` | OTel GenAI | Customer-service refund agent |

## The five queries

`queries.cypher` has five analytic queries plus a bonus convention-coverage check. Each is heavily commented; here are the high-level ideas.

### Q1 — Find traces with parallel tool calls
Sibling spans (same parent) with overlapping time intervals. In flat logs you'd sort by parent and compare timestamps pairwise; in Cypher it's one `MATCH`.

### Q2 — Longest critical path through each trace
A graph path query. The depth distribution across a corpus tells you how deep your agents actually plan.

### Q3 — Tool-call sequence patterns
"What tool gets called after what." Sequential pattern mining in 6 lines of Cypher. If `web_search` is almost always followed by `read_content`, you have a workflow motif.

### Q4 — Where errors cascade
Find error spans, walk descendants, count blast radius. The descendant count tells you which errors cost you the most downstream work.

### Q5 — Common subgraph patterns across traces
The killer query that's near-impossible in flat logs: find structural motifs (3-step shapes), not text patterns. `AGENT → LLM → TOOL` is a different motif from `AGENT → TOOL → LLM` even if the same tool is called.

### Bonus — Convention coverage
How much of your corpus is OpenInference vs OTel GenAI? Useful for tracking ecosystem adoption.

## Limitations and design notes worth flagging

**OTel only captures executed paths.** The parent/child graph is the path that ran, not the set of paths that *could* have run. This is fine for retrospective analytics, less fine if you want to learn from un-taken branches. The only public capture system that preserves un-taken branches is LangGraph's static `StateGraph` definition.

**Idempotent ingestion via `MERGE`.** Re-running on the same file won't duplicate nodes. Trade-off: `MERGE` is slower than `CREATE`. For multi-million-span ingest, swap to `CREATE` and dedupe upstream.

**No batching.** Each span is its own transaction. Fine for tens of thousands of spans, slow for millions. Adding `UNWIND $batch AS span ... MERGE ...` with `apoc.periodic.iterate` is straightforward but not done here for clarity.

**The `:Tool` / `:Agent` / `:Model` denormalization is by name only.** If two services both have a `web_search` tool that means different things, they'll collide. Real production use should namespace by service.

## Why this is interesting beyond just analytics

The schema this project uses for ingested traces is the same shape Neo4j is exploring as the *training data and output format* for a graph-native agent orchestrator (see the consolidated Decision Model brief). The five queries above are also questions you'd want to ask of training data — *how often do agents parallelize, what motifs are common, where do errors cascade*. Same data shape, same tools, same questions. The devrel hook is that graphs are the right abstraction for agent traces *and* for what comes after them.

## License

Apache-2.0 — same as OpenTelemetry, OpenInference, and Neo4j's drivers.
