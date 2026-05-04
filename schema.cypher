// Constraints and indexes for the OTel-in-Neo4j schema.
// Idempotent — safe to re-run. Works on Neo4j 5.x and 6.x.
//
// Run this once per database before ingesting:
//   python ingest.py --init
// or from cypher-shell:
//   :source schema.cypher

// === Uniqueness constraints ===
// Each span has a globally unique span_id, but we also key by (trace_id, span_id)
// to be safe with span_id collisions across traces.
CREATE CONSTRAINT span_id_unique IF NOT EXISTS
  FOR (s:Span) REQUIRE (s.trace_id, s.span_id) IS UNIQUE;

CREATE CONSTRAINT trace_id_unique IF NOT EXISTS
  FOR (t:Trace) REQUIRE t.trace_id IS UNIQUE;

CREATE CONSTRAINT tool_name_unique IF NOT EXISTS
  FOR (t:Tool) REQUIRE t.name IS UNIQUE;

CREATE CONSTRAINT agent_name_unique IF NOT EXISTS
  FOR (a:Agent) REQUIRE a.name IS UNIQUE;

CREATE CONSTRAINT model_name_unique IF NOT EXISTS
  FOR (m:Model) REQUIRE m.name IS UNIQUE;

CREATE CONSTRAINT service_name_unique IF NOT EXISTS
  FOR (s:Service) REQUIRE s.name IS UNIQUE;

CREATE CONSTRAINT document_id_unique IF NOT EXISTS
  FOR (d:Document) REQUIRE d.id IS UNIQUE;

// === Indexes for query performance ===
CREATE INDEX span_kind_idx IF NOT EXISTS FOR (s:Span) ON (s.kind);
CREATE INDEX span_status_idx IF NOT EXISTS FOR (s:Span) ON (s.status);
CREATE INDEX span_start_idx IF NOT EXISTS FOR (s:Span) ON (s.start_time_ns);
CREATE INDEX span_name_idx IF NOT EXISTS FOR (s:Span) ON (s.name);
CREATE INDEX span_io_format_idx IF NOT EXISTS FOR (s:Span) ON (s.io_format);
