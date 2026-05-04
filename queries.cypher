// =====================================================================
// Five analytic queries on OTel agent traces in Neo4j.
//
// Each demonstrates something that's painful in flat span-list form
// (Datadog, Jaeger UI, etc.) but natural in a graph database.
// Run individually in Neo4j Browser, or with cypher-shell:
//   cypher-shell -f queries.cypher
// =====================================================================


// ---------------------------------------------------------------------
// Q1. Find traces with parallel tool calls.
// ---------------------------------------------------------------------
// Parallel calls = sibling spans (same parent) whose time intervals overlap.
// In flat logs you'd have to sort spans by parent and compare timestamps
// pairwise; in Cypher it's one MATCH.
//
// This is exactly the kind of structural pattern the Decision Model needs
// to recognize and emit. Counting them across a corpus tells you how often
// agents *actually* parallelize — usually less than you'd hope.
// ---------------------------------------------------------------------
MATCH (parent:Span)-[:PARENT_OF]->(a:Span),
      (parent)-[:PARENT_OF]->(b:Span)
WHERE a.span_id < b.span_id                          // dedupe pairs
  AND a.start_time_ns < b.end_time_ns
  AND b.start_time_ns < a.end_time_ns                // intervals overlap
  AND (a.kind = 'TOOL' OR b.kind = 'TOOL')
RETURN parent.trace_id          AS trace_id,
       parent.name              AS parent_span,
       collect(DISTINCT a.name) + collect(DISTINCT b.name) AS parallel_calls
LIMIT 25;


// ---------------------------------------------------------------------
// Q2. Longest critical path through each trace.
// ---------------------------------------------------------------------
// Walks the parent-of tree and finds the deepest path. Critical paths are
// where latency comes from. Across a corpus, the distribution of path
// lengths tells you how deep your agents actually plan — a key signal for
// Decision Model training data quality.
//
// Uses APOC if available; falls back to plain Cypher pattern matching.
// ---------------------------------------------------------------------
MATCH path = (root:Span)-[:PARENT_OF*]->(leaf:Span)
WHERE NOT (:Span)-[:PARENT_OF]->(root)               // root has no parent
  AND NOT (leaf)-[:PARENT_OF]->(:Span)               // leaf has no children
WITH root.trace_id AS trace_id,
     length(path)  AS depth,
     [n IN nodes(path) | n.name] AS span_chain,
     reduce(total = 0.0, n IN nodes(path) | total + coalesce(n.duration_ms, 0)) AS path_duration_ms
ORDER BY depth DESC
RETURN trace_id, depth, path_duration_ms, span_chain
LIMIT 10;


// ---------------------------------------------------------------------
// Q3. Tool-call sequence patterns: what gets called after what.
// ---------------------------------------------------------------------
// Finds the most common A-followed-by-B tool pairs across all traces,
// where "followed by" means within the same parent and B starts after A
// ends. This is a sequential pattern miner in 6 lines of Cypher.
//
// For the Decision Model: this surfaces real workflow patterns you can
// validate the model has learned to emit. If "search" is almost always
// followed by "read_content" in your traces, the model should learn that.
// ---------------------------------------------------------------------
MATCH (parent:Span)-[:PARENT_OF]->(a:Span)-[:CALLS_TOOL]->(tool_a:Tool),
      (parent)-[:PARENT_OF]->(b:Span)-[:CALLS_TOOL]->(tool_b:Tool)
WHERE a.end_time_ns <= b.start_time_ns               // a finished before b started
  AND a.span_id <> b.span_id
RETURN tool_a.name AS first_tool,
       tool_b.name AS next_tool,
       count(*)    AS occurrences
ORDER BY occurrences DESC
LIMIT 20;


// ---------------------------------------------------------------------
// Q4. Where errors cascade.
// ---------------------------------------------------------------------
// Find error spans, then walk their descendants to see how much downstream
// work was poisoned. The descendant count tells you blast radius — which
// errors are truly costly versus which are isolated.
//
// In OTel UIs you have to click around to follow the cascade. In Cypher
// it's a graph walk + count. This is also the supervisor-training signal
// for Phase 2a in the consolidated brief: error states whose descendants
// are wasted work are exactly the states that should be labeled re-invoke.
// ---------------------------------------------------------------------
MATCH (err:Span {status: 'ERROR'})
OPTIONAL MATCH (err)-[:PARENT_OF*]->(downstream:Span)
WITH err, count(downstream) AS blast_radius,
     sum(coalesce(downstream.duration_ms, 0)) AS wasted_ms
RETURN err.trace_id        AS trace_id,
       err.name             AS error_span,
       err.kind              AS kind,
       err.status_message    AS message,
       blast_radius,
       wasted_ms
ORDER BY blast_radius DESC, wasted_ms DESC
LIMIT 25;


// ---------------------------------------------------------------------
// Q5. Common subgraph patterns across traces.
// ---------------------------------------------------------------------
// Finds the most common 3-step "shapes" — agent calls tool then LLM, or
// LLM calls tool A then tool B, etc. This is the killer query that's
// nearly impossible in flat logs: you're looking for structural motifs,
// not text patterns.
//
// For the Decision Model thesis: these motifs ARE the decision graphs
// the model is supposed to learn to generate. Finding the top-N motifs
// in a customer's trace data tells you exactly what the per-deployment
// fine-tune should target.
// ---------------------------------------------------------------------
MATCH (a:Span)-[:PARENT_OF]->(b:Span)-[:PARENT_OF]->(c:Span)
WITH a.kind AS kind_1,
     b.kind AS kind_2,
     c.kind AS kind_3,
     count(*) AS occurrences
WHERE occurrences > 1
RETURN kind_1 + ' -> ' + kind_2 + ' -> ' + kind_3 AS motif,
       occurrences
ORDER BY occurrences DESC
LIMIT 15;


// ---------------------------------------------------------------------
// Bonus: Convention coverage check.
// ---------------------------------------------------------------------
// Quick sanity check: how much of your corpus uses each semantic convention?
// Useful for monitoring how the OTel GenAI vs OpenInference adoption splits
// in your customer base.
// ---------------------------------------------------------------------
MATCH (s:Span)
RETURN s.convention AS convention,
       count(*)     AS span_count,
       count(DISTINCT s.trace_id) AS trace_count
ORDER BY span_count DESC;


// ---------------------------------------------------------------------
// Q6. Reconstruct a conversation flow for a single trace.
// ---------------------------------------------------------------------
// Walks the parent-of tree in temporal order and prints what each span
// actually did. This is the "show me what happened" query — closest thing
// to a transcript view in flat-log tools, but you can filter by kind,
// status, etc. with one extra clause.
//
// Replace the trace_id with whatever trace you want to inspect.
// ---------------------------------------------------------------------
MATCH (s:Span {trace_id: '4bf92f3577b34da6a3ce929d0e0e4736'})
RETURN s.start_time_ns       AS t,
       s.kind                AS kind,
       s.name                AS name,
       s.status              AS status,
       substring(coalesce(s.input_text, ''), 0, 80)  AS input_preview,
       substring(coalesce(s.output_text, ''), 0, 80) AS output_preview
ORDER BY t;


// ---------------------------------------------------------------------
// Q7. Find tool calls with specific argument or result patterns.
// ---------------------------------------------------------------------
// Once tool I/O is captured, you can ask content-based questions across
// thousands of traces: "show me every issue_refund call where the amount
// was over $100", "show me lookup_order calls where status was returned
// as 'shipped'", etc. The I/O is JSON in input_text/output_text — Cypher
// CONTAINS works fine for prefix/substring matches; for structured
// queries you'd parse with apoc.convert.fromJsonMap.
// ---------------------------------------------------------------------
MATCH (s:Span {kind: 'TOOL'})-[:CALLS_TOOL]->(t:Tool {name: 'issue_refund'})
WHERE s.output_text CONTAINS '"status": "completed"'
RETURN s.trace_id      AS trace_id,
       s.input_text    AS arguments,
       s.output_text   AS result,
       s.duration_ms   AS duration_ms
LIMIT 25;


// ---------------------------------------------------------------------
// Q8. Most-retrieved documents across the corpus.
// ---------------------------------------------------------------------
// Documents are first-class nodes, so you can ask which ones get fetched
// most and from how many distinct traces. Useful for spotting "trusted
// sources" in retrieval-heavy agents and for cache-warming heuristics.
// ---------------------------------------------------------------------
MATCH (s:Span)-[r:RETRIEVED]->(d:Document)
RETURN d.id                    AS document_id,
       count(DISTINCT r)       AS retrieval_count,
       count(DISTINCT s.trace_id) AS trace_count,
       avg(r.score)            AS avg_score,
       substring(coalesce(d.content_preview, ''), 0, 100) AS content_preview
ORDER BY retrieval_count DESC
LIMIT 15;
