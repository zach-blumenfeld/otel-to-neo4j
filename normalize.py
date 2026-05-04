"""
Normalize OTel spans across competing semantic conventions.

The Decision Model brief identifies two competing conventions on top of OTel:
  - OpenInference (Arize-led):  openinference.span.kind, llm.*, tool.*, retrieval.*
  - OTel GenAI (CNCF):           gen_ai.operation.name, gen_ai.tool.name, gen_ai.agent.name

This module reads attributes from either convention (and a few others) and
returns canonical fields the ingest layer can MERGE on without caring which
SDK produced the trace.

I/O extraction (input/output content) is intentionally tricky because the two
conventions disagree on where I/O lives:
  - OpenInference uses nested attribute arrays:
      llm.input_messages.N.message.role / .content
      llm.output_messages.N.message.role / .content
      retrieval.documents.N.document.content / .score / .id
      tool.parameters (JSON string)
      tool.output_value
  - OTel GenAI semconv v1.37+ uses span EVENTS for messages:
      gen_ai.user.message, gen_ai.assistant.message, gen_ai.tool.message,
      gen_ai.choice — each as a span event with its own attributes.
      Tool I/O via gen_ai.tool.call.arguments / .result.
  - Vercel AI SDK uses ai.prompt / ai.response.text and ai.toolCall.args / .result.

The extract_io() function below pulls all of that into a uniform shape.

References:
  - https://opentelemetry.io/docs/specs/semconv/gen-ai/
  - https://arize-ai.github.io/openinference/spec/
"""

from __future__ import annotations
import json
import os
import re
from typing import Any, Optional


# Truncate large I/O strings to keep the graph navigable. Tunable via env.
# Set IO_MAX_CHARS=0 to disable truncation entirely (production at your own risk).
IO_MAX_CHARS = int(os.getenv("IO_MAX_CHARS", "2000"))


# Canonical span kinds we normalize to. Superset of both conventions.
KIND_LLM = "LLM"
KIND_TOOL = "TOOL"
KIND_AGENT = "AGENT"
KIND_CHAIN = "CHAIN"
KIND_RETRIEVER = "RETRIEVER"
KIND_EMBEDDING = "EMBEDDING"
KIND_RERANKER = "RERANKER"
KIND_GUARDRAIL = "GUARDRAIL"
KIND_EVALUATOR = "EVALUATOR"
KIND_UNKNOWN = "UNKNOWN"

# OTel GenAI operation -> canonical kind
OTEL_GENAI_OPERATION_TO_KIND = {
    "chat": KIND_LLM,
    "text_completion": KIND_LLM,
    "embeddings": KIND_EMBEDDING,
    "execute_tool": KIND_TOOL,
    "create_agent": KIND_AGENT,
    "invoke_agent": KIND_AGENT,
}


def _attrs_to_dict(attributes: list[dict] | dict) -> dict[str, Any]:
    """OTLP attributes can be a list of {key, value: {stringValue/intValue/...}}
    or already a flat dict (some exporters do this). Normalize to flat dict."""
    if isinstance(attributes, dict):
        return attributes
    out: dict[str, Any] = {}
    for attr in attributes or []:
        key = attr.get("key")
        value_obj = attr.get("value", {})
        # OTLP wraps values in type-tagged objects: stringValue, intValue, doubleValue, boolValue, arrayValue
        if "stringValue" in value_obj:
            out[key] = value_obj["stringValue"]
        elif "intValue" in value_obj:
            out[key] = int(value_obj["intValue"])
        elif "doubleValue" in value_obj:
            out[key] = float(value_obj["doubleValue"])
        elif "boolValue" in value_obj:
            out[key] = bool(value_obj["boolValue"])
        elif "arrayValue" in value_obj:
            arr = value_obj["arrayValue"].get("values", [])
            out[key] = [list(v.values())[0] if v else None for v in arr]
        else:
            out[key] = value_obj
    return out


def detect_convention(attrs: dict[str, Any]) -> str:
    """Return 'openinference', 'otel_genai', 'vercel', 'mlflow', 'traceloop',
    or 'unknown' based on which attribute prefix dominates the span."""
    if "openinference.span.kind" in attrs:
        return "openinference"
    if any(k.startswith("gen_ai.") for k in attrs):
        return "otel_genai"
    if any(k.startswith("ai.") for k in attrs):
        return "vercel"
    if any(k.startswith("mlflow.") for k in attrs):
        return "mlflow"
    if any(k.startswith("traceloop.") for k in attrs):
        return "traceloop"
    # OpenInference also commonly emits llm.*/tool.*/retrieval.* without
    # explicit span.kind set — sniff for those.
    if any(k.startswith(("llm.", "tool.", "retrieval.", "embedding."))
           for k in attrs):
        return "openinference"
    return "unknown"


def canonical_kind(attrs: dict[str, Any], convention: str) -> str:
    """Map convention-specific kind/operation to our canonical taxonomy."""
    if convention == "openinference":
        kind = attrs.get("openinference.span.kind", "").upper()
        if kind:
            return kind
        # Fall back to sniffing prefix-based attributes
        if any(k.startswith("llm.") for k in attrs):
            return KIND_LLM
        if any(k.startswith("tool.") for k in attrs):
            return KIND_TOOL
        if any(k.startswith("retrieval.") for k in attrs):
            return KIND_RETRIEVER
        if any(k.startswith("embedding.") for k in attrs):
            return KIND_EMBEDDING

    if convention == "otel_genai":
        op = attrs.get("gen_ai.operation.name", "").lower()
        return OTEL_GENAI_OPERATION_TO_KIND.get(op, KIND_UNKNOWN)

    if convention == "vercel":
        # Vercel AI SDK uses operation names like "ai.generateText", "ai.toolCall"
        op = attrs.get("operation.name", "")
        if "tool" in op.lower():
            return KIND_TOOL
        if "embed" in op.lower():
            return KIND_EMBEDDING
        if op:
            return KIND_LLM

    return KIND_UNKNOWN


def extract_tool_name(attrs: dict[str, Any], convention: str) -> Optional[str]:
    """Extract the tool name from any convention."""
    return (
        attrs.get("tool.name")                  # OpenInference
        or attrs.get("gen_ai.tool.name")        # OTel GenAI
        or attrs.get("ai.toolCall.name")        # Vercel
        or attrs.get("traceloop.entity.name")   # Traceloop (when entity_type=tool)
    )


def extract_agent_name(attrs: dict[str, Any], convention: str) -> Optional[str]:
    """Extract the agent name from any convention."""
    return (
        attrs.get("agent.name")
        or attrs.get("gen_ai.agent.name")
        or attrs.get("openinference.agent.name")
    )


def extract_model_name(attrs: dict[str, Any], convention: str) -> Optional[str]:
    """Extract the LLM model identifier from any convention."""
    return (
        attrs.get("llm.model_name")             # OpenInference
        or attrs.get("gen_ai.request.model")    # OTel GenAI
        or attrs.get("gen_ai.response.model")
        or attrs.get("ai.model.id")             # Vercel
    )


def extract_token_counts(attrs: dict[str, Any]) -> dict[str, Optional[int]]:
    """Extract input/output/total token counts from any convention."""
    return {
        "input_tokens": (
            attrs.get("llm.token_count.prompt")
            or attrs.get("gen_ai.usage.input_tokens")
            or attrs.get("gen_ai.usage.prompt_tokens")
            or attrs.get("ai.usage.promptTokens")
        ),
        "output_tokens": (
            attrs.get("llm.token_count.completion")
            or attrs.get("gen_ai.usage.output_tokens")
            or attrs.get("gen_ai.usage.completion_tokens")
            or attrs.get("ai.usage.completionTokens")
        ),
        "total_tokens": (
            attrs.get("llm.token_count.total")
            or attrs.get("gen_ai.usage.total_tokens")
        ),
    }


def _truncate(s: Optional[str]) -> Optional[str]:
    """Truncate strings over IO_MAX_CHARS, with a marker. None passes through."""
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    if IO_MAX_CHARS and len(s) > IO_MAX_CHARS:
        return s[:IO_MAX_CHARS] + f"... [truncated, {len(s)} total chars]"
    return s


def _collect_indexed(attrs: dict[str, Any], prefix: str) -> list[dict[str, Any]]:
    """OpenInference uses indexed attribute paths like
        llm.input_messages.0.message.role
        llm.input_messages.0.message.content
        llm.input_messages.1.message.role
    Walk all keys with the given prefix and reconstruct an array of dicts.
    Returns [{role: ..., content: ...}, ...]."""
    pattern = re.compile(rf"^{re.escape(prefix)}\.(\d+)\.(.+)$")
    by_index: dict[int, dict[str, Any]] = {}
    for key, value in attrs.items():
        m = pattern.match(key)
        if not m:
            continue
        idx = int(m.group(1))
        # Sub-path like "message.role" or "document.content" — keep the leaf.
        leaf = m.group(2).split(".")[-1]
        by_index.setdefault(idx, {})[leaf] = value
    return [by_index[i] for i in sorted(by_index)]


def _events_by_name(events: list[dict] | None) -> dict[str, list[dict]]:
    """Group span events by event name. Each event has {name, attributes, timeUnixNano}."""
    grouped: dict[str, list[dict]] = {}
    for event in events or []:
        name = event.get("name", "")
        event_attrs = _attrs_to_dict(event.get("attributes", []))
        grouped.setdefault(name, []).append(event_attrs)
    return grouped


def extract_io(span: dict[str, Any], attrs: dict[str, Any], kind: str) -> dict[str, Any]:
    """Extract input/output content from a span across all conventions.

    Returns a dict with three primary fields suitable for storing as Span
    properties, plus structured `messages` and `documents` lists for the
    cases where multi-message / multi-document detail is worth keeping.

        input_text   - canonical input as a single string (truncated)
        output_text  - canonical output as a single string (truncated)
        io_format    - 'text' | 'tool_call' | 'retrieval' | 'unknown'
        messages     - [{role, content}, ...] for LLM calls
        documents    - [{id, content, score}, ...] for retrieval calls
    """
    events = _events_by_name(span.get("events"))

    # === LLM I/O ===
    if kind == "LLM":
        # OpenInference: indexed message arrays
        input_messages = _collect_indexed(attrs, "llm.input_messages")
        output_messages = _collect_indexed(attrs, "llm.output_messages")

        # OTel GenAI v1.37+: messages live in span events, not attributes
        if not input_messages:
            for event_name in ("gen_ai.user.message", "gen_ai.system.message"):
                for evt in events.get(event_name, []):
                    role = "user" if "user" in event_name else "system"
                    content = evt.get("content") or evt.get("gen_ai.event.content")
                    if content:
                        input_messages.append({"role": role, "content": content})
        if not output_messages:
            for evt in events.get("gen_ai.assistant.message", []) + events.get("gen_ai.choice", []):
                content = evt.get("content") or evt.get("message") or evt.get("gen_ai.event.content")
                if content:
                    output_messages.append({"role": "assistant", "content": content})

        # Vercel AI SDK fallback
        if not input_messages and "ai.prompt" in attrs:
            input_messages = [{"role": "user", "content": attrs["ai.prompt"]}]
        if not output_messages and "ai.response.text" in attrs:
            output_messages = [{"role": "assistant", "content": attrs["ai.response.text"]}]

        # Generic fallback: input.value / output.value (used by some OpenInference
        # instrumentors on AGENT / CHAIN spans too).
        if not input_messages and attrs.get("input.value"):
            input_messages = [{"role": "user", "content": attrs["input.value"]}]
        if not output_messages and attrs.get("output.value"):
            output_messages = [{"role": "assistant", "content": attrs["output.value"]}]

        # Collapse to single strings for the Span properties
        input_text = "\n".join(
            f"[{m.get('role', '?')}] {m.get('content', '')}" for m in input_messages
        ) if input_messages else None
        output_text = "\n".join(
            f"[{m.get('role', '?')}] {m.get('content', '')}" for m in output_messages
        ) if output_messages else None

        # Truncate the *content* of each message individually as well so the
        # structured list stays bounded.
        bounded_input_messages = [
            {"role": m.get("role"), "content": _truncate(m.get("content"))}
            for m in input_messages
        ]
        bounded_output_messages = [
            {"role": m.get("role"), "content": _truncate(m.get("content"))}
            for m in output_messages
        ]

        return {
            "input_text": _truncate(input_text),
            "output_text": _truncate(output_text),
            "io_format": "text",
            "messages": bounded_input_messages + bounded_output_messages,
            "documents": [],
        }

    # === Tool I/O ===
    if kind == "TOOL":
        tool_input = (
            attrs.get("tool.parameters")               # OpenInference (JSON string)
            or attrs.get("input.value")
            or attrs.get("gen_ai.tool.call.arguments") # OTel GenAI
            or attrs.get("ai.toolCall.args")           # Vercel
        )
        tool_output = (
            attrs.get("tool.output_value")             # OpenInference
            or attrs.get("output.value")
            or attrs.get("ai.toolCall.result")         # Vercel
        )
        # OTel GenAI tool result via event
        if not tool_output:
            for evt in events.get("gen_ai.tool.message", []):
                tool_output = evt.get("content") or evt.get("gen_ai.event.content")
                if tool_output:
                    break

        # Tool args/results are often dicts at this point — JSON-stringify for storage
        if isinstance(tool_input, (dict, list)):
            tool_input = json.dumps(tool_input)
        if isinstance(tool_output, (dict, list)):
            tool_output = json.dumps(tool_output)

        return {
            "input_text": _truncate(tool_input),
            "output_text": _truncate(tool_output),
            "io_format": "tool_call",
            "messages": [],
            "documents": [],
        }

    # === Retrieval I/O ===
    if kind == "RETRIEVER":
        query = attrs.get("input.value") or attrs.get("retrieval.query")
        documents = _collect_indexed(attrs, "retrieval.documents")
        # Truncate content of each document; keep id + score intact
        bounded_docs = [
            {
                "id": d.get("id"),
                "content": _truncate(d.get("content")),
                "score": d.get("score"),
            }
            for d in documents
        ]
        # Collapse documents to a single output preview
        output_preview = " | ".join(
            f"({d.get('id', '?')}, score={d.get('score', '?')})"
            for d in documents
        ) if documents else None

        return {
            "input_text": _truncate(query),
            "output_text": _truncate(output_preview),
            "io_format": "retrieval",
            "messages": [],
            "documents": bounded_docs,
        }

    # === Generic fallback for AGENT / CHAIN / etc. ===
    return {
        "input_text": _truncate(attrs.get("input.value")),
        "output_text": _truncate(attrs.get("output.value")),
        "io_format": "text" if (attrs.get("input.value") or attrs.get("output.value")) else "unknown",
        "messages": [],
        "documents": [],
    }


def normalize_span(span: dict[str, Any], resource_attrs: dict[str, Any]) -> dict[str, Any]:
    """Take a raw OTLP span dict and return a canonical record ready for MERGE."""
    attrs = _attrs_to_dict(span.get("attributes", []))
    convention = detect_convention(attrs)
    kind = canonical_kind(attrs, convention)

    # OTLP timestamps are nanoseconds since epoch as strings
    start_ns = int(span.get("startTimeUnixNano") or span.get("start_time_unix_nano") or 0)
    end_ns = int(span.get("endTimeUnixNano") or span.get("end_time_unix_nano") or 0)

    status_obj = span.get("status", {}) or {}
    status_code = status_obj.get("code", "STATUS_CODE_UNSET")
    # OTLP can encode status as int (0/1/2) or string — normalize to string
    if isinstance(status_code, int):
        status_code = ["UNSET", "OK", "ERROR"][status_code] if status_code < 3 else "UNSET"
    elif isinstance(status_code, str):
        status_code = status_code.replace("STATUS_CODE_", "")

    io = extract_io(span, attrs, kind)

    # Strip out attribute keys we already stored explicitly so raw_attributes
    # doesn't duplicate the I/O / token / model / tool / agent fields.
    redundant_prefixes = (
        "llm.input_messages.", "llm.output_messages.", "retrieval.documents.",
        "llm.token_count.", "gen_ai.usage.", "ai.usage.",
        "tool.parameters", "tool.output_value", "input.value", "output.value",
        "gen_ai.tool.call.arguments", "ai.toolCall.args", "ai.toolCall.result",
        "ai.prompt", "ai.response.text",
    )
    cleaned_attrs = {
        k: v for k, v in attrs.items()
        if isinstance(v, (str, int, float, bool))
        and not any(k.startswith(p) for p in redundant_prefixes)
    }

    return {
        "trace_id": span["traceId"] if "traceId" in span else span.get("trace_id"),
        "span_id": span["spanId"] if "spanId" in span else span.get("span_id"),
        "parent_span_id": (
            span.get("parentSpanId")
            or span.get("parent_span_id")
            or None
        ) or None,  # treat empty string as None
        "name": span.get("name", ""),
        "kind": kind,
        "convention": convention,
        "status": status_code,
        "status_message": status_obj.get("message", ""),
        "start_time_ns": start_ns,
        "end_time_ns": end_ns,
        "duration_ms": (end_ns - start_ns) / 1_000_000 if end_ns and start_ns else None,
        "service_name": resource_attrs.get("service.name", "unknown"),
        "tool_name": extract_tool_name(attrs, convention),
        "agent_name": extract_agent_name(attrs, convention),
        "model_name": extract_model_name(attrs, convention),
        "token_counts": extract_token_counts(attrs),
        # I/O fields — the answer to "what did this span actually do?"
        "input_text": io["input_text"],
        "output_text": io["output_text"],
        "io_format": io["io_format"],
        "messages": io["messages"],
        "documents": io["documents"],
        # Remaining flat attributes that we didn't extract explicitly
        "raw_attributes": cleaned_attrs,
        # Span links (used for fan-in / scatter-gather patterns)
        "links": [
            {
                "trace_id": link.get("traceId") or link.get("trace_id"),
                "span_id": link.get("spanId") or link.get("span_id"),
            }
            for link in span.get("links", []) or []
        ],
    }
