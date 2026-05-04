"""
Normalize OTel spans across competing semantic conventions.

The Decision Model brief identifies two competing conventions on top of OTel:
  - OpenInference (Arize-led):  openinference.span.kind, llm.*, tool.*, retrieval.*
  - OTel GenAI (CNCF):           gen_ai.operation.name, gen_ai.tool.name, gen_ai.agent.name

This module reads attributes from either convention (and a few others) and
returns canonical fields the ingest layer can MERGE on without caring which
SDK produced the trace.

References:
  - https://opentelemetry.io/docs/specs/semconv/gen-ai/
  - https://arize-ai.github.io/openinference/spec/
"""

from __future__ import annotations
from typing import Any, Optional


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
        # Keep the raw attributes too — useful for debugging and for queries that
        # need a field we didn't anticipate.
        "raw_attributes": {k: v for k, v in attrs.items() if isinstance(v, (str, int, float, bool))},
        # Span links (used for fan-in / scatter-gather patterns)
        "links": [
            {
                "trace_id": link.get("traceId") or link.get("trace_id"),
                "span_id": link.get("spanId") or link.get("span_id"),
            }
            for link in span.get("links", []) or []
        ],
    }
