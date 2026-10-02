"""OpenTelemetry SDK Integration for Entropi.

Provides drop-in OpenTelemetry SpanProcessor and Sampler implementations:
- `EntropiSampler`: Evaluates initial span attributes and traceparent context.
- `EntropiSpanProcessor`: Evaluates complete span outcome (duration, status_code, exceptions)
  and routes high-surprise spans to exporters while maintaining an in-memory ring buffer.

Copyright (c) 2026. Licensed under AGPLv3.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional, Sequence
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.sdk.trace.sampling import (
    Decision,
    Sampler,
    SamplingResult,
)
from opentelemetry.trace import Link, SpanKind, TraceFlags, TraceState

from entropi import RetroactiveRingBuffer, SlidingWindowEntropyLattice, SurpriseEngine


class EntropiSpanProcessor(SpanProcessor):
    """OpenTelemetry SpanProcessor that filters telemetry using Shannon Surprise.

    Only forwards spans with high diagnostic value to the downstream exporter
    (e.g. OTLP, Datadog, Jaeger), while holding recent spans in a circular ring buffer
    for retroactive trace promotion.
    """

    def __init__(
        self,
        exporter: Any = None,
        w: int = 256,
        d: int = 4,
        window_seconds: float = 60.0,
        base_rate: float = 0.001,
        tau_min: float = 4.0,
        tau_crit: float = 12.0,
        ring_buffer_capacity: int = 1024,
    ):
        self.exporter = exporter
        self.lattice = SlidingWindowEntropyLattice(window_seconds=window_seconds, w=w, d=d)
        self.engine = SurpriseEngine(
            self.lattice,
            base_rate=base_rate,
            tau_min=tau_min,
            tau_crit=tau_crit,
        )
        self.ring_buffer = RetroactiveRingBuffer(capacity=ring_buffer_capacity)
        self.flagged_traces: set[str] = set()
        self.spans_seen = 0
        self.spans_emitted = 0
        self.anomalies_caught = 0

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        self.spans_seen += 1

        # Extract span duration
        duration_ms = 0.0
        if span.start_time and span.end_time:
            duration_ms = (span.end_time - span.start_time) / 1_000_000.0

        # Extract status code and exceptions
        status_code = 200
        has_exception = False
        if span.status.status_code.value == 2:  # StatusCode.ERROR
            status_code = 500
            has_exception = True

        # Extract attributes
        attrs = dict(span.attributes) if span.attributes else {}
        attrs["name"] = span.name
        trace_id_hex = format(span.context.trace_id, "032x")
        attrs["trace_id"] = trace_id_hex

        # Check for explicit HTTP attributes
        if "http.status_code" in attrs:
            try:
                status_code = int(attrs["http.status_code"])
            except (ValueError, TypeError):
                pass

        # Evaluate surprise
        surprise, p_sample, should_sample = self.engine.evaluate_span(
            attributes=attrs,
            duration_ms=duration_ms,
            status_code=status_code,
            has_exception=has_exception,
        )

        # Check if this trace was flagged retroactively by downstream call during execution
        if trace_id_hex in self.flagged_traces:
            should_sample = True
            self.flagged_traces.discard(trace_id_hex)

        span_record = {
            "name": span.name,
            "trace_id": trace_id_hex,
            "span_id": format(span.context.span_id, "016x"),
            "duration_ms": duration_ms,
            "status_code": status_code,
            "attributes": attrs,
            "surprise_bits": surprise,
            "sample_probability": p_sample,
            "emitted": should_sample,
            "span_obj": span,
        }

        # Store in circular ring buffer for potential retroactive parent promotion
        self.ring_buffer.push(span_record)

        if should_sample:
            self.spans_emitted += 1
            if status_code >= 500 or surprise >= self.engine.tau_crit:
                self.anomalies_caught += 1

            if self.exporter is not None:
                self.exporter.export([span])

    def promote_trace(self, trace_id: str) -> List[Dict[str, Any]]:
        """Retroactively promotes and exports un-emitted parent spans for trace_id.

        Triggered when a downstream microservice signals an anomaly via W3C or
        HTTP retroactive headers.
        """
        promoted = self.ring_buffer.promote_trace(trace_id)
        if promoted:
            self.spans_emitted += len(promoted)
            self.anomalies_caught += len(promoted)
            if self.exporter is not None:
                spans_to_export = [p["span_obj"] for p in promoted if "span_obj" in p]
                if spans_to_export:
                    self.exporter.export(spans_to_export)
        return promoted

    def handle_downstream_response_headers(self, headers: Dict[str, str]) -> int:
        """Parses downstream response headers and triggers retroactive promotion if flagged.

        Supported headers:
        - `x-entropi-retroactive`: format `sample=1;trace_id=<hex>`
        """
        for k, v in headers.items():
            if k.lower() == "x-entropi-retroactive":
                parts = dict(p.strip().split("=", 1) for p in v.split(";") if "=" in p)
                if parts.get("sample") == "1" and "trace_id" in parts:
                    tid = parts["trace_id"]
                    self.flagged_traces.add(tid)
                    promoted = self.promote_trace(tid)
                    return len(promoted)
        return 0

    def shutdown(self) -> None:
        if self.exporter is not None and hasattr(self.exporter, "shutdown"):
            self.exporter.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        if self.exporter is not None and hasattr(self.exporter, "force_flush"):
            return self.exporter.force_flush(timeout_millis)
        return True

    def stats(self) -> Dict[str, Any]:
        return {
            "spans_seen": self.spans_seen,
            "spans_emitted": self.spans_emitted,
            "anomalies_caught": self.anomalies_caught,
            "reduction_percent": (1.0 - (self.spans_emitted / max(1, self.spans_seen))) * 100.0,
            "memory_bytes": self.lattice.size_bytes() + self.ring_buffer.size_bytes(),
        }


class EntropiFlaskMiddleware:
    """Drop-in Flask middleware for automatic retroactive trace cohesion.

    1. Injects `x-entropi-retroactive: sample=1;trace_id=<hex>` into outgoing responses
       if the current trace encountered high Shannon surprise or an error.
    2. Allows upstream services to immediately retrieve and export suppressed parent spans.
    """

    def __init__(self, app: Any, processor: EntropiSpanProcessor):
        self.app = app
        self.processor = processor
        self._init_middleware()

    def _init_middleware(self) -> None:
        @self.app.after_request
        def _after_request(response: Any) -> Any:
            try:
                curr_span = trace.get_current_span()
                if curr_span and curr_span.get_span_context().is_valid:
                    trace_id = format(curr_span.get_span_context().trace_id, "032x")
                    cached = self.processor.ring_buffer.get_spans_by_trace_id(trace_id)
                    # If any span in this trace was anomalous or emitted
                    if any(
                        s.get("emitted") and (s.get("status_code", 200) >= 500 or s.get("surprise_bits", 0) >= self.processor.engine.tau_crit)
                        for s in cached
                    ):
                        response.headers["x-entropi-retroactive"] = f"sample=1;trace_id={trace_id}"
            except Exception:
                pass
            return response
