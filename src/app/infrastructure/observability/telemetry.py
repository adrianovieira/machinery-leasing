from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


# Inicializa TracerProvider global caso ainda não esteja configurado
if not isinstance(trace.get_tracer_provider(), TracerProvider):
    provider = TracerProvider()
    trace.set_tracer_provider(provider)

_tracer = trace.get_tracer("machinery-leasing", "1.0.0")
_propagator = TraceContextTextMapPropagator()


def get_tracer():
    return _tracer


def extract_trace_context(carrier: dict[str, Any]):
    """Extrai contexto W3C TraceContext a partir de um dicionário carrier."""
    return _propagator.extract(carrier=carrier)


def inject_trace_context(carrier: dict[str, Any]):
    """Injeta contexto W3C TraceContext no carrier."""
    _propagator.inject(carrier=carrier)
    return carrier


def get_current_trace_and_span_ids() -> tuple[str | None, str | None]:
    """Retorna (trace_id_hex, span_id_hex) do Span ativo no contexto atual."""
    span = trace.get_current_span()
    if span and span.is_recording():
        ctx = span.get_span_context()
        if ctx.is_valid:
            trace_id = format(ctx.trace_id, "032x")
            span_id = format(ctx.span_id, "016x")
            return trace_id, span_id
    return None, None
