import time
import uuid

from fastapi import Request
from opentelemetry.trace import SpanKind
from starlette.middleware.base import BaseHTTPMiddleware

from app.infrastructure.observability.logging import (
    clear_log_context,
    set_log_context,
)
from app.infrastructure.observability.metrics import (
    HTTP_REQUEST_DURATION_SECONDS,
    HTTP_REQUESTS_TOTAL,
)
from app.infrastructure.observability.telemetry import (
    extract_trace_context,
    get_tracer,
)


tracer = get_tracer()


class CorrelationAndTelemetryMiddleware(BaseHTTPMiddleware):
    """Middleware para extrair/injetar W3C TraceContext e Correlation ID em requisições HTTP."""

    async def dispatch(self, request: Request, call_next):
        # 1. Extrai cabeçalhos
        headers_dict = dict(request.headers)
        correlation_id = headers_dict.get("x-correlation-id") or str(uuid.uuid4())

        # 2. Extrai ou inicia Contexto OpenTelemetry
        parent_context = extract_trace_context(headers_dict)
        span_name = f"HTTP {request.method} {request.url.path}"

        start_time = time.time()
        with tracer.start_as_current_span(
            name=span_name,
            context=parent_context,
            kind=SpanKind.SERVER,
        ) as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("http.correlation_id", correlation_id)

            # Injeta variáveis no contexto de log
            ctx = span.get_span_context()
            trace_id_hex = format(ctx.trace_id, "032x") if ctx.is_valid else None
            span_id_hex = format(ctx.span_id, "016x") if ctx.is_valid else None

            set_log_context(
                correlation_id=correlation_id,
                trace_id=trace_id_hex,
                span_id=span_id_hex,
                path=request.url.path,
                method=request.method,
            )

            try:
                response = await call_next(request)
                duration = time.time() - start_time

                # Normaliza endpoint para evitar explosão de cardinalidade (ex: /api/v1/transactions/{id})
                normalized_endpoint = request.url.path
                if (
                    request.url.path.startswith("/api/v1/transactions/")
                    and len(request.url.path.split("/")) > 4
                ):
                    normalized_endpoint = "/api/v1/transactions/{id}"

                # Atualiza métricas Prometheus
                HTTP_REQUEST_DURATION_SECONDS.labels(
                    method=request.method,
                    endpoint=normalized_endpoint,
                ).observe(duration)

                HTTP_REQUESTS_TOTAL.labels(
                    method=request.method,
                    endpoint=normalized_endpoint,
                    status_code=str(response.status_code),
                ).inc()

                span.set_attribute("http.status_code", response.status_code)

                # Anexa cabeçalhos de rastreamento na resposta HTTP
                response.headers["X-Correlation-ID"] = correlation_id
                if trace_id_hex and span_id_hex:
                    response.headers["traceparent"] = (
                        f"00-{trace_id_hex}-{span_id_hex}-01"
                    )

                return response

            finally:
                clear_log_context()
