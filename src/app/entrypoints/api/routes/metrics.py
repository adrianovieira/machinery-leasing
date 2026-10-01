from fastapi import APIRouter, Response

from app.infrastructure.observability.metrics import (
    get_prometheus_metrics_payload,
)


router = APIRouter(tags=["Observability"])


@router.get(
    "/metrics", summary="Métricas operacionais e de negócio no formato Prometheus"
)
def prometheus_metrics():
    """Endpoint raspado periodicamente pelo servidor do Prometheus."""
    content = get_prometheus_metrics_payload()
    return Response(
        content=content,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
