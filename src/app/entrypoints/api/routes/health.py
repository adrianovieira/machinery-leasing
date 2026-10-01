from fastapi import APIRouter, Response, status
from sqlalchemy import text

from app.entrypoints.api.dependencies import DbSessionDep
from app.infrastructure.messaging.config import KafkaConfig


router = APIRouter(prefix="/health", tags=["Health"])


@router.get("/live", status_code=status.HTTP_200_OK, summary="Liveness probe")
def liveness_probe():
    """Retorna HTTP 200 se a aplicação FastAPI estiver em execução."""
    return {"status": "ok"}


@router.get("/ready", summary="Readiness probe")
def readiness_probe(response: Response, db: DbSessionDep):
    """Verifica conectividade com dependências essenciais (MySQL e Kafka)."""
    checks = {
        "database": "unknown",
        "messaging": "unknown",
    }
    is_ready = True

    # 1. Checa banco de dados MySQL
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = "healthy"
    except Exception as exc:
        checks["database"] = f"unhealthy: {str(exc)}"
        is_ready = False

    # 2. Checa configuração do Kafka
    try:
        kafka_cfg = KafkaConfig()
        if kafka_cfg.bootstrap_servers:
            checks["messaging"] = "configured"
        else:
            checks["messaging"] = "missing_bootstrap_servers"
            is_ready = False
    except Exception as exc:
        checks["messaging"] = f"unhealthy: {str(exc)}"
        is_ready = False

    if is_ready:
        response.status_code = status.HTTP_200_OK
        return {"status": "ready", "checks": checks}
    else:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unhealthy", "checks": checks}
