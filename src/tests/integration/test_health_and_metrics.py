from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.entrypoints.api.dependencies import get_db
from app.entrypoints.api.main import app
from app.infrastructure.database.models import Base


SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_live_endpoint(client):
    """Valida o endpoint /health/live."""
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_ready_endpoint(client):
    """Valida o endpoint /health/ready."""
    response = client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert data["checks"]["database"] == "healthy"


def test_prometheus_metrics_endpoint(client):
    """Valida exposição do endpoint /metrics e contagem de transações."""
    # 1. Cria uma transação para incrementar contadores
    create_payload = {
        "customer_id": "cust-metrics-1",
        "value": 15000.00,
    }
    create_res = client.post(
        "/api/v1/transactions",
        json=create_payload,
        headers={"X-Correlation-ID": "corr-test-123"},
    )
    assert create_res.status_code == 201
    assert create_res.headers.get("X-Correlation-ID") == "corr-test-123"
    assert "traceparent" in create_res.headers

    # 2. Consulta endpoint de métricas
    metrics_res = client.get("/metrics")
    assert metrics_res.status_code == 200
    metrics_text = metrics_res.text

    # Verifica presença de métricas essenciais
    assert "transactions_created_total" in metrics_text
    assert "http_requests_total" in metrics_text
    assert "http_request_duration_seconds" in metrics_text
    assert "kafka_messages_consumed_total" in metrics_text
    assert "risk_analysis_requests_total" in metrics_text
