from unittest.mock import MagicMock

from confluent_kafka import KafkaException
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.entrypoints.api.dependencies import get_db
from app.entrypoints.api.main import app
from app.entrypoints.workers.outbox_publisher_worker import OutboxPublisherWorker
from app.infrastructure.database.models import (
    Base,
    OutboxEventModel,
    TransactionModel,
)


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
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_scenario_1_kafka_failure_and_resilience_recovery(client: TestClient):
    """Cenário de Falha 1: Queda do Kafka.

    1. Kafka cai / fica indisponível.
    2. API continua aceitando requisições com 201 Created.
    3. Transação e evento Outbox ficam salvos como PENDING no MySQL.
    4. Worker tenta publicar, falha no Kafka, incrementa retry_count e NÃO perde dados.
    5. Kafka volta a operar.
    6. Worker processa e publica com sucesso, atualizando outbox para PUBLISHED.
    """
    # 1. Cria transação via API
    resp = client.post(
        "/api/v1/transactions",
        json={"customer_id": "cust_resilience_1", "value": 7500.0},
    )
    assert resp.status_code == 201
    tx_id = resp.json()["id"]

    # 2. Mock do Kafka Producer com falha simulada (Queda do Kafka)
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = KafkaException(
        "Kafka Broker Unavailable (Connection refused)"
    )

    worker = OutboxPublisherWorker(
        producer=mock_producer,
        session_factory=TestingSessionLocal,
        max_retries=5,
    )

    # 3. Worker executa quando Kafka está DOWN
    processed_during_failure = worker.run_once()
    assert processed_during_failure == 0

    # 4. Verifica que o evento continua PENDING com retry_count incrementado
    db = TestingSessionLocal()
    try:
        event_model = db.query(OutboxEventModel).filter_by(aggregate_id=tx_id).first()
        assert event_model is not None
        assert event_model.status == "PENDING"
        assert event_model.retry_count == 1
        assert event_model.published_at is None

        tx_model = db.query(TransactionModel).filter_by(id=tx_id).first()
        assert tx_model.status == "PENDING"
    finally:
        db.close()

    # 5. Kafka é restabelecido (remove side_effect)
    mock_producer.produce.side_effect = None
    mock_producer.flush.return_value = 0

    # 6. Worker executa novamente com Kafka restabelecido
    processed_after_recovery = worker.run_once()
    assert processed_after_recovery == 1

    # 7. Verifica que o evento foi transicionado para PUBLISHED
    db = TestingSessionLocal()
    try:
        event_model = db.query(OutboxEventModel).filter_by(aggregate_id=tx_id).first()
        assert event_model.status == "PUBLISHED"
        assert event_model.published_at is not None
    finally:
        db.close()
