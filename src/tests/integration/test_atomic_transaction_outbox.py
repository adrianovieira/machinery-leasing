from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.entrypoints.api.dependencies import get_db
from app.entrypoints.api.main import app
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


def test_atomic_transaction_and_outbox_persistence(client: TestClient):
    """Garante que tanto a Transaction quanto o OutboxEvent são gravados na mesma transação atômica."""
    payload = {"customer_id": "cust_atomic_1", "value": 5000.0}
    response = client.post("/api/v1/transactions", json=payload)

    assert response.status_code == 201
    tx_id = response.json()["id"]

    db = TestingSessionLocal()
    try:
        # Verifica transação criada
        tx_model = db.query(TransactionModel).filter_by(id=tx_id).first()
        assert tx_model is not None
        assert tx_model.customer_id == "cust_atomic_1"
        assert float(tx_model.value) == 5000.0
        assert tx_model.status == "PENDING"

        # Verifica outbox event correspondente
        outbox_model = db.query(OutboxEventModel).filter_by(aggregate_id=tx_id).first()
        assert outbox_model is not None
        assert outbox_model.aggregate_type == "TRANSACTION"
        assert outbox_model.topic == "transactions.created.v1"
        assert outbox_model.status == "PENDING"
        assert outbox_model.retry_count == 0
        assert outbox_model.published_at is None

        # Valida dados do payload
        payload_data = outbox_model.payload
        assert payload_data["event_type"] == "TRANSACTION_CREATED"
        assert payload_data["version"] == "v1"
        assert payload_data["aggregate_id"] == tx_id
        assert payload_data["data"]["transaction_id"] == tx_id
        assert payload_data["data"]["customer_id"] == "cust_atomic_1"
        assert payload_data["data"]["value"] == 5000.0
        assert payload_data["data"]["status"] == "PENDING"
    finally:
        db.close()


def test_atomic_rollback_on_outbox_failure(client: TestClient, mocker):
    """Se a gravação da outbox falhar, a transação deve sofrer rollback e nada é persistido."""
    mocker.patch(
        "app.infrastructure.repositories.mysql_outbox_repository.MySQLOutboxRepository.save",
        side_effect=Exception("Database error on outbox table"),
    )

    payload = {"customer_id": "cust_atomic_fail", "value": 1000.0}
    with pytest.raises(Exception, match="Database error on outbox table"):
        client.post("/api/v1/transactions", json=payload)

    db = TestingSessionLocal()
    try:
        tx_count = (
            db.query(TransactionModel).filter_by(customer_id="cust_atomic_fail").count()
        )
        outbox_count = db.query(OutboxEventModel).count()
        assert tx_count == 0
        assert outbox_count == 0
    finally:
        db.close()
