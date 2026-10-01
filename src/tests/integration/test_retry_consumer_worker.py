from decimal import Decimal
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.application.use_cases.process_risk_analysis_use_case import (
    ProcessRiskAnalysisUseCase,
)
from app.domain.entities.risk_evaluation import RiskDecisionEnum, RiskEvaluation
from app.domain.entities.transaction import Transaction
from app.domain.ports.external_risk_client_port import ExternalRiskClientPort
from app.domain.ports.message_producer_port import MessageProducerPort
from app.entrypoints.workers.retry_consumer_worker import (
    RetryConsumerWorker,
)
from app.infrastructure.database.models import (
    Base,
    InboxEventModel,
    TransactionModel,
)
from app.infrastructure.repositories.mysql_inbox_repository import (
    MySQLInboxRepository,
)
from app.infrastructure.repositories.mysql_outbox_repository import (
    MySQLOutboxRepository,
)
from app.infrastructure.repositories.mysql_transaction_repository import (
    MySQLTransactionRepository,
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


class DummyMessage:
    def __init__(self, key: str, value: dict, headers: list):
        self._key = key.encode("utf-8")
        self._value = json.dumps(value).encode("utf-8")
        self._headers = headers

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers


class DummyConsumerAdapter:
    def __init__(self):
        self.committed = []

    def commit(self, message, asynchronous=False):
        self.committed.append(message)


class DummyProducerAdapter(MessageProducerPort):
    def produce(self, topic: str, key: str, value: dict, headers: dict | None = None):
        pass

    def flush(self, timeout: float = 5.0) -> int:
        return 0


class SuccessRiskClient(ExternalRiskClientPort):
    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        return RiskEvaluation(
            decision=RiskDecisionEnum.APPROVED, reason="Aprovado no retry"
        )


def test_retry_consumer_worker_successful_retry():
    """Valida o reprocessamento bem-sucedido de uma mensagem na fila transactions.retry.v1."""
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-retry-worker", value=Decimal("12000.00"))
    MySQLTransactionRepository(db).save(tx)
    # Status inicial RETRYING
    tx.mark_as_processing()
    tx.mark_as_retrying("Falha temporária anterior")
    MySQLTransactionRepository(db).save(tx)
    MySQLInboxRepository(db).try_acquire(
        event_id="evt-retry-work-1", consumer_group="tx-consumer"
    )
    db.commit()
    db.close()

    event_id = "evt-retry-work-1"
    msg_payload = {
        "event_id": event_id,
        "aggregate_id": tx.id,
        "data": {
            "transaction_id": tx.id,
            "customer_id": "cust-retry-worker",
            "value": 12000.00,
        },
    }
    msg_headers = [
        ("x-event-id", event_id.encode("utf-8")),
        ("x-retry-count", b"1"),
    ]
    message = DummyMessage(key=tx.id, value=msg_payload, headers=msg_headers)

    consumer_mock = DummyConsumerAdapter()
    producer_mock = DummyProducerAdapter()
    risk_client = SuccessRiskClient()

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=risk_client,
        session_factory=TestingSessionLocal,
    )

    worker = RetryConsumerWorker(
        consumer=consumer_mock,
        producer=producer_mock,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
        consumer_group="test-retry-group",
    )

    processed = worker.process_message(message)
    assert processed is True
    assert len(consumer_mock.committed) == 1

    # Valida no banco que transicionou para APPROVED
    db = TestingSessionLocal()
    try:
        tx_saved = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_saved.status == "APPROVED"

        inbox_saved = db.query(InboxEventModel).filter_by(event_id=event_id).first()
        assert inbox_saved.status == "COMPLETED"
    finally:
        db.close()
