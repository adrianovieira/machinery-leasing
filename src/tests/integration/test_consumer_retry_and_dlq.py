from decimal import Decimal
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.application.use_cases.process_risk_analysis_use_case import (
    ProcessRiskAnalysisUseCase,
)
from app.domain.entities.risk_evaluation import RiskEvaluation
from app.domain.entities.transaction import Transaction
from app.domain.ports.external_risk_client_port import ExternalRiskClientPort
from app.domain.ports.message_producer_port import MessageProducerPort
from app.entrypoints.workers.transaction_consumer_worker import (
    TransactionConsumerWorker,
)
from app.infrastructure.database.models import (
    Base,
    InboxEventModel,
    TransactionModel,
)
from app.infrastructure.external_services.http_risk_client_adapter import (
    TransientRiskServiceException,
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
        self.committed_messages = []

    def commit(self, message, asynchronous=False):
        self.committed_messages.append(message)


class DummyProducerAdapter(MessageProducerPort):
    def __init__(self):
        self.published_events = []

    def produce(self, topic: str, key: str, value: dict, headers: dict | None = None):
        self.published_events.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )

    def flush(self, timeout: float = 5.0) -> int:
        return 0


class FailingRiskClient(ExternalRiskClientPort):
    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        raise TransientRiskServiceException("HTTP 503 Service Unavailable / Timeout")


def test_cenario_2_retry_com_backoff():
    """Cenário 2 — Teste de Retry:

    Falha transitória com retry_count = 0 -> DB transiciona para RETRYING,
    mensagem é enviada para 'transactions.retry.v1' com retry_count = 1 e offset confirmado.
    """
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-retry", value=Decimal("20000.00"))
    MySQLTransactionRepository(db).save(tx)
    db.commit()
    db.close()

    event_id = "evt-retry-1"
    msg_payload = {
        "event_id": event_id,
        "aggregate_id": tx.id,
        "data": {
            "transaction_id": tx.id,
            "customer_id": "cust-retry",
            "value": 20000.00,
        },
    }
    msg_headers = [
        ("x-event-id", event_id.encode("utf-8")),
        ("x-retry-count", b"0"),
    ]
    message = DummyMessage(key=tx.id, value=msg_payload, headers=msg_headers)

    consumer_mock = DummyConsumerAdapter()
    producer_mock = DummyProducerAdapter()
    failing_client = FailingRiskClient()

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=failing_client,
        session_factory=TestingSessionLocal,
    )

    worker = TransactionConsumerWorker(
        consumer=consumer_mock,
        producer=producer_mock,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
        consumer_group="test-group",
        max_retries=3,
    )

    result = worker.process_message(message)
    assert result is True

    # 1. Verifica publicação no tópico de retry
    assert len(producer_mock.published_events) == 1
    retry_event = producer_mock.published_events[0]
    assert retry_event["topic"] == "transactions.retry.v1"
    assert retry_event["headers"]["x-retry-count"] == "1"
    assert "x-next-retry-timestamp" in retry_event["headers"]

    # 2. Verifica offset original commitado
    assert len(consumer_mock.committed_messages) == 1

    # 3. Verifica estado no banco
    db = TestingSessionLocal()
    try:
        tx_saved = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_saved.status == "RETRYING"
        inbox_saved = db.query(InboxEventModel).filter_by(event_id=event_id).first()
        assert inbox_saved.status == "RETRYING"
    finally:
        db.close()


def test_cenario_3_esgotamento_retries_para_dlq():
    """Cenário 3 — Teste de DLQ:

    Falha persistente com retry_count = 3 (atingiu max_retries) -> DB transiciona para FAILED,
    mensagem é enviada para 'transactions.dlq.v1' com cabeçalhos de diagnóstico e offset confirmado.
    """
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-dlq", value=Decimal("30000.00"))
    MySQLTransactionRepository(db).save(tx)
    db.commit()
    db.close()

    event_id = "evt-dlq-1"
    msg_payload = {
        "event_id": event_id,
        "aggregate_id": tx.id,
        "data": {"transaction_id": tx.id, "customer_id": "cust-dlq", "value": 30000.00},
    }
    msg_headers = [
        ("x-event-id", event_id.encode("utf-8")),
        ("x-retry-count", b"3"),  # Limite máximo de retries
    ]
    message = DummyMessage(key=tx.id, value=msg_payload, headers=msg_headers)

    consumer_mock = DummyConsumerAdapter()
    producer_mock = DummyProducerAdapter()
    failing_client = FailingRiskClient()

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=failing_client,
        session_factory=TestingSessionLocal,
    )

    worker = TransactionConsumerWorker(
        consumer=consumer_mock,
        producer=producer_mock,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
        consumer_group="test-group",
        max_retries=3,
    )

    result = worker.process_message(message)
    assert result is True

    # 1. Verifica publicação na DLQ
    assert len(producer_mock.published_events) == 1
    dlq_event = producer_mock.published_events[0]
    assert dlq_event["topic"] == "transactions.dlq.v1"
    assert dlq_event["headers"]["x-retry-count"] == "3"
    assert dlq_event["headers"]["x-exception-type"] == "TransientRiskServiceException"
    assert "x-exception-message" in dlq_event["headers"]

    # 2. Verifica offset original commitado
    assert len(consumer_mock.committed_messages) == 1

    # 3. Verifica estado no banco
    db = TestingSessionLocal()
    try:
        tx_saved = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_saved.status == "FAILED"
        assert "Limite máximo de retries excedido" in tx_saved.failure_reason

        inbox_saved = db.query(InboxEventModel).filter_by(event_id=event_id).first()
        assert inbox_saved.status == "FAILED"
    finally:
        db.close()
