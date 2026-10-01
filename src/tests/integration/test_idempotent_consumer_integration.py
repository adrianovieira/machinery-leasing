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
from app.entrypoints.workers.transaction_consumer_worker import (
    TransactionConsumerWorker,
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


class DummyRiskClient(ExternalRiskClientPort):
    def __init__(self):
        self.call_count = 0

    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        self.call_count += 1
        return RiskEvaluation(decision=RiskDecisionEnum.APPROVED, reason="OK")


def test_cenario_1_duplicidade_idempotente():
    """Cenário 1 — Teste de Duplicidade:

    Enviar a mesma mensagem Kafka 2 vezes -> Apenas 1 execução no DB e na API externa,
    e ambas as mensagens têm seus offsets confirmados.
    """
    # 1. Cria a transação no banco
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-dup", value=Decimal("15000.00"))
    MySQLTransactionRepository(db).save(tx)
    db.commit()
    db.close()

    # Prepara mensagem Kafka
    event_id = "evt-duplicate-uuid"
    msg_payload = {
        "event_id": event_id,
        "aggregate_id": tx.id,
        "event_type": "TRANSACTION_CREATED",
        "data": {
            "transaction_id": tx.id,
            "customer_id": "cust-dup",
            "value": 15000.00,
        },
    }
    msg_headers = [
        ("x-event-id", event_id.encode("utf-8")),
        ("x-retry-count", b"0"),
    ]

    message = DummyMessage(key=tx.id, value=msg_payload, headers=msg_headers)
    consumer_mock = DummyConsumerAdapter()
    producer_mock = DummyProducerAdapter()
    risk_client = DummyRiskClient()

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=risk_client,
        session_factory=TestingSessionLocal,
    )

    worker = TransactionConsumerWorker(
        consumer=consumer_mock,
        producer=producer_mock,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
        consumer_group="test-tx-consumer",
    )

    # Primeira execução (Mensagem 1)
    res1 = worker.process_message(message)
    assert res1 is True
    assert risk_client.call_count == 1
    assert len(consumer_mock.committed_messages) == 1

    # Segunda execução com a mesma mensagem (Mensagem duplicada)
    res2 = worker.process_message(message)
    assert res2 is True
    # GARANTIA: A API de risco NÃO deve ser chamada novamente!
    assert risk_client.call_count == 1
    # Offset da mensagem duplicada deve ser confirmado para descarte limpo
    assert len(consumer_mock.committed_messages) == 2

    # Verificação de unicidade no Banco de Dados
    db = TestingSessionLocal()
    try:
        inbox_records = db.query(InboxEventModel).filter_by(event_id=event_id).all()
        assert len(inbox_records) == 1
        assert inbox_records[0].status == "COMPLETED"

        tx_record = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_record.status == "APPROVED"
    finally:
        db.close()
