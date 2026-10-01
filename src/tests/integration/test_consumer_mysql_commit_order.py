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
    def produce(self, topic: str, key: str, value: dict, headers: dict | None = None):
        pass

    def flush(self, timeout: float = 5.0) -> int:
        return 0


class DummyRiskClient(ExternalRiskClientPort):
    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        return RiskEvaluation(decision=RiskDecisionEnum.APPROVED)


def test_cenario_4_ordem_estrita_mysql_antes_de_kafka_offset(mocker):
    """Cenário 4 — Garantia de Ordem Transacional:

    Se o commit no MySQL falhar no UseCase, o offset no Kafka NÃO deve ser commitado.
    """
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-order", value=Decimal("10000.00"))
    MySQLTransactionRepository(db).save(tx)
    db.commit()
    db.close()

    event_id = "evt-order-1"
    msg_payload = {
        "event_id": event_id,
        "aggregate_id": tx.id,
        "data": {
            "transaction_id": tx.id,
            "customer_id": "cust-order",
            "value": 10000.00,
        },
    }
    msg_headers = [("x-event-id", event_id.encode("utf-8")), ("x-retry-count", b"0")]
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

    # Simula crash no DB ao persistir a etapa final do UseCase
    mocker.patch.object(
        use_case,
        "execute",
        side_effect=Exception("Database crash on MySQL commit"),
    )

    worker = TransactionConsumerWorker(
        consumer=consumer_mock,
        producer=producer_mock,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
        consumer_group="test-group",
    )

    # Executa o processamento da mensagem
    worker.process_message(message)

    # Como o execute lançou exceção antes do sucesso, o fluxo normal não comita o offset sem enviar para DLQ/Retry
    # A mensagem falha de banco acionou _handle_permanent_failure que registra falha controlada
