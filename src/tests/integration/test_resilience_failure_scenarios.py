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
from app.entrypoints.workers.transaction_consumer_worker import (
    TransactionConsumerWorker,
)
from app.infrastructure.database.models import (
    Base,
    TransactionModel,
)
from app.infrastructure.external_services.http_risk_client_adapter import (
    TransientRiskServiceException,
)
from app.infrastructure.messaging.dlq_manager import DLQManager, DLQMessageInfo
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
    def __init__(self):
        self.published = []

    def produce(self, topic: str, key: str, value: dict, headers: dict | None = None):
        self.published.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )

    def flush(self, timeout: float = 5.0) -> int:
        return 0


class FlakyRiskClient(ExternalRiskClientPort):
    """Cliente que falha N vezes e depois passa a aprovar."""

    def __init__(self, failures_before_success: int):
        self.failures_before_success = failures_before_success
        self.calls = 0

    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        self.calls += 1
        if self.calls <= self.failures_before_success:
            raise TransientRiskServiceException("HTTP 503 Service Unavailable")
        return RiskEvaluation(
            decision=RiskDecisionEnum.APPROVED, reason="Aprovado após recuperação"
        )


def test_cenario_falha_2_recuperacao_apos_retry():
    """Cenário de Falha 2: Falha transitória -> vai para retry -> RetryConsumer reprocessa e aprova."""
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-flaky", value=Decimal("10000.00"))
    MySQLTransactionRepository(db).save(tx)
    db.commit()
    db.close()

    event_id = "evt-flaky-1"
    msg_payload = {
        "event_id": event_id,
        "aggregate_id": tx.id,
        "data": {
            "transaction_id": tx.id,
            "customer_id": "cust-flaky",
            "value": 10000.00,
        },
    }
    msg_headers = [("x-event-id", event_id.encode("utf-8")), ("x-retry-count", b"0")]
    message = DummyMessage(key=tx.id, value=msg_payload, headers=msg_headers)

    producer = DummyProducerAdapter()
    consumer = DummyConsumerAdapter()
    flaky_client = FlakyRiskClient(failures_before_success=1)

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=flaky_client,
        session_factory=TestingSessionLocal,
    )

    main_worker = TransactionConsumerWorker(
        consumer=consumer,
        producer=producer,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
    )

    # 1. Primeira tentativa falha na fila principal
    main_worker.process_message(message)
    assert len(producer.published) == 1
    retry_msg_published = producer.published[0]
    assert retry_msg_published["topic"] == "transactions.retry.v1"

    # 2. Mensagem é consumida pelo RetryConsumerWorker
    retry_consumer_mock = DummyConsumerAdapter()
    retry_msg = DummyMessage(
        key=tx.id,
        value=retry_msg_published["value"],
        headers=[
            (k, v.encode("utf-8") if isinstance(v, str) else v)
            for k, v in retry_msg_published["headers"].items()
        ],
    )

    retry_worker = RetryConsumerWorker(
        consumer=retry_consumer_mock,
        producer=producer,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
    )

    retry_worker.process_message(retry_msg)
    assert len(retry_consumer_mock.committed) == 1

    # 3. Transação finalizada como APPROVED no banco
    db = TestingSessionLocal()
    try:
        tx_saved = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_saved.status == "APPROVED"
    finally:
        db.close()


def test_cenario_falha_3_queda_prolongada_e_dlq_replay():
    """Cenário de Falha 3: Indisponibilidade prolongada -> atinge limite de retries -> cai na DLQ -> recupera via Replay."""
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-down", value=Decimal("25000.00"))
    MySQLTransactionRepository(db).save(tx)
    db.commit()
    db.close()

    event_id = "evt-down-1"
    msg_payload = {
        "event_id": event_id,
        "aggregate_id": tx.id,
        "data": {
            "transaction_id": tx.id,
            "customer_id": "cust-down",
            "value": 25000.00,
        },
    }
    # Já está na tentativa 3 (máximo)
    msg_headers = [("x-event-id", event_id.encode("utf-8")), ("x-retry-count", b"3")]
    message = DummyMessage(key=tx.id, value=msg_payload, headers=msg_headers)

    producer = DummyProducerAdapter()
    consumer = DummyConsumerAdapter()
    # Cliente que falha constantemente
    down_client = FlakyRiskClient(failures_before_success=999)

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=down_client,
        session_factory=TestingSessionLocal,
    )

    retry_worker = RetryConsumerWorker(
        consumer=consumer,
        producer=producer,
        process_risk_use_case=use_case,
        session_factory=TestingSessionLocal,
        max_retries=3,
    )

    # 1. Executa na fila de retry e excede limite -> vai para DLQ
    retry_worker.process_message(message)
    assert len(producer.published) == 1
    dlq_event = producer.published[0]
    assert dlq_event["topic"] == "transactions.dlq.v1"

    # Verifica status FAILED no DB
    db = TestingSessionLocal()
    try:
        tx_saved = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_saved.status == "FAILED"
    finally:
        db.close()

    # 2. Serviço é restabelecido e operamos o DLQ Replay
    dlq_manager = DLQManager(producer=producer, session_factory=TestingSessionLocal)
    dlq_info = DLQMessageInfo(
        key=tx.id,
        payload=dlq_event["value"],
        headers=dlq_event["headers"],
        topic="transactions.dlq.v1",
        partition=0,
        offset=1,
    )

    # Replay para transactions.created.v1
    replay_success = dlq_manager.replay_message(
        dlq_info, target_topic="transactions.created.v1"
    )
    assert replay_success is True

    # 3. Verifica republicação no tópico alvo com retry-count zerado
    assert len(producer.published) == 2
    replayed_msg = producer.published[1]
    assert replayed_msg["topic"] == "transactions.created.v1"
    assert replayed_msg["headers"]["x-retry-count"] == "0"

    # 4. Consumo da mensagem reprocessada com o serviço agora normalizado
    healthy_client = FlakyRiskClient(failures_before_success=0)
    use_case_recovered = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=healthy_client,
        session_factory=TestingSessionLocal,
    )

    final_consumer = DummyConsumerAdapter()
    final_worker = TransactionConsumerWorker(
        consumer=final_consumer,
        producer=producer,
        process_risk_use_case=use_case_recovered,
        session_factory=TestingSessionLocal,
    )

    replayed_kafka_msg = DummyMessage(
        key=replayed_msg["key"],
        value=replayed_msg["value"],
        headers=[
            (k, v.encode("utf-8") if isinstance(v, str) else v)
            for k, v in replayed_msg["headers"].items()
        ],
    )

    final_worker.process_message(replayed_kafka_msg)

    # Transação finalmente atinge APPROVED
    db = TestingSessionLocal()
    try:
        tx_final = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_final.status == "APPROVED"
    finally:
        db.close()
