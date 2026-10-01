from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import json
import random
import threading
import time

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
_raw_session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
_sqlite_lock = threading.Lock()


class ThreadSafeSessionContext:
    def __init__(self, session, lock):
        self._session = session
        self._lock = lock

    def __getattr__(self, name):
        return getattr(self._session, name)

    def __enter__(self):
        self._lock.acquire()
        return self._session.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            return self._session.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._lock.release()


def TestingSessionLocal():
    session = _raw_session_factory()
    # Intercept context manager to protect SQLite in-memory shared connection
    return ThreadSafeSessionContext(session, _sqlite_lock)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


class ConcurrentDummyMessage:
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


class ThreadSafeConsumerAdapter:
    def __init__(self):
        self.committed = []

    def commit(self, message, asynchronous=False):
        self.committed.append(message)


class ThreadSafeProducerAdapter(MessageProducerPort):
    def __init__(self):
        self.published = []

    def produce(self, topic: str, key: str, value: dict, headers: dict | None = None):
        self.published.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )

    def flush(self, timeout: float = 5.0) -> int:
        return 0


class ConcurrentTrackingRiskClient(ExternalRiskClientPort):
    def __init__(self):
        self.call_count = 0

    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        # Simula pequeno atraso de rede para provocar concorrência real entre threads
        time.sleep(random.uniform(0.005, 0.02))
        self.call_count += 1
        return RiskEvaluation(decision=RiskDecisionEnum.APPROVED, reason="OK")


def test_carga_concorrente_zero_perda_e_zero_duplicacao():
    """Validação de Carga Concorrente:

    Submete 50 transações únicas + 50 mensagens duplicadas (100 mensagens no total)
    em paralelo através de 10 threads concorrentes.
    Garante:
    1. Zero perda: Todas as 50 transações únicas são processadas com sucesso como APPROVED.
    2. Zero duplicação: Exatamente 50 registros únicos no Inbox e 50 transações no MySQL.
    3. Idempotência estrita: Exatamente 50 chamadas à API de risco (as 50 duplicatas foram descartadas antes).
    """
    total_unique_transactions = 50
    db = TestingSessionLocal()
    tx_repo = MySQLTransactionRepository(db)

    tx_list = []
    messages = []

    for i in range(total_unique_transactions):
        tx = Transaction.create(customer_id=f"cust-load-{i}", value=Decimal("5000.00"))
        tx_repo.save(tx)
        tx_list.append(tx)

        event_id = f"evt-load-{i}"
        msg_payload = {
            "event_id": event_id,
            "aggregate_id": tx.id,
            "data": {
                "transaction_id": tx.id,
                "customer_id": f"cust-load-{i}",
                "value": 5000.00,
            },
        }
        msg_headers = [
            ("x-event-id", event_id.encode("utf-8")),
            ("x-retry-count", b"0"),
        ]
        msg = ConcurrentDummyMessage(key=tx.id, value=msg_payload, headers=msg_headers)

        # Mensagem original
        messages.append(msg)
        # Mensagem duplicada intencional
        messages.append(msg)

    db.commit()
    db.close()

    # Embaralha a ordem para simular tráfego caótico
    random.shuffle(messages)
    assert len(messages) == 100

    consumer_mock = ThreadSafeConsumerAdapter()
    producer_mock = ThreadSafeProducerAdapter()
    risk_client = ConcurrentTrackingRiskClient()

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=risk_client,
        session_factory=TestingSessionLocal,
    )

    def process_worker_task(msg):
        worker = TransactionConsumerWorker(
            consumer=consumer_mock,
            producer=producer_mock,
            process_risk_use_case=use_case,
            session_factory=TestingSessionLocal,
        )
        return worker.process_message(msg)

    # Execução concorrente com ThreadPool
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_worker_task, msg) for msg in messages]
        results = [f.result() for f in as_completed(futures)]

    # Todas as mensagens foram processadas (ou descartadas como duplicatas) com commit
    assert all(results) is True
    assert len(consumer_mock.committed) == 100

    # 1. Garantia de Idempotência: Exatamente 50 chamadas ao serviço de risco
    assert risk_client.call_count == total_unique_transactions

    # 2. Verificação no Banco de Dados
    db = TestingSessionLocal()
    try:
        total_inbox = db.query(InboxEventModel).count()
        total_completed = (
            db.query(InboxEventModel).filter_by(status="COMPLETED").count()
        )
        assert total_inbox == total_unique_transactions
        assert total_completed == total_unique_transactions

        total_tx = db.query(TransactionModel).count()
        total_approved_tx = (
            db.query(TransactionModel).filter_by(status="APPROVED").count()
        )
        assert total_tx == total_unique_transactions
        assert total_approved_tx == total_unique_transactions
    finally:
        db.close()
