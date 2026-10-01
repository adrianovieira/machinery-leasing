from decimal import Decimal

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
from app.infrastructure.database.models import (
    Base,
    InboxEventModel,
    OutboxEventModel,
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


class DummyRiskClient(ExternalRiskClientPort):
    def __init__(self, decision=RiskDecisionEnum.APPROVED, reason=None):
        self.decision = decision
        self.reason = reason
        self.call_count = 0

    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        self.call_count += 1
        return RiskEvaluation(decision=self.decision, reason=self.reason)


def test_process_risk_analysis_approved():
    db = TestingSessionLocal()
    # Criação da transação no DB
    tx = Transaction.create(customer_id="cust-1", value=Decimal("10000.00"))
    tx_repo = MySQLTransactionRepository(db)
    inbox_repo = MySQLInboxRepository(db)

    tx_repo.save(tx)
    # Simula entrada no inbox pelo worker
    inbox_repo.try_acquire(event_id="evt-1", consumer_group="test-group")
    db.commit()
    db.close()

    risk_client = DummyRiskClient(decision=RiskDecisionEnum.APPROVED)
    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=risk_client,
        session_factory=TestingSessionLocal,
    )

    result_tx = use_case.execute(transaction_id=tx.id, event_id="evt-1")
    assert result_tx.status.value == "APPROVED"
    assert risk_client.call_count == 1

    # Verificações no banco
    db = TestingSessionLocal()
    try:
        tx_saved = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_saved.status == "APPROVED"

        inbox_saved = db.query(InboxEventModel).filter_by(event_id="evt-1").first()
        assert inbox_saved.status == "COMPLETED"
        assert inbox_saved.processed_at is not None

        outbox_saved = (
            db.query(OutboxEventModel)
            .filter_by(aggregate_id=tx.id, topic="transactions.status.v1")
            .first()
        )
        assert outbox_saved is not None
        assert outbox_saved.payload["data"]["new_status"] == "APPROVED"
    finally:
        db.close()


def test_process_risk_analysis_rejected():
    db = TestingSessionLocal()
    tx = Transaction.create(customer_id="cust-2", value=Decimal("80000.00"))
    tx_repo = MySQLTransactionRepository(db)
    inbox_repo = MySQLInboxRepository(db)

    tx_repo.save(tx)
    inbox_repo.try_acquire(event_id="evt-2", consumer_group="test-group")
    db.commit()
    db.close()

    risk_client = DummyRiskClient(
        decision=RiskDecisionEnum.REJECTED, reason="Score insuficiente"
    )
    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=risk_client,
        session_factory=TestingSessionLocal,
    )

    result_tx = use_case.execute(transaction_id=tx.id, event_id="evt-2")
    assert result_tx.status.value == "REJECTED"

    db = TestingSessionLocal()
    try:
        tx_saved = db.query(TransactionModel).filter_by(id=tx.id).first()
        assert tx_saved.status == "REJECTED"
        assert tx_saved.failure_reason == "Score insuficiente"

        inbox_saved = db.query(InboxEventModel).filter_by(event_id="evt-2").first()
        assert inbox_saved.status == "COMPLETED"

        outbox_saved = (
            db.query(OutboxEventModel)
            .filter_by(aggregate_id=tx.id, topic="transactions.status.v1")
            .first()
        )
        assert outbox_saved.payload["data"]["new_status"] == "REJECTED"
        assert outbox_saved.payload["data"]["reason"] == "Score insuficiente"
    finally:
        db.close()
