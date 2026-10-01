import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.domain.entities.inbox_event import InboxStatus
from app.domain.ports.message_producer_port import MessageProducerPort
from app.infrastructure.database.models import Base, InboxEventModel
from app.infrastructure.messaging.dlq_manager import DLQManager, DLQMessageInfo
from app.infrastructure.repositories.mysql_inbox_repository import (
    MySQLInboxRepository,
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


class DummyProducer(MessageProducerPort):
    def __init__(self):
        self.published = []

    def produce(self, topic: str, key: str, value: dict, headers: dict | None = None):
        self.published.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )

    def flush(self, timeout: float = 5.0) -> int:
        return 0


def test_dlq_manager_replay_message():
    db = TestingSessionLocal()
    inbox_repo = MySQLInboxRepository(db)
    # Simula evento que falhou no inbox
    inbox_repo.try_acquire(event_id="evt-failed-1", consumer_group="tx-consumer")
    inbox_repo.update_status(
        event_id="evt-failed-1", status=InboxStatus.FAILED, error_message="Fatal error"
    )
    db.commit()
    db.close()

    producer = DummyProducer()
    manager = DLQManager(producer=producer, session_factory=TestingSessionLocal)

    msg_info = DLQMessageInfo(
        key="tx-123",
        payload={"aggregate_id": "tx-123", "value": 1000.0},
        headers={
            "x-event-id": "evt-failed-1",
            "x-retry-count": "3",
            "x-exception-type": "Exception",
        },
        topic="transactions.dlq.v1",
        partition=0,
        offset=10,
    )

    success = manager.replay_message(msg_info, target_topic="transactions.created.v1")
    assert success is True

    # Verifica republicação limpa
    assert len(producer.published) == 1
    published_item = producer.published[0]
    assert published_item["topic"] == "transactions.created.v1"
    assert published_item["key"] == "tx-123"
    assert published_item["headers"]["x-event-id"] == "evt-failed-1"
    assert published_item["headers"]["x-retry-count"] == "0"
    assert published_item["headers"]["x-replayed-from"] == "transactions.dlq.v1"

    # Verifica reset de status no Inbox para PROCESSING
    db = TestingSessionLocal()
    try:
        inbox_entry = (
            db.query(InboxEventModel).filter_by(event_id="evt-failed-1").first()
        )
        assert inbox_entry.status == "PROCESSING"
    finally:
        db.close()
