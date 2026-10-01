import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.domain.ports.message_producer_port import MessageProducerPort
from app.infrastructure.database.models import Base, InboxEventModel
from app.infrastructure.messaging.dlq_manager import DLQManager, DLQMessageInfo


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


class RecordingProducer(MessageProducerPort):
    def __init__(self):
        self.published = []

    def produce(self, topic: str, key: str, value: dict, headers: dict | None = None):
        self.published.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )

    def flush(self, timeout: float = 5.0) -> int:
        return 0


def test_dlq_replay_cli_flow(monkeypatch, capsys):
    """Valida o fluxo operacional do DLQManager simulando o comportamento da CLI dlq_replay."""
    db = TestingSessionLocal()
    inbox_entry = InboxEventModel(
        event_id="evt-dlq-test-99",
        consumer_group="tx-consumer",
        status="FAILED",
        error_message="Simulated permanent network fault",
    )
    db.add(inbox_entry)
    db.commit()
    db.close()

    producer = RecordingProducer()
    manager = DLQManager(producer=producer, session_factory=TestingSessionLocal)

    mock_dlq_messages = [
        DLQMessageInfo(
            key="tx-uuid-99",
            payload={"aggregate_id": "tx-uuid-99", "value": 7500.0},
            headers={
                "x-event-id": "evt-dlq-test-99",
                "x-retry-count": "3",
                "x-exception-type": "TransientRiskServiceException",
                "x-exception-message": "Service unavailable after 3 attempts",
            },
            topic="transactions.dlq.v1",
            partition=0,
            offset=42,
        )
    ]

    # Simula inspeção
    monkeypatch.setattr(
        manager, "inspect_messages", lambda max_messages=50: mock_dlq_messages
    )

    messages = manager.inspect_messages(max_messages=10)
    assert len(messages) == 1
    assert messages[0].key == "tx-uuid-99"

    # Executa replay
    replayed_count = 0
    for msg in messages:
        if manager.replay_message(
            msg, target_topic="transactions.created.v1", regenerate_event_id=False
        ):
            replayed_count += 1

    assert replayed_count == 1
    assert len(producer.published) == 1
    published_msg = producer.published[0]
    assert published_msg["topic"] == "transactions.created.v1"
    assert published_msg["key"] == "tx-uuid-99"
    assert published_msg["headers"]["x-retry-count"] == "0"
    assert published_msg["headers"]["x-event-id"] == "evt-dlq-test-99"

    # Valida reset no banco para PROCESSING
    db = TestingSessionLocal()
    try:
        updated_inbox = (
            db.query(InboxEventModel).filter_by(event_id="evt-dlq-test-99").first()
        )
        assert updated_inbox is not None
        assert updated_inbox.status == "PROCESSING"
        assert updated_inbox.error_message == "Replay iniciado a partir da DLQ"
    finally:
        db.close()
