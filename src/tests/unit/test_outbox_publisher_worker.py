from unittest.mock import MagicMock

from confluent_kafka import KafkaException
import pytest

from app.domain.entities.outbox_event import OutboxEvent
from app.domain.ports.message_producer_port import MessageProducerPort
from app.entrypoints.workers.outbox_publisher_worker import OutboxPublisherWorker


@pytest.fixture
def mock_producer():
    return MagicMock(spec=MessageProducerPort)


@pytest.fixture
def mock_session():
    return MagicMock()


def test_outbox_publisher_worker_processes_pending_events_successfully(
    mock_producer, mock_session, mocker
):
    event = OutboxEvent.create(
        aggregate_type="TRANSACTION",
        aggregate_id="tx-123",
        topic="transactions.created.v1",
        payload={"data": "test"},
    )

    mocker.patch(
        "app.entrypoints.workers.outbox_publisher_worker.MySQLOutboxRepository.find_pending_events",
        return_value=[event],
    )
    save_mock = mocker.patch(
        "app.entrypoints.workers.outbox_publisher_worker.MySQLOutboxRepository.save"
    )

    worker = OutboxPublisherWorker(
        producer=mock_producer, session_factory=lambda: mock_session
    )
    processed = worker.process_batch(mock_session)

    assert processed == 1
    assert event.status == "PUBLISHED"
    assert event.published_at is not None
    mock_producer.produce.assert_called_once()
    mock_producer.flush.assert_called_once()
    save_mock.assert_called_once_with(event)
    mock_session.commit.assert_called_once()


def test_outbox_publisher_worker_handles_producer_failure_with_retry(
    mock_producer, mock_session, mocker
):
    event = OutboxEvent.create(
        aggregate_type="TRANSACTION",
        aggregate_id="tx-123",
        topic="transactions.created.v1",
        payload={"data": "test"},
    )

    mocker.patch(
        "app.entrypoints.workers.outbox_publisher_worker.MySQLOutboxRepository.find_pending_events",
        return_value=[event],
    )
    save_mock = mocker.patch(
        "app.entrypoints.workers.outbox_publisher_worker.MySQLOutboxRepository.save"
    )

    mock_producer.produce.side_effect = KafkaException("Broker indisponível")

    worker = OutboxPublisherWorker(
        producer=mock_producer,
        session_factory=lambda: mock_session,
        max_retries=3,
    )
    processed = worker.process_batch(mock_session)

    assert processed == 0
    assert event.status == "PENDING"
    assert event.retry_count == 1
    save_mock.assert_called_once_with(event)
    mock_session.commit.assert_called_once()
