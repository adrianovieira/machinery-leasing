from datetime import datetime

from app.domain.entities.inbox_event import InboxEvent, InboxStatus


def test_inbox_event_creation():
    event = InboxEvent(
        event_id="evt-123",
        consumer_group="tx-consumer",
    )
    assert event.event_id == "evt-123"
    assert event.consumer_group == "tx-consumer"
    assert event.status == InboxStatus.PROCESSING
    assert isinstance(event.received_at, datetime)
    assert event.processed_at is None
    assert event.error_message is None


def test_inbox_event_status_transition():
    event = InboxEvent(
        event_id="evt-456",
        consumer_group="tx-consumer",
        status=InboxStatus.PROCESSING,
    )
    event.status = InboxStatus.COMPLETED
    event.processed_at = datetime.utcnow()
    assert event.status == "COMPLETED"
    assert event.processed_at is not None
