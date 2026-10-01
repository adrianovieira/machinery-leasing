from datetime import datetime, timezone

import pytest

from app.domain.entities.outbox_event import OutboxEvent
from app.domain.exceptions.domain_exceptions import DomainValidationError


def test_create_outbox_event_success():
    payload = {
        "event_id": "e1-uuid",
        "event_type": "TRANSACTION_CREATED",
        "aggregate_id": "tx-123",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "v1",
        "data": {
            "transaction_id": "tx-123",
            "customer_id": "cust-99",
            "value": 1500.0,
            "status": "PENDING",
        },
    }

    event = OutboxEvent.create(
        aggregate_type="TRANSACTION",
        aggregate_id="tx-123",
        topic="transactions.created.v1",
        payload=payload,
        event_id="e1-uuid",
    )

    assert event.id == "e1-uuid"
    assert event.aggregate_type == "TRANSACTION"
    assert event.aggregate_id == "tx-123"
    assert event.topic == "transactions.created.v1"
    assert event.payload == payload
    assert event.status == "PENDING"
    assert event.retry_count == 0
    assert event.published_at is None
    assert event.created_at is not None


def test_outbox_event_mark_as_published():
    event = OutboxEvent.create(
        aggregate_type="TRANSACTION",
        aggregate_id="tx-123",
        topic="transactions.created.v1",
        payload={"foo": "bar"},
    )
    now = datetime.now(timezone.utc)
    event.mark_as_published(published_at=now)

    assert event.status == "PUBLISHED"
    assert event.published_at == now


def test_outbox_event_increment_retries_and_failure():
    event = OutboxEvent.create(
        aggregate_type="TRANSACTION",
        aggregate_id="tx-123",
        topic="transactions.created.v1",
        payload={"foo": "bar"},
    )

    event.increment_retry(max_retries=3)
    assert event.retry_count == 1
    assert event.status == "PENDING"

    event.increment_retry(max_retries=3)
    assert event.retry_count == 2
    assert event.status == "PENDING"

    event.increment_retry(max_retries=3)
    assert event.retry_count == 3
    assert event.status == "FAILED"


def test_outbox_event_validations():
    with pytest.raises(DomainValidationError):
        OutboxEvent(
            id="",
            aggregate_type="TRANSACTION",
            aggregate_id="123",
            topic="t1",
            payload={},
        )

    with pytest.raises(DomainValidationError):
        OutboxEvent(
            id="1",
            aggregate_type="",
            aggregate_id="123",
            topic="t1",
            payload={},
        )

    with pytest.raises(DomainValidationError):
        OutboxEvent(
            id="1",
            aggregate_type="TRANSACTION",
            aggregate_id="",
            topic="t1",
            payload={},
        )

    with pytest.raises(DomainValidationError):
        OutboxEvent(
            id="1",
            aggregate_type="TRANSACTION",
            aggregate_id="123",
            topic="",
            payload={},
        )

    with pytest.raises(DomainValidationError):
        OutboxEvent(
            id="1",
            aggregate_type="TRANSACTION",
            aggregate_id="123",
            topic="t1",
            payload={},
            status="INVALID_STATUS",
        )
