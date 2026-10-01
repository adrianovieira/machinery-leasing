from datetime import datetime, timezone
from typing import Any
import uuid

from pydantic import BaseModel, Field


class TransactionCreatedDataPayload(BaseModel):
    transaction_id: str
    customer_id: str
    value: float
    status: str = "PENDING"


class TransactionCreatedEventPayload(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "TRANSACTION_CREATED"
    aggregate_id: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    version: str = "v1"
    data: TransactionCreatedDataPayload

    @classmethod
    def create(
        cls,
        transaction_id: str,
        customer_id: str,
        value: float,
        event_id: str | None = None,
    ) -> "TransactionCreatedEventPayload":
        eid = event_id or str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        return cls(
            event_id=eid,
            event_type="TRANSACTION_CREATED",
            aggregate_id=transaction_id,
            timestamp=now_iso,
            version="v1",
            data=TransactionCreatedDataPayload(
                transaction_id=transaction_id,
                customer_id=customer_id,
                value=value,
                status="PENDING",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class TransactionStatusChangedDataPayload(BaseModel):
    transaction_id: str
    customer_id: str
    old_status: str
    new_status: str
    reason: str | None = None


class TransactionStatusChangedEventPayload(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "TRANSACTION_STATUS_CHANGED"
    aggregate_id: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    version: str = "v1"
    data: TransactionStatusChangedDataPayload

    @classmethod
    def create(
        cls,
        transaction_id: str,
        customer_id: str,
        old_status: str,
        new_status: str,
        reason: str | None = None,
        event_id: str | None = None,
    ) -> "TransactionStatusChangedEventPayload":
        eid = event_id or str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        return cls(
            event_id=eid,
            event_type="TRANSACTION_STATUS_CHANGED",
            aggregate_id=transaction_id,
            timestamp=now_iso,
            version="v1",
            data=TransactionStatusChangedDataPayload(
                transaction_id=transaction_id,
                customer_id=customer_id,
                old_status=old_status,
                new_status=new_status,
                reason=reason,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()
