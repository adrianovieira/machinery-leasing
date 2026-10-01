from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    String,
    Text,
    func,
)
from sqlalchemy import (
    Enum as SQLEnum,
)

from app.domain.entities.inbox_event import InboxEvent, InboxStatus
from app.infrastructure.database.models.transaction_model import Base


class InboxEventModel(Base):
    """Mapeamento ORM SQLAlchemy da tabela 'inbox_events' para barreira de idempotência."""

    __tablename__ = "inbox_events"

    event_id = Column(String(64), primary_key=True, nullable=False)
    consumer_group = Column(String(64), nullable=False)
    status = Column(
        SQLEnum(
            "PROCESSING", "COMPLETED", "RETRYING", "FAILED", name="inbox_status_enum"
        ),
        nullable=False,
        default="PROCESSING",
        server_default="PROCESSING",
    )
    received_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        default=datetime.utcnow,
    )
    processed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)

    __table_args__ = (
        Index("idx_inbox_status", "status"),
        Index("idx_inbox_consumer_group", "consumer_group"),
    )

    def to_domain(self) -> InboxEvent:
        """Converte o modelo ORM para a Entidade Pura de Domínio."""
        return InboxEvent(
            event_id=self.event_id,
            consumer_group=self.consumer_group,
            status=InboxStatus(self.status),
            received_at=self.received_at,
            processed_at=self.processed_at,
            error_message=self.error_message,
        )

    @classmethod
    def from_domain(cls, event: InboxEvent) -> "InboxEventModel":
        """Converte a Entidade Pura de Domínio para o modelo ORM SQLAlchemy."""
        return cls(
            event_id=event.event_id,
            consumer_group=event.consumer_group,
            status=event.status.value
            if isinstance(event.status, InboxStatus)
            else event.status,
            received_at=event.received_at,
            processed_at=event.processed_at,
            error_message=event.error_message,
        )
