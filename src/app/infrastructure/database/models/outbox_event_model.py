from datetime import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy import (
    Enum as SQLEnum,
)

from app.domain.entities.outbox_event import OutboxEvent
from app.infrastructure.database.models.transaction_model import Base


class OutboxEventModel(Base):
    """Mapeamento ORM SQLAlchemy da tabela 'outbox_events'."""

    __tablename__ = "outbox_events"

    id = Column(String(36), primary_key=True, nullable=False)
    aggregate_type = Column(String(64), nullable=False)
    aggregate_id = Column(String(36), nullable=False)
    topic = Column(String(128), nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(
        SQLEnum("PENDING", "PUBLISHED", "FAILED", name="outbox_status_enum"),
        nullable=False,
        default="PENDING",
        server_default="PENDING",
    )
    retry_count = Column(Integer, nullable=False, default=0, server_default="0")
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        default=datetime.utcnow,
    )
    published_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_outbox_status_created", "status", "created_at"),
        Index("idx_outbox_aggregate", "aggregate_id"),
    )

    def to_domain(self) -> OutboxEvent:
        """Converte o modelo ORM para a Entidade Pura de Domínio."""
        return OutboxEvent(
            id=self.id,
            aggregate_type=self.aggregate_type,
            aggregate_id=self.aggregate_id,
            topic=self.topic,
            payload=self.payload,
            status=self.status,
            retry_count=self.retry_count,
            created_at=self.created_at,
            published_at=self.published_at,
        )

    @classmethod
    def from_domain(cls, event: OutboxEvent) -> "OutboxEventModel":
        """Converte a Entidade Pura de Domínio para o modelo ORM SQLAlchemy."""
        return cls(
            id=event.id,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            topic=event.topic,
            payload=event.payload,
            status=event.status,
            retry_count=event.retry_count,
            created_at=event.created_at,
            published_at=event.published_at,
        )
