from sqlalchemy.orm import Session

from app.domain.entities.outbox_event import OutboxEvent
from app.domain.ports.outbox_repository_port import OutboxRepositoryPort
from app.infrastructure.database.models.outbox_event_model import (
    OutboxEventModel,
)


class MySQLOutboxRepository(OutboxRepositoryPort):
    """Implementação concreta de persistência de outbox no MySQL via SQLAlchemy."""

    def __init__(self, session: Session):
        self._session = session

    def save(self, event: OutboxEvent) -> None:
        model = self._session.query(OutboxEventModel).filter_by(id=event.id).first()
        if model:
            model.status = event.status
            model.retry_count = event.retry_count
            model.published_at = event.published_at
        else:
            model = OutboxEventModel.from_domain(event)
            self._session.add(model)
        self._session.flush()

    def find_pending_events(self, limit: int = 50) -> list[OutboxEvent]:
        """Recupera eventos PENDING usando FOR UPDATE SKIP LOCKED para concorrência segura."""
        models = (
            self._session.query(OutboxEventModel)
            .filter(OutboxEventModel.status == "PENDING")
            .order_by(OutboxEventModel.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
            .all()
        )
        return [m.to_domain() for m in models]

    def find_by_id(self, event_id: str) -> OutboxEvent | None:
        model = self._session.query(OutboxEventModel).filter_by(id=event_id).first()
        if not model:
            return None
        return model.to_domain()
