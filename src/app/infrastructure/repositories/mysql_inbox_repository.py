from datetime import datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.domain.entities.inbox_event import InboxEvent, InboxStatus
from app.domain.ports.inbox_repository_port import InboxRepositoryPort
from app.infrastructure.database.models.inbox_event_model import (
    InboxEventModel,
)


class MySQLInboxRepository(InboxRepositoryPort):
    """Implementação concreta de persistência e barreira de Inbox no MySQL via SQLAlchemy."""

    def __init__(self, session: Session):
        self._session = session

    def try_acquire(
        self, event_id: str, consumer_group: str, session: Session | None = None
    ) -> bool:
        """Tenta registrar a mensagem no inbox com status PROCESSING.
        Usa savepoint aninhado para capturar IntegrityError caso o event_id já exista.
        """
        s = session or self._session
        try:
            with s.begin_nested():
                model = InboxEventModel(
                    event_id=event_id,
                    consumer_group=consumer_group,
                    status="PROCESSING",
                    received_at=datetime.utcnow(),
                )
                s.add(model)
                s.flush()
            return True
        except IntegrityError:
            return False

    def get_by_id(
        self, event_id: str, session: Session | None = None
    ) -> InboxEvent | None:
        s = session or self._session
        model = s.query(InboxEventModel).filter_by(event_id=event_id).first()
        if not model:
            return None
        return model.to_domain()

    def update_status(
        self,
        event_id: str,
        status: InboxStatus,
        error_message: str | None = None,
        session: Session | None = None,
    ) -> None:
        s = session or self._session
        model = s.query(InboxEventModel).filter_by(event_id=event_id).first()
        if model:
            model.status = status.value if isinstance(status, InboxStatus) else status
            model.error_message = error_message
            if status == InboxStatus.COMPLETED:
                model.processed_at = datetime.utcnow()
            s.flush()
