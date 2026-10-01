from abc import ABC, abstractmethod

from sqlalchemy.orm import Session

from app.domain.entities.inbox_event import InboxEvent, InboxStatus


class InboxRepositoryPort(ABC):
    """Porta para barreira de idempotência e persistência do Inbox Pattern."""

    @abstractmethod
    def try_acquire(
        self, event_id: str, consumer_group: str, session: Session | None = None
    ) -> bool:
        """Tenta adquirir a chave de idempotência inserindo um registro PROCESSING.
        Retorna True se inserido com sucesso (primeira execução).
        Retorna False se o event_id já existir (mensagem duplicada).
        """
        pass

    @abstractmethod
    def get_by_id(
        self, event_id: str, session: Session | None = None
    ) -> InboxEvent | None:
        """Busca o registro do inbox pelo event_id."""
        pass

    @abstractmethod
    def update_status(
        self,
        event_id: str,
        status: InboxStatus,
        error_message: str | None = None,
        session: Session | None = None,
    ) -> None:
        """Atualiza o status do processamento do inbox (COMPLETED, RETRYING, FAILED)."""
        pass
