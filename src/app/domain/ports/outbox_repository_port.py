from abc import ABC, abstractmethod

from app.domain.entities.outbox_event import OutboxEvent


class OutboxRepositoryPort(ABC):
    """Porta de repositório para persistência e recuperação de eventos outbox."""

    @abstractmethod
    def save(self, event: OutboxEvent) -> None:
        """Persiste um evento outbox."""

    @abstractmethod
    def find_pending_events(self, limit: int = 50) -> list[OutboxEvent]:
        """Busca eventos PENDING com lock pessimista seguro (FOR UPDATE SKIP LOCKED)."""

    @abstractmethod
    def find_by_id(self, event_id: str) -> OutboxEvent | None:
        """Busca um evento outbox por ID."""
