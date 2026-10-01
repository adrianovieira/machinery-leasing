from datetime import datetime, timezone
import json
from typing import Any
import uuid

from app.domain.exceptions.domain_exceptions import DomainValidationError


class OutboxEvent:
    """Entidade pura que representa um evento pendente ou publicado na Transactional Outbox."""

    VALID_STATUSES = {"PENDING", "PUBLISHED", "FAILED"}

    def __init__(
        self,
        id: str,
        aggregate_type: str,
        aggregate_id: str,
        topic: str,
        payload: dict[str, Any] | str,
        status: str = "PENDING",
        retry_count: int = 0,
        created_at: datetime | None = None,
        published_at: datetime | None = None,
    ):
        if not id:
            raise DomainValidationError(
                "O identificador do evento outbox é obrigatório."
            )
        if not aggregate_type or not aggregate_type.strip():
            raise DomainValidationError("O aggregate_type é obrigatório.")
        if not aggregate_id or not aggregate_id.strip():
            raise DomainValidationError("O aggregate_id é obrigatório.")
        if not topic or not topic.strip():
            raise DomainValidationError("O topic é obrigatório.")
        if status not in self.VALID_STATUSES:
            raise DomainValidationError(
                f"Status inválido: {status}. Status permitidos: {self.VALID_STATUSES}"
            )

        self._id = str(id)
        self._aggregate_type = aggregate_type.strip()
        self._aggregate_id = str(aggregate_id).strip()
        self._topic = topic.strip()
        self._payload = json.loads(payload) if isinstance(payload, str) else payload
        self._status = status
        self._retry_count = max(0, int(retry_count))
        self._created_at = created_at or datetime.now(timezone.utc)
        self._published_at = published_at

    @classmethod
    def create(
        cls,
        aggregate_type: str,
        aggregate_id: str,
        topic: str,
        payload: dict[str, Any],
        event_id: str | None = None,
    ) -> "OutboxEvent":
        """Factory method para criação de novo evento outbox com status PENDING."""
        eid = event_id or str(uuid.uuid4())
        return cls(
            id=eid,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            topic=topic,
            payload=payload,
            status="PENDING",
            retry_count=0,
        )

    # ==========================================
    # Getters
    # ==========================================
    @property
    def id(self) -> str:
        return self._id

    @property
    def aggregate_type(self) -> str:
        return self._aggregate_type

    @property
    def aggregate_id(self) -> str:
        return self._aggregate_id

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def payload(self) -> dict[str, Any]:
        return self._payload

    @property
    def status(self) -> str:
        return self._status

    @property
    def retry_count(self) -> int:
        return self._retry_count

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def published_at(self) -> datetime | None:
        return self._published_at

    # ==========================================
    # Transições de Estado
    # ==========================================
    def mark_as_published(self, published_at: datetime | None = None) -> None:
        """Marca o evento outbox como publicado com sucesso."""
        self._status = "PUBLISHED"
        self._published_at = published_at or datetime.now(timezone.utc)

    def mark_as_failed(self) -> None:
        """Marca o evento outbox como permanentemente falho após esgotar retries."""
        self._status = "FAILED"

    def increment_retry(self, max_retries: int = 3) -> None:
        """Incrementa a contagem de tentativas de envio e marca como FAILED se atingir o limite."""
        self._retry_count += 1
        if self._retry_count >= max_retries:
            self.mark_as_failed()
