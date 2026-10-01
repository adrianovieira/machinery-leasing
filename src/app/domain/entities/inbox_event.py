from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class InboxStatus(str, Enum):
    """Status possíveis de uma mensagem na barreira de Inbox."""

    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"


class InboxEvent(BaseModel):
    """Entidade pura de domínio representando o registro de idempotência no Inbox."""

    model_config = ConfigDict(use_enum_values=True)

    event_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="ID único original da mensagem/evento",
    )
    consumer_group: str = Field(
        ..., min_length=1, max_length=64, description="Grupo do consumidor responsável"
    )
    status: InboxStatus = Field(
        default=InboxStatus.PROCESSING,
        description="Status do processamento da mensagem",
    )
    received_at: datetime = Field(
        default_factory=datetime.utcnow, description="Data/hora de recebimento"
    )
    processed_at: datetime | None = Field(
        default=None, description="Data/hora da conclusão do processamento"
    )
    error_message: str | None = Field(
        default=None, description="Mensagem de erro detalhada em caso de falha"
    )
