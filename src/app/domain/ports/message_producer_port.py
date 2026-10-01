from abc import ABC, abstractmethod
from typing import Any


class MessageProducerPort(ABC):
    """Porta abstrata para publicação de mensagens no broker de eventos."""

    @abstractmethod
    def produce(
        self,
        topic: str,
        key: str,
        value: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        """Publica uma mensagem no tópico especificado com confirmação estrita."""

    @abstractmethod
    def flush(self, timeout: float = 5.0) -> int:
        """Aguarda a confirmação de entrega de todas as mensagens pendentes."""
