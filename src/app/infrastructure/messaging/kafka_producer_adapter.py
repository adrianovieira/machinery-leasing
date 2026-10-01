import json
import logging
from typing import Any

from confluent_kafka import KafkaException, Producer

from app.domain.ports.message_producer_port import MessageProducerPort
from app.infrastructure.messaging.config import KafkaConfig


logger = logging.getLogger(__name__)


class KafkaProducerAdapter(MessageProducerPort):
    """Adaptador concreto do Kafka Producer com acks=all e suporte a headers."""

    def __init__(
        self, config: KafkaConfig | None = None, producer: Producer | None = None
    ):
        self._config = config or KafkaConfig()
        self._producer = producer or Producer(self._config.to_producer_dict())

    def produce(
        self,
        topic: str,
        key: str,
        value: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        """Publica mensagem com key, payload JSON e headers opcionais."""
        delivery_error = []

        def delivery_callback(err, msg):
            if err:
                delivery_error.append(err)
                logger.error(
                    f"Falha na entrega Kafka no tópico {msg.topic()} [{msg.partition()}]: {err}"
                )

        serialized_key = key.encode("utf-8") if isinstance(key, str) else key
        serialized_value = (
            json.dumps(value).encode("utf-8") if isinstance(value, dict) else value
        )

        formatted_headers = None
        if headers:
            formatted_headers = [
                (k, v.encode("utf-8") if isinstance(v, str) else v)
                for k, v in headers.items()
            ]

        try:
            self._producer.produce(
                topic=topic,
                key=serialized_key,
                value=serialized_value,
                headers=formatted_headers,
                on_delivery=delivery_callback,
            )
            # Aciona callbacks pendentes
            self._producer.poll(0)
        except Exception as exc:
            logger.error(f"Erro ao disparar mensagem para Kafka: {exc}")
            raise KafkaException(str(exc)) from exc

    def flush(self, timeout: float = 5.0) -> int:
        """Aguarda confirmação de entrega do broker."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            raise KafkaException(
                f"Timeout ao dar flush no Kafka. Mensagens pendentes: {remaining}"
            )
        return remaining
