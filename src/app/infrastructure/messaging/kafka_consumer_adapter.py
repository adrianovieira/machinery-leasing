import logging

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from app.infrastructure.messaging.config import KafkaConfig


logger = logging.getLogger(__name__)


class KafkaConsumerAdapter:
    """Adaptador concreto do Kafka Consumer com desativação de auto-commit (enable.auto.commit=False)."""

    def __init__(
        self,
        topics: list[str],
        config: KafkaConfig | None = None,
        consumer: Consumer | None = None,
        group_id: str | None = None,
    ):
        self._config = config or KafkaConfig()
        self._topics = topics
        self._consumer = consumer or Consumer(
            self._config.to_consumer_dict(group_id=group_id)
        )
        self._consumer.subscribe(self._topics)

    def poll(self, timeout: float = 1.0) -> Message | None:
        """Realiza poll de mensagem única do Kafka."""
        try:
            msg = self._consumer.poll(timeout)
            if msg is None:
                return None
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return None
                logger.error("Erro no consumo do Kafka: %s", msg.error())
                raise KafkaException(msg.error())
            return msg
        except Exception as exc:
            logger.error("Exceção durante poll no Kafka: %s", exc)
            raise

    def commit(self, message: Message, asynchronous: bool = False) -> None:
        """Confirma manualmente o offset da mensagem processada com sucesso no MySQL."""
        self._consumer.commit(message=message, asynchronous=asynchronous)

    def close(self) -> None:
        """Fecha a conexão do consumidor liberando partições."""
        self._consumer.close()
