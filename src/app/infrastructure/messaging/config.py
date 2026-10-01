import os

from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    """Configurações de conexão e resiliência com Apache Kafka."""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    client_id: str = os.getenv("KAFKA_CLIENT_ID", "machinery-leasing-service")
    acks: str = os.getenv("KAFKA_ACKS", "all")
    retries: int = int(os.getenv("KAFKA_RETRIES", "5"))
    enable_idempotence: bool = (
        os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() == "true"
    )
    delivery_timeout_ms: int = int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "15000"))

    group_id: str = os.getenv("KAFKA_GROUP_ID", "machinery-leasing-consumer-group")
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    enable_auto_commit: bool = False

    def to_producer_dict(self) -> dict[str, str | int | bool]:
        """Gera dicionário de configuração no formato do confluent-kafka."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "retries": self.retries,
            "enable.idempotence": self.enable_idempotence,
            "delivery.timeout.ms": self.delivery_timeout_ms,
        }

    def to_consumer_dict(
        self, group_id: str | None = None
    ) -> dict[str, str | int | bool]:
        """Gera dicionário de configuração para o confluent-kafka Consumer garantindo commit manual."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id or self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
        }
