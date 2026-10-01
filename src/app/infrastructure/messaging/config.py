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
