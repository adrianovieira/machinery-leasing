import json
import logging
from typing import Any
import uuid

from confluent_kafka import Consumer, KafkaError

from app.domain.entities.inbox_event import InboxStatus
from app.domain.ports.message_producer_port import MessageProducerPort
from app.infrastructure.database.session import SessionLocal
from app.infrastructure.messaging.config import KafkaConfig
from app.infrastructure.repositories.mysql_inbox_repository import (
    MySQLInboxRepository,
)


logger = logging.getLogger(__name__)


class DLQMessageInfo:
    """Estrutura com os dados inspecionados de uma mensagem na DLQ."""

    def __init__(
        self,
        key: str,
        payload: dict[str, Any],
        headers: dict[str, str],
        topic: str,
        partition: int,
        offset: int,
    ):
        self.key = key
        self.payload = payload
        self.headers = headers
        self.topic = topic
        self.partition = partition
        self.offset = offset


class DLQManager:
    """Gerenciador para inspeção e reprocessamento (replay) de mensagens da Dead Letter Queue."""

    DLQ_TOPIC = "transactions.dlq.v1"
    DEFAULT_TARGET_TOPIC = "transactions.created.v1"

    def __init__(
        self,
        producer: MessageProducerPort,
        kafka_config: KafkaConfig | None = None,
        session_factory=SessionLocal,
    ):
        self._producer = producer
        self._config = kafka_config or KafkaConfig()
        self._session_factory = session_factory

    def inspect_messages(
        self,
        max_messages: int = 50,
        timeout: float = 2.0,
        consumer_group: str = "dlq-inspector-group",
    ) -> list[DLQMessageInfo]:
        """Lê mensagens da DLQ sem comitar offsets (somente leitura para inspeção)."""
        conf = self._config.to_consumer_dict(group_id=consumer_group)
        conf["enable.auto.commit"] = False
        consumer = Consumer(conf)
        consumer.subscribe([self.DLQ_TOPIC])

        messages: list[DLQMessageInfo] = []
        try:
            for _ in range(max_messages):
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    continue

                key_str = (
                    msg.key().decode("utf-8")
                    if isinstance(msg.key(), bytes)
                    else str(msg.key() or "")
                )
                raw_val = msg.value()
                try:
                    payload = (
                        json.loads(raw_val.decode("utf-8"))
                        if isinstance(raw_val, bytes)
                        else (
                            json.loads(raw_val) if isinstance(raw_val, str) else raw_val
                        )
                    )
                except Exception:
                    payload = {"raw": str(raw_val)}

                headers_list = msg.headers() or []
                headers_dict = {
                    k: (v.decode("utf-8") if isinstance(v, bytes) else str(v))
                    for k, v in headers_list
                    if v is not None
                }

                messages.append(
                    DLQMessageInfo(
                        key=key_str,
                        payload=payload,
                        headers=headers_dict,
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                )
        finally:
            consumer.close()

        return messages

    def replay_message(
        self,
        message: DLQMessageInfo | dict[str, Any],
        target_topic: str | None = None,
        regenerate_event_id: bool = False,
    ) -> bool:
        """Reprocessa uma mensagem da DLQ republicando-a no tópico original ou alvo.

        Limpa headers de erro, zera retry-count e atualiza o Inbox para permitir nova execução.
        """
        dest_topic = target_topic or self.DEFAULT_TARGET_TOPIC

        if isinstance(message, DLQMessageInfo):
            payload = dict(message.payload)
            key = message.key
            orig_headers = dict(message.headers)
        else:
            payload = dict(message.get("payload", message))
            key = str(message.get("key", payload.get("aggregate_id", "")))
            orig_headers = dict(message.get("headers", {}))

        # Event ID
        event_id = orig_headers.get("x-event-id") or payload.get("event_id")
        if regenerate_event_id or not event_id:
            event_id = str(uuid.uuid4())
            payload["event_id"] = event_id

        # Headers limpos para replay
        replay_headers = {
            "x-event-id": event_id,
            "x-retry-count": "0",
            "x-replayed-from": self.DLQ_TOPIC,
        }

        # Atualiza o status no Inbox (se reusar event_id) para permitir reexecução
        if not regenerate_event_id:
            with self._session_factory() as session:
                inbox_repo = MySQLInboxRepository(session)
                inbox_entry = inbox_repo.get_by_id(event_id, session=session)
                if inbox_entry:
                    inbox_repo.update_status(
                        event_id=event_id,
                        status=InboxStatus.PROCESSING,
                        error_message="Replay iniciado a partir da DLQ",
                        session=session,
                    )
                    session.commit()

        # Publica no tópico alvo
        self._producer.produce(
            topic=dest_topic,
            key=key,
            value=payload,
            headers=replay_headers,
        )
        self._producer.flush(timeout=5.0)
        logger.info(
            "Mensagem %s reenviada com sucesso para %s a partir da DLQ.",
            key,
            dest_topic,
        )
        return True
