from datetime import datetime
import json
import logging
import time

from confluent_kafka import Message
from opentelemetry.trace import SpanKind

from app.application.use_cases.process_risk_analysis_use_case import (
    ProcessRiskAnalysisUseCase,
)
from app.domain.entities.inbox_event import InboxStatus
from app.domain.exceptions.domain_exceptions import DomainError
from app.domain.ports.message_producer_port import MessageProducerPort
from app.infrastructure.database.session import SessionLocal
from app.infrastructure.external_services.http_risk_client_adapter import (
    PermanentRiskServiceException,
    TransientRiskServiceException,
)
from app.infrastructure.messaging.kafka_consumer_adapter import (
    KafkaConsumerAdapter,
)
from app.infrastructure.messaging.retry_router import RetryBackoffCalculator
from app.infrastructure.observability.logging import (
    clear_log_context,
    set_log_context,
)
from app.infrastructure.observability.metrics import (
    CONSUMER_PROCESSING_DURATION_SECONDS,
    KAFKA_MESSAGES_CONSUMED_TOTAL,
)
from app.infrastructure.observability.telemetry import (
    extract_trace_context,
    get_tracer,
)
from app.infrastructure.repositories.mysql_inbox_repository import (
    MySQLInboxRepository,
)
from app.infrastructure.repositories.mysql_transaction_repository import (
    MySQLTransactionRepository,
)


logger = logging.getLogger(__name__)
tracer = get_tracer()


class RetryConsumerWorker:
    """Worker dedicado para processamento assíncrono do tópico de retries (transactions.retry.v1).

    Respeita estritamente o timestamp agendado (x-next-retry-timestamp) antes de reprocessar.
    """

    DEFAULT_RETRY_GROUP = "machinery-leasing-retry-group"
    RETRY_TOPIC = "transactions.retry.v1"
    DLQ_TOPIC = "transactions.dlq.v1"

    def __init__(
        self,
        consumer: KafkaConsumerAdapter,
        producer: MessageProducerPort,
        process_risk_use_case: ProcessRiskAnalysisUseCase,
        session_factory=SessionLocal,
        consumer_group: str = DEFAULT_RETRY_GROUP,
        max_retries: int = 3,
        backoff_calculator: RetryBackoffCalculator | None = None,
    ):
        self._consumer = consumer
        self._producer = producer
        self._use_case = process_risk_use_case
        self._session_factory = session_factory
        self._consumer_group = consumer_group
        self._max_retries = max_retries
        self._backoff = backoff_calculator or RetryBackoffCalculator()
        self._running = False

    def _extract_headers(self, message: Message) -> dict[str, str]:
        headers_list = message.headers() or []
        headers_dict = {}
        for k, v in headers_list:
            if v is not None:
                headers_dict[k] = v.decode("utf-8") if isinstance(v, bytes) else str(v)
        return headers_dict

    def process_message(self, message: Message) -> bool:
        """Processa a mensagem respeitando o delay do timestamp agendado."""
        start_time = time.time()
        headers = self._extract_headers(message)
        parent_ctx = extract_trace_context(headers)

        with tracer.start_as_current_span(
            "kafka.consume.transactions.retry.v1",
            context=parent_ctx,
            kind=SpanKind.CONSUMER,
        ):
            try:
                raw_value = message.value()
                if isinstance(raw_value, bytes):
                    payload = json.loads(raw_value.decode("utf-8"))
                elif isinstance(raw_value, str):
                    payload = json.loads(raw_value)
                else:
                    payload = raw_value
            except Exception as exc:
                logger.error("Erro ao desserializar JSON na fila de retry: %s", exc)
                KAFKA_MESSAGES_CONSUMED_TOTAL.labels(
                    topic="transactions.retry.v1", status="failed"
                ).inc()
                self._handle_permanent_failure(
                    message=message,
                    exc=exc,
                    transaction_id="unknown",
                    event_id=headers.get("x-event-id", "unknown"),
                    payload={"raw": str(message.value())},
                    retry_count=int(headers.get("x-retry-count", "0")),
                    headers=headers,
                    reason="Poison pill no tópico de retry",
                )
                return True

            event_id = headers.get("x-event-id") or payload.get("event_id")
            transaction_id = payload.get("aggregate_id") or payload.get("data", {}).get(
                "transaction_id"
            )
            retry_count = int(headers.get("x-retry-count", "1"))
            scheduled_retry_str = headers.get("x-next-retry-timestamp")

            set_log_context(
                event_id=event_id,
                transaction_id=transaction_id,
                retry_count=retry_count,
            )

            # 1. Verificação Temporal: aguarda timestamp agendado
            if scheduled_retry_str:
                try:
                    scheduled_time = datetime.fromisoformat(scheduled_retry_str)
                    now = datetime.utcnow()
                    if now < scheduled_time:
                        wait_seconds = (scheduled_time - now).total_seconds()
                        if wait_seconds > 0:
                            logger.info(
                                "Mensagem de retry da transação %s aguardando %0.2fs até %s",
                                transaction_id,
                                wait_seconds,
                                scheduled_retry_str,
                            )
                            time.sleep(min(wait_seconds, 60.0))
                except Exception as exc:
                    logger.warning("Falha ao analisar x-next-retry-timestamp: %s", exc)

            # 2. Execução da análise de risco
            try:
                self._use_case.execute(transaction_id=transaction_id, event_id=event_id)
                self._consumer.commit(message=message, asynchronous=False)
                logger.info(
                    "Transação %s reprocessada com sucesso na fila de retry.",
                    transaction_id,
                )
                KAFKA_MESSAGES_CONSUMED_TOTAL.labels(
                    topic="transactions.retry.v1", status="processed"
                ).inc()
                CONSUMER_PROCESSING_DURATION_SECONDS.labels(
                    topic="transactions.retry.v1"
                ).observe(time.time() - start_time)
                return True

            except TransientRiskServiceException as exc:
                logger.warning(
                    "Falha transitória persistente no retry da transação %s (tentativa %s): %s",
                    transaction_id,
                    retry_count,
                    exc,
                )
                self._handle_retry_or_dlq(
                    message=message,
                    exc=exc,
                    transaction_id=transaction_id,
                    event_id=event_id,
                    payload=payload,
                    retry_count=retry_count,
                    headers=headers,
                )
                KAFKA_MESSAGES_CONSUMED_TOTAL.labels(
                    topic="transactions.retry.v1",
                    status="retried" if retry_count < self._max_retries else "dlq",
                ).inc()
                return True

            except (PermanentRiskServiceException, DomainError, Exception) as exc:
                logger.error(
                    "Falha irrecuperável no retry da transação %s: %s",
                    transaction_id,
                    exc,
                )
                self._handle_permanent_failure(
                    message=message,
                    exc=exc,
                    transaction_id=transaction_id,
                    event_id=event_id,
                    payload=payload,
                    retry_count=retry_count,
                    headers=headers,
                )
                KAFKA_MESSAGES_CONSUMED_TOTAL.labels(
                    topic="transactions.retry.v1", status="dlq"
                ).inc()
                return True

            finally:
                clear_log_context()

    def _handle_retry_or_dlq(
        self,
        message: Message,
        exc: Exception,
        transaction_id: str,
        event_id: str,
        payload: dict,
        retry_count: int,
        headers: dict,
    ) -> None:
        if retry_count < self._max_retries:
            next_retry = retry_count + 1
            next_retry_time = self._backoff.calculate_next_retry_timestamp(next_retry)

            with self._session_factory() as session:
                tx_repo = MySQLTransactionRepository(session)
                inbox_repo = MySQLInboxRepository(session)
                tx = tx_repo.find_by_id(transaction_id)
                if tx:
                    tx.mark_as_retrying(reason=str(exc))
                    tx_repo.save(tx)
                inbox_repo.update_status(
                    event_id=event_id,
                    status=InboxStatus.RETRYING,
                    error_message=str(exc),
                    session=session,
                )
                session.commit()

            retry_headers = dict(headers)
            retry_headers["x-retry-count"] = str(next_retry)
            retry_headers["x-next-retry-timestamp"] = next_retry_time.isoformat()
            retry_headers["x-event-id"] = event_id

            self._producer.produce(
                topic=self.RETRY_TOPIC,
                key=transaction_id,
                value=payload,
                headers=retry_headers,
            )
            self._producer.flush(timeout=5.0)
            self._consumer.commit(message=message, asynchronous=False)
            logger.info(
                "Transação %s reenviada para tópico de retry (tentativa %s).",
                transaction_id,
                next_retry,
            )
        else:
            self._handle_permanent_failure(
                message=message,
                exc=exc,
                transaction_id=transaction_id,
                event_id=event_id,
                payload=payload,
                retry_count=retry_count,
                headers=headers,
                reason="Limite máximo de retries excedido no RetryConsumer",
            )

    def _handle_permanent_failure(
        self,
        message: Message,
        exc: Exception,
        transaction_id: str,
        event_id: str,
        payload: dict,
        retry_count: int,
        headers: dict,
        reason: str | None = None,
    ) -> None:
        err_msg = reason or str(exc)
        with self._session_factory() as session:
            tx_repo = MySQLTransactionRepository(session)
            inbox_repo = MySQLInboxRepository(session)
            tx = tx_repo.find_by_id(transaction_id)
            if tx:
                try:
                    tx.mark_as_failed(reason=err_msg)
                    tx_repo.save(tx)
                except Exception:
                    pass
            inbox_repo.update_status(
                event_id=event_id,
                status=InboxStatus.FAILED,
                error_message=err_msg,
                session=session,
            )
            session.commit()

        dlq_headers = dict(headers)
        dlq_headers["x-retry-count"] = str(retry_count)
        dlq_headers["x-exception-type"] = exc.__class__.__name__
        dlq_headers["x-exception-message"] = str(exc)
        dlq_headers["x-event-id"] = event_id

        self._producer.produce(
            topic=self.DLQ_TOPIC,
            key=transaction_id,
            value=payload,
            headers=dlq_headers,
        )
        self._producer.flush(timeout=5.0)
        self._consumer.commit(message=message, asynchronous=False)
        logger.error(
            "Transação %s enviada para DLQ a partir da fila de retry: %s",
            transaction_id,
            err_msg,
        )

    def run_once(self, timeout: float = 1.0) -> bool:
        msg = self._consumer.poll(timeout=timeout)
        if msg is None:
            return False
        return self.process_message(msg)

    def start(self) -> None:
        self._running = True
        logger.info("RetryConsumerWorker iniciado para grupo %s.", self._consumer_group)
        while self._running:
            try:
                processed = self.run_once(timeout=1.0)
                if not processed:
                    time.sleep(0.1)
            except Exception as exc:
                logger.error("Erro inesperado no loop do RetryConsumerWorker: %s", exc)
                time.sleep(1.0)

    def stop(self) -> None:
        self._running = False
        self._consumer.close()
        logger.info("RetryConsumerWorker finalizado.")


def main():
    """Ponto de entrada para execução do worker consumidor de retries."""
    import os

    from app.application.use_cases.process_risk_analysis_use_case import (
        ProcessRiskAnalysisUseCase,
    )
    from app.infrastructure.external_services.http_risk_client_adapter import (
        HttpRiskClientAdapter,
    )
    from app.infrastructure.messaging.kafka_consumer_adapter import (
        KafkaConsumerAdapter,
    )
    from app.infrastructure.messaging.kafka_producer_adapter import (
        KafkaProducerAdapter,
    )
    from app.infrastructure.observability.logging import configure_logging
    from app.infrastructure.repositories.mysql_inbox_repository import (
        MySQLInboxRepository,
    )
    from app.infrastructure.repositories.mysql_outbox_repository import (
        MySQLOutboxRepository,
    )
    from app.infrastructure.repositories.mysql_transaction_repository import (
        MySQLTransactionRepository,
    )

    configure_logging()

    topic = os.getenv("KAFKA_TOPIC_RETRY", "transactions.retry.v1")
    group_id = os.getenv("KAFKA_GROUP_ID", RetryConsumerWorker.DEFAULT_RETRY_GROUP)

    consumer = KafkaConsumerAdapter(topics=[topic], group_id=group_id)
    producer = KafkaProducerAdapter()
    risk_client = HttpRiskClientAdapter()

    use_case = ProcessRiskAnalysisUseCase(
        transaction_repository=MySQLTransactionRepository(None),
        outbox_repository=MySQLOutboxRepository(None),
        inbox_repository=MySQLInboxRepository(None),
        risk_client=risk_client,
        session_factory=SessionLocal,
    )

    worker = RetryConsumerWorker(
        consumer=consumer,
        producer=producer,
        process_risk_use_case=use_case,
        consumer_group=group_id,
    )
    worker.start()


if __name__ == "__main__":
    main()
