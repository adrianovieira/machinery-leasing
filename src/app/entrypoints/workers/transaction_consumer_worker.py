import json
import logging
import time

from confluent_kafka import Message

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
from app.infrastructure.repositories.mysql_inbox_repository import (
    MySQLInboxRepository,
)
from app.infrastructure.repositories.mysql_transaction_repository import (
    MySQLTransactionRepository,
)


logger = logging.getLogger(__name__)


class TransactionConsumerWorker:
    """Consumidor idempotente de transações no Kafka utilizando o Inbox Pattern,

    com resiliência de retries em fila separada e isolamento em DLQ.
    """

    DEFAULT_CONSUMER_GROUP = "machinery-leasing-consumer-group"
    RETRY_TOPIC = "transactions.retry.v1"
    DLQ_TOPIC = "transactions.dlq.v1"

    def __init__(
        self,
        consumer: KafkaConsumerAdapter,
        producer: MessageProducerPort,
        process_risk_use_case: ProcessRiskAnalysisUseCase,
        session_factory=SessionLocal,
        consumer_group: str = DEFAULT_CONSUMER_GROUP,
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
        """Processa uma única mensagem respeitando idempotência e ordem de commits.

        Retorna True se processado ou ignorado com sucesso e offset commitado.
        """
        headers = self._extract_headers(message)

        try:
            raw_value = message.value()
            if isinstance(raw_value, bytes):
                payload = json.loads(raw_value.decode("utf-8"))
            elif isinstance(raw_value, str):
                payload = json.loads(raw_value)
            else:
                payload = raw_value
        except Exception as exc:
            logger.error("Erro ao desserializar JSON da mensagem: %s", exc)
            self._handle_poison_pill(message, exc, headers)
            return True

        event_id = headers.get("x-event-id") or payload.get("event_id")
        if not event_id:
            logger.error("Mensagem sem 'x-event-id'. Encaminhando para DLQ.")
            self._handle_poison_pill(message, ValueError("Missing x-event-id"), headers)
            return True

        transaction_id = payload.get("aggregate_id") or payload.get("data", {}).get(
            "transaction_id"
        )
        retry_count = int(headers.get("x-retry-count", "0"))

        # 1. Barreira de Idempotência do Inbox
        with self._session_factory() as session:
            inbox_repo = MySQLInboxRepository(session)
            acquired = inbox_repo.try_acquire(
                event_id=event_id, consumer_group=self._consumer_group
            )
            session.commit()

        if not acquired:
            logger.info(
                "Mensagem duplicada detectada: %s. Ignorando reexecução.", event_id
            )
            self._consumer.commit(message=message, asynchronous=False)
            return True

        # 2. Execução da Regra de Negócio e Máquina de Estados
        try:
            self._use_case.execute(transaction_id=transaction_id, event_id=event_id)
            # Confirma offset Kafka estritamente APÓS sucesso no banco
            self._consumer.commit(message=message, asynchronous=False)
            return True

        except TransientRiskServiceException as exc:
            logger.warning(
                "Falha transitória na análise de risco da transação %s (tentativa %s): %s",
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
            return True

        except (PermanentRiskServiceException, DomainError, Exception) as exc:
            logger.error(
                "Falha definitiva ou não recuperável na transação %s: %s",
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
            return True

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
        """Encaminha para fila de retry ou DLQ se exceder max_retries."""
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
                "Transação %s enviada para tópico de retry %s (tentativa %s).",
                transaction_id,
                self.RETRY_TOPIC,
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
                reason="Limite máximo de retries excedido",
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
        """Marca como FAILED no DB e envia para DLQ."""
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
            "Transação %s descartada e encaminhada para DLQ %s: %s",
            transaction_id,
            self.DLQ_TOPIC,
            err_msg,
        )

    def _handle_poison_pill(
        self, message: Message, exc: Exception, headers: dict
    ) -> None:
        """Trata poison pills que não puderam ser decodificadas."""
        dlq_headers = dict(headers)
        dlq_headers["x-exception-type"] = exc.__class__.__name__
        dlq_headers["x-exception-message"] = str(exc)

        key = message.key()
        key_str = (
            key.decode("utf-8")
            if isinstance(key, bytes)
            else (str(key) if key else "unknown")
        )

        self._producer.produce(
            topic=self.DLQ_TOPIC,
            key=key_str,
            value={"raw": str(message.value())},
            headers=dlq_headers,
        )
        self._producer.flush(timeout=5.0)
        self._consumer.commit(message=message, asynchronous=False)

    def run_once(self, timeout: float = 1.0) -> bool:
        """Executa um ciclo único de poll e processamento."""
        msg = self._consumer.poll(timeout=timeout)
        if msg is None:
            return False
        return self.process_message(msg)

    def start(self) -> None:
        """Inicia o loop contínuo do consumidor (daemon)."""
        self._running = True
        logger.info(
            "TransactionConsumerWorker iniciado para grupo %s.", self._consumer_group
        )
        while self._running:
            try:
                processed = self.run_once(timeout=1.0)
                if not processed:
                    time.sleep(0.1)
            except Exception as exc:
                logger.error(
                    "Erro inesperado no loop do TransactionConsumerWorker: %s", exc
                )
                time.sleep(1.0)

    def stop(self) -> None:
        """Finaliza o loop do consumidor."""
        self._running = False
        self._consumer.close()
        logger.info("TransactionConsumerWorker finalizado.")
