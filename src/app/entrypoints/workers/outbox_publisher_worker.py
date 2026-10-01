from datetime import datetime, timezone
import logging
import time

from sqlalchemy.orm import Session

from app.domain.ports.message_producer_port import MessageProducerPort
from app.domain.ports.outbox_repository_port import OutboxRepositoryPort
from app.infrastructure.database.session import SessionLocal
from app.infrastructure.repositories.mysql_outbox_repository import (
    MySQLOutboxRepository,
)


logger = logging.getLogger(__name__)


class OutboxPublisherWorker:
    """Daemon Worker para processamento e publicação resiliente de eventos outbox no Kafka."""

    def __init__(
        self,
        producer: MessageProducerPort,
        session_factory=SessionLocal,
        batch_size: int = 50,
        poll_interval_seconds: float = 1.0,
        max_retries: int = 3,
    ):
        self._producer = producer
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds
        self._max_retries = max_retries
        self._running = False

    def process_batch(self, session: Session) -> int:
        """Processa um lote de eventos PENDING em uma sessão do banco com FOR UPDATE SKIP LOCKED.

        Retorna o número de eventos processados com sucesso.
        """
        outbox_repo: OutboxRepositoryPort = MySQLOutboxRepository(session)
        pending_events = outbox_repo.find_pending_events(limit=self._batch_size)

        if not pending_events:
            return 0

        processed_count = 0
        for event in pending_events:
            try:
                headers = {
                    "x-event-id": event.id,
                    "x-aggregate-id": event.aggregate_id,
                    "x-timestamp": event.created_at.isoformat(),
                }
                self._producer.produce(
                    topic=event.topic,
                    key=event.aggregate_id,
                    value=event.payload,
                    headers=headers,
                )
                self._producer.flush(timeout=5.0)

                event.mark_as_published(published_at=datetime.now(timezone.utc))
                outbox_repo.save(event)
                processed_count += 1
                logger.info(
                    f"Evento outbox {event.id} publicado com sucesso no tópico {event.topic}."
                )
            except Exception as exc:
                logger.error(f"Erro ao publicar evento outbox {event.id}: {exc}")
                event.increment_retry(max_retries=self._max_retries)
                outbox_repo.save(event)
                # Interrompe o lote para evitar falhas em cascata no mesmo ciclo
                break

        session.commit()
        return processed_count

    def run_once(self) -> int:
        """Executa um ciclo único de processamento."""
        session = self._session_factory()
        try:
            return self.process_batch(session)
        except Exception as exc:
            session.rollback()
            logger.error(f"Erro durante execução do ciclo outbox: {exc}")
            raise
        finally:
            session.close()

    def start(self) -> None:
        """Inicia o loop contínuo de publicação (daemon)."""
        self._running = True
        logger.info("OutboxPublisherWorker iniciado.")
        while self._running:
            try:
                processed = self.run_once()
                if processed == 0:
                    time.sleep(self._poll_interval)
            except Exception as exc:
                logger.error(f"Exceção não tratada no loop do worker: {exc}")
                time.sleep(self._poll_interval)

    def stop(self) -> None:
        """Para a execução do daemon."""
        self._running = False
        logger.info("OutboxPublisherWorker finalizando...")
