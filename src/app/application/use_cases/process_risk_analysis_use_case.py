import logging

from app.application.dtos.event_dtos import (
    TransactionStatusChangedEventPayload,
)
from app.domain.entities.inbox_event import InboxStatus
from app.domain.entities.outbox_event import OutboxEvent
from app.domain.entities.risk_evaluation import RiskDecisionEnum, RiskEvaluation
from app.domain.entities.transaction import Transaction
from app.domain.exceptions.domain_exceptions import TransactionNotFoundError
from app.domain.ports.external_risk_client_port import ExternalRiskClientPort
from app.domain.ports.inbox_repository_port import InboxRepositoryPort
from app.domain.ports.outbox_repository_port import OutboxRepositoryPort
from app.domain.ports.transaction_repository_port import (
    TransactionRepositoryPort,
)


logger = logging.getLogger(__name__)


class ProcessRiskAnalysisUseCase:
    """Caso de uso responsável por coordenar a análise de risco externa,

    transições de status e publicação atômica na outbox.
    """

    TOPIC_STATUS_CHANGED = "transactions.status.v1"
    AGGREGATE_TYPE = "TRANSACTION"

    def __init__(
        self,
        transaction_repository: TransactionRepositoryPort,
        outbox_repository: OutboxRepositoryPort,
        inbox_repository: InboxRepositoryPort,
        risk_client: ExternalRiskClientPort,
        session_factory,
    ):
        self._tx_repository = transaction_repository
        self._outbox_repository = outbox_repository
        self._inbox_repository = inbox_repository
        self._risk_client = risk_client
        self._session_factory = session_factory

    def execute(self, transaction_id: str, event_id: str) -> Transaction:
        """Executa a máquina de estados:

        1. Transiciona transação para PROCESSING (atômico no DB)
        2. Executa chamada externa ao serviço de risco (fora de transação de banco)
        3. Atualiza status final (APPROVED / REJECTED), outbox_events e inbox_events em commit único.
        """
        # 1. Transição da transação para PROCESSING
        with self._session_factory() as session:
            tx_repo = self._tx_repository.__class__(session)
            tx = tx_repo.find_by_id(transaction_id)
            if not tx:
                raise TransactionNotFoundError(transaction_id)

            old_status = tx.status.value
            tx.mark_as_processing()
            tx_repo.save(tx)
            session.commit()
            customer_id = tx.customer_id
            tx_value = tx.value.to_decimal()

        # 2. Avaliação de risco externa
        logger.info(
            "Iniciando avaliação de risco externa para transação %s (cliente %s)",
            transaction_id,
            customer_id,
        )
        risk_evaluation: RiskEvaluation = self._risk_client.evaluate(
            customer_id=customer_id, value=tx_value
        )

        # 3. Persistência final atômica
        with self._session_factory() as session:
            tx_repo = self._tx_repository.__class__(session)
            outbox_repo = self._outbox_repository.__class__(session)
            inbox_repo = self._inbox_repository.__class__(session)

            tx = tx_repo.find_by_id(transaction_id)
            if not tx:
                raise TransactionNotFoundError(transaction_id)

            if risk_evaluation.decision == RiskDecisionEnum.APPROVED:
                tx.mark_as_approved()
            else:
                tx.mark_as_rejected(reason=risk_evaluation.reason)

            tx_repo.save(tx)

            # Emissão do evento de mudança de status
            status_event_payload = TransactionStatusChangedEventPayload.create(
                transaction_id=tx.id,
                customer_id=tx.customer_id,
                old_status=old_status,
                new_status=tx.status.value,
                reason=risk_evaluation.reason,
            )

            outbox_event = OutboxEvent.create(
                aggregate_type=self.AGGREGATE_TYPE,
                aggregate_id=tx.id,
                topic=self.TOPIC_STATUS_CHANGED,
                payload=status_event_payload.to_dict(),
                event_id=status_event_payload.event_id,
            )
            outbox_repo.save(outbox_event)

            # Marcação do Inbox como COMPLETED
            inbox_repo.update_status(
                event_id=event_id,
                status=InboxStatus.COMPLETED,
                session=session,
            )

            session.commit()
            logger.info(
                "Transação %s finalizada com status %s (evento %s)",
                tx.id,
                tx.status.value,
                event_id,
            )
            return tx
