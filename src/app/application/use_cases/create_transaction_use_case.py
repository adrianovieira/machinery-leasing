from app.application.dtos.event_dtos import (
    TransactionCreatedEventPayload,
)
from app.application.dtos.transaction_dtos import (
    CreateTransactionRequestDTO,
    TransactionResponseDTO,
)
from app.domain.entities.outbox_event import OutboxEvent
from app.domain.entities.transaction import Transaction
from app.domain.ports.outbox_repository_port import OutboxRepositoryPort
from app.domain.ports.transaction_repository_port import (
    TransactionRepositoryPort,
)


class CreateTransactionUseCase:
    """Caso de uso para criação e persistência atômica de uma nova transação e evento outbox."""

    TOPIC = "transactions.created.v1"
    AGGREGATE_TYPE = "TRANSACTION"

    def __init__(
        self,
        transaction_repository: TransactionRepositoryPort,
        outbox_repository: OutboxRepositoryPort,
    ):
        self._tx_repository = transaction_repository
        self._outbox_repository = outbox_repository

    def execute(
        self, request_dto: CreateTransactionRequestDTO
    ) -> TransactionResponseDTO:
        transaction = Transaction.create(
            customer_id=request_dto.customer_id,
            value=request_dto.value,
        )

        event_payload = TransactionCreatedEventPayload.create(
            transaction_id=transaction.id,
            customer_id=transaction.customer_id,
            value=transaction.value.to_float(),
        )

        outbox_event = OutboxEvent.create(
            aggregate_type=self.AGGREGATE_TYPE,
            aggregate_id=transaction.id,
            topic=self.TOPIC,
            payload=event_payload.to_dict(),
            event_id=event_payload.event_id,
        )

        self._tx_repository.save(transaction)
        self._outbox_repository.save(outbox_event)

        return TransactionResponseDTO.from_domain(transaction)
