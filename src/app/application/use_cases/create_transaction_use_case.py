from app.application.dtos.transaction_dtos import (
    CreateTransactionRequestDTO,
    TransactionResponseDTO,
)
from app.domain.entities.transaction import Transaction
from app.domain.ports.transaction_repository_port import (
    TransactionRepositoryPort,
)


class CreateTransactionUseCase:
    """Caso de uso para criação e persistência síncrona de uma nova transação."""

    def __init__(self, repository: TransactionRepositoryPort):
        self._repository = repository

    def execute(
        self, request_dto: CreateTransactionRequestDTO
    ) -> TransactionResponseDTO:
        transaction = Transaction.create(
            customer_id=request_dto.customer_id,
            value=request_dto.value,
        )
        self._repository.save(transaction)
        return TransactionResponseDTO.from_domain(transaction)
