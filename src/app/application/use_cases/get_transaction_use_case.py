from app.application.dtos.transaction_dtos import TransactionResponseDTO
from app.domain.exceptions.domain_exceptions import TransactionNotFoundError
from app.domain.ports.transaction_repository_port import (
    TransactionRepositoryPort,
)


class GetTransactionUseCase:
    """Caso de uso para consulta de transação por identificador único."""

    def __init__(self, repository: TransactionRepositoryPort):
        self._repository = repository

    def execute(self, transaction_id: str) -> TransactionResponseDTO:
        transaction = self._repository.find_by_id(transaction_id)
        if not transaction:
            raise TransactionNotFoundError(transaction_id)
        return TransactionResponseDTO.from_domain(transaction)
