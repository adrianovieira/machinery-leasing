from abc import ABC, abstractmethod

from app.domain.entities.transaction import Transaction


class TransactionRepositoryPort(ABC):
    """Porta abstrata de persistência e recuperação de transações."""

    @abstractmethod
    def save(self, transaction: Transaction) -> None:
        """Persiste ou atualiza uma entidade Transaction."""

    @abstractmethod
    def find_by_id(self, transaction_id: str) -> Transaction | None:
        """Recupera uma transação pelo seu ID único."""
