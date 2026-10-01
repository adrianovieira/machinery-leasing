from sqlalchemy.orm import Session

from app.domain.entities.transaction import Transaction
from app.domain.ports.transaction_repository_port import (
    TransactionRepositoryPort,
)
from app.infrastructure.database.models.transaction_model import (
    TransactionModel,
)


class MySQLTransactionRepository(TransactionRepositoryPort):
    """Implementação concreta de persistência no MySQL via SQLAlchemy."""

    def __init__(self, session: Session):
        self._session = session

    def save(self, transaction: Transaction) -> None:
        model = (
            self._session.query(TransactionModel).filter_by(id=transaction.id).first()
        )
        if model:
            model.status = (
                transaction.status.value
                if hasattr(transaction.status, "value")
                else str(transaction.status)
            )
            model.failure_reason = transaction.failure_reason
            model.updated_at = transaction.updated_at
        else:
            model = TransactionModel.from_domain(transaction)
            self._session.add(model)
        self._session.flush()

    def find_by_id(self, transaction_id: str) -> Transaction | None:
        model = (
            self._session.query(TransactionModel).filter_by(id=transaction_id).first()
        )
        if not model:
            return None
        return model.to_domain()
